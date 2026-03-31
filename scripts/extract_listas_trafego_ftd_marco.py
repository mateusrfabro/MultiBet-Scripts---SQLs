"""
Listas de FTD março/2026 para Tráfego — Google (464673) e Meta (297657)

Demanda: Head de Tráfego
Entrega: 2 CSVs (1 por canal) com todos que fizeram FTD em março/2026
Campos: user, external_user, nome, email, telefone

Abordagem:
- Base de REGs: bireports_ec2.tbl_ecr (fonte confiável por affiliate,
  evita registros inflados do dim_user para affiliate 297657)
- FTD check + dados pessoais: ps_bi.dim_user (ftd_datetime, nome, email, telefone)
- Período: 01/mar a D-1 (26/mar/2026) em BRT
- Validação cruzada com BigQuery (Smartico CRM)
"""

import sys
import logging
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, ".")
from db.athena import query_athena
from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURAÇÃO
# ============================================================
hoje = date.today()
d1 = hoje - timedelta(days=1)  # D-1
dt_inicio = date(2026, 3, 1)
dt_fim = d1  # 2026-03-26

# Affiliates: Google e Meta
AFFILIATES = {
    "464673": "Google",
    "297657": "Meta",
}

log.info(f"Hoje: {hoje} | D-1: {d1}")
log.info(f"Período FTD: {dt_inicio} a {dt_fim}")
log.info(f"Affiliates: {AFFILIATES}")

# ============================================================
# EXTRAÇÃO POR AFFILIATE
# ============================================================
resultados = {}

for aff_id, canal in AFFILIATES.items():
    log.info("=" * 60)
    log.info(f"EXTRAINDO: {canal} (affiliate_id = {aff_id})")
    log.info("=" * 60)

    # Query: bireports como base de REGs + dim_user para FTD + dados pessoais
    # Motivo: dim_user tem registros inflados para affiliate 297657 (Meta)
    # Ref: memory/feedback_ftd_query_correta.md
    sql = f"""
    WITH regs AS (
        -- Fonte confiável de REGs por affiliate (bireports_ec2)
        SELECT DISTINCT b.c_ecr_id
        FROM bireports_ec2.tbl_ecr b
        WHERE CAST(b.c_affiliate_id AS VARCHAR) = '{aff_id}'
          AND b.c_test_user = false
    )
    SELECT
        r.c_ecr_id                                             AS user,
        u.external_id                                          AS external_user,
        TRIM(
            COALESCE(u.first_name, '') || ' ' || COALESCE(u.last_name, '')
        )                                                      AS nome,
        u.email,
        u.mobile_number                                        AS telefone,
        u.ftd_date,
        u.ftd_datetime AT TIME ZONE 'UTC'
                       AT TIME ZONE 'America/Sao_Paulo'        AS ftd_datetime_brt
    FROM regs r
    JOIN ps_bi.dim_user u
      ON u.ecr_id = r.c_ecr_id
    WHERE (u.is_test = false OR u.is_test IS NULL)
      AND CAST(
            u.ftd_datetime AT TIME ZONE 'UTC'
                           AT TIME ZONE 'America/Sao_Paulo'
            AS DATE
          ) BETWEEN DATE '{dt_inicio}' AND DATE '{dt_fim}'
    ORDER BY u.ftd_datetime DESC
    """

    log.info(f"Executando query Athena — {canal}...")
    df = query_athena(sql, database="bireports_ec2")
    log.info(f"{canal}: {len(df)} jogadores com FTD em março")

    # Stats rápidas
    log.info(f"  Com email:    {df['email'].notna().sum()} ({df['email'].notna().mean()*100:.1f}%)")
    log.info(f"  Com telefone: {df['telefone'].notna().sum()} ({df['telefone'].notna().mean()*100:.1f}%)")
    log.info(f"  Com nome:     {(df['nome'].str.strip() != '').sum()} ({(df['nome'].str.strip() != '').mean()*100:.1f}%)")

    resultados[canal] = df

# ============================================================
# VALIDAÇÃO CRUZADA COM BIGQUERY
# ============================================================
log.info("=" * 60)
log.info("VALIDAÇÃO CRUZADA — BigQuery (Smartico CRM)")
log.info("=" * 60)

for aff_id, canal in AFFILIATES.items():
    df_athena = resultados[canal]

    # Contar FTDs no BigQuery para o mesmo affiliate/período
    sql_bq = f"""
    SELECT
      COUNT(*) AS ftd_count
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE CAST(core_affiliate_id AS STRING) = '{aff_id}'
      AND DATE(core_registration_date, 'America/Sao_Paulo')
          BETWEEN '{dt_inicio}' AND '{dt_fim}'
      AND acc_last_deposit_date IS NOT NULL
    """

    log.info(f"Validando {canal} no BigQuery...")
    try:
        df_bq = query_bigquery(sql_bq)
        bq_count = int(df_bq['ftd_count'].iloc[0])
    except Exception as e:
        log.warning(f"BigQuery falhou para {canal}: {e}")
        bq_count = -1

    athena_count = len(df_athena)

    if bq_count >= 0:
        diff_pct = abs(athena_count - bq_count) / max(athena_count, 1) * 100
        status = "OK" if diff_pct <= 10 else "ATENÇÃO"
        log.info(f"  {canal}: Athena={athena_count} | BigQuery={bq_count} | Diff={diff_pct:.1f}% — {status}")
    else:
        log.info(f"  {canal}: Athena={athena_count} | BigQuery=ERRO")

    # Sample check: top 50 external_ids
    if len(df_athena) > 0 and bq_count >= 0:
        sample_ids = df_athena['external_user'].dropna().head(50).tolist()
        ids_str = ", ".join([f"'{str(x)}'" for x in sample_ids])

        sql_bq_check = f"""
        SELECT COUNT(*) AS encontrados
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE user_ext_id IN ({ids_str})
        """
        try:
            df_check = query_bigquery(sql_bq_check)
            match = int(df_check['encontrados'].iloc[0])
            log.info(f"  {canal}: Sample match BigQuery = {match}/{min(len(df_athena), 50)}")
        except Exception as e:
            log.warning(f"  Sample check falhou: {e}")

# ============================================================
# EXPORTAR CSVs
# ============================================================
log.info("=" * 60)
log.info("EXPORTANDO LISTAS")
log.info("=" * 60)

# Colunas finais para entrega (sem ftd_date/ftd_datetime_brt interno)
colunas_entrega = ["user", "external_user", "nome", "email", "telefone"]

arquivos = []
for canal, df in resultados.items():
    nome_arquivo = f"reports/lista_ftd_marco_{canal.lower()}_FINAL.csv"
    df_entrega = df[colunas_entrega].copy()
    df_entrega.to_csv(nome_arquivo, index=False, encoding="utf-8-sig")
    arquivos.append(nome_arquivo)
    log.info(f"  {canal}: {nome_arquivo} ({len(df_entrega)} linhas)")

# ============================================================
# LEGENDA
# ============================================================
legenda = f"""LEGENDA — Listas FTD Março/2026 para Tráfego
{'=' * 60}
Gerado em: {hoje}
Período FTD: {dt_inicio} a {dt_fim} (março/2026, até D-1)
Fonte: bireports_ec2.tbl_ecr (base REGs) + ps_bi.dim_user (FTD + dados pessoais)
Validação cruzada: BigQuery Smartico CRM (j_user)
Filtros: is_test = false | FTD em março BRT | affiliate_id por canal
Demanda: Head de Tráfego

ARQUIVOS
--------
lista_ftd_marco_google_FINAL.csv — Affiliate 464673 (Google)
lista_ftd_marco_meta_FINAL.csv   — Affiliate 297657 (Meta)

DICIONÁRIO DE COLUNAS
---------------------
user            — ID interno do jogador (ecr_id, 18 dígitos)
external_user   — ID externo do jogador (= user_ext_id no Smartico CRM)
nome            — Nome completo (first_name + last_name)
email           — E-mail cadastrado pelo jogador
telefone        — Telefone celular cadastrado (mobile_number)

DEFINIÇÕES
----------
FTD = First Time Deposit = primeiro depósito confirmado do jogador
Affiliate = canal de aquisição de tráfego
  464673 = Google (campanhas Google Ads)
  297657 = Meta (campanhas Facebook/Instagram Ads)

COMO USAR
---------
Estas listas contêm jogadores que se registraram via cada canal de tráfego
e fizeram seu primeiro depósito durante março/2026 (até {dt_fim}).
Usar para audiências de remarketing/lookalike no Google Ads e Meta Ads.
"""

legenda_path = "reports/lista_ftd_marco_trafego_FINAL_legenda.txt"
with open(legenda_path, "w", encoding="utf-8") as f:
    f.write(legenda)
arquivos.append(legenda_path)

# ============================================================
# RESUMO FINAL
# ============================================================
print("\n" + "=" * 70)
print("RESUMO DA ENTREGA")
print("=" * 70)
for canal, df in resultados.items():
    aff_id = [k for k, v in AFFILIATES.items() if v == canal][0]
    print(f"\n{canal} (affiliate {aff_id}):")
    print(f"  Jogadores FTD março: {len(df):,}")
    print(f"  Com email:           {df['email'].notna().sum():,} ({df['email'].notna().mean()*100:.1f}%)")
    print(f"  Com telefone:        {df['telefone'].notna().sum():,} ({df['telefone'].notna().mean()*100:.1f}%)")
    print(f"  Com nome:            {(df['nome'].str.strip() != '').sum():,} ({(df['nome'].str.strip() != '').mean()*100:.1f}%)")

print(f"\nArquivos gerados:")
for a in arquivos:
    print(f"  {a}")
print("\nEntrega pronta para tráfego.")
