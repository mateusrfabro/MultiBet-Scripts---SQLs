"""
Extrações para Head de Tráfego — 25/03/2026

Base 1: Jogadores com turnover > R$ 5.000 nos últimos 90 dias
Base 2: Registrou e NÃO fez FTD nos últimos 15 dias

Ambas incluem: email + telefone (obrigatório p/ demandas de tráfego)
Validação cruzada com BigQuery (Smartico CRM)
Fonte: ps_bi (Athena) + j_user (BigQuery)
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
# DATAS
# ============================================================
hoje = date.today()
d1 = hoje - timedelta(days=1)                 # D-1 = 2026-03-24
dt_inicio_90d = d1 - timedelta(days=89)       # 90 dias desde D-1
dt_inicio_15d = d1 - timedelta(days=14)       # 15 dias desde D-1

log.info(f"Hoje: {hoje} | D-1: {d1}")
log.info(f"Base 1 — Turnover 90d: {dt_inicio_90d} a {d1}")
log.info(f"Base 2 — Reg sem FTD 15d: {dt_inicio_15d} a {d1}")

# ============================================================
# BASE 1 — Turnover > R$ 5.000 (últimos 90 dias)
# ============================================================
log.info("=" * 60)
log.info("BASE 1: Turnover > R$ 5.000 nos últimos 90 dias")
log.info("=" * 60)

sql_base1 = f"""
WITH turnover_90d AS (
    SELECT
        f.player_id,
        COALESCE(SUM(f.casino_realbet_local), 0)
          + COALESCE(SUM(f.casino_bonusbet_local), 0)
          + COALESCE(SUM(f.sb_realbet_local), 0)
          + COALESCE(SUM(f.sb_bonusbet_local), 0)          AS turnover_total,
        COALESCE(SUM(f.casino_realbet_local), 0)
          + COALESCE(SUM(f.sb_realbet_local), 0)            AS turnover_real,
        COALESCE(SUM(f.casino_bonusbet_local), 0)
          + COALESCE(SUM(f.sb_bonusbet_local), 0)           AS turnover_bonus,
        COALESCE(SUM(f.casino_realbet_local), 0)
          + COALESCE(SUM(f.casino_bonusbet_local), 0)       AS turnover_casino,
        COALESCE(SUM(f.sb_realbet_local), 0)
          + COALESCE(SUM(f.sb_bonusbet_local), 0)           AS turnover_sportsbook,
        COALESCE(SUM(f.ggr_local), 0)                       AS ggr,
        COALESCE(SUM(f.ngr_local), 0)                       AS ngr,
        COUNT(DISTINCT f.activity_date)                      AS dias_ativos,
        COALESCE(SUM(f.login_count), 0)                      AS total_logins,
        MIN(f.activity_date)                                 AS primeira_atividade,
        MAX(f.activity_date)                                 AS ultima_atividade
    FROM ps_bi.fct_player_activity_daily f
    WHERE f.activity_date BETWEEN DATE '{dt_inicio_90d}' AND DATE '{d1}'
    GROUP BY f.player_id
)
SELECT
    t.player_id,
    u.external_id,
    u.email,
    u.mobile_number,
    ROUND(t.turnover_total, 2)       AS turnover_total_brl,
    ROUND(t.turnover_real, 2)        AS turnover_real_brl,
    ROUND(t.turnover_bonus, 2)       AS turnover_bonus_brl,
    ROUND(t.turnover_casino, 2)      AS turnover_casino_brl,
    ROUND(t.turnover_sportsbook, 2)  AS turnover_sportsbook_brl,
    ROUND(t.ggr, 2)                  AS ggr_brl,
    ROUND(t.ngr, 2)                  AS ngr_brl,
    t.dias_ativos,
    t.total_logins,
    t.primeira_atividade,
    t.ultima_atividade
FROM turnover_90d t
JOIN ps_bi.dim_user u
  ON u.ecr_id = t.player_id
WHERE u.is_test = false
  AND t.turnover_total >= 5000
ORDER BY t.turnover_total DESC
"""

log.info("Executando query Base 1 no Athena...")
df1 = query_athena(sql_base1, database="ps_bi")
log.info(f"Base 1 — jogadores: {len(df1)}")

# ============================================================
# BASE 2 — Registrou e NÃO fez FTD (últimos 15 dias)
# ============================================================
log.info("=" * 60)
log.info("BASE 2: Registrou e NÃO fez FTD nos últimos 15 dias")
log.info("=" * 60)

sql_base2 = f"""
SELECT
    u.ecr_id          AS player_id,
    u.external_id,
    u.email,
    u.mobile_number,
    u.registration_date,
    u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS signup_datetime_brt,
    u.registration_status,
    u.has_ftd,
    u.affiliate_id,
    u.tracker_id,
    u.signup_channel,
    u.signup_device,
    u.utm_source,
    u.utm_medium,
    u.utm_campaign
FROM ps_bi.dim_user u
WHERE u.is_test = false
  AND u.registration_date BETWEEN DATE '{dt_inicio_15d}' AND DATE '{d1}'
  AND u.ftd_date IS NULL
ORDER BY u.registration_date DESC
"""

log.info("Executando query Base 2 no Athena...")
df2 = query_athena(sql_base2, database="ps_bi")
log.info(f"Base 2 — jogadores: {len(df2)}")

# ============================================================
# VALIDAÇÃO CRUZADA COM BIGQUERY
# ============================================================
log.info("=" * 60)
log.info("VALIDAÇÃO CRUZADA — BigQuery (Smartico CRM)")
log.info("=" * 60)

# --- Validação 1: Contagem de REG últimos 15 dias no BigQuery ---
sql_bq_reg = f"""
SELECT
  COUNT(*) AS total_reg,
  COUNT(CASE WHEN acc_last_deposit_date IS NULL THEN 1 END) AS sem_deposito
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE DATE(core_registration_date, 'America/Sao_Paulo')
      BETWEEN '{dt_inicio_15d}' AND '{d1}'
"""

log.info("Validando REG 15d no BigQuery...")
df_bq_reg = query_bigquery(sql_bq_reg)
bq_reg_total = int(df_bq_reg['total_reg'].iloc[0])
bq_sem_dep = int(df_bq_reg['sem_deposito'].iloc[0])

# --- Validação 2: Sample de external_ids da Base 1 no BigQuery ---
# Pegar top 100 external_ids e verificar se existem no Smartico
if len(df1) > 0:
    sample_ids = df1['external_id'].dropna().head(100).tolist()
    # Converter para string para BigQuery
    ids_str = ", ".join([f"'{str(x)}'" for x in sample_ids])

    sql_bq_check1 = f"""
    SELECT
      COUNT(*) AS encontrados
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE user_ext_id IN ({ids_str})
    """

    log.info("Validando amostra Base 1 no BigQuery...")
    df_bq_check1 = query_bigquery(sql_bq_check1)
    bq_encontrados_b1 = int(df_bq_check1['encontrados'].iloc[0])
else:
    bq_encontrados_b1 = 0

# --- Validação 3: Sample de external_ids da Base 2 no BigQuery ---
if len(df2) > 0:
    sample_ids2 = df2['external_id'].dropna().head(100).tolist()
    ids_str2 = ", ".join([f"'{str(x)}'" for x in sample_ids2])

    sql_bq_check2 = f"""
    SELECT
      COUNT(*) AS encontrados
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE user_ext_id IN ({ids_str2})
    """

    log.info("Validando amostra Base 2 no BigQuery...")
    df_bq_check2 = query_bigquery(sql_bq_check2)
    bq_encontrados_b2 = int(df_bq_check2['encontrados'].iloc[0])
else:
    bq_encontrados_b2 = 0

# ============================================================
# RELATÓRIO DE VALIDAÇÃO
# ============================================================
print("\n" + "=" * 70)
print("RELATÓRIO DE VALIDAÇÃO CRUZADA")
print("=" * 70)

# Base 1
print(f"\n--- BASE 1: Turnover > R$ 5k (90d) ---")
print(f"Jogadores Athena:     {len(df1):,}")
print(f"Com email:            {df1['email'].notna().sum():,} ({df1['email'].notna().mean()*100:.1f}%)")
print(f"Com telefone:         {df1['mobile_number'].notna().sum():,} ({df1['mobile_number'].notna().mean()*100:.1f}%)")
print(f"Turnover total:       R$ {df1['turnover_total_brl'].sum():,.2f}")
print(f"GGR total:            R$ {df1['ggr_brl'].sum():,.2f}")
print(f"BigQuery match (top 100): {bq_encontrados_b1}/100 ({bq_encontrados_b1}%)")

# Base 2
print(f"\n--- BASE 2: Registrou sem FTD (15d) ---")
print(f"Jogadores Athena:     {len(df2):,}")
print(f"Com email:            {df2['email'].notna().sum():,} ({df2['email'].notna().mean()*100:.1f}%)")
print(f"Com telefone:         {df2['mobile_number'].notna().sum():,} ({df2['mobile_number'].notna().mean()*100:.1f}%)")
print(f"BigQuery REG total 15d: {bq_reg_total:,}")
print(f"BigQuery sem depósito:  {bq_sem_dep:,}")
print(f"BigQuery match (top 100): {bq_encontrados_b2}/{min(len(df2), 100)} ({bq_encontrados_b2/min(len(df2), 100)*100:.0f}%)" if len(df2) > 0 else "Base vazia")

# Divergência REG
# Athena total REG 15d (com e sem FTD) para comparar com BigQuery
sql_athena_reg_total = f"""
SELECT COUNT(*) AS total
FROM ps_bi.dim_user
WHERE is_test = false
  AND registration_date BETWEEN DATE '{dt_inicio_15d}' AND DATE '{d1}'
"""
df_athena_reg = query_athena(sql_athena_reg_total, database="ps_bi")
athena_reg_total = int(df_athena_reg['total'].iloc[0])

print(f"\n--- CROSS-CHECK REG 15d ---")
print(f"Athena total REG:       {athena_reg_total:,}")
print(f"BigQuery total REG:     {bq_reg_total:,}")
divergencia = abs(athena_reg_total - bq_reg_total) / max(athena_reg_total, 1) * 100
print(f"Divergência:            {divergencia:.1f}%", end="")
if divergencia <= 5:
    print(" OK (aceitavel)")
else:
    print(" ATENCAO — verificar filtros de test user")

# ============================================================
# EXPORTAR CSV
# ============================================================
print("\n" + "=" * 70)
print("EXPORTANDO BASES")
print("=" * 70)

out1 = "reports/base1_turnover_5k_90d_FINAL.csv"
out2 = "reports/base2_reg_sem_ftd_15d_FINAL.csv"

df1.to_csv(out1, index=False, encoding="utf-8-sig")
log.info(f"Base 1 salva: {out1}")

df2.to_csv(out2, index=False, encoding="utf-8-sig")
log.info(f"Base 2 salva: {out2}")

# ============================================================
# LEGENDAS
# ============================================================
legenda1 = f"""LEGENDA — base1_turnover_5k_90d_FINAL.csv
{'=' * 60}
Gerado em: {hoje}
Período: {dt_inicio_90d} a {d1} (90 dias, até D-1)
Fonte: ps_bi.fct_player_activity_daily + ps_bi.dim_user (Athena)
Filtro: turnover_total >= R$ 5.000 | is_test = false
Demanda: Head de Tráfego

DICIONÁRIO DE COLUNAS
---------------------
player_id              — ID interno (ecr_id, 18 dígitos)
external_id            — ID externo (= user_ext_id no Smartico)
email                  — E-mail cadastrado pelo jogador
mobile_number          — Telefone celular cadastrado
turnover_total_brl     — Total apostado (casino + sportsbook, real + bônus), R$
turnover_real_brl      — Apostas com dinheiro real, R$
turnover_bonus_brl     — Apostas com saldo de bônus, R$
turnover_casino_brl    — Apostas em casino (slots + live), R$
turnover_sportsbook_brl — Apostas em sportsbook, R$
ggr_brl                — Gross Gaming Revenue (apostas - ganhos jogador), R$
ngr_brl                — Net Gaming Revenue (GGR - custos bônus), R$
dias_ativos            — Dias distintos com atividade no período
total_logins           — Logins no período
primeira_atividade     — Data da 1ª atividade no período
ultima_atividade       — Data da última atividade no período

GLOSSÁRIO: GGR = receita bruta | NGR = receita líquida | Turnover = volume apostado
"""

legenda2 = f"""LEGENDA — base2_reg_sem_ftd_15d_FINAL.csv
{'=' * 60}
Gerado em: {hoje}
Período de registro: {dt_inicio_15d} a {d1} (15 dias, até D-1)
Fonte: ps_bi.dim_user (Athena)
Filtro: registration_date no período + ftd_date IS NULL | is_test = false
Demanda: Head de Tráfego

DICIONÁRIO DE COLUNAS
---------------------
player_id              — ID interno (ecr_id, 18 dígitos)
external_id            — ID externo (= user_ext_id no Smartico)
email                  — E-mail cadastrado
mobile_number          — Telefone celular cadastrado
registration_date      — Data de registro (UTC truncado)
signup_datetime_brt    — Data/hora exata de registro (BRT, America/Sao_Paulo)
registration_status    — Status do cadastro
has_ftd                — Se fez primeiro depósito (0 = NÃO fez FTD, filtro: ftd_date IS NULL)
affiliate_id           — ID do afiliado (tráfego)
tracker_id             — ID do tracker
signup_channel         — Canal de cadastro
signup_device          — Dispositivo usado no cadastro
utm_source             — UTM source (origem do tráfego)
utm_medium             — UTM medium (tipo de mídia)
utm_campaign           — UTM campaign (campanha)

NOTA: Jogadores que se registraram no período mas AINDA NÃO fizeram
o primeiro depósito (FTD). Útil para ações de conversão/remarketing.
"""

with open("reports/base1_turnover_5k_90d_FINAL_legenda.txt", "w", encoding="utf-8") as f:
    f.write(legenda1)
with open("reports/base2_reg_sem_ftd_15d_FINAL_legenda.txt", "w", encoding="utf-8") as f:
    f.write(legenda2)

print(f"\nArquivos gerados:")
print(f"  1. {out1} ({len(df1):,} linhas)")
print(f"  2. {out2} ({len(df2):,} linhas)")
print(f"  3. reports/base1_turnover_5k_90d_FINAL_legenda.txt")
print(f"  4. reports/base2_reg_sem_ftd_15d_FINAL_legenda.txt")
