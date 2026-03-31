"""
Auditoria de sincronizacao de status: PGS (Athena) vs SMT (BigQuery) — v2
==========================================================================
Objetivo: Snapshot COMPLETO de todos os jogadores em ambas as bases,
com comparativo de status PGS vs SMT.

Sem filtro de mudanca de status — pega TODOS os registros.

Fontes:
  - PGS (Pragmatic) = Athena bireports_ec2.tbl_ecr
  - SMT (Smartico)  = BigQuery j_user

Entrega: Excel com 4 abas
  - base_pgs: todos os jogadores do PGS com status atual
  - base_smt: todos os jogadores do SMT com status atual
  - comparacao: merge completo + analise de divergencia
  - legenda: dicionario de colunas
"""

import sys
import logging
import pandas as pd
from datetime import datetime

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.athena import query_athena
from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PARAMETROS
# ---------------------------------------------------------------------------
ts = datetime.now().strftime("%Y%m%d_%H%M")
output_dir = "c:/Users/NITRO/OneDrive - PGX/MultiBet/reports/"
output_file = f"{output_dir}auditoria_sync_status_completa_FINAL_{ts}.xlsx"

# ---------------------------------------------------------------------------
# 1. EXTRACAO PGS (Athena) — TODOS os jogadores
# ---------------------------------------------------------------------------
log.info("[PGS] Extraindo TODOS os jogadores de bireports_ec2.tbl_ecr...")

sql_pgs = """
SELECT
    c_external_id                 AS ext_id,
    c_ecr_id                      AS ecr_id,
    c_category                    AS pgs_status,
    c_old_category                AS pgs_status_anterior,
    c_ecr_status                  AS pgs_tipo_conta,
    c_category_updated_time
        AT TIME ZONE 'UTC'
        AT TIME ZONE 'America/Sao_Paulo'
                                  AS pgs_status_atualizado_brt,
    c_category_change_source      AS pgs_fonte_mudanca,
    c_sign_up_time
        AT TIME ZONE 'UTC'
        AT TIME ZONE 'America/Sao_Paulo'
                                  AS pgs_data_registro_brt
FROM bireports_ec2.tbl_ecr
WHERE c_test_user = false
  AND c_external_id IS NOT NULL
"""

df_pgs = query_athena(sql_pgs, database="bireports_ec2")
log.info(f"[PGS] {len(df_pgs):,} jogadores extraidos")

# ---------------------------------------------------------------------------
# 2. EXTRACAO SMT (BigQuery) — TODOS os jogadores
# ---------------------------------------------------------------------------
log.info("[SMT] Extraindo TODOS os jogadores de j_user...")

sql_smt = """
SELECT
    user_ext_id                     AS ext_id,
    core_external_account_status    AS smt_ext_status,
    core_account_status             AS smt_internal_status,
    update_date                     AS smt_ultima_atualizacao
FROM `smartico-bq6.dwh_ext_24105.j_user`
"""

df_smt = query_bigquery(sql_smt)
log.info(f"[SMT] {len(df_smt):,} jogadores extraidos")

# ---------------------------------------------------------------------------
# 3. MERGE e COMPARACAO
# ---------------------------------------------------------------------------
log.info("Montando comparacao PGS x SMT...")

# Normalizar ext_id como string
df_pgs["ext_id_str"] = df_pgs["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
df_smt["ext_id_str"] = df_smt["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)

# Outer join para capturar registros em ambas as direcoes
df_comp = df_pgs.merge(df_smt, on="ext_id_str", how="outer", suffixes=("_pgs", "_smt"), indicator=True)

# Normalizar para comparacao
df_comp["pgs_status_upper"] = df_comp["pgs_status"].fillna("SEM_REGISTRO_PGS").str.strip().str.upper()
df_comp["smt_ext_status_upper"] = df_comp["smt_ext_status"].fillna("SEM_REGISTRO_SMT").str.strip().str.upper()
df_comp["smt_int_status_upper"] = df_comp["smt_internal_status"].fillna("SEM_REGISTRO_SMT").str.strip().str.upper()

# Comparacao direta: PGS status vs SMT ext_status
df_comp["match_direto"] = df_comp["pgs_status_upper"] == df_comp["smt_ext_status_upper"]

# Resultado
df_comp["resultado"] = "DIVERGENTE"
df_comp.loc[df_comp["match_direto"], "resultado"] = "OK_DIRETO"
df_comp.loc[df_comp["_merge"] == "left_only", "resultado"] = "SO_PGS"
df_comp.loc[df_comp["_merge"] == "right_only", "resultado"] = "SO_SMT"

# Para contas ativas (REAL_USER/PLAY_USER), SMT pode ter ACTIVE como ext_status
# Isso e comportamento normal (mapeamento simplificado)
mask_active_match = (
    df_comp["pgs_status_upper"].isin(["REAL_USER", "PLAY_USER"]) &
    (df_comp["smt_ext_status_upper"] == "ACTIVE")
)
df_comp.loc[mask_active_match, "resultado"] = "OK_MAPEADO"

# CLOSED no PGS com CLOSED na SMT = OK
mask_closed_match = (
    (df_comp["pgs_status_upper"] == "CLOSED") &
    (df_comp["smt_ext_status_upper"] == "CLOSED")
)
df_comp.loc[mask_closed_match, "resultado"] = "OK_DIRETO"

# SUSPENDED no PGS com INACTIVE na SMT = OK mapeado
mask_suspended_match = (
    (df_comp["pgs_status_upper"] == "SUSPENDED") &
    (df_comp["smt_ext_status_upper"] == "INACTIVE")
)
df_comp.loc[mask_suspended_match, "resultado"] = "OK_MAPEADO"

# RG_CLOSED no PGS com TRUE na SMT = OK (self_excluded flag)
mask_rg_closed_match = (
    (df_comp["pgs_status_upper"] == "RG_CLOSED") &
    (df_comp["smt_ext_status_upper"] == "TRUE")
)
df_comp.loc[mask_rg_closed_match, "resultado"] = "OK_MAPEADO"

# RG_COOL_OFF no PGS com ACTIVE na SMT = OK mapeado
mask_rg_cooloff_match = (
    (df_comp["pgs_status_upper"] == "RG_COOL_OFF") &
    (df_comp["smt_ext_status_upper"] == "ACTIVE")
)
df_comp.loc[mask_rg_cooloff_match, "resultado"] = "OK_MAPEADO"

# FRAUD no PGS com INACTIVE/BLOCKED na SMT = OK
mask_fraud_match = (
    (df_comp["pgs_status_upper"] == "FRAUD") &
    (df_comp["smt_ext_status_upper"].isin(["INACTIVE", "BLOCKED"]))
)
df_comp.loc[mask_fraud_match, "resultado"] = "OK_MAPEADO"

# ---------------------------------------------------------------------------
# 4. RESUMO NO CONSOLE
# ---------------------------------------------------------------------------
total = len(df_comp)
resultado_counts = df_comp["resultado"].value_counts()

print("\n" + "=" * 70)
print("AUDITORIA COMPLETA DE STATUS PGS x SMT")
print(f"Data da extracao: {datetime.now().strftime('%Y-%m-%d %H:%M')} BRT")
print("=" * 70)
print(f"Total jogadores PGS:  {len(df_pgs):,}")
print(f"Total jogadores SMT:  {len(df_smt):,}")
print(f"Total apos merge:     {total:,}")
print()

# Match stats
both = (df_comp["_merge"] == "both").sum()
only_pgs = (df_comp["_merge"] == "left_only").sum()
only_smt = (df_comp["_merge"] == "right_only").sum()
print(f"Em ambas as bases:    {both:,}")
print(f"So no PGS:            {only_pgs:,}")
print(f"So na SMT:            {only_smt:,}")
print()

print(f"RESULTADO DA COMPARACAO:")
for resultado, count in resultado_counts.sort_values(ascending=False).items():
    pct = count / total * 100
    print(f"  {resultado:20s} {count:>8,} ({pct:.1f}%)")

# Resumo simplificado
ok_total = resultado_counts.get("OK_DIRETO", 0) + resultado_counts.get("OK_MAPEADO", 0)
div_total = resultado_counts.get("DIVERGENTE", 0)
print(f"\n  >>> SYNC OK: {ok_total:,} ({ok_total/total*100:.1f}%) | DIVERGENTE: {div_total:,} ({div_total/total*100:.1f}%)")

# Crosstab de divergentes
divergentes = df_comp[df_comp["resultado"] == "DIVERGENTE"]
if len(divergentes) > 0:
    print(f"\n--- Divergencias por combinacao de status (top 20) ---")
    cross = (
        divergentes
        .groupby(["pgs_status_upper", "smt_ext_status_upper", "smt_int_status_upper"])
        .size()
        .reset_index(name="qtd")
        .sort_values("qtd", ascending=False)
        .head(20)
    )
    print(cross.to_string(index=False))

# Distribuicao de status no PGS
print(f"\n--- Distribuicao de status PGS ---")
pgs_dist = df_pgs["pgs_status"].str.upper().value_counts()
for status, count in pgs_dist.items():
    print(f"  {status:20s} {count:>8,}")

# ---------------------------------------------------------------------------
# 5. EXPORTA EXCEL
# ---------------------------------------------------------------------------
log.info("Exportando arquivos...")

# Remover timezone de colunas datetime para export
for df in [df_pgs, df_smt, df_comp]:
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

# -----------------------------------------------------------------------
# CSV 1: base_pgs (> 1M linhas, nao cabe em Excel)
# -----------------------------------------------------------------------
cols_pgs = [
    "ext_id", "ecr_id", "pgs_status", "pgs_status_anterior",
    "pgs_tipo_conta", "pgs_status_atualizado_brt", "pgs_fonte_mudanca",
    "pgs_data_registro_brt"
]
df_export_pgs = df_pgs[cols_pgs].copy()
df_export_pgs["ext_id"] = df_export_pgs["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
csv_pgs = f"{output_dir}base_pgs_{ts}.csv"
df_export_pgs.to_csv(csv_pgs, index=False, encoding="utf-8-sig")
log.info(f"CSV PGS exportado: {csv_pgs} ({len(df_export_pgs):,} linhas)")

# -----------------------------------------------------------------------
# CSV 2: base_smt (somente registros com match no PGS)
# -----------------------------------------------------------------------
df_smt_matched = df_smt[df_smt["ext_id_str"].isin(df_pgs["ext_id_str"])].copy()
cols_smt = ["ext_id", "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao"]
df_export_smt = df_smt_matched[cols_smt].copy()
df_export_smt["ext_id"] = df_export_smt["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
csv_smt = f"{output_dir}base_smt_{ts}.csv"
df_export_smt.to_csv(csv_smt, index=False, encoding="utf-8-sig")
log.info(f"CSV SMT exportado: {csv_smt} ({len(df_export_smt):,} linhas)")

# -----------------------------------------------------------------------
# CSV 3: comparacao completa (matched only — sem SO_SMT)
# -----------------------------------------------------------------------
df_comp_matched = df_comp[df_comp["_merge"] == "both"].copy()
cols_comp = [
    "ext_id_str", "pgs_status", "pgs_status_anterior", "pgs_tipo_conta",
    "pgs_status_atualizado_brt", "pgs_fonte_mudanca",
    "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
    "match_direto", "resultado"
]
df_export_comp = df_comp_matched[cols_comp].copy()
df_export_comp.rename(columns={"ext_id_str": "ext_id"}, inplace=True)
csv_comp = f"{output_dir}comparacao_status_{ts}.csv"
df_export_comp.to_csv(csv_comp, index=False, encoding="utf-8-sig")
log.info(f"CSV comparacao exportado: {csv_comp} ({len(df_export_comp):,} linhas)")

# -----------------------------------------------------------------------
# EXCEL: analise (divergencias + resumo + legenda) — cabe em Excel
# -----------------------------------------------------------------------
output_excel = f"{output_dir}auditoria_sync_status_completa_FINAL_{ts}.xlsx"

# Aba 1: resumo executivo
resumo_data = []
# Volumetria
resumo_data.append({"Metrica": "--- VOLUMETRIA ---", "Valor": "", "Detalhe": ""})
resumo_data.append({"Metrica": "Total jogadores PGS", "Valor": f"{len(df_pgs):,}", "Detalhe": "bireports_ec2.tbl_ecr (excl. test users)"})
resumo_data.append({"Metrica": "Total jogadores SMT", "Valor": f"{len(df_smt):,}", "Detalhe": "j_user (todos os registros)"})
resumo_data.append({"Metrica": "Match (ambas bases)", "Valor": f"{both:,}", "Detalhe": "ext_id encontrado em PGS e SMT"})
resumo_data.append({"Metrica": "So PGS", "Valor": f"{only_pgs:,}", "Detalhe": "ext_id no PGS sem match na SMT"})
resumo_data.append({"Metrica": "So SMT", "Valor": f"{only_smt:,}", "Detalhe": "ext_id na SMT sem match no PGS (outras marcas?)"})
resumo_data.append({"Metrica": "", "Valor": "", "Detalhe": ""})
# Resultado sync (sobre matched)
matched_total = both
ok_d = resultado_counts.get("OK_DIRETO", 0)
ok_m = resultado_counts.get("OK_MAPEADO", 0)
div_t = resultado_counts.get("DIVERGENTE", 0)
resumo_data.append({"Metrica": "--- RESULTADO SYNC (matched) ---", "Valor": "", "Detalhe": ""})
resumo_data.append({"Metrica": "OK_DIRETO", "Valor": f"{ok_d:,} ({ok_d/matched_total*100:.1f}%)", "Detalhe": "Status PGS == SMT ext_status (match exato)"})
resumo_data.append({"Metrica": "OK_MAPEADO", "Valor": f"{ok_m:,} ({ok_m/matched_total*100:.1f}%)", "Detalhe": "Status equivalente pelo mapeamento (ex: REAL_USER -> ACTIVE)"})
resumo_data.append({"Metrica": "DIVERGENTE", "Valor": f"{div_t:,} ({div_t/matched_total*100:.1f}%)", "Detalhe": "Status NAO corresponde — possivel falha de sync"})
resumo_data.append({"Metrica": "TOTAL SYNC OK", "Valor": f"{ok_d+ok_m:,} ({(ok_d+ok_m)/matched_total*100:.1f}%)", "Detalhe": "OK_DIRETO + OK_MAPEADO"})
resumo_data.append({"Metrica": "", "Valor": "", "Detalhe": ""})
# Distribuicao PGS
resumo_data.append({"Metrica": "--- DISTRIBUICAO STATUS PGS ---", "Valor": "", "Detalhe": ""})
for status, count in pgs_dist.items():
    resumo_data.append({"Metrica": status, "Valor": f"{count:,}", "Detalhe": f"{count/len(df_pgs)*100:.1f}%"})
df_resumo = pd.DataFrame(resumo_data)

# Aba 2: divergencias detalhadas
df_divergencias = df_comp[df_comp["resultado"] == "DIVERGENTE"].copy()
cols_div = [
    "ext_id_str", "pgs_status", "pgs_status_anterior", "pgs_tipo_conta",
    "pgs_status_atualizado_brt", "pgs_fonte_mudanca",
    "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
    "resultado"
]
df_export_div = df_divergencias[cols_div].copy()
df_export_div.rename(columns={"ext_id_str": "ext_id"}, inplace=True)

# Aba 3: crosstab divergencias
cross_full = (
    df_divergencias
    .groupby(["pgs_status_upper", "smt_ext_status_upper", "smt_int_status_upper"])
    .size()
    .reset_index(name="qtd")
    .sort_values("qtd", ascending=False)
)

# Aba 4: legenda
legenda_data = {
    "Campo": [
        "ext_id", "ecr_id",
        "pgs_status", "pgs_status_anterior", "pgs_tipo_conta",
        "pgs_status_atualizado_brt", "pgs_fonte_mudanca", "pgs_data_registro_brt",
        "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
        "match_direto", "resultado",
        "", "",
        "--- VALORES pgs_status ---", "",
        "real_user", "play_user", "suspended", "closed",
        "rg_closed", "rg_cool_off", "fraud",
        "", "",
        "--- VALORES resultado ---", "",
        "OK_DIRETO", "OK_MAPEADO", "DIVERGENTE", "SO_PGS", "SO_SMT",
        "", "",
        "--- FONTES ---", "",
        "PGS", "SMT",
    ],
    "Descricao": [
        "ID externo do jogador (chave entre PGS e SMT)",
        "ID interno PGS (18 digitos)",
        "Status atual da conta no PGS (Athena/Pragmatic)",
        "Status anterior ao ultimo cambio no PGS",
        "Tipo de conta: play ou real",
        "Data/hora da ultima mudanca de status no PGS (BRT)",
        "Fonte da mudanca (ex: SIGAP = regulatorio)",
        "Data de registro do jogador (BRT)",
        "Status externo na Smartico (core_external_account_status)",
        "Status interno na Smartico (core_account_status)",
        "Data/hora da ultima atualizacao na Smartico",
        "TRUE = pgs_status == smt_ext_status (comparacao direta)",
        "Resultado final da comparacao (ver tabela abaixo)",
        "", "",
        "", "",
        "Conta ativa dinheiro real (SMT espera: ACTIVE)",
        "Conta ativa modo teste (SMT espera: ACTIVE)",
        "Conta suspensa (SMT espera: INACTIVE)",
        "Conta encerrada (SMT espera: CLOSED)",
        "Autoexclusao indefinida (SMT espera: TRUE)",
        "Cool-off temporario (SMT espera: ACTIVE)",
        "Fraude detectada (SMT espera: INACTIVE/BLOCKED)",
        "", "",
        "", "",
        "Status PGS == SMT ext_status (match exato)",
        "Status PGS != SMT ext_status, mas equivale pelo mapeamento",
        "Status PGS e SMT NAO correspondem (possivel falha de sync)",
        "Jogador existe no PGS mas NAO na SMT",
        "Jogador existe na SMT mas NAO no PGS",
        "", "",
        "", "",
        "Athena bireports_ec2.tbl_ecr (Pragmatic Solutions)",
        "BigQuery smartico-bq6.dwh_ext_24105.j_user (Smartico CRM)",
    ],
}
df_legenda = pd.DataFrame(legenda_data)

with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
    df_resumo.to_excel(writer, sheet_name="resumo", index=False)
    df_export_div.to_excel(writer, sheet_name="divergencias", index=False)
    cross_full.to_excel(writer, sheet_name="crosstab_divergencias", index=False)
    df_legenda.to_excel(writer, sheet_name="legenda", index=False)

log.info(f"Excel exportado: {output_excel}")

print(f"\n{'='*70}")
print(f"ARQUIVOS ENTREGUES:")
print(f"{'='*70}")
print(f"  DADOS COMPLETOS (CSV):")
print(f"    base_pgs_{ts}.csv           ({len(df_export_pgs):,} jogadores PGS)")
print(f"    base_smt_{ts}.csv           ({len(df_export_smt):,} jogadores SMT matched)")
print(f"    comparacao_status_{ts}.csv  ({len(df_export_comp):,} registros comparados)")
print(f"")
print(f"  ANALISE (Excel):")
print(f"    auditoria_sync_status_completa_FINAL_{ts}.xlsx")
print(f"      - resumo:               volumetria + resultado sync + distribuicao")
print(f"      - divergencias:         {len(df_export_div):,} registros divergentes (detalhe)")
print(f"      - crosstab_divergencias: combinacoes de status divergentes")
print(f"      - legenda:              dicionario completo")
print("\nAuditoria completa concluida!")