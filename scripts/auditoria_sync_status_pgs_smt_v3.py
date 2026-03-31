"""
Auditoria de sincronizacao de status: PGS (Athena) vs SMT (BigQuery) — v3
==========================================================================
Objetivo: Extrair registros atualizados de sabado (21/03) ate hoje (24/03)
com comparativo DIRETO (sem mapeamento) entre PGS status e SMT ext_status.

Entrega conforme solicitado:
  - base_smt: DWH STM (ExtID, ext status, internal status)
  - base_pgs: PGS (ExtID, status)
  - comparacao: merge lado a lado
  - crosstab: distribuicao PGS status vs SMT ext_status
  - legenda
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
DATA_INICIO = "2026-03-21"  # sabado
DATA_FIM = "2026-03-25"     # exclusivo
ts = datetime.now().strftime("%Y%m%d_%H%M")
output_dir = "c:/Users/NITRO/OneDrive - PGX/MultiBet/reports/"
output_file = f"{output_dir}auditoria_sync_status_FINAL_{ts}.xlsx"

# ---------------------------------------------------------------------------
# 1. PGS (Athena) — base completa de status
# ---------------------------------------------------------------------------
log.info("[PGS] Extraindo base completa...")

sql_pgs = """
SELECT
    c_external_id                 AS ext_id,
    c_category                    AS pgs_status,
    c_ecr_status                  AS pgs_tipo_conta,
    c_category_updated_time
        AT TIME ZONE 'UTC'
        AT TIME ZONE 'America/Sao_Paulo'
                                  AS pgs_status_atualizado_brt
FROM bireports_ec2.tbl_ecr
WHERE c_test_user = false
  AND c_external_id IS NOT NULL
"""

df_pgs = query_athena(sql_pgs, database="bireports_ec2")
df_pgs["ext_id_str"] = df_pgs["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
log.info(f"[PGS] {len(df_pgs):,} jogadores")

# ---------------------------------------------------------------------------
# 2. SMT (BigQuery) — registros atualizados no periodo
# ---------------------------------------------------------------------------
log.info(f"[SMT] Extraindo update_date entre {DATA_INICIO} e {DATA_FIM}...")

sql_smt = f"""
SELECT
    user_ext_id                     AS ext_id,
    core_external_account_status    AS smt_ext_status,
    core_account_status             AS smt_internal_status,
    update_date                     AS smt_ultima_atualizacao
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE update_date >= '{DATA_INICIO}'
  AND update_date <  '{DATA_FIM}'
"""

df_smt = query_bigquery(sql_smt)
df_smt["smt_ultima_atualizacao"] = pd.to_datetime(df_smt["smt_ultima_atualizacao"])
df_smt["ext_id_str"] = df_smt["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
df_smt = df_smt.sort_values("smt_ultima_atualizacao", ascending=False).drop_duplicates("ext_id_str", keep="first")
log.info(f"[SMT] {len(df_smt):,} registros unicos")

# ---------------------------------------------------------------------------
# 3. INNER JOIN — registros em ambas as bases
# ---------------------------------------------------------------------------
log.info("Inner join PGS x SMT...")

df_comp = df_smt.merge(df_pgs, on="ext_id_str", how="inner", suffixes=("_smt", "_pgs"))
smt_only = len(df_smt) - len(df_comp)
log.info(f"Match: {len(df_comp):,} | SMT sem PGS: {smt_only:,}")

# ---------------------------------------------------------------------------
# 4. CROSSTAB — PGS status vs SMT ext_status (dados crus)
# ---------------------------------------------------------------------------
cross_ext = pd.crosstab(
    df_comp["pgs_status"].str.upper(),
    df_comp["smt_ext_status"].str.upper(),
    margins=True,
    margins_name="TOTAL"
)

cross_int = pd.crosstab(
    df_comp["pgs_status"].str.upper(),
    df_comp["smt_internal_status"].str.upper(),
    margins=True,
    margins_name="TOTAL"
)

# ---------------------------------------------------------------------------
# 5. RESUMO NO CONSOLE
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("AUDITORIA SYNC STATUS: PGS x SMT")
print(f"Periodo: {DATA_INICIO} (sabado) a 2026-03-24 (hoje)")
print("=" * 70)
print(f"Registros SMT atualizados no periodo:  {len(df_smt):,}")
print(f"Registros PGS (base completa):         {len(df_pgs):,}")
print(f"Match (inner join):                    {len(df_comp):,}")
print(f"SMT sem match PGS:                     {smt_only:,}")

print(f"\n--- CROSSTAB: PGS status vs SMT ext_status ---")
print(cross_ext.to_string())

print(f"\n--- CROSSTAB: PGS status vs SMT internal_status ---")
print(cross_int.to_string())

# ---------------------------------------------------------------------------
# 6. EXPORT EXCEL
# ---------------------------------------------------------------------------
log.info(f"Exportando para {output_file}...")

# Remover timezone para Excel
for df in [df_pgs, df_smt, df_comp]:
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

# Aba 1: base_smt
df_exp_smt = df_smt[df_smt["ext_id_str"].isin(df_comp["ext_id_str"])][
    ["ext_id_str", "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao"]
].copy().rename(columns={"ext_id_str": "ext_id"})

# Aba 2: base_pgs
df_exp_pgs = df_pgs[df_pgs["ext_id_str"].isin(df_comp["ext_id_str"])][
    ["ext_id_str", "pgs_status", "pgs_tipo_conta", "pgs_status_atualizado_brt"]
].copy().rename(columns={"ext_id_str": "ext_id"})

# Aba 3: comparacao (dados lado a lado, sem classificacao)
df_exp_comp = df_comp[[
    "ext_id_str",
    "pgs_status", "pgs_tipo_conta", "pgs_status_atualizado_brt",
    "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
]].copy().rename(columns={"ext_id_str": "ext_id"})

# Aba 4: legenda
legenda = pd.DataFrame({
    "Campo": [
        "ext_id",
        "pgs_status", "pgs_tipo_conta", "pgs_status_atualizado_brt",
        "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
        "", "",
        "--- FONTES ---", "",
        "PGS (Pragmatic)", "SMT (Smartico)",
        "", "",
        "--- PERIODO ---", "",
        "Filtro SMT", "Filtro PGS",
    ],
    "Descricao": [
        "ID externo do jogador (chave entre PGS e SMT)",
        "Status atual da conta no PGS (c_category)",
        "Tipo de conta no PGS: play ou real",
        "Ultima mudanca de status no PGS (horario BRT)",
        "Status externo na Smartico (core_external_account_status)",
        "Status interno na Smartico (core_account_status)",
        "Ultima atualizacao do registro na Smartico",
        "", "",
        "", "",
        "Athena bireports_ec2.tbl_ecr — base completa (excl. test users)",
        "BigQuery smartico-bq6.dwh_ext_24105.j_user — update_date >= 21/03",
        "", "",
        "", "",
        f"update_date >= {DATA_INICIO} AND < {DATA_FIM}",
        "Base completa (todos os jogadores, sem filtro de data)",
    ],
})

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    df_exp_smt.to_excel(writer, sheet_name="base_smt", index=False)
    df_exp_pgs.to_excel(writer, sheet_name="base_pgs", index=False)
    df_exp_comp.to_excel(writer, sheet_name="comparacao", index=False)
    cross_ext.to_excel(writer, sheet_name="crosstab_ext_status")
    cross_int.to_excel(writer, sheet_name="crosstab_int_status")
    legenda.to_excel(writer, sheet_name="legenda", index=False)

log.info(f"Excel exportado: {output_file}")

print(f"\n{'='*70}")
print(f"ARQUIVO: {output_file}")
print(f"{'='*70}")
print(f"  base_smt:            {len(df_exp_smt):,} registros")
print(f"  base_pgs:            {len(df_exp_pgs):,} registros")
print(f"  comparacao:          {len(df_exp_comp):,} registros")
print(f"  crosstab_ext_status: PGS status vs SMT ext_status")
print(f"  crosstab_int_status: PGS status vs SMT internal_status")
print(f"  legenda:             dicionario de campos e fontes")
print("\nConcluido!")