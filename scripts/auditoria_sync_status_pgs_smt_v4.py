"""
Auditoria sync status PGS x SMT — v4
=====================================
Recorte: registros (cadastros) de fato feitos de 21/03 a 24/03.
Filtro: c_sign_up_time no PGS (data de registro do jogador).
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

DATA_INICIO = "2026-03-21"
DATA_FIM = "2026-03-25"
ts = datetime.now().strftime("%Y%m%d_%H%M")
output_dir = "c:/Users/NITRO/OneDrive - PGX/MultiBet/reports/"
output_file = f"{output_dir}auditoria_sync_status_novos_registros_FINAL_{ts}.xlsx"

# ---------------------------------------------------------------------------
# 1. PGS — novos cadastros no periodo
# ---------------------------------------------------------------------------
log.info(f"[PGS] Novos cadastros entre {DATA_INICIO} e {DATA_FIM}...")

sql_pgs = f"""
SELECT
    c_external_id           AS ext_id,
    c_category              AS pgs_status,
    c_ecr_status            AS pgs_tipo_conta,
    c_sign_up_time
        AT TIME ZONE 'UTC'
        AT TIME ZONE 'America/Sao_Paulo'
                            AS pgs_data_registro_brt,
    c_category_updated_time
        AT TIME ZONE 'UTC'
        AT TIME ZONE 'America/Sao_Paulo'
                            AS pgs_status_atualizado_brt
FROM bireports_ec2.tbl_ecr
WHERE c_sign_up_time >= TIMESTAMP '{DATA_INICIO}'
  AND c_sign_up_time <  TIMESTAMP '{DATA_FIM}'
  AND c_test_user = false
  AND c_external_id IS NOT NULL
ORDER BY c_sign_up_time DESC
"""

df_pgs = query_athena(sql_pgs, database="bireports_ec2")
df_pgs["ext_id_str"] = df_pgs["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
log.info(f"[PGS] {len(df_pgs):,} novos cadastros")

# ---------------------------------------------------------------------------
# 2. SMT — buscar os mesmos ext_ids
# ---------------------------------------------------------------------------
log.info("[SMT] Buscando mesmos IDs na Smartico...")

ext_ids = df_pgs["ext_id"].dropna().unique().tolist()
CHUNK_SIZE = 10_000
chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]

dfs_smt = []
for i, chunk in enumerate(chunks, 1):
    log.info(f"  Chunk {i}/{len(chunks)} ({len(chunk):,} IDs)...")
    ids_str = ",".join(f"'{int(eid)}'" for eid in chunk)
    sql_smt = f"""
    SELECT
        user_ext_id                     AS ext_id,
        core_external_account_status    AS smt_ext_status,
        core_account_status             AS smt_internal_status,
        update_date                     AS smt_ultima_atualizacao
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE user_ext_id IN ({ids_str})
    """
    dfs_smt.append(query_bigquery(sql_smt))

df_smt = pd.concat(dfs_smt, ignore_index=True) if dfs_smt else pd.DataFrame()
df_smt["ext_id_str"] = df_smt["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
log.info(f"[SMT] {len(df_smt):,} registros encontrados")

# ---------------------------------------------------------------------------
# 3. MERGE
# ---------------------------------------------------------------------------
log.info("Merge PGS x SMT...")

df_comp = df_pgs.merge(df_smt, on="ext_id_str", how="left", suffixes=("_pgs", "_smt"), indicator=True)
matched = (df_comp["_merge"] == "both").sum()
sem_match = (df_comp["_merge"] == "left_only").sum()
log.info(f"Match: {matched:,} | Sem match SMT: {sem_match:,}")

# ---------------------------------------------------------------------------
# 4. CROSSTAB
# ---------------------------------------------------------------------------
df_matched = df_comp[df_comp["_merge"] == "both"].copy()

cross_ext = pd.crosstab(
    df_matched["pgs_status"].str.upper(),
    df_matched["smt_ext_status"].str.upper(),
    margins=True, margins_name="TOTAL"
)

cross_int = pd.crosstab(
    df_matched["pgs_status"].str.upper(),
    df_matched["smt_internal_status"].str.upper(),
    margins=True, margins_name="TOTAL"
)

# ---------------------------------------------------------------------------
# 5. RESUMO
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("AUDITORIA SYNC — NOVOS REGISTROS (sign-ups)")
print(f"Periodo: {DATA_INICIO} (sabado) a 2026-03-24 (hoje)")
print("=" * 70)
print(f"Novos cadastros PGS:    {len(df_pgs):,}")
print(f"Encontrados na SMT:     {matched:,}")
print(f"Sem match na SMT:       {sem_match:,}")

# Distribuicao por dia
df_pgs["dia_registro"] = pd.to_datetime(df_pgs["pgs_data_registro_brt"]).dt.date
print(f"\n--- Cadastros por dia ---")
print(df_pgs["dia_registro"].value_counts().sort_index().to_string())

print(f"\n--- CROSSTAB: PGS status vs SMT ext_status ---")
print(cross_ext.to_string())

print(f"\n--- CROSSTAB: PGS status vs SMT internal_status ---")
print(cross_int.to_string())

# ---------------------------------------------------------------------------
# 6. EXPORT EXCEL
# ---------------------------------------------------------------------------
log.info(f"Exportando para {output_file}...")

for df in [df_pgs, df_smt, df_comp]:
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

# base_smt
df_smt_exp = df_smt[df_smt["ext_id_str"].isin(df_comp["ext_id_str"])][
    ["ext_id_str", "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao"]
].copy().rename(columns={"ext_id_str": "ext_id"})

# base_pgs
df_pgs_exp = df_pgs[[
    "ext_id_str", "pgs_status", "pgs_tipo_conta",
    "pgs_data_registro_brt", "pgs_status_atualizado_brt"
]].copy().rename(columns={"ext_id_str": "ext_id"})

# comparacao
df_comp_exp = df_comp[[
    "ext_id_str",
    "pgs_status", "pgs_tipo_conta", "pgs_data_registro_brt", "pgs_status_atualizado_brt",
    "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
]].copy().rename(columns={"ext_id_str": "ext_id"})

# legenda
legenda = pd.DataFrame({
    "Campo": [
        "ext_id", "pgs_status", "pgs_tipo_conta",
        "pgs_data_registro_brt", "pgs_status_atualizado_brt",
        "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
        "", "",
        "--- FILTRO APLICADO ---", "",
        "Periodo", "Criterio",
        "", "",
        "--- FONTES ---", "",
        "PGS", "SMT",
    ],
    "Descricao": [
        "ID externo do jogador (chave entre PGS e SMT)",
        "Status atual da conta no PGS (c_category)",
        "Tipo de conta no PGS: play ou real",
        "Data/hora de cadastro do jogador (BRT)",
        "Ultima mudanca de status no PGS (BRT)",
        "Status externo na Smartico (core_external_account_status)",
        "Status interno na Smartico (core_account_status)",
        "Ultima atualizacao do registro na Smartico",
        "", "",
        "", "",
        f"{DATA_INICIO} a 2026-03-24",
        "Novos cadastros (c_sign_up_time no periodo), excl. test users",
        "", "",
        "", "",
        "Athena bireports_ec2.tbl_ecr",
        "BigQuery smartico-bq6.dwh_ext_24105.j_user",
    ],
})

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    df_smt_exp.to_excel(writer, sheet_name="base_smt", index=False)
    df_pgs_exp.to_excel(writer, sheet_name="base_pgs", index=False)
    df_comp_exp.to_excel(writer, sheet_name="comparacao", index=False)
    cross_ext.to_excel(writer, sheet_name="crosstab_ext_status")
    cross_int.to_excel(writer, sheet_name="crosstab_int_status")
    legenda.to_excel(writer, sheet_name="legenda", index=False)

log.info(f"Excel exportado: {output_file}")

print(f"\n{'='*70}")
print(f"ARQUIVO: {output_file}")
print(f"{'='*70}")
print(f"  base_smt:    {len(df_smt_exp):,}")
print(f"  base_pgs:    {len(df_pgs_exp):,}")
print(f"  comparacao:  {len(df_comp_exp):,}")
print(f"  crosstabs + legenda")
print("\nConcluido!")
