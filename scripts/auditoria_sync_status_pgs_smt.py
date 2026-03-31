"""
Auditoria de sincronizacao de status: PGS (Athena) vs SMT (BigQuery)
=====================================================================
Objetivo: Verificar se as correcoes recentes na integracao de status
entre PGS e SMT foram efetivas.

Periodo: Sabado 21/03/2026 ate hoje 24/03/2026
Fontes:
  - PGS (Pragmatic) = Athena bireports_ec2.tbl_ecr
  - SMT (Smartico)  = BigQuery j_user

Entrega: Excel com 3 abas + legenda
  - base_pgs: ExtID, status (c_category)
  - base_smt: ExtID, ext_status, internal_status
  - comparacao: merge + analise de divergencias
  - legenda: dicionario de colunas e glossario
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
DATA_FIM = "2026-03-25"     # exclusivo (ate fim do dia 24/03)
ts = datetime.now().strftime("%Y%m%d_%H%M")
output_dir = "c:/Users/NITRO/OneDrive - PGX/MultiBet/reports/"
output_file = f"{output_dir}auditoria_sync_status_pgs_smt_FINAL_{ts}.xlsx"

# Mapeamento esperado: c_category (PGS) -> core_account_status (SMT internal)
MAPA_PGS_TO_SMT_INT = {
    "REAL_USER":   "ACTIVE",
    "PLAY_USER":   "ACTIVE",
    "SUSPENDED":   "SUSPENDED",
    "CLOSED":      "DEACTIVATED",
    "RG_CLOSED":   "SELF_EXCLUDED",
    "RG_COOL_OFF": "SELF_EXCLUDED",
    "FRAUD":       "BLOCKED",
}

# Mapeamento esperado: c_category (PGS) -> core_external_account_status (SMT ext)
MAPA_PGS_TO_SMT_EXT = {
    "REAL_USER":   "ACTIVE",
    "PLAY_USER":   "ACTIVE",
    "SUSPENDED":   "INACTIVE",
    "CLOSED":      "CLOSED",
    "RG_CLOSED":   "TRUE",       # self_excluded = TRUE
    "RG_COOL_OFF": "ACTIVE",
    "FRAUD":       "INACTIVE",
}

# ---------------------------------------------------------------------------
# 1. EXTRACAO PGS (Athena) — base verdade
# ---------------------------------------------------------------------------
log.info(f"[PGS] Extraindo de bireports_ec2.tbl_ecr ({DATA_INICIO} a {DATA_FIM})...")

sql_pgs = f"""
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
    c_rg_cool_off                 AS pgs_rg_cool_off,
    c_rg_self_exclusion           AS pgs_rg_self_exclusion,
    c_rg_closed                   AS pgs_rg_closed
FROM bireports_ec2.tbl_ecr
WHERE c_category_updated_time >= TIMESTAMP '{DATA_INICIO}'
  AND c_category_updated_time <  TIMESTAMP '{DATA_FIM}'
  AND c_test_user = false
  AND c_external_id IS NOT NULL
ORDER BY c_category_updated_time DESC
"""

df_pgs = query_athena(sql_pgs, database="bireports_ec2")
log.info(f"[PGS] {len(df_pgs):,} registros extraidos")

# ---------------------------------------------------------------------------
# 2. EXTRACAO SMT (BigQuery) — CRM
# ---------------------------------------------------------------------------
log.info("[SMT] Extraindo de j_user (BigQuery)...")

# Pegar os ext_ids encontrados no PGS para buscar na SMT
ext_ids_pgs = df_pgs["ext_id"].dropna().unique().tolist()
log.info(f"[SMT] Buscando {len(ext_ids_pgs):,} IDs na Smartico...")

# Chunked query para nao estourar limite do IN clause
CHUNK_SIZE = 10_000
chunks = [ext_ids_pgs[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids_pgs), CHUNK_SIZE)]

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
log.info(f"[SMT] {len(df_smt):,} registros extraidos")

# ---------------------------------------------------------------------------
# 3. COMPARACAO — merge PGS x SMT
# ---------------------------------------------------------------------------
log.info("Montando comparacao PGS x SMT...")

# Normalizar ext_id como string para join
df_pgs["ext_id_str"] = df_pgs["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)
df_smt["ext_id_str"] = df_smt["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)

df_comp = df_pgs.merge(df_smt, on="ext_id_str", how="left", suffixes=("_pgs", "_smt"))

# Normalizar para comparacao (uppercase, strip)
df_comp["pgs_status_upper"] = df_comp["pgs_status"].str.strip().str.upper()
df_comp["smt_ext_status_upper"] = df_comp["smt_ext_status"].fillna("SEM_MATCH").str.strip().str.upper()
df_comp["smt_int_status_upper"] = df_comp["smt_internal_status"].fillna("SEM_MATCH").str.strip().str.upper()

# Comparacao 1: PGS status mapeado vs SMT ext_status
df_comp["smt_ext_esperado"] = df_comp["pgs_status_upper"].map(MAPA_PGS_TO_SMT_EXT)
df_comp["match_ext_status"] = df_comp["smt_ext_status_upper"] == df_comp["smt_ext_esperado"]

# Comparacao 2: PGS status mapeado vs SMT internal_status
df_comp["smt_int_esperado"] = df_comp["pgs_status_upper"].map(MAPA_PGS_TO_SMT_INT)
df_comp["match_int_status"] = df_comp["smt_int_status_upper"] == df_comp["smt_int_esperado"]

# Resultado final
df_comp["resultado"] = "OK"
df_comp.loc[~df_comp["match_ext_status"], "resultado"] = "DIVERGENTE_EXT"
df_comp.loc[~df_comp["match_int_status"], "resultado"] = "DIVERGENTE_INT"
df_comp.loc[
    (~df_comp["match_ext_status"]) & (~df_comp["match_int_status"]),
    "resultado"
] = "DIVERGENTE_AMBOS"
df_comp.loc[df_comp["smt_ext_status_upper"] == "SEM_MATCH", "resultado"] = "SEM_MATCH_SMT"

# ---------------------------------------------------------------------------
# 4. RESUMO NO CONSOLE
# ---------------------------------------------------------------------------
total = len(df_comp)
sem_match = (df_comp["resultado"] == "SEM_MATCH_SMT").sum()
ok = (df_comp["resultado"] == "OK").sum()
div_ext = (df_comp["resultado"] == "DIVERGENTE_EXT").sum()
div_int = (df_comp["resultado"] == "DIVERGENTE_INT").sum()
div_ambos = (df_comp["resultado"] == "DIVERGENTE_AMBOS").sum()

print("\n" + "=" * 70)
print("AUDITORIA DE SINCRONIZACAO PGS x SMT")
print(f"Periodo: {DATA_INICIO} (sabado) a 2026-03-24 (hoje)")
print("=" * 70)
print(f"Registros PGS (status alterado no periodo): {len(df_pgs):,}")
print(f"Registros SMT encontrados:                  {len(df_smt):,}")
print(f"Sem match na SMT:                           {sem_match:,}")
print()
print(f"RESULTADO DA COMPARACAO:")
print(f"  OK (sync correto):          {ok:,} ({ok/total*100:.1f}%)")
print(f"  DIVERGENTE ext_status:      {div_ext:,} ({div_ext/total*100:.1f}%)")
print(f"  DIVERGENTE internal_status: {div_int:,} ({div_int/total*100:.1f}%)")
print(f"  DIVERGENTE ambos:           {div_ambos:,} ({div_ambos/total*100:.1f}%)")
print(f"  Sem match SMT:              {sem_match:,} ({sem_match/total*100:.1f}%)")

# Detalhamento divergencias
divergentes = df_comp[~df_comp["resultado"].isin(["OK", "SEM_MATCH_SMT"])]
if len(divergentes) > 0:
    print(f"\n--- Detalhamento divergencias (por combinacao de status) ---")
    cross = (
        divergentes
        .groupby(["pgs_status_upper", "smt_ext_status_upper", "smt_int_status_upper"])
        .size()
        .reset_index(name="qtd")
        .sort_values("qtd", ascending=False)
    )
    print(cross.to_string(index=False))

# Analise por dia
print(f"\n--- Resultado por dia ---")
df_comp["dia"] = pd.to_datetime(df_comp["pgs_status_atualizado_brt"]).dt.date
dia_resumo = df_comp.groupby("dia").agg(
    total=("resultado", "count"),
    ok=("resultado", lambda x: (x == "OK").sum()),
    divergente=("resultado", lambda x: (~x.isin(["OK", "SEM_MATCH_SMT"])).sum()),
    sem_match=("resultado", lambda x: (x == "SEM_MATCH_SMT").sum()),
).reset_index()
dia_resumo["pct_ok"] = (dia_resumo["ok"] / dia_resumo["total"] * 100).round(1)
print(dia_resumo.to_string(index=False))

# ---------------------------------------------------------------------------
# 5. EXPORTA EXCEL com 4 abas
# ---------------------------------------------------------------------------
log.info(f"Exportando para {output_file}...")

# Remover timezone de colunas datetime para Excel
for col in df_pgs.select_dtypes(include=["datetimetz"]).columns:
    df_pgs[col] = df_pgs[col].dt.tz_localize(None)
for col in df_smt.select_dtypes(include=["datetimetz"]).columns:
    df_smt[col] = df_smt[col].dt.tz_localize(None)
for col in df_comp.select_dtypes(include=["datetimetz"]).columns:
    df_comp[col] = df_comp[col].dt.tz_localize(None)

# Preparar abas
# Aba 1: base_pgs
cols_pgs = [
    "ext_id", "ecr_id", "pgs_status", "pgs_status_anterior",
    "pgs_tipo_conta", "pgs_status_atualizado_brt", "pgs_fonte_mudanca",
    "pgs_rg_cool_off", "pgs_rg_self_exclusion", "pgs_rg_closed"
]
df_export_pgs = df_pgs[cols_pgs].copy()
df_export_pgs["ext_id"] = df_export_pgs["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)

# Aba 2: base_smt
cols_smt = ["ext_id", "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao"]
df_export_smt = df_smt[cols_smt].copy()
df_export_smt["ext_id"] = df_export_smt["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)

# Aba 3: comparacao
cols_comp = [
    "ext_id_str", "pgs_status", "pgs_status_anterior", "pgs_tipo_conta",
    "pgs_status_atualizado_brt", "pgs_fonte_mudanca",
    "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
    "smt_ext_esperado", "match_ext_status",
    "smt_int_esperado", "match_int_status",
    "resultado", "dia"
]
df_export_comp = df_comp[cols_comp].copy()
df_export_comp.rename(columns={"ext_id_str": "ext_id"}, inplace=True)

# Aba 4: legenda
legenda_data = {
    "Campo": [
        "ext_id", "ecr_id",
        "pgs_status", "pgs_status_anterior", "pgs_tipo_conta",
        "pgs_status_atualizado_brt", "pgs_fonte_mudanca",
        "pgs_rg_cool_off", "pgs_rg_self_exclusion", "pgs_rg_closed",
        "smt_ext_status", "smt_internal_status", "smt_ultima_atualizacao",
        "smt_ext_esperado", "match_ext_status",
        "smt_int_esperado", "match_int_status",
        "resultado", "dia",
        "", "",
        "--- MAPEAMENTO PGS -> SMT ext_status ---", "",
        "real_user", "play_user", "suspended", "closed",
        "rg_closed", "rg_cool_off", "fraud",
        "", "",
        "--- MAPEAMENTO PGS -> SMT internal_status ---", "",
        "real_user", "play_user", "suspended", "closed",
        "rg_closed", "rg_cool_off", "fraud",
        "", "",
        "--- VALORES resultado ---", "",
        "OK", "DIVERGENTE_EXT", "DIVERGENTE_INT",
        "DIVERGENTE_AMBOS", "SEM_MATCH_SMT",
    ],
    "Descricao": [
        "ID externo do jogador (chave entre PGS e SMT)",
        "ID interno PGS (18 digitos)",
        "Status atual da conta no PGS (Athena/Pragmatic)",
        "Status anterior ao ultimo cambio no PGS",
        "Tipo de conta: play ou real",
        "Data/hora da ultima mudanca de status no PGS (horario BRT)",
        "Fonte da mudanca (ex: SIGAP = regulatorio, manual, etc.)",
        "Cool-off ativo? (active/inactive)",
        "Autoexclusao ativa? (active/inactive)",
        "RG closed ativo? (active/inactive)",
        "Status externo real na Smartico (core_external_account_status)",
        "Status interno real na Smartico (core_account_status)",
        "Data/hora da ultima atualizacao na Smartico",
        "Status externo ESPERADO na SMT (baseado no mapeamento PGS->SMT)",
        "TRUE = smt_ext_status == smt_ext_esperado",
        "Status interno ESPERADO na SMT (baseado no mapeamento PGS->SMT)",
        "TRUE = smt_internal_status == smt_int_esperado",
        "Resultado final da comparacao (ver tabela abaixo)",
        "Dia da mudanca de status (BRT)",
        "", "",
        "", "",
        "-> ACTIVE", "-> ACTIVE", "-> INACTIVE", "-> CLOSED",
        "-> TRUE (self_excluded)", "-> ACTIVE", "-> INACTIVE",
        "", "",
        "", "",
        "-> ACTIVE", "-> ACTIVE", "-> SUSPENDED", "-> DEACTIVATED",
        "-> SELF_EXCLUDED", "-> SELF_EXCLUDED", "-> BLOCKED",
        "", "",
        "", "",
        "Sincronizacao correta em ambos os campos",
        "ext_status diverge (smt_ext_status != esperado)",
        "internal_status diverge (smt_int_status != esperado)",
        "Ambos os campos divergem",
        "Jogador nao encontrado na Smartico",
    ],
    "Fonte": [
        "PGS + SMT", "PGS",
        "PGS (bireports_ec2.tbl_ecr.c_category)", "PGS", "PGS",
        "PGS (convertido UTC->BRT)", "PGS",
        "PGS", "PGS", "PGS",
        "SMT (j_user.core_external_account_status)", "SMT (j_user.core_account_status)", "SMT",
        "Mapeamento", "Calculado",
        "Mapeamento", "Calculado",
        "Calculado", "Calculado",
        "", "",
        "", "",
        "", "", "", "",
        "", "", "",
        "", "",
        "", "",
        "", "", "", "",
        "", "", "",
        "", "",
        "", "",
        "", "", "",
        "", "",
    ],
}
df_legenda = pd.DataFrame(legenda_data)

# Escrever Excel
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    df_export_pgs.to_excel(writer, sheet_name="base_pgs", index=False)
    df_export_smt.to_excel(writer, sheet_name="base_smt", index=False)
    df_export_comp.to_excel(writer, sheet_name="comparacao", index=False)
    df_legenda.to_excel(writer, sheet_name="legenda", index=False)

log.info(f"Excel exportado: {output_file}")

# ---------------------------------------------------------------------------
# 6. CONCLUSAO
# ---------------------------------------------------------------------------
pct_ok = ok / total * 100 if total > 0 else 0
pct_div = (div_ext + div_int + div_ambos) / total * 100 if total > 0 else 0

print("\n" + "=" * 70)
print("CONCLUSAO")
print("=" * 70)
if pct_ok >= 95:
    print(f"A sincronizacao esta SAUDAVEL: {pct_ok:.1f}% dos registros estao corretos.")
elif pct_ok >= 80:
    print(f"A sincronizacao MELHOROU mas ainda ha divergencias: {pct_div:.1f}% divergente.")
else:
    print(f"ATENCAO: sincronizacao com problemas significativos ({pct_div:.1f}% divergente).")

print(f"\nArquivo entregue: {output_file}")
print(f"  - base_pgs:    {len(df_export_pgs):,} registros")
print(f"  - base_smt:    {len(df_export_smt):,} registros")
print(f"  - comparacao:  {len(df_export_comp):,} registros")
print(f"  - legenda:     dicionario completo de campos e mapeamentos")
print("\nAuditoria concluida!")