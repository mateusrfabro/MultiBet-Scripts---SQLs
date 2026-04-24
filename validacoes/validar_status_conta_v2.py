"""
Validacao v2: Status de conta Athena (PGS) vs Smartico (BigQuery)
=================================================================
Objetivo: Garantir que o status de conta (c_category) no Athena esta
sincronizado com os campos core_account_status e core_external_account_status
na Smartico.

Base verdade: Athena bireports_ec2.tbl_ecr
Periodo: contas com c_category_updated_time entre 01/01/2026 e 21/03/2026

v2 (2026-03-20): Migrado de Redshift para Athena (Presto/Trino).
    Adicionado filtro c_test_user = false.
    Adicionado arquivo de acao CRM (divergentes para correcao manual).
"""

import sys
import logging
import pandas as pd
from datetime import datetime

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")
from db.athena import query_athena
from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# PARAMETROS
# ---------------------------------------------------------------------------
DATA_INICIO = "2026-01-01"
# DATA_FIM dinamico: usa a data de hoje (BRT) como limite exclusivo
# O banco esta em UTC, mas CURRENT_DATE no Athena retorna UTC.
# Como BRT = UTC-3, no pior caso pegamos registros ate 02:59 do "amanha" BRT,
# o que e aceitavel (e melhor do que perder registros do dia atual).
DATA_FIM_EXPR = "CURRENT_DATE + INTERVAL '1' DAY"  # ate o fim do dia UTC de hoje
output_path = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/validacoes/"
ts = datetime.now().strftime("%Y%m%d_%H%M")

# ---------------------------------------------------------------------------
# 1. EXTRACAO - Athena (base verdade)
# ---------------------------------------------------------------------------
log.info("Extraindo dados do Athena (bireports_ec2.tbl_ecr)...")

sql_athena = f"""
SELECT
    c_external_id,
    c_ecr_id,
    c_category,                       -- status atual (real_user, suspended, closed, etc.)
    c_old_category,                   -- status anterior
    c_ecr_status,                     -- play / real
    c_category_updated_time,
    c_category_change_source,
    c_rg_cool_off,                    -- cool-off status (active/inactive)
    c_rg_self_exclusion,              -- self-exclusion status (active/inactive)
    c_rg_closed                       -- rg closed status
FROM bireports_ec2.tbl_ecr
WHERE c_category_updated_time >= TIMESTAMP '{DATA_INICIO}'
  AND c_category_updated_time <  {DATA_FIM_EXPR}
  AND c_test_user = false
"""

df_athena = query_athena(sql_athena, database="bireports_ec2")
log.info(f"Athena: {len(df_athena):,} registros extraidos")

# ---------------------------------------------------------------------------
# 2. EXTRACAO - Smartico (BigQuery)
# ---------------------------------------------------------------------------
log.info("Extraindo dados da Smartico (j_user)...")

ext_ids = df_athena["c_external_id"].dropna().unique().tolist()
log.info(f"IDs unicos para buscar na Smartico: {len(ext_ids):,}")

CHUNK_SIZE = 10_000
chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]

dfs_smartico = []
for i, chunk in enumerate(chunks, 1):
    log.info(f"  Chunk {i}/{len(chunks)} ({len(chunk):,} IDs)...")
    ids_str = ",".join(f"'{int(eid)}'" for eid in chunk)
    sql_bq = f"""
    SELECT
        user_ext_id,
        core_account_status,
        core_external_account_status,
        update_date
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE user_ext_id IN ({ids_str})
    """
    dfs_smartico.append(query_bigquery(sql_bq))

df_smartico = pd.concat(dfs_smartico, ignore_index=True)
log.info(f"Smartico: {len(df_smartico):,} registros extraidos")

# ---------------------------------------------------------------------------
# 3. JOIN - Merge entre Athena e Smartico
# ---------------------------------------------------------------------------
log.info("Fazendo merge entre Athena e Smartico...")

df_athena["ext_id_str"] = df_athena["c_external_id"].astype(str)
df_smartico["ext_id_str"] = df_smartico["user_ext_id"].astype(str)

df = df_athena.merge(df_smartico, on="ext_id_str", how="left", indicator=True)

match_counts = df["_merge"].value_counts()
log.info(f"Match: {match_counts.to_dict()}")

# ---------------------------------------------------------------------------
# 4. COMPARACAO 1 - c_category vs core_external_account_status
# ---------------------------------------------------------------------------
log.info("=== COMPARACAO 1: c_category vs core_external_account_status ===")

df["athena_cat"] = df["c_category"].str.strip().str.upper()
df["smartico_ext_status"] = df["core_external_account_status"].fillna("NULL").str.strip().str.upper()

df["diverge_ext_status"] = df["athena_cat"] != df["smartico_ext_status"]

total = len(df)
divergentes_ext = df["diverge_ext_status"].sum()
log.info(f"Total: {total:,} | Divergentes: {divergentes_ext:,} ({divergentes_ext/total*100:.1f}%)")

if divergentes_ext > 0:
    cross_ext = (
        df[df["diverge_ext_status"]]
        .groupby(["athena_cat", "smartico_ext_status"])
        .size()
        .reset_index(name="qty")
        .sort_values("qty", ascending=False)
    )
    print("\n--- Divergencias: c_category (Athena) vs core_external_account_status (Smartico) ---")
    print(cross_ext.to_string(index=False))

# ---------------------------------------------------------------------------
# 5. COMPARACAO 2 - c_category vs core_account_status
# ---------------------------------------------------------------------------
log.info("=== COMPARACAO 2: c_category vs core_account_status ===")

df["smartico_acc_status"] = df["core_account_status"].fillna("NULL").str.strip().str.upper()

MAPA_CATEGORY_TO_ACC_STATUS = {
    "REAL_USER":   "ACTIVE",
    "PLAY_USER":   "ACTIVE",
    "SUSPENDED":   "SUSPENDED",
    "CLOSED":      "DEACTIVATED",
    "RG_CLOSED":   "ACTIVE",
    "RG_COOL_OFF": "ACTIVE",
    "FRAUD":       "BLOCKED",
}

df["expected_acc_status"] = df["athena_cat"].map(MAPA_CATEGORY_TO_ACC_STATUS)
df["diverge_acc_status"] = df["smartico_acc_status"] != df["expected_acc_status"]

divergentes_acc = df["diverge_acc_status"].sum()
log.info(f"Total: {total:,} | Divergentes: {divergentes_acc:,} ({divergentes_acc/total*100:.1f}%)")

if divergentes_acc > 0:
    cross_acc = (
        df[df["diverge_acc_status"]]
        .groupby(["athena_cat", "expected_acc_status", "smartico_acc_status"])
        .size()
        .reset_index(name="qty")
        .sort_values("qty", ascending=False)
    )
    print("\n--- Divergencias: c_category (Athena) -> esperado vs real (Smartico) ---")
    print(cross_acc.to_string(index=False))

# ---------------------------------------------------------------------------
# 6. RESUMO GERAL
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("RESUMO DA VALIDACAO")
print("=" * 70)
data_fim_display = datetime.now().strftime("%Y-%m-%d")
print(f"Periodo: {DATA_INICIO} a {data_fim_display} (CURRENT_DATE)")
print(f"Contas analisadas (Athena): {len(df_athena):,}")
print(f"Contas encontradas na Smartico: {match_counts.get('both', 0):,}")
print(f"Contas SEM match na Smartico: {match_counts.get('left_only', 0):,}")
print(f"")
print(f"COMP 1 - c_category (Athena) vs core_external_account_status (Smartico):")
print(f"  Consistentes: {total - divergentes_ext:,} ({(total - divergentes_ext)/total*100:.1f}%)")
print(f"  Divergentes:  {divergentes_ext:,} ({divergentes_ext/total*100:.1f}%)")
print(f"")
print(f"COMP 2 - c_category (Athena) vs core_account_status (Smartico, com mapeamento):")
print(f"  Consistentes: {total - divergentes_acc:,} ({(total - divergentes_acc)/total*100:.1f}%)")
print(f"  Divergentes:  {divergentes_acc:,} ({divergentes_acc/total*100:.1f}%)")

# ---------------------------------------------------------------------------
# 7. ANALISE TEMPORAL - taxa de sincronizacao por semana
# ---------------------------------------------------------------------------
log.info("=== ANALISE TEMPORAL: taxa de sync por semana ===")

df["category_time"] = pd.to_datetime(df["c_category_updated_time"])
# Remover timezone se tiver (Athena retorna com tz)
if df["category_time"].dt.tz is not None:
    df["category_time"] = df["category_time"].dt.tz_localize(None)
df["semana"] = df["category_time"].dt.isocalendar().week.astype(int)

# 7a. core_external_account_status por semana
df["ext_match"] = df["athena_cat"] == df["smartico_ext_status"]
sync_ext_semana = df.groupby(["semana", "athena_cat"]).agg(
    total=("ext_match", "count"),
    corretos=("ext_match", "sum")
).reset_index()
sync_ext_semana["pct_sync"] = (sync_ext_semana["corretos"] / sync_ext_semana["total"] * 100).round(1)

print("\n--- Taxa de sync: core_external_account_status (Smartico) por semana ---")
print(sync_ext_semana.to_string(index=False))

# 7b. core_account_status por semana
df["acc_match"] = ~df["diverge_acc_status"]
sync_acc_semana = df.groupby(["semana", "athena_cat"]).agg(
    total=("acc_match", "count"),
    corretos=("acc_match", "sum")
).reset_index()
sync_acc_semana["pct_sync"] = (sync_acc_semana["corretos"] / sync_acc_semana["total"] * 100).round(1)

print("\n--- Taxa de sync: core_account_status (Smartico) por semana ---")
print(sync_acc_semana.to_string(index=False))

# ---------------------------------------------------------------------------
# 8. DETALHAMENTO CONTAS CLOSED
# ---------------------------------------------------------------------------
log.info("=== DETALHAMENTO: contas closed ===")

df_closed = df[df["athena_cat"] == "CLOSED"].copy()
df_closed["ja_era_closed"] = df_closed["c_old_category"].str.strip().str.upper() == "CLOSED"

detail_closed = df_closed.groupby(
    ["ja_era_closed", "c_old_category", "c_category_change_source", "smartico_acc_status"]
).size().reset_index(name="qty").sort_values("qty", ascending=False)

print("\n--- Contas CLOSED no Athena: origem e status na Smartico ---")
print(detail_closed.to_string(index=False))

# ---------------------------------------------------------------------------
# 9. INVESTIGACAO DE CAUSA RAIZ
# ---------------------------------------------------------------------------
log.info("=== INVESTIGACAO DE CAUSA RAIZ ===")

# 9a. Smartico update_date vs Athena c_category_updated_time
df["smartico_updated_at"] = pd.to_datetime(df["update_date"])
if df["smartico_updated_at"].dt.tz is not None:
    df["smartico_updated_at"] = df["smartico_updated_at"].dt.tz_localize(None)
df["dias_diff"] = (df["smartico_updated_at"] - df["category_time"]).dt.total_seconds() / 86400
df["smartico_atualizado_depois"] = df["smartico_updated_at"] >= df["category_time"]

# Foco nas contas closed (maior problema)
df_cl = df[df["athena_cat"] == "CLOSED"].copy()
df_cl["smartico_diverge"] = df_cl["smartico_acc_status"] == "ACTIVE"

print("\n--- CAUSA RAIZ: Smartico recebeu atualizacoes DEPOIS do fechamento no Athena? ---")
for label, grupo in df_cl.groupby("smartico_diverge"):
    status = "DIVERGENTES (closed no Athena, ACTIVE na Smartico)" if label else "CORRETAS (closed no Athena, DEACTIVATED na Smartico)"
    t = len(grupo)
    if t == 0:
        continue
    depois = grupo["smartico_atualizado_depois"].sum()
    print(f"\n{status} ({t:,} contas):")
    print(f"  Smartico atualizou DEPOIS do fechamento no Athena: {depois:,} ({depois/t*100:.1f}%)")
    print(f"  Smartico atualizou ANTES do fechamento no Athena:  {t - depois:,} ({(t - depois)/t*100:.1f}%)")
    print(f"  Diferenca media (dias): {grupo['dias_diff'].mean():.1f}")
    print(f"  Diferenca mediana (dias): {grupo['dias_diff'].median():.1f}")

# 9b. Divergencia por tipo de mudanca (old_category)
df_cl["ja_era_closed"] = df_cl["c_old_category"].str.strip().str.upper() == "CLOSED"

print("\n--- CAUSA RAIZ: Divergencia por tipo de mudanca ---")
test_tipo = df_cl.groupby(["ja_era_closed", "c_old_category"]).agg(
    total=("smartico_diverge", "count"),
    divergentes=("smartico_diverge", "sum")
).reset_index()
test_tipo["pct_diverg"] = (test_tipo["divergentes"] / test_tipo["total"] * 100).round(1)
print(test_tipo.to_string(index=False))

# 9c. Divergencia por semana — mudancas REAIS (play_user/real_user -> closed)
df_real_changes = df_cl[~df_cl["ja_era_closed"]].copy()

print("\n--- CAUSA RAIZ: Divergencia por semana (mudancas REAIS para closed) ---")
test_semana_real = df_real_changes.groupby("semana").agg(
    total=("smartico_diverge", "count"),
    divergentes=("smartico_diverge", "sum")
).reset_index()
test_semana_real["pct_diverg"] = (test_semana_real["divergentes"] / test_semana_real["total"] * 100).round(1)
print(test_semana_real.to_string(index=False))

# 9d. Divergencia por semana — re-processamentos (closed -> closed)
df_reprocess = df_cl[df_cl["ja_era_closed"]].copy()

print("\n--- CAUSA RAIZ: Divergencia por semana (re-processamentos closed->closed) ---")
test_semana_reprocess = df_reprocess.groupby("semana").agg(
    total=("smartico_diverge", "count"),
    divergentes=("smartico_diverge", "sum")
).reset_index()
test_semana_reprocess["pct_diverg"] = (test_semana_reprocess["divergentes"] / test_semana_reprocess["total"] * 100).round(1)
print(test_semana_reprocess.to_string(index=False))

# ---------------------------------------------------------------------------
# 10. EXPORTA ARQUIVOS
# ---------------------------------------------------------------------------
log.info("Exportando arquivos...")

# 10a. Divergencias completas
df_diverg = df[df["diverge_ext_status"] | df["diverge_acc_status"]].copy()
cols_export = [
    "c_external_id", "c_ecr_id", "c_category", "c_old_category",
    "c_ecr_status", "c_category_updated_time", "c_category_change_source",
    "c_rg_cool_off", "c_rg_self_exclusion", "c_rg_closed",
    "core_account_status", "core_external_account_status",
    "athena_cat", "smartico_ext_status", "smartico_acc_status",
    "diverge_ext_status", "diverge_acc_status", "semana",
    "smartico_updated_at", "dias_diff"
]
df_diverg[cols_export].to_csv(
    f"{output_path}divergencias_status_{ts}.csv",
    index=False, encoding="utf-8-sig"
)

# 10b. Tabela cruzada
cross_full = (
    df.groupby(["athena_cat", "smartico_ext_status", "smartico_acc_status"])
    .size()
    .reset_index(name="qty")
    .sort_values("qty", ascending=False)
)
cross_full.to_csv(
    f"{output_path}cross_status_completo_{ts}.csv",
    index=False, encoding="utf-8-sig"
)

# 10c. Analise temporal
sync_ext_semana.to_csv(f"{output_path}sync_ext_status_por_semana_{ts}.csv", index=False, encoding="utf-8-sig")
sync_acc_semana.to_csv(f"{output_path}sync_acc_status_por_semana_{ts}.csv", index=False, encoding="utf-8-sig")

# 10d. Detalhamento closed
detail_closed.to_csv(f"{output_path}detalhe_contas_closed_{ts}.csv", index=False, encoding="utf-8-sig")

# 10e. Causa raiz
test_tipo.to_csv(f"{output_path}causa_raiz_por_tipo_{ts}.csv", index=False, encoding="utf-8-sig")
test_semana_real.to_csv(f"{output_path}causa_raiz_mudancas_reais_semana_{ts}.csv", index=False, encoding="utf-8-sig")
test_semana_reprocess.to_csv(f"{output_path}causa_raiz_reprocessamento_semana_{ts}.csv", index=False, encoding="utf-8-sig")

# 10f. Dataset completo
cols_full = [
    "c_external_id", "c_ecr_id", "c_category", "c_old_category",
    "c_ecr_status", "c_category_updated_time", "c_category_change_source",
    "c_rg_cool_off", "c_rg_self_exclusion", "c_rg_closed",
    "core_account_status", "core_external_account_status",
    "athena_cat", "smartico_ext_status", "smartico_acc_status",
    "diverge_ext_status", "diverge_acc_status", "semana",
    "smartico_updated_at", "dias_diff"
]
df[cols_full].to_csv(
    f"{output_path}dataset_completo_FINAL_{ts}.csv",
    index=False, encoding="utf-8-sig"
)

# ---------------------------------------------------------------------------
# 11. ARQUIVO DE ACAO CRM — divergentes para correcao manual na Smartico
# ---------------------------------------------------------------------------
log.info("=== GERANDO ARQUIVO DE ACAO CRM ===")

# Selecionar contas onde o status na Smartico NAO reflete a verdade do Athena
df_acao = df[df["diverge_acc_status"]].copy()

# Mapear status correto que o CRM deveria ter
df_acao["status_correto_smartico"] = df_acao["athena_cat"].map(MAPA_CATEGORY_TO_ACC_STATUS)

# Informacao complementar de RG (jogo responsavel)
df_acao["tipo_restricao"] = "N/A"
df_acao.loc[df_acao["athena_cat"] == "RG_CLOSED", "tipo_restricao"] = "Autoexclusao Indefinida"
df_acao.loc[df_acao["athena_cat"] == "RG_COOL_OFF", "tipo_restricao"] = "Cool-off / Autoexclusao Temporaria"
df_acao.loc[
    (df_acao["athena_cat"] == "CLOSED") &
    (df_acao["c_category_change_source"].str.contains("SIGAP", case=False, na=False)),
    "tipo_restricao"
] = "Fechamento regulatorio (SIGAP)"
df_acao.loc[
    (df_acao["athena_cat"] == "CLOSED") &
    (~df_acao["c_category_change_source"].str.contains("SIGAP", case=False, na=False)),
    "tipo_restricao"
] = "Conta fechada"

cols_crm = [
    "c_external_id",           # ext_id para o CRM identificar o jogador
    "c_category",              # status real no PGS (base verdade)
    "smartico_acc_status",     # status ATUAL errado na Smartico
    "status_correto_smartico", # status que DEVERIA ter na Smartico
    "tipo_restricao",          # contexto para o time de CRM
    "c_category_updated_time", # quando mudou no PGS
    "c_old_category",          # de qual status veio
    "c_category_change_source" # fonte da mudanca
]

df_crm = df_acao[cols_crm].copy()
df_crm.columns = [
    "ext_id", "status_pgs", "status_smartico_atual",
    "status_smartico_correto", "tipo_restricao",
    "data_mudanca_pgs", "status_anterior", "fonte_mudanca"
]

# Garantir ext_id como string limpa
df_crm["ext_id"] = df_crm["ext_id"].astype(str).str.replace(r"\.0$", "", regex=True)

df_crm.to_csv(
    f"{output_path}acao_crm_correcao_status_FINAL_{ts}.csv",
    index=False, encoding="utf-8-sig", sep=";"
)

# Resumo por tipo de correcao
print("\n" + "=" * 70)
print("ARQUIVO DE ACAO CRM — RESUMO")
print("=" * 70)
print(f"Total de contas para correcao: {len(df_crm):,}")
print(f"\nPor tipo de restricao:")
print(df_crm["tipo_restricao"].value_counts().to_string())
print(f"\nPor correcao necessaria:")
correcao = df_crm.groupby(["status_smartico_atual", "status_smartico_correto"]).size().reset_index(name="qty")
print(correcao.sort_values("qty", ascending=False).to_string(index=False))

log.info(f"Arquivos exportados para {output_path}")
print(f"\n{'='*70}")
print(f"ARQUIVOS GERADOS:")
print(f"{'='*70}")
print(f"  DIAGNOSTICO:")
print(f"  - divergencias_status_{ts}.csv")
print(f"  - cross_status_completo_{ts}.csv")
print(f"  - sync_ext_status_por_semana_{ts}.csv")
print(f"  - sync_acc_status_por_semana_{ts}.csv")
print(f"  - detalhe_contas_closed_{ts}.csv")
print(f"  - causa_raiz_por_tipo_{ts}.csv")
print(f"  - causa_raiz_mudancas_reais_semana_{ts}.csv")
print(f"  - causa_raiz_reprocessamento_semana_{ts}.csv")
print(f"  - dataset_completo_FINAL_{ts}.csv")
print(f"")
print(f"  ACAO CRM:")
print(f"  - acao_crm_correcao_status_FINAL_{ts}.csv  <-- ENTREGAR AO CRM")
print(f"")
print("Validacao v2 concluida!")
