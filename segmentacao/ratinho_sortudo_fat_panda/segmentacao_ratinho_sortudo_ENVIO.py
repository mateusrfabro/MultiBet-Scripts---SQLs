"""
Segmentação Gire & Ganhe — Ratinho Sortudo | 16/03/2026
========================================================
Promoção  : GIRE_GANHE_RATINHOSORTUDO_160326
Jogo      : Ratinho Sortudo — Pragmatic Play (game_id: vs10forwild)
Período   : 16/03/2026 17h50 BRT → 23h59 BRT
            (UTC: 2026-03-16 20:50:00 → 2026-03-17 02:59:59)

Regras de negócio:
  - Usuários com opt-in (mark GIRE_GANHE_RATINHOSORTUDO_160326 no Smartico)
  - Rollback (txn_type=72) desclassifica o usuário
  - Faixas exclusivas — usuário fica na mais alta atingida:
      Faixa 1: R$50,00 – R$99,99
      Faixa 2: R$100,00 – R$399,99
      Faixa 3: R$400,00 – R$699,99
      Faixa 4: ≥ R$700,00

Fontes de dados:
  - BigQuery (Smartico): opt-in via j_user.core_tags
  - Athena (Iceberg Data Lake): transações via fund_ec2.tbl_real_fund_txn

Saída: CSV com todos os opt-in segmentados por faixa de turnover.
"""

import sys
import os
import logging
import zipfile
import pandas as pd

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT  = os.path.dirname(os.path.abspath(__file__))
MULTIBET_ROOT = os.path.dirname(PROJECT_ROOT)
sys.path.insert(0, MULTIBET_ROOT)

from db.athena import query_athena
from db.bigquery import query_bigquery

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Parâmetros da promoção
# ---------------------------------------------------------------------------
MARK_TAG  = "GIRE_GANHE_RATINHOSORTUDO_160326"
GAME_IDS  = ["vs10forwild"]

# Período em UTC (BRT = UTC-3)
START_UTC = "2026-03-16 20:50:00"
END_UTC   = "2026-03-17 02:59:59"

TXN_BET      = 27   # CASINO_BUYIN (aposta)
TXN_ROLLBACK = 72   # CASINO_BUYIN_CANCEL (rollback)

CHUNK_SIZE = 5_000

# Faixas em centavos (avaliadas da maior para a menor)
FAIXAS = [
    ("Faixa 4", 70000, float("inf")),   # ≥ R$700
    ("Faixa 3", 40000, 69999),           # R$400 – R$699,99
    ("Faixa 2", 10000, 39999),           # R$100 – R$399,99
    ("Faixa 1",  5000,  9999),           # R$50  – R$99,99
]

OUTPUT_CSV = os.path.join(PROJECT_ROOT, "segmentacao_ratinho_sortudo_fatpanda_160326_FINAL.csv")
OUTPUT_ZIP = os.path.join(PROJECT_ROOT, "segmentacao_ratinho_sortudo_fatpanda_160326_FINAL.zip")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fmt_brl(v: float) -> str:
    """Formata valor em pt-BR: R$ 1.234,56"""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ---------------------------------------------------------------------------
# Etapa 1: BigQuery — buscar usuários com opt-in
# ---------------------------------------------------------------------------
def fetch_marked_users() -> pd.DataFrame:
    """Retorna usuários marcados com a tag da promoção no Smartico."""
    log.info(f"Etapa 1: Buscando opt-in '{MARK_TAG}' no BigQuery...")

    sql = f"""
    SELECT
        user_id     AS smartico_user_id,
        user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE '{MARK_TAG}' IN UNNEST(core_tags)
    """

    df = query_bigquery(sql)
    log.info(f"  → {len(df):,} usuários com opt-in")
    return df


# ---------------------------------------------------------------------------
# Etapa 2: Athena — consultar transações em chunks
# ---------------------------------------------------------------------------
def build_sql(chunk_ids: list) -> str:
    """
    SQL Athena (Presto/Trino) — Early Filter + Late Join:
      1. CTE 'participantes': resolve external_id → ecr_id (INNER JOIN, filtra cedo)
      2. CTE 'transacoes': agrega apostas e rollbacks no período/jogo
      3. SELECT final: LEFT JOIN para preservar IDs sem atividade
    """
    ids_str   = ", ".join(str(i) for i in chunk_ids)
    games_str = ", ".join(f"'{g}'" for g in GAME_IDS)

    return f"""
    WITH participantes AS (
        SELECT DISTINCT
            e.c_ecr_id,
            e.c_external_id
        FROM ecr_ec2.tbl_ecr e
        WHERE e.c_external_id IN ({ids_str})
    ),
    transacoes AS (
        SELECT
            f.c_ecr_id,
            SUM(CASE WHEN f.c_txn_type = {TXN_BET}
                     THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_bet_cents,
            COUNT_IF(f.c_txn_type = {TXN_ROLLBACK})         AS qtd_rollbacks,
            COUNT_IF(f.c_txn_type = {TXN_BET})              AS qtd_apostas
        FROM fund_ec2.tbl_real_fund_txn f
        INNER JOIN participantes p ON f.c_ecr_id = p.c_ecr_id
        WHERE f.c_start_time BETWEEN TIMESTAMP '{START_UTC}' AND TIMESTAMP '{END_UTC}'
          AND f.c_game_id IN ({games_str})
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_txn_type IN ({TXN_BET}, {TXN_ROLLBACK})
        GROUP BY 1
    )
    SELECT
        p.c_external_id               AS user_ext_id,
        COALESCE(t.total_bet_cents, 0) AS total_bet_cents,
        COALESCE(t.qtd_rollbacks,   0) AS qtd_rollbacks,
        COALESCE(t.qtd_apostas,     0) AS qtd_apostas
    FROM participantes p
    LEFT JOIN transacoes t ON p.c_ecr_id = t.c_ecr_id
    """


def fetch_athena_data(ext_ids: list) -> pd.DataFrame:
    """Consulta transações no Athena em chunks para evitar queries muito grandes."""
    chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]
    log.info(f"Etapa 2: Consultando Athena — {len(ext_ids):,} IDs em {len(chunks)} chunk(s)...")

    frames = []
    for idx, chunk in enumerate(chunks, 1):
        log.info(f"  Chunk {idx}/{len(chunks)} ({len(chunk):,} IDs)...")
        sql = build_sql(chunk)
        df  = query_athena(sql, database="fund_ec2")
        if not df.empty:
            frames.append(df)
        log.info(f"    → {len(df):,} registros")

    if frames:
        result = pd.concat(frames, ignore_index=True)
        log.info(f"  Total consolidado: {len(result):,} linhas")
        return result

    log.warning("  Nenhuma transação encontrada no Athena.")
    return pd.DataFrame(columns=["user_ext_id", "total_bet_cents", "qtd_rollbacks", "qtd_apostas"])


# ---------------------------------------------------------------------------
# Etapa 3: Classificação de faixas
# ---------------------------------------------------------------------------
def classificar_faixa(total_bet_cents: float, tem_rollback: bool) -> str:
    """
    Rollback desclassifica. Caso contrário, retorna a faixa mais alta atingida.
    """
    if tem_rollback:
        return "Desclassificado (rollback)"
    for nome, low, high in FAIXAS:
        if low <= total_bet_cents <= high:
            return nome
    if total_bet_cents >= 70000:
        return "Faixa 4"
    if total_bet_cents > 0:
        return "Abaixo do Mínimo"
    return "Sem Atividade"


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def main():
    log.info(f"Início — game_id: {GAME_IDS} (Ratinho Sortudo — Pragmatic Play)")

    # Etapa 1: BigQuery → opt-in
    df_marked = fetch_marked_users()
    df_marked["user_ext_id"] = (
        pd.to_numeric(df_marked["user_ext_id"], errors="coerce").astype("Int64")
    )
    df_marked = df_marked.dropna(subset=["user_ext_id"])
    ext_ids = df_marked["user_ext_id"].tolist()
    log.info(f"  IDs válidos: {len(ext_ids):,}")

    if not ext_ids:
        log.error("Nenhum usuário encontrado com a mark. Abortando.")
        return

    # Etapa 2: Athena → transações
    df_txn = fetch_athena_data(ext_ids)

    if not df_txn.empty:
        df_txn["user_ext_id"]     = df_txn["user_ext_id"].astype("Int64")
        df_txn["total_bet_cents"] = pd.to_numeric(df_txn["total_bet_cents"], errors="coerce").fillna(0)
        df_txn["qtd_rollbacks"]   = pd.to_numeric(df_txn["qtd_rollbacks"],   errors="coerce").fillna(0).astype(int)
        df_txn["qtd_apostas"]     = pd.to_numeric(df_txn["qtd_apostas"],     errors="coerce").fillna(0).astype(int)
        df_txn["total_bet_brl"]   = df_txn["total_bet_cents"] / 100.0
        df_txn["tem_rollback"]    = df_txn["qtd_rollbacks"] > 0
        df_txn["faixa_segmentacao"] = df_txn.apply(
            lambda r: classificar_faixa(r["total_bet_cents"], r["tem_rollback"]), axis=1
        )

    # Etapa 3: merge — preserva TODOS os opt-in
    df_final = df_marked.merge(df_txn, on="user_ext_id", how="left")
    df_final["total_bet_brl"]     = df_final["total_bet_brl"].fillna(0.0)
    df_final["qtd_rollbacks"]     = df_final["qtd_rollbacks"].fillna(0).astype(int)
    df_final["qtd_apostas"]       = df_final["qtd_apostas"].fillna(0).astype(int)
    df_final["tem_rollback"]      = df_final["tem_rollback"].fillna(False)
    df_final["faixa_segmentacao"] = df_final["faixa_segmentacao"].fillna("Sem Atividade")
    df_final["total_bet_cents"]   = df_final["total_bet_cents"].fillna(0)
    df_final["total_bet_brl_fmt"] = df_final["total_bet_brl"].apply(fmt_brl)

    # Colunas de saída
    cols_out = [
        "smartico_user_id", "user_ext_id",
        "qtd_apostas", "qtd_rollbacks", "tem_rollback",
        "total_bet_brl", "total_bet_brl_fmt",
        "faixa_segmentacao",
    ]
    df_final = df_final[cols_out].sort_values("total_bet_brl", ascending=False)

    # Exportar CSV + ZIP
    df_final.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")
    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(OUTPUT_CSV, os.path.basename(OUTPUT_CSV))

    # Resumo
    total     = len(df_final)
    jogaram   = len(df_final[df_final["faixa_segmentacao"] != "Sem Atividade"])
    apostado  = df_final["total_bet_brl"].sum()

    log.info(f"Concluído: {total:,} opt-in | {jogaram:,} jogaram | {fmt_brl(apostado)} apostado")
    log.info(f"CSV: {OUTPUT_CSV}")
    log.info(f"ZIP: {OUTPUT_ZIP}")

    return df_final


if __name__ == "__main__":
    main()
