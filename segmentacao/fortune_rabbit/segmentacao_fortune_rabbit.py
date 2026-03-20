"""
Segmentação Fortune Rabbit — Demanda Castrin / James
=====================================================
Promoção: GIRE_GANHE_RABBIT_090326
Jogo: Fortune Rabbit (PG Soft) — game_id 8842 no Redshift
Período: 10/03/2026 11:00 BRT → 10/03/2026 23:59 BRT
         (UTC: 2026-03-10 14:00:00 → 2026-03-11 02:59:59)

Regras:
  - Usuários com opt-in (mark GIRE_GANHE_RABBIT_090326 no Smartico)
  - Net Bet = Total Bets − Rollbacks
  - Quem tiver QUALQUER rollback é DESCLASSIFICADO
  - Faixa 1: R$30 a R$99,99
  - Faixa 2: R$100 a R$299,99
  - Faixa 3: R$300 a R$599,99
  - Faixa 4: R$600 ou mais
  - Cada usuário fica na faixa mais alta que atingiu (sem duplicidade)

Fluxo:
  1. Puxa IDs do BigQuery (j_user.core_tags com mark)
  2. Chunka IDs em blocos de 5.000
  3. Consulta Redshift (fund.tbl_real_fund_txn + ecr.tbl_ecr)
  4. Desclassifica quem teve rollback
  5. Aplica faixas de segmentação
  6. Gera CSV final com left join (todos os marcados)
"""

import sys
import os
import logging
import locale
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
MULTIBET_ROOT = os.path.dirname(PROJECT_ROOT)
sys.path.insert(0, MULTIBET_ROOT)

from db.redshift import query_redshift
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
# Parâmetros da demanda
# ---------------------------------------------------------------------------
MARK_TAG      = "GIRE_GANHE_RABBIT_090326"
GAME_ID       = "8842"           # Fortune Rabbit (PG Soft) no catálogo Redshift
TXN_BET       = 27               # CASINO_BUYIN
TXN_ROLLBACK  = 72               # CASINO_BUYIN_CANCEL
START_UTC     = "2026-03-10 14:00:00"   # 10/03 11h BRT → UTC
END_UTC       = "2026-03-11 02:59:59"   # 10/03 23:59 BRT → UTC
CHUNK_SIZE    = 5000

# Faixas (limites em BRL)
FAIXAS = [
    ("Faixa 4", 600.00, float("inf")),
    ("Faixa 3", 300.00, 599.99),
    ("Faixa 2", 100.00, 299.99),
    ("Faixa 1",  30.00,  99.99),
]

OUTPUT_CSV = os.path.join(PROJECT_ROOT, "segmentacao_fortune_rabbit.csv")

# ---------------------------------------------------------------------------
# 1. Buscar usuários marcados no BigQuery
# ---------------------------------------------------------------------------
def fetch_marked_users() -> pd.DataFrame:
    """Retorna DataFrame com user_id e user_ext_id dos marcados."""
    log.info(f"Buscando usuarios com mark '{MARK_TAG}' no BigQuery...")
    sql = f"""
    SELECT
        user_id   AS smartico_user_id,
        user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE '{MARK_TAG}' IN UNNEST(core_tags)
    """
    df = query_bigquery(sql)
    log.info(f"  → {len(df)} usuarios marcados encontrados")
    return df


# ---------------------------------------------------------------------------
# 2. Consultar Redshift em chunks
# ---------------------------------------------------------------------------
def build_sql(chunk_ids: list) -> str:
    """Monta SQL para um bloco de IDs."""
    ids_str = ", ".join(str(i) for i in chunk_ids)
    return f"""
    WITH params AS (
        SELECT '{START_UTC}'::timestamp AS start_ts,
               '{END_UTC}'::timestamp   AS end_ts
    )
    SELECT
        e.c_external_id                AS user_ext_id,
        SUM(CASE WHEN f.c_txn_type = {TXN_BET}
                 THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_bet_cents,
        SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK}
                 THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_rollback_cents,
        SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK}
                 THEN 1 ELSE 0 END)                     AS qtd_rollbacks
    FROM fund.tbl_real_fund_txn f
    INNER JOIN ecr.tbl_ecr e
        ON e.c_ecr_id = f.c_ecr_id
    CROSS JOIN params p
    WHERE f.c_start_time BETWEEN p.start_ts AND p.end_ts
      AND f.c_game_id   = '{GAME_ID}'
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type  IN ({TXN_BET}, {TXN_ROLLBACK})
      AND e.c_external_id IN ({ids_str})
    GROUP BY 1
    """


def fetch_redshift_data(ext_ids: list) -> pd.DataFrame:
    """Consulta Redshift em chunks e retorna DataFrame consolidado."""
    chunks = [ext_ids[i:i + CHUNK_SIZE] for i in range(0, len(ext_ids), CHUNK_SIZE)]
    log.info(f"Consultando Redshift: {len(ext_ids)} IDs em {len(chunks)} chunk(s)...")

    frames = []
    for idx, chunk in enumerate(chunks, 1):
        log.info(f"  Chunk {idx}/{len(chunks)} ({len(chunk)} IDs)...")
        sql = build_sql(chunk)
        df = query_redshift(sql)
        if not df.empty:
            frames.append(df)
        log.info(f"    → {len(df)} jogadores com transações")

    if frames:
        result = pd.concat(frames, ignore_index=True)
        log.info(f"  Total Redshift: {len(result)} jogadores com transações")
        return result
    else:
        log.warning("  Nenhuma transação encontrada no Redshift!")
        return pd.DataFrame(columns=["user_ext_id", "total_bet_cents",
                                      "total_rollback_cents", "qtd_rollbacks"])


# ---------------------------------------------------------------------------
# 3. Classificar faixas
# ---------------------------------------------------------------------------
def classificar_faixa(net_bet_brl: float, tem_rollback: bool) -> str:
    """
    Retorna a faixa de segmentação.
    Se teve rollback → desclassificado.
    Se net_bet < R$30 → Abaixo do Mínimo.
    """
    if tem_rollback:
        return "Desclassificado (rollback)"
    for nome, low, high in FAIXAS:
        if low <= net_bet_brl <= high:
            return nome
    if net_bet_brl < 30.0:
        return "Abaixo do Mínimo"
    return "Sem classificação"


# ---------------------------------------------------------------------------
# 4. Pipeline principal
# ---------------------------------------------------------------------------
def main():
    # --- 1. BigQuery: buscar marcados ---
    df_marked = fetch_marked_users()

    # Converter user_ext_id para inteiro (remover decimais)
    df_marked["user_ext_id"] = (
        pd.to_numeric(df_marked["user_ext_id"], errors="coerce")
          .astype("Int64")
    )
    df_marked = df_marked.dropna(subset=["user_ext_id"])
    ext_ids = df_marked["user_ext_id"].tolist()
    log.info(f"IDs válidos para consulta: {len(ext_ids)}")

    # --- 2. Redshift: buscar transações ---
    df_txn = fetch_redshift_data(ext_ids)

    if not df_txn.empty:
        # Converter tipos
        df_txn["user_ext_id"]         = df_txn["user_ext_id"].astype("Int64")
        df_txn["total_bet_cents"]     = pd.to_numeric(df_txn["total_bet_cents"], errors="coerce").fillna(0)
        df_txn["total_rollback_cents"]= pd.to_numeric(df_txn["total_rollback_cents"], errors="coerce").fillna(0)
        df_txn["qtd_rollbacks"]       = pd.to_numeric(df_txn["qtd_rollbacks"], errors="coerce").fillna(0).astype(int)

        # Calcular valores em BRL (centavos → reais)
        df_txn["total_bet_brl"]      = df_txn["total_bet_cents"] / 100
        df_txn["total_rollback_brl"] = df_txn["total_rollback_cents"] / 100
        df_txn["net_bet_brl"]        = df_txn["total_bet_brl"] - df_txn["total_rollback_brl"]

        # Flag de rollback
        df_txn["tem_rollback"] = df_txn["qtd_rollbacks"] > 0

        # Classificar faixa
        df_txn["faixa_segmentacao"] = df_txn.apply(
            lambda r: classificar_faixa(r["net_bet_brl"], r["tem_rollback"]), axis=1
        )

    # --- 3. Merge: left join para manter todos os marcados ---
    df_final = df_marked.merge(df_txn, on="user_ext_id", how="left")

    # Preencher quem não jogou
    df_final["total_bet_brl"]      = df_final["total_bet_brl"].fillna(0)
    df_final["total_rollback_brl"] = df_final["total_rollback_brl"].fillna(0)
    df_final["net_bet_brl"]        = df_final["net_bet_brl"].fillna(0)
    df_final["qtd_rollbacks"]      = df_final["qtd_rollbacks"].fillna(0).astype(int)
    df_final["tem_rollback"]       = df_final["tem_rollback"].fillna(False)
    df_final["faixa_segmentacao"]  = df_final["faixa_segmentacao"].fillna("Não jogou")

    # Formatar valores em BRL (pt-BR)
    def fmt_brl(v):
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    df_final["net_bet_brl_fmt"]        = df_final["net_bet_brl"].apply(fmt_brl)
    df_final["total_bet_brl_fmt"]      = df_final["total_bet_brl"].apply(fmt_brl)
    df_final["total_rollback_brl_fmt"] = df_final["total_rollback_brl"].apply(fmt_brl)

    # Selecionar e ordenar colunas
    cols_out = [
        "smartico_user_id", "user_ext_id",
        "total_bet_brl", "total_rollback_brl", "net_bet_brl",
        "qtd_rollbacks", "tem_rollback",
        "net_bet_brl_fmt", "total_bet_brl_fmt", "total_rollback_brl_fmt",
        "faixa_segmentacao",
    ]
    df_final = df_final[cols_out].sort_values("net_bet_brl", ascending=False)

    # --- 4. Salvar CSV ---
    df_final.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"CSV salvo: {OUTPUT_CSV}")
    log.info(f"  → {len(df_final)} linhas, {len(df_final.columns)} colunas")

    # --- 5. Resumo ---
    total_marcados  = len(df_final)
    jogaram         = df_final[df_final["faixa_segmentacao"] != "Não jogou"]
    total_jogaram   = len(jogaram)
    desclassificados = len(df_final[df_final["faixa_segmentacao"] == "Desclassificado (rollback)"])
    elegíveis       = jogaram[~jogaram["faixa_segmentacao"].isin(["Não jogou", "Desclassificado (rollback)", "Abaixo do Mínimo"])]

    log.info("")
    log.info("=" * 60)
    log.info("RESUMO DA SEGMENTAÇÃO — FORTUNE RABBIT")
    log.info("=" * 60)
    log.info(f"Usuários marcados (opt-in):    {total_marcados}")
    log.info(f"Jogaram no período:            {total_jogaram}")
    log.info(f"Desclassificados (rollback):   {desclassificados}")
    log.info(f"Total apostado (bruto):        {fmt_brl(df_final['total_bet_brl'].sum())}")
    log.info(f"Total rollbacks:               {fmt_brl(df_final['total_rollback_brl'].sum())}")
    log.info(f"Net Bet total:                 {fmt_brl(df_final['net_bet_brl'].sum())}")
    log.info("")

    # Distribuição por faixa
    log.info("Distribuição por faixa:")
    faixa_counts = df_final["faixa_segmentacao"].value_counts()
    for faixa in ["Faixa 4", "Faixa 3", "Faixa 2", "Faixa 1", "Abaixo do Mínimo",
                  "Desclassificado (rollback)", "Não jogou"]:
        if faixa in faixa_counts.index:
            n = faixa_counts[faixa]
            vol = df_final[df_final["faixa_segmentacao"] == faixa]["net_bet_brl"].sum()
            log.info(f"  {faixa:35s}  {n:5d} jogadores  {fmt_brl(vol):>15s}")

    log.info("")
    log.info("Top 10 jogadores (net bet):")
    top10 = df_final.head(10)
    for _, row in top10.iterrows():
        log.info(f"  ext_id={row['user_ext_id']}  net_bet={row['net_bet_brl_fmt']}  faixa={row['faixa_segmentacao']}")

    return df_final


if __name__ == "__main__":
    main()
