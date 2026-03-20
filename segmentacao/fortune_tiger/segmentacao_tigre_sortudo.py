"""
Segmentação Tigre Sortudo — Smartico x Redshift
================================================
Fluxo:
  1. Lê o CSV do Smartico (user_ext_id, separador ;)
  2. Divide os IDs em blocos de 5.000 (limite seguro para cláusula IN no Redshift)
  3. Para cada bloco, consulta bets e rollbacks do Tigre Sortudo no Redshift
  4. Calcula Net Bet em BRL e aplica faixas de segmentação
  5. Faz left join com o CSV original e gera segmentacao_smartico_tigre_sortudo.csv

Período de análise: 2026-03-07 19:00:00 UTC → 2026-03-09 02:59:59 UTC
"""

import sys
import os
import logging
import pandas as pd

# ── Garante que o módulo db/ seja encontrado ──────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# db/ está na raiz do projeto MultiBet, um nível acima
MULTIBET_ROOT = os.path.dirname(PROJECT_ROOT)
sys.path.insert(0, MULTIBET_ROOT)

from db.redshift import query_redshift

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configurações ─────────────────────────────────────────────────────────────
CSV_PATH   = r"C:\Users\NITRO\Downloads\segment-export-smartic (1).csv"
OUTPUT_CSV = os.path.join(PROJECT_ROOT, "segmentacao_smartico_tigre_sortudo.csv")
CHUNK_SIZE = 5_000
START_UTC  = "2026-03-07 19:00:00"
END_UTC    = "2026-03-09 02:59:59"

# Tigre Sortudo (Pragmatic Play) → c_game_id = 'vs5luckytig'
# Confirmado via bireports.tbl_vendor_games_mapping_data (vendor: pragmaticplay)
# Existe também 'vs5luckytig1k' (Tigre Sortudo 1000) — NÃO incluído nesta análise
GAME_ID    = "vs5luckytig"

# Tipos de transação na fund.tbl_real_fund_txn:
#   27 = CASINO_BUYIN (Aposta)   | 72 = CASINO_BUYIN_CANCEL (Rollback de aposta)
TXN_BET      = 27
TXN_ROLLBACK = 72


# ── Classificação de faixas ───────────────────────────────────────────────────
def classify_tier(val: float) -> str:
    """Retorna a faixa de segmentação com base no Net Bet em BRL."""
    if val >= 1_000:
        return "Faixa 4: Apostas de R$1.000,00 ou mais"
    if val >= 500:
        return "Faixa 3: Apostas entre R$500 a R$999,99"
    if val >= 200:
        return "Faixa 2: Apostas entre R$200 a R$499,99"
    if val >= 50:
        return "Faixa 1: Apostas entre R$50 a R$199,99"
    return "Abaixo do Mínimo"


# ── SQL Template ──────────────────────────────────────────────────────────────
def build_sql(chunk_ids: list) -> str:
    """
    Monta a query para um bloco de IDs.

    Mapeamento de IDs:
      Smartico user_ext_id → ecr.tbl_ecr.c_external_id
                           → ecr.tbl_ecr.c_ecr_id (ID interno)
                           → fund.tbl_real_fund_txn.c_ecr_id

    Net Bet = Bet (tipo 27) - Rollback (tipo 72)
    Valores em centavos BRL (c_amount_in_ecr_ccy).
    c_txn_status = 'SUCCESS' | c_is_cancelled disponível mas não usado aqui.
    """
    ids_str = ", ".join(str(i) for i in chunk_ids)
    return f"""
WITH params AS (
    SELECT
        '{START_UTC}'::timestamp AS start_ts,
        '{END_UTC}'::timestamp   AS end_ts
)
SELECT
    e.c_external_id AS user_ext_id,
    SUM(CASE WHEN f.c_txn_type = {TXN_BET}      THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_bet_cents,
    SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK}  THEN f.c_amount_in_ecr_ccy ELSE 0 END) AS total_rollback_cents
FROM fund.tbl_real_fund_txn f
INNER JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
CROSS JOIN params p
WHERE f.c_start_time BETWEEN p.start_ts AND p.end_ts
  AND f.c_game_id    = '{GAME_ID}'
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type   IN ({TXN_BET}, {TXN_ROLLBACK})
  AND e.c_external_id IN ({ids_str})
GROUP BY 1
"""


# ── Principal ─────────────────────────────────────────────────────────────────
def main():
    # 1. Carrega CSV do Smartico
    log.info(f"Lendo CSV: {CSV_PATH}")
    df_input = pd.read_csv(CSV_PATH, sep=";", dtype={"user_ext_id": str, "smartico_user_id": str})
    log.info(f"  → {len(df_input):,} registros | colunas: {list(df_input.columns)}")

    ids = df_input["user_ext_id"].dropna().unique().tolist()
    log.info(f"  → {len(ids):,} IDs únicos para consultar")

    # 2. Divide em blocos de 5.000
    chunks = [ids[i : i + CHUNK_SIZE] for i in range(0, len(ids), CHUNK_SIZE)]
    log.info(f"  → {len(chunks)} bloco(s) de até {CHUNK_SIZE:,} IDs")

    # 3. Consulta Redshift bloco a bloco
    frames = []
    for idx, chunk in enumerate(chunks, 1):
        log.info(f"Consultando bloco {idx}/{len(chunks)} ({len(chunk):,} IDs)...")
        try:
            sql = build_sql(chunk)
            df_chunk = query_redshift(sql)
            log.info(f"  → {len(df_chunk):,} usuário(s) com bets retornados")
            frames.append(df_chunk)
        except Exception as e:
            log.error(f"  ✗ Falha no bloco {idx}: {e}")
            raise

    # 4. Consolida resultados
    if frames:
        df_bets = pd.concat(frames, ignore_index=True)
        df_bets["user_ext_id"] = df_bets["user_ext_id"].astype(str)
        log.info(f"Total de usuários com bets encontrados: {len(df_bets):,}")
    else:
        log.warning("Nenhum resultado retornado do Redshift. CSV final terá apenas dados do Smartico.")
        df_bets = pd.DataFrame(columns=["user_ext_id", "total_bet_cents", "total_rollback_cents"])

    # 5. Calcula Net Bet em BRL (centavos → reais)
    df_bets["total_bet_cents"]      = pd.to_numeric(df_bets["total_bet_cents"],      errors="coerce").fillna(0)
    df_bets["total_rollback_cents"] = pd.to_numeric(df_bets["total_rollback_cents"], errors="coerce").fillna(0)
    df_bets["net_bet_brl"]          = (df_bets["total_bet_cents"] - df_bets["total_rollback_cents"]) / 100.0
    df_bets["total_bet_brl"]        = df_bets["total_bet_cents"]        / 100.0
    df_bets["total_rollback_brl"]   = df_bets["total_rollback_cents"]   / 100.0

    # 6. Aplica faixas de segmentação
    df_bets["faixa_segmentacao"] = df_bets["net_bet_brl"].apply(classify_tier)

    # Formatação BRL (vírgula como separador decimal — padrão brasileiro)
    for col in ["net_bet_brl", "total_bet_brl", "total_rollback_brl"]:
        df_bets[f"{col}_fmt"] = df_bets[col].apply(
            lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        )

    log.info("Distribuição por faixa:")
    for faixa, cnt in df_bets["faixa_segmentacao"].value_counts().items():
        log.info(f"  {faixa}: {cnt:,} usuário(s)")

    # 7. Merge com CSV original (left join → preserva todos os usuários do Smartico)
    df_input["user_ext_id"] = df_input["user_ext_id"].astype(str)
    df_final = df_input.merge(
        df_bets[["user_ext_id", "total_bet_brl", "total_rollback_brl", "net_bet_brl",
                 "net_bet_brl_fmt", "total_bet_brl_fmt", "total_rollback_brl_fmt",
                 "faixa_segmentacao"]],
        on="user_ext_id",
        how="left",
    )

    # Usuários sem bets → preenche com zeros e faixa mínima
    df_final["net_bet_brl"]          = df_final["net_bet_brl"].fillna(0.0)
    df_final["total_bet_brl"]        = df_final["total_bet_brl"].fillna(0.0)
    df_final["total_rollback_brl"]   = df_final["total_rollback_brl"].fillna(0.0)
    df_final["net_bet_brl_fmt"]      = df_final["net_bet_brl_fmt"].fillna("R$ 0,00")
    df_final["total_bet_brl_fmt"]    = df_final["total_bet_brl_fmt"].fillna("R$ 0,00")
    df_final["total_rollback_brl_fmt"] = df_final["total_rollback_brl_fmt"].fillna("R$ 0,00")
    df_final["faixa_segmentacao"]    = df_final["faixa_segmentacao"].fillna("Abaixo do Mínimo")

    # 8. Exporta CSV final
    df_final.to_csv(OUTPUT_CSV, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"\n✓ Arquivo gerado: {OUTPUT_CSV}")
    log.info(f"  → {len(df_final):,} linhas | {len(df_final.columns)} colunas")

    # Resumo executivo
    com_bet = (df_final["net_bet_brl"] > 0).sum()
    sem_bet = (df_final["net_bet_brl"] == 0).sum()
    log.info(f"\nResumo:")
    log.info(f"  Usuários com apostas no Tigre Sortudo: {com_bet:,}")
    log.info(f"  Usuários sem apostas (Net Bet = R$0):  {sem_bet:,}")
    log.info(f"  Net Bet total do segmento: R$ {df_final['net_bet_brl'].sum():,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))


if __name__ == "__main__":
    main()
