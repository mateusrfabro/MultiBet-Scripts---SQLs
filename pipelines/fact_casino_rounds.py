"""
Pipeline: fact_casino_rounds (ps_bi — Performance de Jogos Casino)
===================================================================
Dominio: Produto e Performance de Jogos (Prioridade 4 — tabela 18)

Grao: dt (dia) x game_id
Fonte PRIMARIA: ps_bi.fct_casino_activity_daily (pre-agregado, BRL, real/bonus split)
  - Vantagem: dados ja validados pelo dbt/Pragmatic, custo Athena muito menor
  - Baseline de validacao: fct_casino_activity.py (fund_ec2, sub-fund isolation Mauro)

KPIs por jogo por dia:
    - qty_players, total_rounds (bet_count), rounds_per_player
    - turnover (real/bonus/total), wins, ggr
    - hold_rate_pct = GGR / Turnover * 100
    - rtp_pct = Wins / Turnover * 100
    - jackpot_win, jackpot_contribution
    - free_spins_bet, free_spins_win

Fontes (Athena ps_bi):
    1. ps_bi.fct_casino_activity_daily  → Atividade casino player/game/dia (BRL)
    2. ps_bi.dim_game                    → Catalogo de jogos (nome, vendor, categoria)
    3. bireports_ec2.tbl_ecr            → Filtro test users (c_test_user = false)

Destino: Super Nova DB -> multibet.fact_casino_rounds
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fact_casino_rounds.py
"""

import sys, os, logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# --- DDL ------------------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_casino_rounds (
    dt                      DATE,
    game_id                 VARCHAR(50),
    game_name               VARCHAR(255),
    vendor_id               VARCHAR(50),
    sub_vendor_id           VARCHAR(50),
    game_category           VARCHAR(100),
    qty_players             INTEGER DEFAULT 0,
    total_rounds            INTEGER DEFAULT 0,
    rounds_per_player       NUMERIC(10,2) DEFAULT 0,
    turnover_real           NUMERIC(18,2) DEFAULT 0,
    wins_real               NUMERIC(18,2) DEFAULT 0,
    ggr_real                NUMERIC(18,2) DEFAULT 0,
    turnover_bonus          NUMERIC(18,2) DEFAULT 0,
    wins_bonus              NUMERIC(18,2) DEFAULT 0,
    ggr_bonus               NUMERIC(18,2) DEFAULT 0,
    turnover_total          NUMERIC(18,2) DEFAULT 0,
    wins_total              NUMERIC(18,2) DEFAULT 0,
    ggr_total               NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct           NUMERIC(10,4) DEFAULT 0,
    rtp_pct                 NUMERIC(10,4) DEFAULT 0,
    jackpot_win             NUMERIC(18,2) DEFAULT 0,
    jackpot_contribution    NUMERIC(18,2) DEFAULT 0,
    free_spins_bet          NUMERIC(18,2) DEFAULT 0,
    free_spins_win          NUMERIC(18,2) DEFAULT 0,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, game_id)
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_fcr_vendor ON multibet.fact_casino_rounds (vendor_id, dt);
CREATE INDEX IF NOT EXISTS idx_fcr_ggr ON multibet.fact_casino_rounds (dt, ggr_total DESC);
CREATE INDEX IF NOT EXISTS idx_fcr_category ON multibet.fact_casino_rounds (game_category, dt);
"""

# --- Query Athena (ps_bi — pre-agregado, BRL, validado) -------------------
# ps_bi.fct_casino_activity_daily: grao = player_id x game_id x activity_date
# Agregamos por game_id x activity_date (removendo dimensao player)
# Valores ja em BRL (sem divisao por 100)
# Filtro test users via bireports_ec2.tbl_ecr

QUERY_ATHENA = """
WITH valid_players AS (
    SELECT c_ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Deduplicar dim_game: 1 linha por game_id
game_cat AS (
    SELECT game_id,
           MAX(game_desc) AS game_name,
           MAX(vendor_id) AS vendor_id,
           MAX(game_category) AS game_category
    FROM ps_bi.dim_game
    GROUP BY game_id
)

SELECT
    fca.activity_date AS dt,
    fca.game_id,
    COALESCE(gc.game_name, 'Desconhecido') AS game_name,
    COALESCE(gc.vendor_id, COALESCE(MAX(fca.sub_vendor_id), 'unknown')) AS vendor_id,
    COALESCE(MAX(fca.sub_vendor_id), '') AS sub_vendor_id,
    COALESCE(gc.game_category, 'Outros') AS game_category,

    COUNT(DISTINCT fca.player_id) AS qty_players,
    CAST(SUM(fca.bet_count) AS INTEGER) AS total_rounds,
    CAST(SUM(fca.bet_count) AS DOUBLE) / NULLIF(COUNT(DISTINCT fca.player_id), 0) AS rounds_per_player,

    SUM(COALESCE(fca.real_bet_amount_local, 0)) AS turnover_real,
    SUM(COALESCE(fca.real_win_amount_local, 0)) AS wins_real,
    SUM(COALESCE(fca.real_bet_amount_local, 0)) - SUM(COALESCE(fca.real_win_amount_local, 0)) AS ggr_real,

    SUM(COALESCE(fca.bonus_bet_amount_local, 0)) AS turnover_bonus,
    SUM(COALESCE(fca.bonus_win_amount_local, 0)) AS wins_bonus,
    SUM(COALESCE(fca.bonus_bet_amount_local, 0)) - SUM(COALESCE(fca.bonus_win_amount_local, 0)) AS ggr_bonus,

    SUM(COALESCE(fca.bet_amount_local, 0)) AS turnover_total,
    SUM(COALESCE(fca.win_amount_local, 0)) AS wins_total,
    SUM(COALESCE(fca.ggr_local, 0)) AS ggr_total,

    CASE WHEN SUM(COALESCE(fca.bet_amount_local, 0)) > 0
         THEN SUM(COALESCE(fca.ggr_local, 0)) * 100.0
              / SUM(COALESCE(fca.bet_amount_local, 0))
         ELSE 0 END AS hold_rate_pct,

    CASE WHEN SUM(COALESCE(fca.bet_amount_local, 0)) > 0
         THEN SUM(COALESCE(fca.win_amount_local, 0)) * 100.0
              / SUM(COALESCE(fca.bet_amount_local, 0))
         ELSE 0 END AS rtp_pct,

    SUM(COALESCE(fca.jackpot_win_amount_local, 0)) AS jackpot_win,
    SUM(COALESCE(fca.jackpot_contribution_local, 0)) AS jackpot_contribution,

    SUM(COALESCE(fca.free_spins_bet_amount_local, 0)) AS free_spins_bet,
    SUM(COALESCE(fca.free_spins_win_amount_local, 0)) AS free_spins_win

FROM ps_bi.fct_casino_activity_daily fca
LEFT JOIN game_cat gc ON fca.game_id = gc.game_id
JOIN valid_players vp ON fca.player_id = vp.c_ecr_id
WHERE fca.activity_date >= DATE '2025-10-01'
  AND LOWER(fca.product_id) = 'casino'
GROUP BY fca.activity_date, fca.game_id, gc.game_name, gc.vendor_id, gc.game_category
ORDER BY 1 DESC, ggr_total DESC
"""


def setup_table():
    log.info("Criando tabela fact_casino_rounds...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    try:
        execute_supernova(DDL_INDEX)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (ps_bi — Casino por Jogo)...")
    log.info("Fonte: ps_bi.fct_casino_activity_daily + dim_game + filtro test users")
    df = query_athena(QUERY_ATHENA, database="ps_bi")
    log.info(f"{len(df)} linhas obtidas (jogos x dias).")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_casino_rounds
            (dt, game_id, game_name, vendor_id, sub_vendor_id, game_category,
             qty_players, total_rounds, rounds_per_player,
             turnover_real, wins_real, ggr_real,
             turnover_bonus, wins_bonus, ggr_bonus,
             turnover_total, wins_total, ggr_total,
             hold_rate_pct, rtp_pct,
             jackpot_win, jackpot_contribution,
             free_spins_bet, free_spins_win,
             refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            str(row["game_id"] or ""),
            str(row["game_name"] or "Desconhecido"),
            str(row["vendor_id"] or "unknown"),
            str(row["sub_vendor_id"] or ""),
            str(row["game_category"] or "Outros"),
            int(row["qty_players"]),
            int(row["total_rounds"] or 0),
            float(row["rounds_per_player"] or 0),
            float(row["turnover_real"] or 0),
            float(row["wins_real"] or 0),
            float(row["ggr_real"] or 0),
            float(row["turnover_bonus"] or 0),
            float(row["wins_bonus"] or 0),
            float(row["ggr_bonus"] or 0),
            float(row["turnover_total"] or 0),
            float(row["wins_total"] or 0),
            float(row["ggr_total"] or 0),
            float(row["hold_rate_pct"] or 0),
            float(row["rtp_pct"] or 0),
            float(row["jackpot_win"] or 0),
            float(row["jackpot_contribution"] or 0),
            float(row["free_spins_bet"] or 0),
            float(row["free_spins_win"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_casino_rounds;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    # Resumo
    total_ggr = sum(r[17] for r in records)
    total_turnover = sum(r[15] for r in records)
    total_rounds = sum(r[7] for r in records)
    total_jackpot = sum(r[20] for r in records)
    unique_games = len(set(r[1] for r in records))
    unique_vendors = len(set(r[3] for r in records))
    avg_hold = (total_ggr / total_turnover * 100) if total_turnover > 0 else 0

    log.info(f"{len(records)} linhas inseridas | {unique_games} jogos | {unique_vendors} vendors")
    log.info(f"  Turnover: R$ {total_turnover:,.2f} | GGR: R$ {total_ggr:,.2f}")
    log.info(f"  Hold Rate medio: {avg_hold:.2f}% | Rodadas: {total_rounds:,}")
    log.info(f"  Jackpot Wins: R$ {total_jackpot:,.2f}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_casino_rounds (ps_bi — por Jogo) ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
