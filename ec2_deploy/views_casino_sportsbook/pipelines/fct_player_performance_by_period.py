"""
Pipeline: fct_player_performance_by_period (performance por jogador × periodo × vertical)
==========================================================================================
Atende pedido do Gusta (10/04/2026 — apos feedback v4.1):
Ultima view pra aba Players com Top Winners/Losers + tabelas de performance.

Mesmo padrao da `vw_active_players_period` mas com grao por jogador.

Grao: user_id × period × vertical (~500K-800K linhas esperadas)
Periodos: yesterday (D-1), last_7d, last_30d, last_90d, mtd, ytd
Verticals: casino, sports, both (jogador que jogou nas DUAS verticais no mesmo periodo)

KPIs:
  - player_result: NGR do jogador (positivo = player ganhou = -GGR_casa)
    Convencao: Winners ORDER BY DESC, Losers ORDER BY ASC
  - turnover: total apostado (BRL real)
  - deposit_total: depositos confirmados no periodo (cashier_ec2, BRL real)
  - qty_sessions: dias distintos ativos no periodo (player-days)
    NOTA: definicao como player-days. Se front precisar de sessao de login
    real (com heuristica de timeout), trocar a logica de COUNT(DISTINCT dt_brt)

Fontes (Athena):
  - fund_ec2.tbl_real_fund_txn + tbl_real_fund_txn_type_mst — gaming txns (turnover/wins/GGR)
  - ecr_ec2.tbl_ecr_flags — filtro test users
  - cashier_ec2.tbl_cashier_deposit — depositos confirmados

Destino: Super Nova DB -> multibet.fct_player_performance_by_period
         + view multibet.vw_player_performance_period
Estrategia: TRUNCATE + INSERT (refresh diario)
Corte: D-1 BRT (nunca D-0)

Dependencia de ordem (cron): rodar APOS fct_casino_activity, fct_sports_activity
e fct_active_players_by_period pra garantir consistencia das metricas agregadas.

Execucao:
    python pipelines/fct_player_performance_by_period.py
"""

import sys
import os
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- DDL ------------------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fct_player_performance_by_period (
    user_id         BIGINT,
    period          VARCHAR(15),    -- 'yesterday'|'last_7d'|'last_30d'|'last_90d'|'mtd'|'ytd'
    period_label    VARCHAR(40),
    period_start    DATE,
    period_end      DATE,
    vertical        VARCHAR(15),    -- 'casino'|'sports'|'both'
    player_result   NUMERIC(18,2)   DEFAULT 0,  -- positivo = player ganhou
    turnover        NUMERIC(18,2)   DEFAULT 0,
    deposit_total   NUMERIC(18,2)   DEFAULT 0,
    qty_sessions    INTEGER         DEFAULT 0,  -- player-days (proxy)
    refreshed_at    TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (user_id, period, vertical)
);
"""

# Indices criticos pro front-end (Top Winners/Losers queries)
DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_fppp_period_vertical_result_desc
    ON multibet.fct_player_performance_by_period (period, vertical, player_result DESC);
CREATE INDEX IF NOT EXISTS idx_fppp_period_vertical_result_asc
    ON multibet.fct_player_performance_by_period (period, vertical, player_result ASC);
CREATE INDEX IF NOT EXISTS idx_fppp_period_vertical_turnover
    ON multibet.fct_player_performance_by_period (period, vertical, turnover DESC);
"""

DDL_VIEW = """
CREATE OR REPLACE VIEW multibet.vw_player_performance_period AS
SELECT
    user_id,
    period,
    period_label,
    period_start,
    period_end,
    vertical,
    player_result,
    turnover,
    deposit_total,
    qty_sessions,
    refreshed_at
FROM multibet.fct_player_performance_by_period;
"""

# --- Query Athena ---------------------------------------------------------
# Estrategia:
#   1. gaming_events: fund_ec2 agregado por (ecr_id, product_id, dt_brt) com
#      turnover/wins. UTC->BRT antes de truncar pra data. Test users fora.
#   2. deposit_events: cashier_ec2 agregado por (ecr_id, dt_brt) com deposito confirmado.
#   3. periods: 6 ranges ancorados em D-1.
#   4. player_period_stats: pivot por player×periodo com colunas separadas
#      pra casino e sports (turnover, ggr_house, days).
#   5. player_period_deposit: deposito total por player×periodo (global, nao split).
#   6. Output: 3 UNIONs — row casino (se ativo casino), row sports, row both (se ambos).
QUERY_ATHENA = """
WITH
-- Boundaries em BRT ancoradas em D-1
bounds AS (
    SELECT
        CAST(CAST(NOW() AT TIME ZONE 'America/Sao_Paulo' AS DATE) - INTERVAL '1' DAY AS DATE) AS d_minus_1
),

-- Gaming events: 1 linha por (ecr_id, product_id, dt_brt) com turnover e GGR_house
-- Replica mesma logica do fct_sports_activity/fct_casino_activity (direto fund_ec2,
-- sem sub-fund isolation — pra performance, afinal grao e por jogador ja agregado)
gaming_events AS (
    SELECT
        t.c_ecr_id,
        t.c_product_id,
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt_brt,
        -- Turnover liquido: DB (aposta) menos cancels
        SUM(CASE WHEN m.c_op_type = 'DB' AND m.c_is_cancel_txn = false
                 THEN CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2))
                 WHEN m.c_op_type = 'CR' AND m.c_is_cancel_txn = true
                 THEN -CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2))
                 ELSE 0 END) / 100.0 AS turnover_brl,
        -- GGR_house: turnover - wins (positivo = casa ganhou, negativo = player ganhou)
        (SUM(CASE WHEN m.c_op_type = 'DB' AND m.c_is_cancel_txn = false
                  THEN CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2))
                  WHEN m.c_op_type = 'CR' AND m.c_is_cancel_txn = true
                  THEN -CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2)) ELSE 0 END)
       - SUM(CASE WHEN m.c_op_type = 'CR' AND m.c_is_cancel_txn = false
                  THEN CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2))
                  WHEN m.c_op_type = 'DB' AND m.c_is_cancel_txn = true
                  THEN -CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2)) ELSE 0 END)) / 100.0 AS ggr_house_brl
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN fund_ec2.tbl_real_fund_txn_type_mst m
        ON t.c_txn_type = m.c_txn_type
    LEFT JOIN ecr_ec2.tbl_ecr_flags f
        ON t.c_ecr_id = f.c_ecr_id
    WHERE t.c_start_time >= TIMESTAMP '2026-01-01 03:00:00'
      AND t.c_txn_status = 'SUCCESS'
      AND m.c_is_gaming_txn = 'Y'
      AND t.c_product_id IN ('CASINO', 'SPORTS_BOOK')
      AND COALESCE(f.c_test_user, false) = false
    GROUP BY t.c_ecr_id, t.c_product_id,
             CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
),

-- Deposit events: cashier_ec2 agregado por (ecr_id, dt_brt)
deposit_events AS (
    SELECT
        d.c_ecr_id,
        CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt_brt,
        SUM(CAST(d.c_confirmed_amount_in_ecr_ccy AS DECIMAL(18,2))) / 100.0 AS deposit_amount
    FROM cashier_ec2.tbl_cashier_deposit d
    WHERE d.c_created_time >= TIMESTAMP '2026-01-01 03:00:00'
      AND d.c_txn_status = 'txn_confirmed_success'
    GROUP BY d.c_ecr_id,
             CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
),

-- Definicao dos 6 periodos ancorados em D-1
periods AS (
    SELECT 'yesterday' AS period, 'Ontem (D-1)'         AS period_label,
           b.d_minus_1 AS period_start, b.d_minus_1 AS period_end FROM bounds b
    UNION ALL
    SELECT 'last_7d',   'Ultimos 7 dias',
           CAST(b.d_minus_1 - INTERVAL '6'  DAY AS DATE), b.d_minus_1 FROM bounds b
    UNION ALL
    SELECT 'last_30d',  'Ultimos 30 dias',
           CAST(b.d_minus_1 - INTERVAL '29' DAY AS DATE), b.d_minus_1 FROM bounds b
    UNION ALL
    SELECT 'last_90d',  'Ultimos 90 dias',
           CAST(b.d_minus_1 - INTERVAL '89' DAY AS DATE), b.d_minus_1 FROM bounds b
    UNION ALL
    SELECT 'mtd',       'Mes corrente (MTD)',
           CAST(date_trunc('month', b.d_minus_1) AS DATE), b.d_minus_1 FROM bounds b
    UNION ALL
    SELECT 'ytd',       'Ano corrente (YTD)',
           CAST(date_trunc('year',  b.d_minus_1) AS DATE), b.d_minus_1 FROM bounds b
),

-- Pivot por periodo × player: casino e sports lado a lado
player_period_stats AS (
    SELECT
        p.period,
        p.period_label,
        p.period_start,
        p.period_end,
        g.c_ecr_id AS user_id,
        SUM(CASE WHEN g.c_product_id = 'CASINO'
                 THEN g.turnover_brl ELSE 0 END) AS casino_turnover,
        SUM(CASE WHEN g.c_product_id = 'CASINO'
                 THEN g.ggr_house_brl ELSE 0 END) AS casino_ggr_house,
        COUNT(DISTINCT CASE WHEN g.c_product_id = 'CASINO'
                            THEN g.dt_brt END) AS casino_days,
        SUM(CASE WHEN g.c_product_id = 'SPORTS_BOOK'
                 THEN g.turnover_brl ELSE 0 END) AS sports_turnover,
        SUM(CASE WHEN g.c_product_id = 'SPORTS_BOOK'
                 THEN g.ggr_house_brl ELSE 0 END) AS sports_ggr_house,
        COUNT(DISTINCT CASE WHEN g.c_product_id = 'SPORTS_BOOK'
                            THEN g.dt_brt END) AS sports_days
    FROM periods p
    INNER JOIN gaming_events g
        ON g.dt_brt BETWEEN p.period_start AND p.period_end
    GROUP BY p.period, p.period_label, p.period_start, p.period_end, g.c_ecr_id
),

-- Deposit por player × periodo (global — nao split por vertical)
player_period_deposit AS (
    SELECT
        p.period,
        d.c_ecr_id AS user_id,
        SUM(d.deposit_amount) AS deposit_total
    FROM periods p
    INNER JOIN deposit_events d
        ON d.dt_brt BETWEEN p.period_start AND p.period_end
    GROUP BY p.period, d.c_ecr_id
)

-- Row CASINO: player ativo em casino
SELECT
    s.user_id,
    s.period,
    s.period_label,
    s.period_start,
    s.period_end,
    'casino' AS vertical,
    -- player_result = -GGR_casa (positivo = player ganhou)
    CAST(-s.casino_ggr_house AS DECIMAL(18,2)) AS player_result,
    CAST(s.casino_turnover AS DECIMAL(18,2)) AS turnover,
    CAST(COALESCE(d.deposit_total, 0) AS DECIMAL(18,2)) AS deposit_total,
    CAST(s.casino_days AS INTEGER) AS qty_sessions
FROM player_period_stats s
LEFT JOIN player_period_deposit d
    ON s.period = d.period AND s.user_id = d.user_id
WHERE s.casino_turnover > 0

UNION ALL

-- Row SPORTS: player ativo em sportsbook
SELECT
    s.user_id,
    s.period,
    s.period_label,
    s.period_start,
    s.period_end,
    'sports' AS vertical,
    CAST(-s.sports_ggr_house AS DECIMAL(18,2)) AS player_result,
    CAST(s.sports_turnover AS DECIMAL(18,2)) AS turnover,
    CAST(COALESCE(d.deposit_total, 0) AS DECIMAL(18,2)) AS deposit_total,
    CAST(s.sports_days AS INTEGER) AS qty_sessions
FROM player_period_stats s
LEFT JOIN player_period_deposit d
    ON s.period = d.period AND s.user_id = d.user_id
WHERE s.sports_turnover > 0

UNION ALL

-- Row BOTH: player ativo em ambos (omnichannel) — agrega casino + sports
SELECT
    s.user_id,
    s.period,
    s.period_label,
    s.period_start,
    s.period_end,
    'both' AS vertical,
    CAST(-(s.casino_ggr_house + s.sports_ggr_house) AS DECIMAL(18,2)) AS player_result,
    CAST(s.casino_turnover + s.sports_turnover AS DECIMAL(18,2)) AS turnover,
    CAST(COALESCE(d.deposit_total, 0) AS DECIMAL(18,2)) AS deposit_total,
    CAST(s.casino_days + s.sports_days AS INTEGER) AS qty_sessions
FROM player_period_stats s
LEFT JOIN player_period_deposit d
    ON s.period = d.period AND s.user_id = d.user_id
WHERE s.casino_turnover > 0 AND s.sports_turnover > 0
"""


def setup_table():
    log.info("Criando tabela + indices + view...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    try:
        execute_supernova(DDL_INDEXES)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    execute_supernova(DDL_VIEW)
    log.info("Setup concluido.")


def refresh():
    log.info("Executando query no Athena (player performance × periodo × vertical)...")
    log.info("Fontes: fund_ec2 (gaming) + cashier_ec2 (deposit) + ecr_ec2 (test filter)")
    df = query_athena(QUERY_ATHENA, database="fund_ec2")
    log.info(f"{len(df):,} linhas obtidas do Athena.")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fct_player_performance_by_period
            (user_id, period, period_label, period_start, period_end,
             vertical, player_result, turnover, deposit_total, qty_sessions,
             refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            int(row["user_id"]),
            str(row["period"]),
            str(row["period_label"]),
            row["period_start"],
            row["period_end"],
            str(row["vertical"]),
            float(row["player_result"] or 0),
            float(row["turnover"] or 0),
            float(row["deposit_total"] or 0),
            int(row["qty_sessions"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fct_player_performance_by_period;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    log.info(f"{len(records):,} linhas inseridas.")

    # Sumario por (period, vertical)
    from collections import defaultdict
    by_pv = defaultdict(lambda: {"rows": 0, "turnover": 0.0, "winners": 0, "losers": 0})
    for r in records:
        key = (r[1], r[5])  # period, vertical
        by_pv[key]["rows"] += 1
        by_pv[key]["turnover"] += r[7]  # turnover
        if r[6] > 0:
            by_pv[key]["winners"] += 1
        elif r[6] < 0:
            by_pv[key]["losers"] += 1

    period_order = ['yesterday', 'last_7d', 'last_30d', 'last_90d', 'mtd', 'ytd']
    vertical_order = ['casino', 'sports', 'both']
    log.info("Resumo por periodo × vertical:")
    log.info(f"  {'Periodo':<12} | {'Vertical':<10} | {'Players':>10} | {'Winners':>9} | {'Losers':>9} | {'Turnover (BRL)':>18}")
    log.info(f"  {'-'*12}-+-{'-'*10}-+-{'-'*10}-+-{'-'*9}-+-{'-'*9}-+-{'-'*18}")
    for p in period_order:
        for v in vertical_order:
            d = by_pv.get((p, v))
            if d:
                log.info(
                    f"  {p:<12} | {v:<10} | {d['rows']:>10,} | "
                    f"{d['winners']:>9,} | {d['losers']:>9,} | "
                    f"R$ {d['turnover']:>15,.0f}"
                )


if __name__ == "__main__":
    try:
        log.info("=== Iniciando pipeline fct_player_performance_by_period ===")
        setup_table()
        refresh()
        log.info("=== Pipeline concluido ===")
    except Exception as e:
        log.error(f"Pipeline falhou: {e}", exc_info=True)
        sys.exit(1)
