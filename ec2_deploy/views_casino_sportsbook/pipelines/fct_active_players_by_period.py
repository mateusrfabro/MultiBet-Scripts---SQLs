"""
Pipeline: fct_active_players_by_period (jogadores unicos por periodo x produto)
================================================================================
Atende gap menor #7 do gaps_views_gold_v3.md (Gusta, 10/04/2026):
"Criar vw_active_players_period ou confirmar player-days nas pies da Overview".

Player-days NAO serve para pies de Overview (double count). Solucao: tabela
pequena (18 linhas) pre-agregada com COUNT DISTINCT por periodo x produto,
refresh diario. Front consome direto da view sem precisar agregar nada.

Grao: period x product (~18 linhas — 6 periodos x 3 produtos)
Periodos: yesterday (D-1), last_7d, last_30d, last_90d, mtd, ytd
Produtos: casino, sportsbook, both (intersecao = jogadores omnichannel)
Corte: D-1 BRT (nunca D-0 — feedback_sempre_usar_d_menos_1)

Fonte: fund_ec2.tbl_real_fund_txn (gaming txns) + ecr_ec2.tbl_ecr_flags (test filter)
       JOIN com fund_ec2.tbl_real_fund_txn_type_mst para c_is_gaming_txn = 'Y'

Destino: Super Nova DB -> multibet.fct_active_players_by_period
         + view multibet.vw_active_players_period
Estrategia: TRUNCATE + INSERT (refresh diario, dataset pequeno)

Execucao:
    python pipelines/fct_active_players_by_period.py

Dependencia de ordem (cron): rodar APOS fct_casino_activity e fct_sports_activity
para garantir consistencia das contagens diarias do mesmo dia.
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
CREATE TABLE IF NOT EXISTS multibet.fct_active_players_by_period (
    period          VARCHAR(15),    -- 'yesterday'|'last_7d'|'last_30d'|'last_90d'|'mtd'|'ytd'
    period_label    VARCHAR(40),
    period_start    DATE,
    period_end      DATE,
    product         VARCHAR(15),    -- 'casino'|'sportsbook'|'both'
    unique_players  INTEGER         DEFAULT 0,
    refreshed_at    TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (period, product)
);
"""

DDL_VIEW = """
CREATE OR REPLACE VIEW multibet.vw_active_players_period AS
SELECT
    period,
    period_label,
    period_start,
    period_end,
    product,
    unique_players,
    refreshed_at
FROM multibet.fct_active_players_by_period
ORDER BY
    CASE period
        WHEN 'yesterday' THEN 1
        WHEN 'last_7d'   THEN 2
        WHEN 'last_30d'  THEN 3
        WHEN 'last_90d'  THEN 4
        WHEN 'mtd'       THEN 5
        WHEN 'ytd'       THEN 6
        ELSE 99
    END,
    CASE product
        WHEN 'casino'     THEN 1
        WHEN 'sportsbook' THEN 2
        WHEN 'both'       THEN 3
        ELSE 99
    END;
"""

# --- Query Athena (validada por extractor agent 10/04/2026) ---------------
# Estrategia:
#   1. base_events: 1 linha distinta por (ecr_id, produto, dia BRT) — gaming txns SUCCESS
#   2. periods: define os 6 ranges ancorados em D-1 BRT
#   3. player_period_product: cruza eventos com cada periodo
#   4. player_flags: pivota para flag de casino/sb por jogador no periodo
#   5. UNION ALL final com 3 produtos: casino, sportsbook, both (intersecao)
# Output: 18 linhas (6 periodos x 3 produtos), ~3-4 meses de scan filtrado.
QUERY_ATHENA = """
WITH
-- Boundaries em BRT ancoradas em D-1
bounds AS (
    SELECT
        CAST(CAST(NOW() AT TIME ZONE 'America/Sao_Paulo' AS DATE) - INTERVAL '1' DAY AS DATE) AS d_minus_1
),

-- Base de eventos: 1 linha por (ecr_id, produto, dia BRT)
-- Filtra gaming txns, status SUCCESS, produto valido, sem test users
-- Converte UTC->BRT antes de truncar para data
base_events AS (
    SELECT DISTINCT
        t.c_ecr_id AS ecr_id,
        t.c_product_id AS product_id,
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS event_date_brt
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN fund_ec2.tbl_real_fund_txn_type_mst m
        ON t.c_txn_type = m.c_txn_type
    LEFT JOIN ecr_ec2.tbl_ecr_flags f
        ON t.c_ecr_id = f.c_ecr_id
    WHERE t.c_start_time >= TIMESTAMP '2026-01-01 03:00:00'  -- 2026-01-01 BRT em UTC (cobre YTD)
      AND t.c_txn_status = 'SUCCESS'
      AND m.c_is_gaming_txn = 'Y'
      AND t.c_product_id IN ('CASINO', 'SPORTS_BOOK')
      AND COALESCE(f.c_test_user, false) = false
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

-- Cruza eventos com cada periodo (1 linha por ecr_id/produto/periodo se ativo)
player_period_product AS (
    SELECT
        p.period,
        p.period_label,
        p.period_start,
        p.period_end,
        e.ecr_id,
        e.product_id
    FROM periods p
    INNER JOIN base_events e
        ON e.event_date_brt BETWEEN p.period_start AND p.period_end
    GROUP BY p.period, p.period_label, p.period_start, p.period_end, e.ecr_id, e.product_id
),

-- Pivota por jogador/periodo: flags de casino e sportsbook no mesmo range
player_flags AS (
    SELECT
        period,
        period_label,
        period_start,
        period_end,
        ecr_id,
        MAX(CASE WHEN product_id = 'CASINO'      THEN 1 ELSE 0 END) AS played_casino,
        MAX(CASE WHEN product_id = 'SPORTS_BOOK' THEN 1 ELSE 0 END) AS played_sb
    FROM player_period_product
    GROUP BY period, period_label, period_start, period_end, ecr_id
)

-- Output final: 3 produtos por periodo
-- "both" = jogou casino E sportsbook no mesmo periodo (intersecao, nao uniao)
SELECT period, period_label, period_start, period_end,
       'casino' AS product,
       COUNT_IF(played_casino = 1) AS unique_players
FROM player_flags
GROUP BY period, period_label, period_start, period_end

UNION ALL

SELECT period, period_label, period_start, period_end,
       'sportsbook' AS product,
       COUNT_IF(played_sb = 1) AS unique_players
FROM player_flags
GROUP BY period, period_label, period_start, period_end

UNION ALL

SELECT period, period_label, period_start, period_end,
       'both' AS product,
       COUNT_IF(played_casino = 1 AND played_sb = 1) AS unique_players
FROM player_flags
GROUP BY period, period_label, period_start, period_end
"""


def setup_table():
    log.info("Criando tabela fct_active_players_by_period e view...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_VIEW)
    log.info("Tabela + view prontas.")


def refresh():
    log.info("Executando query no Athena (jogadores unicos por periodo x produto)...")
    log.info("Fonte: fund_ec2.tbl_real_fund_txn + tbl_real_fund_txn_type_mst + ecr_flags")
    df = query_athena(QUERY_ATHENA, database="fund_ec2")
    log.info(f"{len(df)} linhas obtidas (esperado: 18 = 6 periodos x 3 produtos).")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fct_active_players_by_period
            (period, period_label, period_start, period_end, product, unique_players, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            str(row["period"]),
            str(row["period_label"]),
            row["period_start"],
            row["period_end"],
            str(row["product"]),
            int(row["unique_players"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fct_active_players_by_period;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=100)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    log.info(f"{len(records)} linhas inseridas.")
    # Sumario por periodo (debug)
    by_period = {}
    for r in records:
        period, product, n = r[0], r[4], r[5]
        if period not in by_period:
            by_period[period] = {}
        by_period[period][product] = n
    period_order = ['yesterday', 'last_7d', 'last_30d', 'last_90d', 'mtd', 'ytd']
    log.info("Resumo (jogadores unicos):")
    log.info(f"  {'Periodo':<12} | {'Casino':>10} | {'Sports':>10} | {'Both':>10}")
    log.info(f"  {'-'*12}-+-{'-'*10}-+-{'-'*10}-+-{'-'*10}")
    for p in period_order:
        if p in by_period:
            d = by_period[p]
            log.info(
                f"  {p:<12} | {d.get('casino', 0):>10,} | "
                f"{d.get('sportsbook', 0):>10,} | {d.get('both', 0):>10,}"
            )


if __name__ == "__main__":
    try:
        log.info("=== Iniciando pipeline fct_active_players_by_period ===")
        setup_table()
        refresh()
        log.info("=== Pipeline concluido ===")
    except Exception as e:
        log.error(f"Pipeline falhou: {e}", exc_info=True)
        sys.exit(1)
