"""
Pipeline: fact_player_activity (DAU/WAU/MAU por dia)
=====================================================
Grao: dt (dia)
KPIs: DAU, WAU, MAU, stickiness, GGR/DAU, avg bets per active player

Fonte: fund_ec2.tbl_real_fund_txn (atividade de jogo)
"""
import sys, os, logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS multibet.fact_player_activity (
    dt                  DATE PRIMARY KEY,
    dau                 INTEGER DEFAULT 0,
    wau                 INTEGER DEFAULT 0,
    mau                 INTEGER DEFAULT 0,
    stickiness_pct      NUMERIC(10,4) DEFAULT 0,
    total_bets          INTEGER DEFAULT 0,
    avg_bets_per_player NUMERIC(10,2) DEFAULT 0,
    total_ggr           NUMERIC(18,2) DEFAULT 0,
    ggr_per_dau         NUMERIC(18,2) DEFAULT 0,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW()
);
"""

QUERY = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),
registrations AS (
    SELECT c_ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= (SELECT start_date FROM params)
),
-- Atividade diaria: quem jogou em cada dia
daily_players AS (
    SELECT
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        t.c_ecr_id,
        COUNT(*) AS bets,
        SUM(CASE WHEN t.c_txn_type IN (27,28,59) THEN CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2))/100.0 ELSE 0 END)
        - SUM(CASE WHEN t.c_txn_type IN (45,80,72,112) THEN CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2))/100.0 ELSE 0 END) AS ggr
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN registrations r ON t.c_ecr_id = r.c_ecr_id
    WHERE t.c_start_time >= (SELECT start_date FROM params)
      AND t.c_txn_status = 'SUCCESS'
      AND t.c_txn_type IN (27,28,45,59,72,80,112)
    GROUP BY 1, 2
),
-- DAU por dia
dau_daily AS (
    SELECT dt, COUNT(DISTINCT c_ecr_id) AS dau,
           SUM(bets) AS total_bets, SUM(ggr) AS total_ggr
    FROM daily_players
    GROUP BY 1
),
-- WAU: jogadores unicos nos ultimos 7 dias
wau_daily AS (
    SELECT d1.dt,
           COUNT(DISTINCT d2.c_ecr_id) AS wau
    FROM (SELECT DISTINCT dt FROM daily_players) d1
    CROSS JOIN daily_players d2
    WHERE d2.dt BETWEEN date_add('day', -6, d1.dt) AND d1.dt
    GROUP BY 1
),
-- MAU: jogadores unicos nos ultimos 30 dias
mau_daily AS (
    SELECT d1.dt,
           COUNT(DISTINCT d2.c_ecr_id) AS mau
    FROM (SELECT DISTINCT dt FROM daily_players) d1
    CROSS JOIN daily_players d2
    WHERE d2.dt BETWEEN date_add('day', -29, d1.dt) AND d1.dt
    GROUP BY 1
)
SELECT
    d.dt, d.dau,
    COALESCE(w.wau, 0) AS wau,
    COALESCE(m.mau, 0) AS mau,
    CASE WHEN COALESCE(m.mau, 0) > 0 THEN ROUND(CAST(d.dau AS DOUBLE) / m.mau * 100, 4) ELSE 0 END AS stickiness_pct,
    d.total_bets,
    CASE WHEN d.dau > 0 THEN ROUND(CAST(d.total_bets AS DOUBLE) / d.dau, 2) ELSE 0 END AS avg_bets_per_player,
    d.total_ggr,
    CASE WHEN d.dau > 0 THEN ROUND(d.total_ggr / d.dau, 2) ELSE 0 END AS ggr_per_dau
FROM dau_daily d
LEFT JOIN wau_daily w ON d.dt = w.dt
LEFT JOIN mau_daily m ON d.dt = m.dt
ORDER BY 1 DESC
"""


def refresh():
    log.info("Criando tabela...")
    execute_supernova("CREATE SCHEMA IF NOT EXISTS multibet;")
    execute_supernova(DDL)

    log.info("Executando query Athena (DAU/WAU/MAU por dia)...")
    log.info("AVISO: Query pesada - CROSS JOIN para WAU e MAU")
    df = query_athena(QUERY, database="fund_ec2")
    log.info(f"{len(df)} dias obtidos.")

    if df.empty:
        return

    now_utc = datetime.now(timezone.utc)
    insert_sql = """
        INSERT INTO multibet.fact_player_activity
            (dt, dau, wau, mau, stickiness_pct, total_bets, avg_bets_per_player, total_ggr, ggr_per_dau, refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"], int(row["dau"]), int(row["wau"]), int(row["mau"]),
            float(row["stickiness_pct"] or 0), int(row["total_bets"]),
            float(row["avg_bets_per_player"] or 0), float(row["total_ggr"] or 0),
            float(row["ggr_per_dau"] or 0), now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_player_activity;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    avg_dau = sum(r[1] for r in records) / max(len(records), 1)
    avg_mau = sum(r[3] for r in records) / max(len(records), 1)
    log.info(f"{len(records)} dias | Avg DAU: {avg_dau:,.0f} | Avg MAU: {avg_mau:,.0f} | Stickiness: {avg_dau/max(avg_mau,1)*100:.1f}%")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_player_activity ===")
    refresh()
    log.info("=== Pipeline concluido ===")
