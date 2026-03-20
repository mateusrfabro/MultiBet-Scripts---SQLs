"""
Pipeline: fact_player_engagement_daily (Fase 3: Retencao/Churn)
================================================================
Grao: c_ecr_id (player-level)

Metricas:
    - days_active_since_ftd: duracao de vida (FTD ate ultima atividade)
    - total_active_days: dias distintos com apostas
    - avg_bets_per_day: media de apostas por dia ativo (intensidade)
    - last_active_date: ultima data com atividade
    - days_since_last_active: dias desde ultima atividade
    - is_churned: 1 se inativo > 30 dias

Fontes:
    1. bireports_ec2.tbl_ecr (Gatekeeper + tracker/affiliate)
    2. cashier_ec2.tbl_cashier_deposit (FTD)
    3. fund_ec2.tbl_real_fund_txn (atividade de jogo)

Destino: Super Nova DB -> multibet.fact_player_engagement_daily
"""

import sys, os, logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"
DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_player_engagement_daily (
    c_ecr_id BIGINT PRIMARY KEY,
    c_tracker_id VARCHAR(255), source VARCHAR(100),
    ftd_date DATE, first_active_date DATE, last_active_date DATE,
    days_active_since_ftd INTEGER DEFAULT 0,
    total_active_days INTEGER DEFAULT 0,
    total_bets_count INTEGER DEFAULT 0,
    avg_bets_per_day NUMERIC(10,2) DEFAULT 0,
    total_ggr NUMERIC(18,2) DEFAULT 0,
    days_since_last_active INTEGER,
    is_churned SMALLINT DEFAULT 0,
    refreshed_at TIMESTAMPTZ DEFAULT NOW()
);
"""

QUERY_ATHENA = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),

-- 1. Gatekeeper
registrations AS (
    SELECT c_ecr_id,
           COALESCE(NULLIF(TRIM(c_tracker_id), ''), CAST(c_affiliate_id AS VARCHAR), 'sem_tracker') AS c_tracker_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= (SELECT start_date FROM params)
),

-- 2. FTD
ftds AS (
    SELECT c_ecr_id, ftd_time FROM (
        SELECT c_ecr_id, c_created_time AS ftd_time,
               ROW_NUMBER() OVER(PARTITION BY c_ecr_id ORDER BY c_created_time) AS rn
        FROM cashier_ec2.tbl_cashier_deposit
        WHERE c_txn_status = 'txn_confirmed_success'
    ) WHERE rn = 1
),

-- 3. Atividade de jogo (bets) por player/dia
player_activity AS (
    SELECT
        t.c_ecr_id,
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS game_date,
        COUNT(*) AS bets_count,
        SUM(CASE WHEN t.c_txn_type IN (27, 28, 59)
                 THEN CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2)) / 100.0 ELSE 0 END)
        - SUM(CASE WHEN t.c_txn_type IN (45, 80, 72, 112)
                   THEN CAST(t.c_amount_in_ecr_ccy AS DECIMAL(18,2)) / 100.0 ELSE 0 END)
        AS daily_ggr
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN registrations r ON t.c_ecr_id = r.c_ecr_id
    WHERE t.c_start_time >= (SELECT start_date FROM params)
      AND t.c_txn_status = 'SUCCESS'
      AND t.c_txn_type IN (27, 28, 45, 59, 72, 80, 112)
    GROUP BY 1, 2
),

-- 4. Agregacao por player
player_stats AS (
    SELECT
        pa.c_ecr_id,
        MIN(pa.game_date) AS first_active_date,
        MAX(pa.game_date) AS last_active_date,
        COUNT(DISTINCT pa.game_date) AS total_active_days,
        SUM(pa.bets_count) AS total_bets_count,
        SUM(pa.daily_ggr) AS total_ggr
    FROM player_activity pa
    GROUP BY 1
)

-- 5. Consolidacao final
SELECT
    r.c_ecr_id,
    r.c_tracker_id,
    CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS ftd_date,
    ps.first_active_date,
    ps.last_active_date,
    -- Dias entre FTD e ultima atividade
    COALESCE(date_diff('day',
        CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE),
        ps.last_active_date), 0) AS days_active_since_ftd,
    COALESCE(ps.total_active_days, 0) AS total_active_days,
    COALESCE(ps.total_bets_count, 0) AS total_bets_count,
    -- Media de bets por dia ativo
    CASE WHEN COALESCE(ps.total_active_days, 0) > 0
         THEN ROUND(CAST(ps.total_bets_count AS DOUBLE) / ps.total_active_days, 2)
         ELSE 0 END AS avg_bets_per_day,
    COALESCE(ps.total_ggr, 0) AS total_ggr,
    -- Dias desde ultima atividade (para churn)
    date_diff('day',
        COALESCE(ps.last_active_date, CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)),
        CURRENT_DATE) AS days_since_last_active
FROM (
    -- Dedup: pegar o primeiro tracker por player
    SELECT c_ecr_id, c_tracker_id,
           ROW_NUMBER() OVER(PARTITION BY c_ecr_id ORDER BY c_tracker_id) AS rn_reg
    FROM registrations
) r
INNER JOIN ftds f ON r.c_ecr_id = f.c_ecr_id
LEFT JOIN player_stats ps ON r.c_ecr_id = ps.c_ecr_id
WHERE r.rn_reg = 1
"""


def setup_table():
    log.info("Criando tabela fact_player_engagement_daily...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    log.info("Tabela pronta.")


def load_mapping():
    rows = execute_supernova("SELECT tracker_id, source FROM multibet.dim_marketing_mapping", fetch=True)
    return {r[0]: r[1] for r in (rows or [])}


def refresh():
    log.info("Executando query no Athena (engagement player-level)...")
    df = query_athena(QUERY_ATHENA, database="fund_ec2")
    log.info(f"{len(df)} players obtidos.")

    if df.empty:
        return

    mapping = load_mapping()
    df["source"] = df["c_tracker_id"].map(mapping).fillna("unmapped_orphans")
    # Churn: inativo > 30 dias
    df["is_churned"] = (df["days_since_last_active"] > 30).astype(int)

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_player_engagement_daily
            (c_ecr_id, c_tracker_id, source, ftd_date, first_active_date, last_active_date,
             days_active_since_ftd, total_active_days, total_bets_count, avg_bets_per_day,
             total_ggr, days_since_last_active, is_churned, refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            int(row["c_ecr_id"]),
            row["c_tracker_id"],
            row["source"],
            row["ftd_date"],
            row["first_active_date"] if row["first_active_date"] is not None else None,
            row["last_active_date"] if row["last_active_date"] is not None else None,
            int(row["days_active_since_ftd"] or 0),
            int(row["total_active_days"] or 0),
            int(row["total_bets_count"] or 0),
            float(row["avg_bets_per_day"] or 0),
            float(row["total_ggr"] or 0),
            int(row["days_since_last_active"]) if row["days_since_last_active"] is not None else None,
            int(row["is_churned"]),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_player_engagement_daily;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=2000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    churned = sum(1 for r in records if r[12] == 1)
    active = len(records) - churned
    avg_days = sum(r[7] for r in records) / max(len(records), 1)
    log.info(f"{len(records):,} players | Active: {active:,} | Churned: {churned:,} ({churned/max(len(records),1)*100:.1f}%) | Avg active days: {avg_days:.1f}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_player_engagement_daily ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
