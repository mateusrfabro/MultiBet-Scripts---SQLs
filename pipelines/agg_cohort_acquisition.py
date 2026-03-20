"""
Pipeline: agg_cohort_acquisition (LTV por Cohort de FTD)
=========================================================
Grao: month_of_ftd (safra) x source x player_id

Calcula GGR acumulado em janelas D0, D7, D30 por jogador,
permitindo analise de ROI de longo prazo por fonte de trafego.

Fontes:
    1. Athena: bireports_ec2.tbl_ecr (Gatekeeper + affiliate)
    2. Athena: cashier_ec2.tbl_cashier_deposit (FTD + 2nd deposit)
    3. Athena: fund_ec2.tbl_real_fund_txn (GGR por player/dia)
    4. Super Nova DB: dim_marketing_mapping (source)
    5. Super Nova DB: fact_attribution (spend por source/mes)

Destino: Super Nova DB -> multibet.agg_cohort_acquisition
View: multibet.vw_cohort_roi (ROI por safra x source)

Execucao:
    python pipelines/agg_cohort_acquisition.py
"""

import sys
import os
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection

import pandas as pd
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- DDL ----------------------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.agg_cohort_acquisition (
    c_ecr_id            BIGINT NOT NULL,
    month_of_ftd        VARCHAR(7) NOT NULL,        -- YYYY-MM da safra
    source              VARCHAR(100) DEFAULT 'unmapped_orphans',
    c_tracker_id        VARCHAR(255),
    ftd_date            DATE,
    ftd_amount          NUMERIC(18,2),
    ggr_d0              NUMERIC(18,2) DEFAULT 0,     -- GGR no dia do FTD
    ggr_d7              NUMERIC(18,2) DEFAULT 0,     -- GGR acumulado D0-D7
    ggr_d30             NUMERIC(18,2) DEFAULT 0,     -- GGR acumulado D0-D30
    is_2nd_depositor    SMALLINT DEFAULT 0,          -- 0 ou 1
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (c_ecr_id)
);
"""

DDL_DROP_VIEW = "DROP VIEW IF EXISTS multibet.vw_cohort_roi;"
DDL_VIEW = """
CREATE VIEW multibet.vw_cohort_roi AS
SELECT
    c.month_of_ftd,
    c.source,
    COUNT(*) AS qty_players,
    ROUND(AVG(c.ftd_amount)::numeric, 2) AS avg_ftd_amount,
    ROUND(SUM(c.ggr_d0)::numeric, 2) AS total_ggr_d0,
    ROUND(SUM(c.ggr_d7)::numeric, 2) AS total_ggr_d7,
    ROUND(SUM(c.ggr_d30)::numeric, 2) AS total_ggr_d30,
    ROUND(AVG(c.ggr_d30)::numeric, 2) AS avg_ltv_d30,
    ROUND(SUM(c.is_2nd_depositor)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS pct_2nd_deposit,
    -- Spend mensal por source (da fact_attribution)
    s.monthly_spend,
    -- ROI D30 = GGR_D30 / Spend * 100
    CASE WHEN s.monthly_spend > 0
         THEN ROUND(SUM(c.ggr_d30)::numeric / s.monthly_spend * 100, 2)
         ELSE NULL END AS roi_d30_pct,
    -- Payback = Spend / GGR_D30 (quantos D30 para pagar)
    CASE WHEN SUM(c.ggr_d30) > 0
         THEN ROUND(s.monthly_spend / SUM(c.ggr_d30)::numeric, 2)
         ELSE NULL END AS payback_ratio
FROM multibet.agg_cohort_acquisition c
LEFT JOIN (
    SELECT
        TO_CHAR(dt, 'YYYY-MM') AS month_ref,
        source,
        SUM(marketing_spend) AS monthly_spend
    FROM multibet.fact_attribution
    GROUP BY 1, 2
) s ON c.month_of_ftd = s.month_ref AND c.source = s.source
GROUP BY c.month_of_ftd, c.source, s.monthly_spend
ORDER BY c.month_of_ftd DESC, total_ggr_d30 DESC;
"""

# --- Query Athena (player-level cohort) ---------------------------------------
QUERY_ATHENA = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),

-- 1. Gatekeeper com tracker+affiliate
registrations AS (
    SELECT
        c_ecr_id,
        COALESCE(NULLIF(TRIM(c_tracker_id), ''), CAST(c_affiliate_id AS VARCHAR), 'sem_tracker') AS c_tracker_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= (SELECT start_date FROM params)
),

-- 2. FTD por player (1o deposito)
first_deposits AS (
    SELECT c_ecr_id, ftd_time, ftd_amount FROM (
        SELECT
            c_ecr_id,
            c_created_time AS ftd_time,
            CAST(c_confirmed_amount_in_inhouse_ccy AS DECIMAL(18,2)) / 100.0 AS ftd_amount,
            ROW_NUMBER() OVER(PARTITION BY c_ecr_id ORDER BY c_created_time) AS rn
        FROM cashier_ec2.tbl_cashier_deposit
        WHERE c_txn_status = 'txn_confirmed_success'
    ) WHERE rn = 1
),

-- 3. 2nd deposit flag
second_deposits AS (
    SELECT c_ecr_id FROM (
        SELECT
            c_ecr_id,
            ROW_NUMBER() OVER(PARTITION BY c_ecr_id ORDER BY c_created_time) AS rn
        FROM cashier_ec2.tbl_cashier_deposit
        WHERE c_txn_status = 'txn_confirmed_success'
    ) WHERE rn = 2
),

-- 4. GGR por player/dia (bets - wins)
player_daily_ggr AS (
    SELECT
        t.c_ecr_id,
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS game_date,
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

-- 5. Cohort: FTD + GGR janelas + 2nd deposit
cohort AS (
    SELECT
        r.c_ecr_id,
        r.c_tracker_id,
        CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS ftd_date,
        f.ftd_amount,
        -- GGR D0 (dia do FTD)
        COALESCE(SUM(CASE
            WHEN g.game_date = CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
            THEN g.daily_ggr END), 0) AS ggr_d0,
        -- GGR D7 (acumulado D0 a D7)
        COALESCE(SUM(CASE
            WHEN g.game_date <= date_add('day', 7, CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE))
            THEN g.daily_ggr END), 0) AS ggr_d7,
        -- GGR D30 (acumulado D0 a D30)
        COALESCE(SUM(CASE
            WHEN g.game_date <= date_add('day', 30, CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE))
            THEN g.daily_ggr END), 0) AS ggr_d30,
        -- 2nd deposit flag
        CASE WHEN sd.c_ecr_id IS NOT NULL THEN 1 ELSE 0 END AS is_2nd_depositor
    FROM registrations r
    INNER JOIN first_deposits f ON r.c_ecr_id = f.c_ecr_id
    LEFT JOIN player_daily_ggr g ON r.c_ecr_id = g.c_ecr_id
    LEFT JOIN second_deposits sd ON r.c_ecr_id = sd.c_ecr_id
    GROUP BY r.c_ecr_id, r.c_tracker_id, f.ftd_time, f.ftd_amount,
             CASE WHEN sd.c_ecr_id IS NOT NULL THEN 1 ELSE 0 END
)

SELECT
    c_ecr_id,
    c_tracker_id,
    ftd_date,
    ftd_amount,
    ggr_d0,
    ggr_d7,
    ggr_d30,
    is_2nd_depositor
FROM cohort
"""


def setup_table():
    log.info("Criando tabela e view agg_cohort_acquisition...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_DROP_VIEW)
    execute_supernova(DDL_VIEW)
    log.info("Tabela e view prontas.")


def load_mapping() -> dict:
    rows = execute_supernova(
        "SELECT tracker_id, source FROM multibet.dim_marketing_mapping",
        fetch=True,
    )
    return {r[0]: r[1] for r in (rows or [])}


def refresh():
    log.info("Executando query no Athena (cohort player-level, D0/D7/D30)...")
    log.info("AVISO: Query pesada — JOIN de FTDs + GGR diario + 2nd deposit")
    df = query_athena(QUERY_ATHENA, database="fund_ec2")
    log.info(f"{len(df)} players obtidos do Athena.")

    if df.empty:
        log.warning("Nenhum dado. Abortando.")
        return

    # Mapear source
    mapping = load_mapping()
    df["source"] = df["c_tracker_id"].map(mapping).fillna("unmapped_orphans")
    df["month_of_ftd"] = pd.to_datetime(df["ftd_date"]).dt.strftime("%Y-%m")

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.agg_cohort_acquisition
            (c_ecr_id, month_of_ftd, source, c_tracker_id, ftd_date, ftd_amount,
             ggr_d0, ggr_d7, ggr_d30, is_2nd_depositor, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            int(row["c_ecr_id"]),
            row["month_of_ftd"],
            row["source"],
            row["c_tracker_id"],
            row["ftd_date"],
            float(row["ftd_amount"] or 0),
            float(row["ggr_d0"] or 0),
            float(row["ggr_d7"] or 0),
            float(row["ggr_d30"] or 0),
            int(row["is_2nd_depositor"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.agg_cohort_acquisition;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=2000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total = len(records)
    avg_d30 = sum(r[8] for r in records) / max(total, 1)
    pct_2nd = sum(r[9] for r in records) / max(total, 1) * 100
    log.info(f"{total:,} players inseridos | "
             f"Avg LTV D30: R$ {avg_d30:,.2f} | "
             f"2nd deposit rate: {pct_2nd:.1f}%")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline agg_cohort_acquisition ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
