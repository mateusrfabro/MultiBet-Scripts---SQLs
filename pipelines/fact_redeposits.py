"""
Pipeline: fact_redeposits (Metricas de Redeposito — P3)
========================================================
KPIs por player:
    - total_deposits: total de depositos confirmados
    - redeposit_count: depositos apos o FTD (total - 1)
    - is_redepositor_d7: flag se fez 2o deposito em ate 7 dias
    - avg_redeposit_amount: ticket medio dos redepositos
    - avg_days_between_deposits: intervalo medio entre depositos consecutivos
    - deposits_per_month: frequencia mensal de depositos

Fontes (Athena):
    1. bireports_ec2.tbl_ecr (Gatekeeper)
    2. cashier_ec2.tbl_cashier_deposit (todos depositos confirmados)

Destino: Super Nova DB -> multibet.fact_redeposits
Estrategia: TRUNCATE + INSERT
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
CREATE TABLE IF NOT EXISTS multibet.fact_redeposits (
    c_ecr_id                    BIGINT PRIMARY KEY,
    c_tracker_id                VARCHAR(255),
    ftd_date                    DATE,
    ftd_amount                  NUMERIC(18,2),
    total_deposits              INTEGER DEFAULT 0,
    redeposit_count             INTEGER DEFAULT 0,
    is_redepositor_d7           SMALLINT DEFAULT 0,
    second_deposit_date         DATE,
    days_to_second_deposit      INTEGER,
    avg_redeposit_amount        NUMERIC(18,2),
    total_redeposit_amount      NUMERIC(18,2) DEFAULT 0,
    avg_days_between_deposits   NUMERIC(10,2),
    deposits_per_month          NUMERIC(10,2),
    refreshed_at                TIMESTAMPTZ DEFAULT NOW()
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

-- 2. Todos depositos confirmados com ranking
all_deposits AS (
    SELECT
        d.c_ecr_id,
        CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dep_date,
        CAST(d.c_confirmed_amount_in_inhouse_ccy AS DECIMAL(18,2)) / 100.0 AS dep_amount,
        ROW_NUMBER() OVER(PARTITION BY d.c_ecr_id ORDER BY d.c_created_time) AS dep_rank,
        LAG(CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE))
            OVER(PARTITION BY d.c_ecr_id ORDER BY d.c_created_time) AS prev_dep_date
    FROM cashier_ec2.tbl_cashier_deposit d
    INNER JOIN registrations r ON d.c_ecr_id = r.c_ecr_id
    WHERE d.c_txn_status = 'txn_confirmed_success'
),

-- 3. Agregacao por player
player_deposits AS (
    SELECT
        c_ecr_id,
        -- FTD (rank 1)
        MIN(CASE WHEN dep_rank = 1 THEN dep_date END) AS ftd_date,
        MIN(CASE WHEN dep_rank = 1 THEN dep_amount END) AS ftd_amount,
        -- 2nd deposit
        MIN(CASE WHEN dep_rank = 2 THEN dep_date END) AS second_deposit_date,
        -- Totais
        COUNT(*) AS total_deposits,
        -- Redeposits (excluindo FTD)
        COUNT(*) - 1 AS redeposit_count,
        -- Ticket medio redeposits (excluindo FTD)
        AVG(CASE WHEN dep_rank > 1 THEN dep_amount END) AS avg_redeposit_amount,
        SUM(CASE WHEN dep_rank > 1 THEN dep_amount ELSE 0 END) AS total_redeposit_amount,
        -- Intervalo medio entre depositos (dias)
        AVG(CASE WHEN prev_dep_date IS NOT NULL
                 THEN date_diff('day', prev_dep_date, dep_date) END) AS avg_days_between
    FROM all_deposits
    GROUP BY 1
)

-- 4. Resultado final
SELECT
    pd.c_ecr_id,
    r.c_tracker_id,
    pd.ftd_date,
    pd.ftd_amount,
    pd.total_deposits,
    pd.redeposit_count,
    -- Flag: fez 2o deposito em ate 7 dias
    CASE WHEN pd.second_deposit_date IS NOT NULL
          AND date_diff('day', pd.ftd_date, pd.second_deposit_date) <= 7
         THEN 1 ELSE 0 END AS is_redepositor_d7,
    pd.second_deposit_date,
    -- Dias ate 2o deposito
    CASE WHEN pd.second_deposit_date IS NOT NULL
         THEN date_diff('day', pd.ftd_date, pd.second_deposit_date) END AS days_to_second_deposit,
    pd.avg_redeposit_amount,
    pd.total_redeposit_amount,
    pd.avg_days_between AS avg_days_between_deposits,
    -- Frequencia mensal: depositos / meses desde FTD
    CASE WHEN date_diff('day', pd.ftd_date, CURRENT_DATE) > 30
         THEN ROUND(CAST(pd.total_deposits AS DOUBLE) / (date_diff('day', pd.ftd_date, CURRENT_DATE) / 30.0), 2)
         ELSE CAST(pd.total_deposits AS DOUBLE) END AS deposits_per_month
FROM player_deposits pd
INNER JOIN (
    SELECT c_ecr_id, c_tracker_id,
           ROW_NUMBER() OVER(PARTITION BY c_ecr_id ORDER BY c_tracker_id) AS rn
    FROM registrations
) r ON pd.c_ecr_id = r.c_ecr_id AND r.rn = 1
WHERE pd.ftd_date IS NOT NULL
"""


def setup_table():
    log.info("Criando tabela fact_redeposits...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (redeposits player-level)...")
    df = query_athena(QUERY_ATHENA, database="cashier_ec2")
    log.info(f"{len(df)} players obtidos.")

    if df.empty:
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_redeposits
            (c_ecr_id, c_tracker_id, ftd_date, ftd_amount, total_deposits,
             redeposit_count, is_redepositor_d7, second_deposit_date,
             days_to_second_deposit, avg_redeposit_amount, total_redeposit_amount,
             avg_days_between_deposits, deposits_per_month, refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    import pandas as pd

    def safe_int(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(val)

    def safe_float(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return float(val)

    records = []
    for _, row in df.iterrows():
        records.append((
            int(row["c_ecr_id"]),
            row["c_tracker_id"],
            row["ftd_date"] if not pd.isna(row["ftd_date"]) else None,
            safe_float(row["ftd_amount"]),
            safe_int(row["total_deposits"]) or 0,
            safe_int(row["redeposit_count"]) or 0,
            safe_int(row["is_redepositor_d7"]) or 0,
            row["second_deposit_date"] if not pd.isna(row.get("second_deposit_date", None) or float('nan')) else None,
            safe_int(row["days_to_second_deposit"]),
            safe_float(row["avg_redeposit_amount"]),
            safe_float(row["total_redeposit_amount"]) or 0,
            safe_float(row["avg_days_between_deposits"]),
            safe_float(row["deposits_per_month"]),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_redeposits;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total_players = len(records)
    redepositors = sum(1 for r in records if r[5] > 0)
    d7_rate = sum(1 for r in records if r[6] == 1) / total_players * 100 if total_players else 0
    avg_ticket = sum(r[9] or 0 for r in records if r[9]) / max(sum(1 for r in records if r[9]), 1)
    avg_interval = sum(r[11] or 0 for r in records if r[11]) / max(sum(1 for r in records if r[11]), 1)

    log.info(f"{total_players:,} players | {redepositors:,} redepositors ({redepositors/total_players*100:.1f}%)")
    log.info(f"  2nd deposit D7 rate: {d7_rate:.1f}%")
    log.info(f"  Avg redeposit ticket: R$ {avg_ticket:,.2f}")
    log.info(f"  Avg days between deposits: {avg_interval:.1f} dias")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_redeposits ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
