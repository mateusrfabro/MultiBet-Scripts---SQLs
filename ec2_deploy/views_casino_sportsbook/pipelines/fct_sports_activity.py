"""
Pipeline: fct_sports_activity (Sub-Fund Isolation — Logica Mauro adaptada)
==========================================================================
Mesmo padrao da fct_casino_activity, filtrado por c_product_id = 'SPORTS_BOOK'.

Grao: dt (dia BRT)
Destino: Super Nova DB -> multibet.fct_sports_activity
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fct_sports_activity.py
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
CREATE TABLE IF NOT EXISTS multibet.fct_sports_activity (
    dt                      DATE PRIMARY KEY,
    qty_players             INTEGER DEFAULT 0,
    qty_bets                INTEGER DEFAULT 0,
    sports_real_bet         NUMERIC(18,2) DEFAULT 0,
    sports_real_win         NUMERIC(18,2) DEFAULT 0,
    sports_real_ggr         NUMERIC(18,2) DEFAULT 0,
    sports_bonus_bet        NUMERIC(18,2) DEFAULT 0,
    sports_bonus_win        NUMERIC(18,2) DEFAULT 0,
    sports_bonus_ggr        NUMERIC(18,2) DEFAULT 0,
    sports_total_bet        NUMERIC(18,2) DEFAULT 0,
    sports_total_win        NUMERIC(18,2) DEFAULT 0,
    sports_total_ggr        NUMERIC(18,2) DEFAULT 0,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Garante a coluna em ambientes que ja tinham a tabela criada antes do v4
ALTER TABLE multibet.fct_sports_activity
    ADD COLUMN IF NOT EXISTS qty_bets INTEGER DEFAULT 0;
"""

QUERY_ATHENA = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),
sub_real AS (
    SELECT c_fund_txn_id,
           SUM(CAST(c_amount_in_ecr_ccy AS DECIMAL(18,2))) AS real_amount
    FROM fund_ec2.tbl_realcash_sub_fund_txn
    GROUP BY 1
),
sub_bonus AS (
    SELECT c_fund_txn_id,
           SUM(CAST(c_drp_amount_in_ecr_ccy AS DECIMAL(18,2))) AS drp_amount,
           SUM(CAST(c_crp_amount_in_ecr_ccy AS DECIMAL(18,2))
             + CAST(c_wrp_amount_in_ecr_ccy AS DECIMAL(18,2))
             + CAST(c_rrp_amount_in_ecr_ccy AS DECIMAL(18,2))) AS bonus_points
    FROM fund_ec2.tbl_bonus_sub_fund_txn
    GROUP BY 1
),
base AS (
    SELECT
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        t.c_ecr_id,
        t.c_txn_type AS txn_type,
        m.c_op_type,
        m.c_is_cancel_txn,
        COALESCE(r.real_amount, 0) + COALESCE(b.drp_amount, 0) AS real_val,
        COALESCE(b.bonus_points, 0) AS bonus_val
    FROM fund_ec2.tbl_real_fund_txn t
    LEFT JOIN sub_real r ON t.c_txn_id = r.c_fund_txn_id
    LEFT JOIN sub_bonus b ON t.c_txn_id = b.c_fund_txn_id
    JOIN fund_ec2.tbl_real_fund_txn_type_mst m ON t.c_txn_type = m.c_txn_type
    JOIN ecr_ec2.tbl_ecr_flags ef ON t.c_ecr_id = ef.c_ecr_id
    WHERE t.c_start_time >= (SELECT start_date FROM params)
      AND t.c_txn_status = 'SUCCESS'
      AND m.c_is_gaming_txn = 'Y'
      AND t.c_product_id = 'SPORTS_BOOK'
      AND ef.c_test_user = false
)
SELECT
    dt,
    COUNT(DISTINCT c_ecr_id) AS qty_players,
    -- FIX Gusta gap menor #4 (10/04/2026): qty_bets via SB_BUYIN (c_txn_type=59)
    -- Conta bilhetes na MESMA fonte do GGR (fund_ec2 c_start_time), entao bate
    -- 1-pra-1 com sports_real_bet. Solucao Option D do auditor: zero divergencia
    -- com back-office (settle vs txn date) e custo marginal zero.
    SUM(CASE WHEN txn_type = 59 AND c_is_cancel_txn = false THEN 1 ELSE 0 END) AS qty_bets,
    SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val
             WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -real_val
             ELSE 0 END) / 100.0 AS sports_real_bet,
    SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val
             WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -real_val
             ELSE 0 END) / 100.0 AS sports_real_win,
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -real_val ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -real_val ELSE 0 END)) / 100.0 AS sports_real_ggr,
    SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN bonus_val
             WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -bonus_val
             ELSE 0 END) / 100.0 AS sports_bonus_bet,
    SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN bonus_val
             WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -bonus_val
             ELSE 0 END) / 100.0 AS sports_bonus_win,
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN bonus_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -bonus_val ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN bonus_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -bonus_val ELSE 0 END)) / 100.0 AS sports_bonus_ggr,
    SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
             WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
             ELSE 0 END) / 100.0 AS sports_total_bet,
    SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
             WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
             ELSE 0 END) / 100.0 AS sports_total_win,
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)) / 100.0 AS sports_total_ggr
FROM base
GROUP BY 1
ORDER BY 1 DESC
"""


def setup_table():
    log.info("Criando tabela fct_sports_activity...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Sub-Fund Isolation Sports)...")
    df = query_athena(QUERY_ATHENA, database="fund_ec2")
    log.info(f"{len(df)} dias obtidos do Athena.")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fct_sports_activity
            (dt, qty_players, qty_bets,
             sports_real_bet, sports_real_win, sports_real_ggr,
             sports_bonus_bet, sports_bonus_win, sports_bonus_ggr,
             sports_total_bet, sports_total_win, sports_total_ggr, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            int(row["qty_players"]),
            int(row["qty_bets"] or 0),
            float(row["sports_real_bet"] or 0),
            float(row["sports_real_win"] or 0),
            float(row["sports_real_ggr"] or 0),
            float(row["sports_bonus_bet"] or 0),
            float(row["sports_bonus_win"] or 0),
            float(row["sports_bonus_ggr"] or 0),
            float(row["sports_total_bet"] or 0),
            float(row["sports_total_win"] or 0),
            float(row["sports_total_ggr"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fct_sports_activity;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    # Indices ajustados pra acomodar qty_bets na posicao 2 (v4)
    total_real_ggr = sum(r[5] for r in records)
    total_bonus_ggr = sum(r[8] for r in records)
    total_ggr = sum(r[11] for r in records)
    total_bets = sum(r[2] for r in records)
    log.info(f"{len(records)} dias inseridos")
    log.info(f"  Bilhetes: {total_bets:,}")
    log.info(f"  Real GGR:  R$ {total_real_ggr:,.2f}")
    log.info(f"  Bonus GGR: R$ {total_bonus_ggr:,.2f}")
    log.info(f"  Total GGR: R$ {total_ggr:,.2f}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fct_sports_activity (Sub-Fund Isolation) ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
