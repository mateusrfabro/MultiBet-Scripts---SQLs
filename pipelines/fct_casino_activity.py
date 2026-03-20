"""
Pipeline: fct_casino_activity (Sub-Fund Isolation — Logica Mauro adaptada)
==========================================================================
Separacao precisa de Real vs Bonus usando sub-tabelas oficiais da Pragmatic:
    - tbl_realcash_sub_fund_txn (parcela dinheiro real)
    - tbl_bonus_sub_fund_txn (parcela bonus: drp, crp, wrp, rrp)
    - tbl_real_fund_txn_type_mst (classificacao oficial DB/CR, cancel)

Grao: dt (dia BRT)
KPIs:
    - casino_real_bet, casino_real_win, casino_real_ggr
    - casino_bonus_bet, casino_bonus_win, casino_bonus_ggr
    - casino_total_bet, casino_total_win, casino_total_ggr
    - qty_players

Fontes (Athena fund_ec2):
    1. tbl_real_fund_txn (master)
    2. tbl_realcash_sub_fund_txn (sub - real cash)
    3. tbl_bonus_sub_fund_txn (sub - bonus)
    4. tbl_real_fund_txn_type_mst (tipos)
    5. ecr_ec2.tbl_ecr_flags (excluir test users)

Destino: Super Nova DB -> multibet.fct_casino_activity
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fct_casino_activity.py
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
CREATE TABLE IF NOT EXISTS multibet.fct_casino_activity (
    dt                      DATE PRIMARY KEY,
    qty_players             INTEGER DEFAULT 0,
    casino_real_bet         NUMERIC(18,2) DEFAULT 0,
    casino_real_win         NUMERIC(18,2) DEFAULT 0,
    casino_real_ggr         NUMERIC(18,2) DEFAULT 0,
    casino_bonus_bet        NUMERIC(18,2) DEFAULT 0,
    casino_bonus_win        NUMERIC(18,2) DEFAULT 0,
    casino_bonus_ggr        NUMERIC(18,2) DEFAULT 0,
    casino_total_bet        NUMERIC(18,2) DEFAULT 0,
    casino_total_win        NUMERIC(18,2) DEFAULT 0,
    casino_total_ggr        NUMERIC(18,2) DEFAULT 0,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW()
);
"""

# --- Query Athena (Sub-Fund Isolation — adaptado do Mauro) ----------------
# Diferenças vs Mauro:
#   - Schema: fund -> fund_ec2, ecr -> ecr_ec2
#   - Timezone: CONVERT_TIMEZONE -> AT TIME ZONE (Presto)
#   - Boolean: c_is_cancel_txn = 0/1 -> = false/true (Athena)
#   - Valores: c_amount_in_house_ccy (Mauro) vs c_amount_in_ecr_ccy (mesmo em BRL)
#   - Sub-CTEs pre-agregadas por c_fund_txn_id para evitar fan-out

QUERY_ATHENA = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),

-- Pre-agregar sub-tabelas por txn_id (evita fan-out no JOIN)
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

-- Transacoes base com sub-fund isolation
base AS (
    SELECT
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        t.c_ecr_id,
        m.c_op_type,
        m.c_is_cancel_txn,
        -- Real = realcash + deposit reward points (drp)
        COALESCE(r.real_amount, 0) + COALESCE(b.drp_amount, 0) AS real_val,
        -- Bonus = crp + wrp + rrp (bonus points)
        COALESCE(b.bonus_points, 0) AS bonus_val
    FROM fund_ec2.tbl_real_fund_txn t
    LEFT JOIN sub_real r ON t.c_txn_id = r.c_fund_txn_id
    LEFT JOIN sub_bonus b ON t.c_txn_id = b.c_fund_txn_id
    JOIN fund_ec2.tbl_real_fund_txn_type_mst m ON t.c_txn_type = m.c_txn_type
    JOIN ecr_ec2.tbl_ecr_flags ef ON t.c_ecr_id = ef.c_ecr_id
    WHERE t.c_start_time >= (SELECT start_date FROM params)
      AND t.c_txn_status = 'SUCCESS'
      AND m.c_is_gaming_txn = 'Y'
      AND t.c_product_id = 'CASINO'
      AND ef.c_test_user = false
)

-- Agregacao diaria
SELECT
    dt,
    COUNT(DISTINCT c_ecr_id) AS qty_players,
    -- Real
    SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val
             WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -real_val
             ELSE 0 END) / 100.0 AS casino_real_bet,
    SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val
             WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -real_val
             ELSE 0 END) / 100.0 AS casino_real_win,
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -real_val
              ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -real_val
              ELSE 0 END)) / 100.0 AS casino_real_ggr,
    -- Bonus
    SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN bonus_val
             WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -bonus_val
             ELSE 0 END) / 100.0 AS casino_bonus_bet,
    SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN bonus_val
             WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -bonus_val
             ELSE 0 END) / 100.0 AS casino_bonus_win,
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN bonus_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -bonus_val
              ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN bonus_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -bonus_val
              ELSE 0 END)) / 100.0 AS casino_bonus_ggr,
    -- Total (Real + Bonus)
    SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
             WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
             ELSE 0 END) / 100.0 AS casino_total_bet,
    SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
             WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
             ELSE 0 END) / 100.0 AS casino_total_win,
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
              ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
              ELSE 0 END)) / 100.0 AS casino_total_ggr
FROM base
GROUP BY 1
ORDER BY 1 DESC
"""


def setup_table():
    log.info("Criando tabela fct_casino_activity...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Sub-Fund Isolation Casino)...")
    log.info("Fontes: tbl_real_fund_txn + tbl_realcash_sub_fund_txn + tbl_bonus_sub_fund_txn + tbl_real_fund_txn_type_mst")
    df = query_athena(QUERY_ATHENA, database="fund_ec2")
    log.info(f"{len(df)} dias obtidos do Athena.")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fct_casino_activity
            (dt, qty_players, casino_real_bet, casino_real_win, casino_real_ggr,
             casino_bonus_bet, casino_bonus_win, casino_bonus_ggr,
             casino_total_bet, casino_total_win, casino_total_ggr, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            int(row["qty_players"]),
            float(row["casino_real_bet"] or 0),
            float(row["casino_real_win"] or 0),
            float(row["casino_real_ggr"] or 0),
            float(row["casino_bonus_bet"] or 0),
            float(row["casino_bonus_win"] or 0),
            float(row["casino_bonus_ggr"] or 0),
            float(row["casino_total_bet"] or 0),
            float(row["casino_total_win"] or 0),
            float(row["casino_total_ggr"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fct_casino_activity;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total_real_ggr = sum(r[4] for r in records)
    total_bonus_ggr = sum(r[7] for r in records)
    total_ggr = sum(r[10] for r in records)
    total_real_bet = sum(r[2] for r in records)
    total_bonus_bet = sum(r[5] for r in records)
    log.info(f"{len(records)} dias inseridos")
    log.info(f"  Real:  Bet R$ {total_real_bet:,.2f} | GGR R$ {total_real_ggr:,.2f}")
    log.info(f"  Bonus: Bet R$ {total_bonus_bet:,.2f} | GGR R$ {total_bonus_ggr:,.2f}")
    log.info(f"  Total: GGR R$ {total_ggr:,.2f}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fct_casino_activity (Sub-Fund Isolation) ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
