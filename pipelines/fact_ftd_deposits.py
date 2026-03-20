"""
Pipeline: fact_ftd_deposits (Visao Diaria de Ticket Medio FTD)
==============================================================
Dominio 2 — Aquisicao de Jogadores (Prioridade 2)

KPI: Ticket medio FTD = Valor medio do 1o deposito por dia

Fontes (Athena, read-only):
    1. cashier_ec2.tbl_cashier_deposit  -> FTD (1o deposito confirmado)
    2. bireports_ec2.tbl_ecr            -> filtro safra (cadastros >= 2025-10-01)

Destino: Super Nova DB (PostgreSQL) -> multibet.fact_ftd_deposits

Estrategia: TRUNCATE + INSERT (snapshot completo a cada execucao).

Execucao:
    python pipelines/fact_ftd_deposits.py
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

# --- DDL (idempotente) --------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_ftd_deposits (
    dt                      DATE,
    c_tracker_id            VARCHAR(255),
    qty_ftds                INTEGER NOT NULL DEFAULT 0,
    total_ftd_amount        NUMERIC(18,2) DEFAULT 0,
    avg_ticket_ftd          NUMERIC(18,2),
    min_ticket_ftd          NUMERIC(18,2),
    max_ticket_ftd          NUMERIC(18,2),
    qty_ftds_below_50       INTEGER DEFAULT 0,
    qty_ftds_50_to_500      INTEGER DEFAULT 0,
    qty_ftds_above_500      INTEGER DEFAULT 0,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, c_tracker_id)
);
"""

# --- Query Athena -------------------------------------------------------------
QUERY_ATHENA = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),

-- 1. Gatekeeper com tracker
registrations AS (
    SELECT c_ecr_id, COALESCE(NULLIF(TRIM(c_tracker_id), ''), CAST(c_affiliate_id AS VARCHAR), 'sem_tracker') AS c_tracker_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= (SELECT start_date FROM params)
),

-- 2. Primeiro deposito de TODOS (sem filtro de data - Gatekeeper unico = registrations)
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

-- 3. JOIN: filtro de safra elimina quem nao deve estar no relatorio
ftds_joined AS (
    SELECT
        CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        r.c_tracker_id,
        f.ftd_amount,
        f.c_ecr_id
    FROM first_deposits f
    INNER JOIN registrations r ON f.c_ecr_id = r.c_ecr_id
)

-- 4. Agregacao por dt + tracker com Value Bands
SELECT
    dt,
    c_tracker_id,
    COUNT(DISTINCT c_ecr_id) AS qty_ftds,
    SUM(ftd_amount) AS total_ftd_amount,
    AVG(ftd_amount) AS avg_ticket_ftd,
    MIN(ftd_amount) AS min_ticket_ftd,
    MAX(ftd_amount) AS max_ticket_ftd,
    COUNT_IF(ftd_amount < 50) AS qty_ftds_below_50,
    COUNT_IF(ftd_amount >= 50 AND ftd_amount < 500) AS qty_ftds_50_to_500,
    COUNT_IF(ftd_amount >= 500) AS qty_ftds_above_500
FROM ftds_joined
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC
"""


def setup_table():
    """Cria schema e tabela no Super Nova DB (idempotente)."""
    log.info("Verificando/criando tabela multibet.fact_ftd_deposits...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    log.info("Tabela pronta.")


def refresh():
    """Busca dados do Athena e faz TRUNCATE + INSERT no Super Nova DB."""

    log.info("Executando query no Athena (FTD ticket medio diario)...")
    df = query_athena(QUERY_ATHENA, database="cashier_ec2")
    log.info(f"{len(df)} dias obtidos do Athena.")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_ftd_deposits
            (dt, c_tracker_id, qty_ftds, total_ftd_amount, avg_ticket_ftd,
             min_ticket_ftd, max_ticket_ftd,
             qty_ftds_below_50, qty_ftds_50_to_500, qty_ftds_above_500,
             refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            row["c_tracker_id"],
            int(row["qty_ftds"]),
            float(row["total_ftd_amount"]) if row["total_ftd_amount"] is not None else 0,
            float(row["avg_ticket_ftd"]) if row["avg_ticket_ftd"] is not None else None,
            float(row["min_ticket_ftd"]) if row["min_ticket_ftd"] is not None else None,
            float(row["max_ticket_ftd"]) if row["max_ticket_ftd"] is not None else None,
            int(row["qty_ftds_below_50"]),
            int(row["qty_ftds_50_to_500"]),
            int(row["qty_ftds_above_500"]),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_ftd_deposits;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total_ftds = sum(r[2] for r in records)
    total_amount = sum(r[3] for r in records)
    avg_global = total_amount / max(total_ftds, 1)
    log.info(f"{len(records)} dias inseridos | "
             f"Total FTDs: {total_ftds:,} | "
             f"Volume: R$ {total_amount:,.2f} | "
             f"Ticket medio global: R$ {avg_global:,.2f}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_ftd_deposits ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
