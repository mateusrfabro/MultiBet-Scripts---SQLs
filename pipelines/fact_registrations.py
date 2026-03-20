"""
Pipeline: fact_registrations (Visão Diária de Aquisição)
=========================================================
Domínio 2 — Aquisição de Jogadores (Prioridade 2)

Fontes (Athena, read-only):
    1. bireports_ec2.tbl_ecr             → cadastros, data registro
    2. ecr_ec2.tbl_ecr_signup_info       → dispositivo (c_channel)
    3. cashier_ec2.tbl_cashier_deposit   → FTD (1° depósito confirmado)
    4. ecr_ec2.tbl_ecr_kyc_level         → KYC aprovado (c_level)

Destino: Super Nova DB (PostgreSQL) → multibet.fact_registrations

KPIs por dia:
    - qty_registrations      : novos cadastros
    - qty_ftds               : cadastros que fizeram FTD (visão coorte)
    - ftd_rate               : qty_ftds / qty_registrations × 100
    - avg_time_to_ftd_h    : média de minutos entre cadastro e 1° depósito
    - kyc_pass_rate          : % de cadastros com KYC aprovado
    - device_mobile/desktop/tablet/nao_informado : split por dispositivo

Estratégia: TRUNCATE + INSERT (snapshot completo a cada execução).

Execução:
    python pipelines/fact_registrations.py
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

# ─── DDL (idempotente) ────────────────────────────────────────────────────────
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_registrations (
    dt                      DATE PRIMARY KEY,
    qty_registrations       INTEGER NOT NULL,
    qty_ftds                INTEGER NOT NULL DEFAULT 0,
    ftd_rate                NUMERIC(10,4) DEFAULT 0,
    avg_time_to_ftd_h       NUMERIC(10,2),
    kyc_pass_rate           NUMERIC(10,4) DEFAULT 0,
    device_mobile           INTEGER DEFAULT 0,
    device_desktop          INTEGER DEFAULT 0,
    device_tablet           INTEGER DEFAULT 0,
    device_nao_informado    INTEGER DEFAULT 0,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW()
);
"""

# ─── Query Athena (visão diária de coorte) ────────────────────────────────────
# Fontes validadas empiricamente em 18/03/2026:
#   - bireports_ec2.tbl_ecr           → c_ecr_id, c_sign_up_time
#   - ecr_ec2.tbl_ecr_signup_info     → c_ecr_id, c_channel (mobile/web/desktop/tablet)
#   - cashier_ec2.tbl_cashier_deposit → FTD via ROW_NUMBER, c_txn_status = 'txn_confirmed_success'
#   - ecr_ec2.tbl_ecr_kyc_level       → c_level IN ('KYC_1','KYC_2') = aprovado

QUERY_ATHENA = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),

-- 1. Base de Cadastros e Dispositivos (Dia do Cadastro)
registrations AS (
    SELECT
        e.c_ecr_id,
        e.c_sign_up_time,
        CAST(e.c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        CASE
            WHEN s.c_channel = 'mobile' THEN 'Mobile'
            WHEN s.c_channel IN ('desktop','web','WEB') THEN 'Desktop'
            WHEN s.c_channel = 'tablet' THEN 'Tablet'
            ELSE 'Nao Informado'
        END AS device
    FROM bireports_ec2.tbl_ecr e
    LEFT JOIN ecr_ec2.tbl_ecr_signup_info s ON e.c_ecr_id = s.c_ecr_id
    WHERE e.c_sign_up_time >= (SELECT start_date FROM params)
),

-- 2. Base de FTDs (Primeiro deposito confirmado por player)
ftds AS (
    SELECT c_ecr_id, ftd_time FROM (
        SELECT
            c_ecr_id,
            c_created_time AS ftd_time,
            ROW_NUMBER() OVER(PARTITION BY c_ecr_id ORDER BY c_created_time) AS rn
        FROM cashier_ec2.tbl_cashier_deposit
        WHERE c_txn_status = 'txn_confirmed_success'
    ) WHERE rn = 1
),

-- 3. Base de KYC Aprovado
kyc_approved AS (
    SELECT DISTINCT c_ecr_id
    FROM ecr_ec2.tbl_ecr_kyc_level
    WHERE c_level IN ('KYC_1', 'KYC_2')
),

-- 4. Agregacao Diaria de Cadastros
reg_daily AS (
    SELECT
        r.dt,
        COUNT(DISTINCT r.c_ecr_id) AS qty_registrations,
        COUNT(DISTINCT k.c_ecr_id) AS kyc_aprovados,
        COUNT_IF(r.device = 'Mobile') AS device_mobile,
        COUNT_IF(r.device = 'Desktop') AS device_desktop,
        COUNT_IF(r.device = 'Tablet') AS device_tablet,
        COUNT_IF(r.device = 'Nao Informado') AS device_nao_informado
    FROM registrations r
    LEFT JOIN kyc_approved k ON r.c_ecr_id = k.c_ecr_id
    GROUP BY 1
),

-- 5. Agregacao Diaria de FTDs (No dia do deposito)
ftd_daily AS (
    SELECT
        CAST(f.ftd_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        COUNT(DISTINCT f.c_ecr_id) AS qty_ftds,
        -- Tempo medio convertido para HORAS (compliance KPI Map)
        AVG(date_diff('second', r.c_sign_up_time, f.ftd_time)) / 3600.0 AS avg_time_h
    FROM ftds f
    -- INNER JOIN: so contamos FTDs de jogadores que existem na CTE registrations (safra >= 2025-10-01)
    INNER JOIN registrations r ON f.c_ecr_id = r.c_ecr_id
    GROUP BY 1
)

-- 6. Consolidacao Final
SELECT
    COALESCE(rd.dt, fd.dt) AS dt,
    COALESCE(rd.qty_registrations, 0) AS qty_registrations,
    COALESCE(fd.qty_ftds, 0) AS qty_ftds,
    CASE
        WHEN COALESCE(rd.qty_registrations, 0) > 0
        THEN (CAST(COALESCE(fd.qty_ftds, 0) AS DOUBLE) / rd.qty_registrations) * 100
        ELSE 0
    END AS ftd_rate,
    fd.avg_time_h AS avg_time_to_ftd_h,  -- NULL quando nao ha FTDs no dia
    CASE
        WHEN COALESCE(rd.qty_registrations, 0) > 0
        THEN (CAST(rd.kyc_aprovados AS DOUBLE) / rd.qty_registrations) * 100
        ELSE 0
    END AS kyc_pass_rate,
    COALESCE(rd.device_mobile, 0) AS device_mobile,
    COALESCE(rd.device_desktop, 0) AS device_desktop,
    COALESCE(rd.device_tablet, 0) AS device_tablet,
    COALESCE(rd.device_nao_informado, 0) AS device_nao_informado
FROM reg_daily rd
FULL OUTER JOIN ftd_daily fd ON rd.dt = fd.dt
ORDER BY 1 DESC
"""


def setup_table():
    """Cria schema e tabela no Super Nova DB (idempotente)."""
    log.info("Verificando/criando tabela multibet.fact_registrations...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    log.info("Tabela pronta.")


def refresh():
    """Busca dados do Athena e faz TRUNCATE + INSERT no Super Nova DB."""

    # 1. Query no Athena (4 fontes, 6 KPIs)
    log.info("Executando query no Athena (4 fontes, visão diária de coorte)...")
    log.info("Fontes: bireports_ec2.tbl_ecr + ecr_ec2.tbl_ecr_signup_info "
             "+ cashier_ec2.tbl_cashier_deposit + ecr_ec2.tbl_ecr_kyc_level")
    df = query_athena(QUERY_ATHENA, database="bireports_ec2")
    log.info(f"{len(df)} dias obtidos do Athena.")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    # 2. Inserir no Super Nova DB
    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_registrations
            (dt, qty_registrations, qty_ftds, ftd_rate, avg_time_to_ftd_h,
             kyc_pass_rate, device_mobile, device_desktop, device_tablet,
             device_nao_informado, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            int(row["qty_registrations"]),
            int(row["qty_ftds"]),
            float(row["ftd_rate"]) if row["ftd_rate"] is not None else 0,
            float(row["avg_time_to_ftd_h"]) if row["avg_time_to_ftd_h"] is not None else None,
            float(row["kyc_pass_rate"]) if row["kyc_pass_rate"] is not None else 0,
            int(row["device_mobile"]),
            int(row["device_desktop"]),
            int(row["device_tablet"]),
            int(row["device_nao_informado"]),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_registrations;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    # 3. Log resumo
    total_regs = sum(r[1] for r in records)
    total_ftds = sum(r[2] for r in records)
    log.info(f"{len(records)} dias inseridos | "
             f"Total cadastros: {total_regs:,} | Total FTDs: {total_ftds:,} | "
             f"FTD Rate global: {total_ftds/max(total_regs,1)*100:.2f}%")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_registrations (visão diária) ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluído ===")
