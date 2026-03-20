"""
Pipeline: fact_gaming_activity_daily (GGR por Tracker/Dia — Sub-Fund Isolation v2)
===================================================================================
Dominio 2 — Aquisicao de Jogadores (Prioridade 2)
Tabela base para fact_attribution (CPA, CAC, Receita por afiliado)

KPIs por dia x tracker:
    - qty_players, total_bets, total_wins, ggr, margin_pct
    - bonus_cost (via sub-fund isolation), ngr (ggr - bonus_cost)
    - ggr_casino, ggr_sports (split por c_product_id)
    - max_single_win_val, rollback_count, rollback_total (monitoramento)

Fontes (Athena — Sub-Fund Isolation):
    1. bireports_ec2.tbl_ecr                -> Gatekeeper (safra + tracker_id)
    2. fund_ec2.tbl_real_fund_txn           -> Transacoes master
    3. fund_ec2.tbl_realcash_sub_fund_txn   -> Parcela dinheiro real
    4. fund_ec2.tbl_bonus_sub_fund_txn      -> Parcela bonus (drp, crp, wrp, rrp)
    5. fund_ec2.tbl_real_fund_txn_type_mst  -> Classificacao oficial (DB/CR, cancel)
    6. ecr_ec2.tbl_ecr_flags               -> Filtro test users

Metodo validado 19/03/2026:
    - Sub-fund isolation (mesma logica do Mauro/Redshift)
    - Novembro/2025: diff R$ 13,98 em R$ 270M (0.000%)
    - Test users filtrados (c_test_user = false)
    - c_is_cancel_txn = boolean no Athena (nao integer)

Destino: Super Nova DB -> multibet.fact_gaming_activity_daily
Estrategia: TRUNCATE + INSERT

Execucao:
    python pipelines/fact_gaming_activity_daily.py
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
CREATE TABLE IF NOT EXISTS multibet.fact_gaming_activity_daily (
    dt                      DATE,
    c_tracker_id            VARCHAR(255),
    qty_players             INTEGER DEFAULT 0,
    total_bets              NUMERIC(18,2) DEFAULT 0,
    total_wins              NUMERIC(18,2) DEFAULT 0,
    ggr                     NUMERIC(18,2) DEFAULT 0,
    bonus_cost              NUMERIC(18,2) DEFAULT 0,
    ngr                     NUMERIC(18,2) DEFAULT 0,
    margin_pct              NUMERIC(10,4) DEFAULT 0,
    ggr_casino              NUMERIC(18,2) DEFAULT 0,
    ggr_sports              NUMERIC(18,2) DEFAULT 0,
    max_single_win_val      NUMERIC(18,2),
    rollback_count          INTEGER DEFAULT 0,
    rollback_total          NUMERIC(18,2) DEFAULT 0,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, c_tracker_id)
);
"""

# --- Query Athena (Sub-Fund Isolation — validado com Mauro 19/03/2026) ----
QUERY_ATHENA = """
WITH params AS (
    SELECT TIMESTAMP '2025-10-01' AS start_date
),

-- 1. Gatekeeper (safra + tracker via COALESCE affiliate)
registrations AS (
    SELECT c_ecr_id,
           COALESCE(NULLIF(TRIM(c_tracker_id), ''), CAST(c_affiliate_id AS VARCHAR), 'sem_tracker') AS c_tracker_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= (SELECT start_date FROM params)
),

-- 2. Sub-tabelas pre-agregadas (evita fan-out)
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

-- 3. Transacoes com sub-fund isolation + Gatekeeper + test user filter
base AS (
    SELECT
        reg.c_tracker_id,
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        t.c_ecr_id,
        t.c_product_id,
        m.c_op_type,
        m.c_is_cancel_txn,
        -- Real = realcash + deposit reward points
        COALESCE(r.real_amount, 0) + COALESCE(b.drp_amount, 0) AS real_val,
        -- Bonus = crp + wrp + rrp
        COALESCE(b.bonus_points, 0) AS bonus_val
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN registrations reg ON t.c_ecr_id = reg.c_ecr_id
    LEFT JOIN sub_real r ON t.c_txn_id = r.c_fund_txn_id
    LEFT JOIN sub_bonus b ON t.c_txn_id = b.c_fund_txn_id
    JOIN fund_ec2.tbl_real_fund_txn_type_mst m ON t.c_txn_type = m.c_txn_type
    JOIN ecr_ec2.tbl_ecr_flags ef ON t.c_ecr_id = ef.c_ecr_id
    WHERE t.c_start_time >= (SELECT start_date FROM params)
      AND t.c_txn_status = 'SUCCESS'
      AND m.c_is_gaming_txn = 'Y'
      AND ef.c_test_user = false
)

-- 4. Agregacao por dia x tracker
SELECT
    dt,
    c_tracker_id,
    COUNT(DISTINCT c_ecr_id) AS qty_players,

    -- Bets (Real + Bonus)
    SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
             WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
             ELSE 0 END) / 100.0 AS total_bets,

    -- Wins (Real + Bonus)
    SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
             WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val)
             ELSE 0 END) / 100.0 AS total_wins,

    -- GGR = Bets - Wins (Real + Bonus)
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)) / 100.0 AS ggr,

    -- Bonus Cost = Bonus Bets - Bonus Wins (custo liquido do bonus)
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN bonus_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -bonus_val ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN bonus_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -bonus_val ELSE 0 END)) / 100.0 AS bonus_cost,

    -- NGR = GGR Real (sem bonus)
    (SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val
              WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -real_val ELSE 0 END)
   - SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val
              WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -real_val ELSE 0 END)) / 100.0 AS ngr,

    -- Margin %
    CASE WHEN SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
                       WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END) > 0
         THEN ((SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
                         WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)
              -  SUM(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
                          WHEN c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END))
              / SUM(CASE WHEN c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
                         WHEN c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)) * 100
         ELSE 0 END AS margin_pct,

    -- GGR Casino (Total = Real + Bonus, filtrado por product_id)
    (SUM(CASE WHEN c_product_id = 'CASINO' AND c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_product_id = 'CASINO' AND c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)
   - SUM(CASE WHEN c_product_id = 'CASINO' AND c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_product_id = 'CASINO' AND c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)) / 100.0 AS ggr_casino,

    -- GGR Sports
    (SUM(CASE WHEN c_product_id = 'SPORTS_BOOK' AND c_op_type = 'DB' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_product_id = 'SPORTS_BOOK' AND c_op_type = 'CR' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)
   - SUM(CASE WHEN c_product_id = 'SPORTS_BOOK' AND c_op_type = 'CR' AND c_is_cancel_txn = false THEN real_val + bonus_val
              WHEN c_product_id = 'SPORTS_BOOK' AND c_op_type = 'DB' AND c_is_cancel_txn = true  THEN -(real_val + bonus_val) ELSE 0 END)) / 100.0 AS ggr_sports,

    -- Max single win (outlier monitor)
    MAX(CASE WHEN c_op_type = 'CR' AND c_is_cancel_txn = false THEN (real_val + bonus_val) / 100.0 ELSE 0 END) AS max_single_win_val,

    -- Rollback monitor
    COUNT(CASE WHEN c_is_cancel_txn = true THEN 1 END) AS rollback_count,
    SUM(CASE WHEN c_is_cancel_txn = true THEN (real_val + bonus_val) / 100.0 ELSE 0 END) AS rollback_total

FROM base
GROUP BY 1, 2
ORDER BY 1 DESC, 6 DESC
"""


def setup_table():
    log.info("Verificando/criando tabela multibet.fact_gaming_activity_daily...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Sub-Fund Isolation v2, por tracker/dia)...")
    log.info("Fontes: tbl_real_fund_txn + sub_real + sub_bonus + txn_type_mst + ecr_flags")
    df = query_athena(QUERY_ATHENA, database="fund_ec2")
    log.info(f"{len(df)} linhas obtidas do Athena (dias x trackers).")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_gaming_activity_daily
            (dt, c_tracker_id, qty_players, total_bets, total_wins, ggr,
             bonus_cost, ngr, margin_pct, ggr_casino, ggr_sports,
             max_single_win_val, rollback_count, rollback_total, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            row["c_tracker_id"],
            int(row["qty_players"]),
            float(row["total_bets"] or 0),
            float(row["total_wins"] or 0),
            float(row["ggr"] or 0),
            float(row["bonus_cost"] or 0),
            float(row["ngr"] or 0),
            float(row["margin_pct"] or 0),
            float(row["ggr_casino"] or 0),
            float(row["ggr_sports"] or 0),
            float(row["max_single_win_val"]) if row["max_single_win_val"] is not None else None,
            int(row["rollback_count"] or 0),
            float(row["rollback_total"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_gaming_activity_daily;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total_ggr = sum(r[5] for r in records)
    total_bonus = sum(r[6] for r in records)
    total_ngr = sum(r[7] for r in records)
    total_bets = sum(r[3] for r in records)
    total_wins = sum(r[4] for r in records)
    trackers = len(set(r[1] for r in records))
    margin = (total_ggr / total_bets * 100) if total_bets > 0 else 0
    log.info(f"{len(records)} linhas inseridas | {trackers} trackers")
    log.info(f"  Bets: R$ {total_bets:,.2f} | Wins: R$ {total_wins:,.2f}")
    log.info(f"  GGR: R$ {total_ggr:,.2f} | Bonus Cost: R$ {total_bonus:,.2f} | NGR: R$ {total_ngr:,.2f}")
    log.info(f"  Margin: {margin:.2f}%")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_gaming_activity_daily (Sub-Fund Isolation v2) ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
