"""
Pipeline: fact_sports_odds_performance
=======================================
Dominio: Sportsbook Performance (Win/Loss por faixa de odds)

Grao: dt (BRT) x odds_range x bet_mode (Live | PreMatch)

KPIs por dia/faixa/modo:
    - total_bets, unique_players
    - bets_casa_ganha, bets_casa_perde
    - total_stake, total_payout, ggr
    - hold_rate_pct, avg_odds, avg_ticket

Fonte: vendor_ec2.tbl_sports_book_bets_info (Athena)
Destino: multibet.fact_sports_odds_performance (Super Nova DB)
Estrategia: INCREMENTAL com UPSERT (rolling window 7 dias)
Backfill: desde 2026-01-01 (vendor_ec2 sem dados confiaveis antes)

Periodo confiavel: Jan/2026+ (cross-validado com ps_bi, divergencia <1%)

Validacao auditada (15/04/2026):
  - Cross-check ps_bi: <1% divergencia Jan-Mar
  - Cashouts/refunds: 3% de impacto (aceitavel)
  - Free bets: 0% nos dados
  - Test users excluidos: 0.56%

Execucao:
    python pipelines/fact_sports_odds_performance.py             # incremental (D-1 a D-7)
    python pipelines/fact_sports_odds_performance.py --backfill  # desde 2026-01-01
    python pipelines/fact_sports_odds_performance.py --dry-run   # so query, sem persistir
    python pipelines/fact_sports_odds_performance.py --days 14   # rolling window custom
"""

import sys, os, logging, argparse
from datetime import datetime, timezone, date, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# --- Constantes ---
BACKFILL_START = "2026-01-01"  # vendor_ec2 sem dados confiaveis antes
DEFAULT_ROLLING_DAYS = 7        # rolling window padrao (incremental)


# --- DDL ----------------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_sports_odds_performance (
    dt                DATE         NOT NULL,
    odds_range        VARCHAR(20)  NOT NULL,
    odds_order        SMALLINT     NOT NULL,
    bet_mode          VARCHAR(10)  NOT NULL,
    total_bets        INTEGER      DEFAULT 0,
    unique_players    INTEGER      DEFAULT 0,
    bets_casa_ganha   INTEGER      DEFAULT 0,
    bets_casa_perde   INTEGER      DEFAULT 0,
    pct_casa_ganha    NUMERIC(10,4) DEFAULT 0,
    total_stake       NUMERIC(18,2) DEFAULT 0,
    total_payout      NUMERIC(18,2) DEFAULT 0,
    ggr               NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct     NUMERIC(10,4) DEFAULT 0,
    avg_odds          DOUBLE PRECISION DEFAULT 0,
    avg_ticket        NUMERIC(18,2) DEFAULT 0,
    refreshed_at      TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (dt, odds_range, bet_mode)
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fsop_dt ON multibet.fact_sports_odds_performance (dt DESC);",
    "CREATE INDEX IF NOT EXISTS idx_fsop_range ON multibet.fact_sports_odds_performance (odds_range, bet_mode);",
    "CREATE INDEX IF NOT EXISTS idx_fsop_ggr ON multibet.fact_sports_odds_performance (dt, ggr DESC);",
]


# --- Query Athena -------------------------------------------------------
# Filtros: c_bet_state='C' (settled), test users excluidos, dedup por slip
# Casa ganha = c_total_return <= 0 (player perdeu)
# Casa perde = c_total_return > 0 (player ganhou)
QUERY_ATHENA_TEMPLATE = """
WITH
valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

raw_bets AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        c_total_return,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 0), 9999) AS odds,
        c_bet_state,
        c_bet_type,
        c_is_live,
        c_created_time,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '{start_date}'
      AND c_created_time <  TIMESTAMP '{end_date}'
),

bets AS (
    SELECT rb.*,
        CAST(rb.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt_brt,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN '1.01 - 2.00'
            WHEN odds >  2.00 AND odds <= 5.00 THEN '2.01 - 5.00'
            WHEN odds >  5.00 AND odds <= 20.00 THEN '5.01 - 20.00'
            WHEN odds > 20.00 THEN '20.00+'
            ELSE 'Invalido'
        END AS odds_range,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN 1
            WHEN odds >  2.00 AND odds <= 5.00 THEN 2
            WHEN odds >  5.00 AND odds <= 20.00 THEN 3
            WHEN odds > 20.00 THEN 4
            ELSE 0
        END AS odds_order,
        CASE WHEN rb.c_bet_type = 'Live' OR rb.c_is_live = true
             THEN 'Live' ELSE 'PreMatch' END AS bet_mode
    FROM raw_bets rb
    WHERE rn = 1
      AND odds >= 1.01
)

SELECT
    b.dt_brt AS dt,
    b.odds_range,
    b.odds_order,
    b.bet_mode,
    COUNT(*) AS total_bets,
    COUNT(DISTINCT b.c_customer_id) AS unique_players,
    SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) AS bets_casa_ganha,
    SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) AS bets_casa_perde,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) * 100.0 / COUNT(*), 4) AS pct_casa_ganha,
    ROUND(SUM(b.c_total_stake), 2) AS total_stake,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END), 2) AS total_payout,
    ROUND(SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END), 2) AS ggr,
    ROUND(
        (SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END))
        * 100.0 / NULLIF(SUM(b.c_total_stake), 0), 4
    ) AS hold_rate_pct,
    ROUND(AVG(b.odds), 4) AS avg_odds,
    ROUND(AVG(b.c_total_stake), 2) AS avg_ticket
FROM bets b
JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
GROUP BY b.dt_brt, b.odds_range, b.odds_order, b.bet_mode
ORDER BY 1, 3, 4
"""


def setup_table():
    log.info("Criando tabela multibet.fact_sports_odds_performance...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    for ddl in DDL_INDEXES:
        try:
            execute_supernova(ddl)
        except Exception as e:
            log.warning(f"Indice ja existe ou erro menor: {e}")
    log.info("Tabela e indices prontos.")


def run(start_date: str, end_date: str, dry_run: bool = False):
    """
    Executa pipeline para periodo [start_date, end_date)
    Persistencia: DELETE periodo + INSERT (efetivamente UPSERT por (dt, odds_range, bet_mode))
    """
    log.info(f"Periodo: {start_date} ate {end_date} (exclusive)")
    log.info("Executando query no Athena...")

    sql = QUERY_ATHENA_TEMPLATE.format(start_date=start_date, end_date=end_date)
    df = query_athena(sql, database="vendor_ec2")
    log.info(f"{len(df)} linhas obtidas (dt x odds_range x bet_mode)")

    if df.empty:
        log.warning("Nenhum dado retornado.")
        return

    # Sumario para log
    total_bets = int(df["total_bets"].sum())
    total_stake = float(df["total_stake"].sum())
    total_ggr = float(df["ggr"].sum())
    hold = (total_ggr / total_stake * 100) if total_stake > 0 else 0
    log.info(f"  Total bets: {total_bets:,} | Stake: R$ {total_stake:,.2f} "
             f"| GGR: R$ {total_ggr:,.2f} | Hold: {hold:.2f}%")

    if dry_run:
        log.info("DRY-RUN: nao persistindo no Super Nova DB.")
        # Salvar CSV pra inspecao
        os.makedirs("output", exist_ok=True)
        out_csv = f"output/fact_sports_odds_performance_{start_date}_{end_date}.csv"
        df.to_csv(out_csv, index=False)
        log.info(f"CSV salvo: {out_csv}")
        return

    # --- Persistencia ---
    now_utc = datetime.now(timezone.utc)

    # Estrategia incremental: DELETE periodo + INSERT (idempotente)
    delete_sql = """
        DELETE FROM multibet.fact_sports_odds_performance
        WHERE dt >= %s AND dt < %s
    """

    insert_sql = """
        INSERT INTO multibet.fact_sports_odds_performance (
            dt, odds_range, odds_order, bet_mode,
            total_bets, unique_players, bets_casa_ganha, bets_casa_perde,
            pct_casa_ganha, total_stake, total_payout, ggr,
            hold_rate_pct, avg_odds, avg_ticket, refreshed_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            row["odds_range"],
            int(row["odds_order"]),
            row["bet_mode"],
            int(row["total_bets"] or 0),
            int(row["unique_players"] or 0),
            int(row["bets_casa_ganha"] or 0),
            int(row["bets_casa_perde"] or 0),
            float(row["pct_casa_ganha"] or 0),
            float(row["total_stake"] or 0),
            float(row["total_payout"] or 0),
            float(row["ggr"] or 0),
            float(row["hold_rate_pct"] or 0),
            float(row["avg_odds"] or 0),
            float(row["avg_ticket"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(delete_sql, (start_date, end_date))
            deleted = cur.rowcount
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    log.info(f"  Deletado: {deleted} linhas existentes")
    log.info(f"  Inserido: {len(records)} linhas")
    log.info(f"  Periodo coberto: {df['dt'].min()} a {df['dt'].max()}")


def main():
    parser = argparse.ArgumentParser(description="Pipeline fact_sports_odds_performance (Sportsbook por faixa de odds)")
    parser.add_argument("--backfill", action="store_true",
                        help=f"Backfill completo desde {BACKFILL_START}")
    parser.add_argument("--days", type=int, default=DEFAULT_ROLLING_DAYS,
                        help=f"Rolling window em dias (default {DEFAULT_ROLLING_DAYS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="So executa query e salva CSV, nao persiste no Super Nova")
    parser.add_argument("--start", type=str, help="Data inicio custom (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="Data fim exclusiva custom (YYYY-MM-DD)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("PIPELINE: fact_sports_odds_performance")
    log.info("Sportsbook Win/Loss por faixa de odds")
    log.info("=" * 60)

    # Determinar periodo
    today = date.today()
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
        log.info(f"Modo: CUSTOM ({start_date} -> {end_date})")
    elif args.backfill:
        start_date = BACKFILL_START
        end_date = (today + timedelta(days=1)).isoformat()  # ate hoje inclusive
        log.info(f"Modo: BACKFILL desde {BACKFILL_START}")
    else:
        # Incremental: rolling window de N dias (default 7)
        start_date = (today - timedelta(days=args.days)).isoformat()
        end_date = (today + timedelta(days=1)).isoformat()  # ate hoje inclusive
        log.info(f"Modo: INCREMENTAL (rolling window {args.days} dias)")

    if not args.dry_run:
        setup_table()

    run(start_date, end_date, dry_run=args.dry_run)

    log.info("Pipeline concluido.")


if __name__ == "__main__":
    main()
