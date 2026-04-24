"""
Pipeline: fact_sports_bets (vendor_ec2 — Apostas Esportivas por Esporte)
=========================================================================
Dominio: Produto e Performance de Jogos (Prioridade 4 — tabela 19)

Grao: dt (dia BRT) x sport_name
Fonte: vendor_ec2 (tabelas sportsbook) — colunas validadas em 19/03/2026

KPIs por esporte por dia:
    - qty_bets, qty_players
    - turnover (total apostado), total_return, ggr, margin_pct
    - avg_ticket, avg_odds
    - qty/turnover pre-match vs live, pct_pre_match, pct_live

Tabela adicional: fact_sports_open_bets (projecao de apostas abertas)

Fontes (Athena vendor_ec2):
    1. tbl_sports_book_bets_info   → Header bilhete (stake, return, odds, bet_type)
       - c_total_odds = VARCHAR (precisa CAST)
       - c_bet_type = 'PreLive' | 'Live' | 'Mixed'
       - c_bet_state = 'O' (open) | 'C' (closed/settled) | 'W' (won) | 'L' (lost)
       - Valores em BRL REAL (nao centavos)
    2. tbl_sports_book_bet_details → Legs (esporte, torneio, pre/live)
       - c_sport_type_name = 'Futebol', etc. (NOME do esporte)
       - c_tournament_name = liga/torneio
       - c_is_live = boolean
    3. bireports_ec2.tbl_ecr       → Filtro test users

Destino: Super Nova DB -> multibet.fact_sports_bets + multibet.fact_sports_open_bets
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fact_sports_bets.py
    python pipelines/fact_sports_bets.py --discover   # descobre colunas das tabelas
"""

import sys, os, logging, argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# --- FIX Gusta bloqueador #3 (10/04/2026): mojibake em sport_name -----------
# Mesmo fix aplicado em fact_sports_bets_by_sport.py — ver comentario de la.
MOJIBAKE_FIX = {
    "T\ufffdnis":          "Tenis",
    "V\ufffdlei":          "Volei",
    "H\ufffdquei no Gelo": "Hoquei no Gelo",
    "T\ufffdnis de mesa":  "Tenis de mesa",
}


def fix_sport_name(raw):
    if raw is None:
        return "Outros"
    s = str(raw).strip()
    if not s:
        return "Outros"
    if s in MOJIBAKE_FIX:
        return MOJIBAKE_FIX[s]
    for broken, fixed in MOJIBAKE_FIX.items():
        if broken in s:
            s = s.replace(broken, fixed)
    if "\ufffd" in s:
        s = s.replace("\ufffd", "").strip()
    return s or "Outros"

# --- DDL ------------------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_sports_bets (
    dt                  DATE,
    sport_name          VARCHAR(255),
    qty_bets            INTEGER DEFAULT 0,
    qty_players         INTEGER DEFAULT 0,
    turnover            NUMERIC(18,2) DEFAULT 0,
    total_return        NUMERIC(18,2) DEFAULT 0,
    ggr                 NUMERIC(18,2) DEFAULT 0,
    margin_pct          NUMERIC(10,4) DEFAULT 0,
    avg_ticket          NUMERIC(18,2) DEFAULT 0,
    avg_odds            DOUBLE PRECISION DEFAULT 0,
    qty_pre_match       INTEGER DEFAULT 0,
    qty_live            INTEGER DEFAULT 0,
    turnover_pre_match  NUMERIC(18,2) DEFAULT 0,
    turnover_live       NUMERIC(18,2) DEFAULT 0,
    pct_pre_match       NUMERIC(10,4) DEFAULT 0,
    pct_live            NUMERIC(10,4) DEFAULT 0,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, sport_name)
);
"""

DDL_TABLE_OPEN = """
CREATE TABLE IF NOT EXISTS multibet.fact_sports_open_bets (
    snapshot_dt         DATE,
    sport_name          VARCHAR(255),
    qty_open_bets       INTEGER DEFAULT 0,
    total_stake_open    NUMERIC(18,2) DEFAULT 0,
    avg_odds_open       DOUBLE PRECISION DEFAULT 0,
    projected_liability NUMERIC(18,2) DEFAULT 0,
    projected_ggr       NUMERIC(18,2) DEFAULT 0,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (snapshot_dt, sport_name)
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_fsb_ggr ON multibet.fact_sports_bets (dt, ggr DESC);
"""

# --- Discovery: mapear colunas reais das tabelas sportsbook ---------------
def discover_schema():
    """Descobre colunas reais das tabelas sportsbook no vendor_ec2."""
    tables = [
        "tbl_sports_book_bets_info",
        "tbl_sports_book_bet_details",
        "tbl_sports_book_info",
    ]
    for tbl in tables:
        log.info(f"\n{'='*60}")
        log.info(f"SCHEMA: vendor_ec2.{tbl}")
        log.info(f"{'='*60}")
        try:
            df = query_athena(f"SHOW COLUMNS FROM {tbl}", database="vendor_ec2")
            for _, row in df.iterrows():
                log.info(f"  {row.iloc[0]}")
        except Exception as e:
            log.error(f"  Erro ao descobrir schema: {e}")

    log.info("\n--- Amostra tbl_sports_book_bets_info (5 rows) ---")
    try:
        df = query_athena("SELECT * FROM tbl_sports_book_bets_info LIMIT 5", database="vendor_ec2")
        log.info(f"Colunas: {list(df.columns)}")
        for _, row in df.iterrows():
            log.info(dict(row))
    except Exception as e:
        log.error(f"Erro: {e}")


# --- Query principal: apostas liquidadas por esporte/dia -------------------
# Estrategia:
#   1. Deduplicar bets_info por c_bet_slip_id (pegar ultimo registro = settled)
#   2. Join com bet_details para pegar esporte (c_sport_type_name)
#   3. Filtrar settled: c_bet_state != 'O'
#   4. Filtrar test users via c_customer_id = external_id no bireports_ec2.tbl_ecr
#   5. Valores em BRL real (sem divisao por 100)

# QUERY v4: HIBRIDA — financeiro do ps_bi (rapido, 45s) + pre/live do vendor_ec2 (sem bet_details)
# ps_bi ja tem product_id = 'sports_book' com GGR, turnover, wins pre-calculados
# vendor_ec2 bets_info adiciona: pre/live split, odds, ticket medio
QUERY_ATHENA = """
WITH valid_players AS (
    SELECT c_ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Pre/live split via vendor_ec2 (apenas contagem, sem JOIN pesado)
prelive_daily AS (
    SELECT
        CAST(bi.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
        COUNT(DISTINCT bi.c_bet_slip_id) AS total_bets,
        COUNT(DISTINCT CASE WHEN bi.c_bet_type = 'Live' OR bi.c_is_live = true
                            THEN bi.c_bet_slip_id END) AS live_bets,
        COUNT(DISTINCT CASE WHEN bi.c_bet_type = 'PreLive' OR (bi.c_is_live = false AND bi.c_bet_type != 'Live')
                            THEN bi.c_bet_slip_id END) AS pre_bets,
        AVG(TRY_CAST(bi.c_total_odds AS DOUBLE)) AS avg_odds,
        AVG(bi.c_total_stake) AS avg_ticket
    FROM vendor_ec2.tbl_sports_book_bets_info bi
    WHERE bi.c_created_time >= TIMESTAMP '2025-10-01'
      AND bi.c_transaction_type = 'P'
    GROUP BY 1
)

SELECT
    fca.activity_date AS dt,
    'all_sports' AS sport_name,

    -- Financeiro do ps_bi (rapido, BRL, pre-calculado)
    COUNT(DISTINCT fca.player_id) AS qty_players,
    CAST(SUM(fca.bet_count) AS INTEGER) AS qty_bets,
    SUM(COALESCE(fca.bet_amount_local, 0)) AS turnover,
    SUM(COALESCE(fca.win_amount_local, 0)) AS total_return,
    SUM(COALESCE(fca.ggr_local, 0)) AS ggr,

    CASE WHEN SUM(COALESCE(fca.bet_amount_local, 0)) > 0
         THEN SUM(COALESCE(fca.ggr_local, 0)) * 100.0
              / SUM(COALESCE(fca.bet_amount_local, 0))
         ELSE 0 END AS margin_pct,

    -- Ticket e odds do vendor_ec2 (complementar)
    COALESCE(MAX(pl.avg_ticket), 0) AS avg_ticket,
    COALESCE(LEAST(MAX(pl.avg_odds), 999999), 0) AS avg_odds,

    -- Pre/live split do vendor_ec2
    COALESCE(MAX(pl.pre_bets), 0) AS qty_pre_match,
    COALESCE(MAX(pl.live_bets), 0) AS qty_live,

    -- Turnover split estimado (proporcional)
    CASE WHEN COALESCE(MAX(pl.total_bets), 0) > 0
         THEN SUM(COALESCE(fca.bet_amount_local, 0))
              * COALESCE(MAX(pl.pre_bets), 0) / MAX(pl.total_bets)
         ELSE SUM(COALESCE(fca.bet_amount_local, 0)) END AS turnover_pre_match,
    CASE WHEN COALESCE(MAX(pl.total_bets), 0) > 0
         THEN SUM(COALESCE(fca.bet_amount_local, 0))
              * COALESCE(MAX(pl.live_bets), 0) / MAX(pl.total_bets)
         ELSE 0 END AS turnover_live,

    -- % pre/live
    CASE WHEN COALESCE(MAX(pl.total_bets), 0) > 0
         THEN COALESCE(MAX(pl.pre_bets), 0) * 100.0 / MAX(pl.total_bets)
         ELSE 100 END AS pct_pre_match,
    CASE WHEN COALESCE(MAX(pl.total_bets), 0) > 0
         THEN COALESCE(MAX(pl.live_bets), 0) * 100.0 / MAX(pl.total_bets)
         ELSE 0 END AS pct_live

FROM ps_bi.fct_casino_activity_daily fca
LEFT JOIN prelive_daily pl ON fca.activity_date = pl.dt
JOIN valid_players vp ON fca.player_id = vp.c_ecr_id
WHERE fca.activity_date >= DATE '2025-10-01'
  AND LOWER(fca.product_id) = 'sports_book'
GROUP BY fca.activity_date
ORDER BY 1 DESC
"""

# --- Query apostas abertas (projecao) ------------------------------------
QUERY_OPEN_BETS = """
WITH valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Bets abertas (ultimo registro por slip)
-- FIX Gusta bloqueador #2 (10/04/2026): c_total_odds tem overflow no Altenar
--   (ja corrigido em fact_sports_bets_by_sport via LEAST 999999, mas aqui NAO estava).
--   Resultado: avg_odds_open de 702x, projected_liability de R$ 1.59B (impossivel).
--   Cap individual a 50x (cobre outliers legitimos de pre-match exotico e corta lixo).
--   NULL/invalid vira 1.0 (liability 0 — aposta neutralizada).
open_bets_raw AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 1.0), 50.0) AS total_odds,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_created_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'O'
),

open_bets AS (
    SELECT ob.*, bs.sport_name
    FROM open_bets_raw ob
    LEFT JOIN (
        SELECT c_bet_slip_id,
               COALESCE(MAX(c_sport_type_name), 'Outros') AS sport_name
        FROM vendor_ec2.tbl_sports_book_bet_details
        GROUP BY c_bet_slip_id
    ) bs ON ob.c_bet_slip_id = bs.c_bet_slip_id
    WHERE ob.rn = 1
)

SELECT
    CURRENT_DATE AS snapshot_dt,
    ob.sport_name,
    COUNT(DISTINCT ob.c_bet_slip_id) AS qty_open_bets,
    SUM(ob.c_total_stake) AS total_stake_open,
    AVG(ob.total_odds) AS avg_odds_open,
    -- Liability = stake * (odds - 1)
    SUM(ob.c_total_stake * (COALESCE(ob.total_odds, 1) - 1)) AS projected_liability,
    -- GGR projetado = stake - liability
    SUM(ob.c_total_stake) - SUM(ob.c_total_stake * (COALESCE(ob.total_odds, 1) - 1)) AS projected_ggr
FROM open_bets ob
JOIN valid_players vp ON CAST(ob.c_customer_id AS VARCHAR) = vp.ext_id
GROUP BY 1, 2
ORDER BY total_stake_open DESC
"""


def setup_table():
    log.info("Criando tabelas fact_sports_bets e fact_sports_open_bets...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_TABLE_OPEN)
    try:
        execute_supernova(DDL_INDEX)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    log.info("Tabelas prontas.")


def refresh():
    # --- 1. Apostas liquidadas (historico) --------------------------------
    log.info("Executando query no Athena (Sports Bets por Esporte)...")
    log.info("Fontes: vendor_ec2.tbl_sports_book_bets_info + bet_details")
    df = query_athena(QUERY_ATHENA, database="vendor_ec2")
    log.info(f"{len(df)} linhas obtidas (esportes x dias).")

    if df.empty:
        log.warning("Nenhum dado retornado para bets liquidadas.")
    else:
        now_utc = datetime.now(timezone.utc)

        insert_sql = """
            INSERT INTO multibet.fact_sports_bets
                (dt, sport_name, qty_bets, qty_players,
                 turnover, total_return, ggr, margin_pct,
                 avg_ticket, avg_odds,
                 qty_pre_match, qty_live,
                 turnover_pre_match, turnover_live,
                 pct_pre_match, pct_live, refreshed_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        records = []
        for _, row in df.iterrows():
            records.append((
                row["dt"],
                fix_sport_name(row["sport_name"]),
                int(row["qty_bets"]),
                int(row["qty_players"]),
                float(row["turnover"] or 0),
                float(row["total_return"] or 0),
                float(row["ggr"] or 0),
                float(row["margin_pct"] or 0),
                float(row["avg_ticket"] or 0),
                float(row["avg_odds"] or 0),
                int(row["qty_pre_match"] or 0),
                int(row["qty_live"] or 0),
                float(row["turnover_pre_match"] or 0),
                float(row["turnover_live"] or 0),
                float(row["pct_pre_match"] or 0),
                float(row["pct_live"] or 0),
                now_utc,
            ))

        ssh, conn = get_supernova_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE multibet.fact_sports_bets;")
                psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
            conn.commit()
        finally:
            conn.close()
            ssh.close()

        total_ggr = sum(r[6] for r in records)
        total_turnover = sum(r[4] for r in records)
        total_bets = sum(r[2] for r in records)
        sports = len(set(r[1] for r in records))
        margin = (total_ggr / total_turnover * 100) if total_turnover > 0 else 0
        log.info(f"{len(records)} linhas inseridas | {sports} esportes")
        log.info(f"  Turnover: R$ {total_turnover:,.2f} | GGR: R$ {total_ggr:,.2f} | Margin: {margin:.2f}%")
        if total_bets > 0:
            log.info(f"  Total apostas: {total_bets:,} | Ticket medio: R$ {total_turnover/total_bets:,.2f}")

    # --- 2. Apostas abertas (projecao) ------------------------------------
    log.info("Executando query de apostas abertas (projecao)...")
    try:
        df_open = query_athena(QUERY_OPEN_BETS, database="vendor_ec2")
        log.info(f"{len(df_open)} linhas de apostas abertas.")

        if not df_open.empty:
            now_utc = datetime.now(timezone.utc)
            insert_open = """
                INSERT INTO multibet.fact_sports_open_bets
                    (snapshot_dt, sport_name, qty_open_bets, total_stake_open,
                     avg_odds_open, projected_liability, projected_ggr, refreshed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            records_open = []
            for _, row in df_open.iterrows():
                records_open.append((
                    row["snapshot_dt"],
                    fix_sport_name(row["sport_name"]),
                    int(row["qty_open_bets"]),
                    float(row["total_stake_open"] or 0),
                    float(row["avg_odds_open"] or 0),
                    float(row["projected_liability"] or 0),
                    float(row["projected_ggr"] or 0),
                    now_utc,
                ))

            ssh, conn = get_supernova_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE multibet.fact_sports_open_bets;")
                    psycopg2.extras.execute_batch(cur, insert_open, records_open, page_size=500)
                conn.commit()
            finally:
                conn.close()
                ssh.close()

            total_stake = sum(r[3] for r in records_open)
            total_liability = sum(r[5] for r in records_open)
            log.info(f"  {len(records_open)} esportes com apostas abertas")
            log.info(f"  Stake aberto: R$ {total_stake:,.2f} | Liability: R$ {total_liability:,.2f}")
    except Exception as e:
        log.warning(f"Erro na query de apostas abertas (nao-critico): {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--discover", action="store_true", help="Descobrir colunas das tabelas sportsbook")
    args = parser.parse_args()

    if args.discover:
        log.info("=== Modo Discovery: mapeando colunas sportsbook ===")
        discover_schema()
    else:
        log.info("=== Iniciando pipeline fact_sports_bets ===")
        setup_table()
        refresh()
        log.info("=== Pipeline concluido ===")
