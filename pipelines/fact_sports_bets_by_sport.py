"""
Pipeline: fact_sports_bets_by_sport (vendor_ec2 — Apostas por Esporte REAL)
============================================================================
Correcao do gap critico: fact_sports_bets original hardcoda 'all_sports'.
Este pipeline faz JOIN com tbl_sports_book_bet_details para obter
o breakdown por esporte (c_sport_type_name).

Grao: dt (dia BRT) x sport_name
Fonte: vendor_ec2 (bets_info + bet_details) — valores em BRL real (NAO centavos)
Destino: Super Nova DB -> multibet.fact_sports_bets_by_sport

KPIs por esporte por dia:
    - qty_bets, qty_players
    - turnover (stake), total_return (payout), ggr, margin_pct
    - avg_ticket, avg_odds
    - qty/turnover pre-match vs live

Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fact_sports_bets_by_sport.py
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

# --- FIX Gusta bloqueador #3 (10/04/2026): mojibake em sport_name -----------
# Caracteres acentuados em c_sport_type_name voltam do Athena como bytes latin1
# interpretados como UTF-8 invalido — no df vira U+FFFD (replacement char).
# Os bytes originais foram perdidos no caminho Athena -> boto3 -> pandas, entao
# mapa explicito das 4 variantes conhecidas + fallback que remove U+FFFD isolado.
MOJIBAKE_FIX = {
    "T\ufffdnis":          "Tenis",
    "V\ufffdlei":          "Volei",
    "H\ufffdquei no Gelo": "Hoquei no Gelo",
    "T\ufffdnis de mesa":  "Tenis de mesa",
}


def fix_sport_name(raw):
    """Normaliza sport_name corrompido (mojibake latin1->utf8) pra UTF-8 limpo.
    Nota: usa grafia sem acento ('Tenis', 'Volei') pra evitar dependencia
    de encoding do terminal/log. Front-end pode reacentuar visualmente se quiser.
    """
    if raw is None:
        return "Outros"
    s = str(raw).strip()
    if not s:
        return "Outros"
    # Mapa exato (cobre os 4 casos observados pelo Gusta 10/04/2026)
    if s in MOJIBAKE_FIX:
        return MOJIBAKE_FIX[s]
    # Mapa parcial: corrige casos compostos onde variante aparece no meio
    for broken, fixed in MOJIBAKE_FIX.items():
        if broken in s:
            s = s.replace(broken, fixed)
    # Fallback: se ainda sobrou replacement char, remove
    if "\ufffd" in s:
        s = s.replace("\ufffd", "").strip()
    return s or "Outros"

# --- DDL ------------------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_sports_bets_by_sport (
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

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_fsbbs_ggr
    ON multibet.fact_sports_bets_by_sport (dt, ggr DESC);
CREATE INDEX IF NOT EXISTS idx_fsbbs_sport
    ON multibet.fact_sports_bets_by_sport (sport_name, dt);
"""

# --- Query Athena: apostas liquidadas por esporte REAL --------------------
# Logica:
#   1. Deduplicar bets_info por c_bet_slip_id (ROW_NUMBER, pegar ultimo = settled)
#   2. JOIN com bet_details para c_sport_type_name
#   3. Atribuir cada bilhete ao esporte majoritario (MAX por slip)
#   4. Filtrar: settled (c_transaction_type = 'P' = payout)
#   5. Filtrar test users via bireports_ec2.tbl_ecr
#   6. Valores ja em BRL real (vendor_ec2 NAO usa centavos)
QUERY_ATHENA = """
WITH valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Esporte principal de cada bilhete (majoritario nas legs)
bet_sport AS (
    SELECT
        c_bet_slip_id,
        -- Pega o esporte mais frequente nas legs do bilhete
        COALESCE(
            MAX(c_sport_type_name),
            'Outros'
        ) AS sport_name,
        -- Live se qualquer leg for live
        MAX(CASE WHEN c_is_live = true THEN 1 ELSE 0 END) AS has_live_leg
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2025-10-01'
    GROUP BY c_bet_slip_id
),

-- Bilhetes liquidados (payout), deduplicados, com flag live
settled_bets AS (
    SELECT
        bi.c_customer_id,
        bi.c_bet_slip_id,
        bi.c_total_stake,
        bi.c_total_return,
        TRY_CAST(bi.c_total_odds AS DOUBLE) AS total_odds,
        bi.c_bet_type,
        bi.c_is_live,
        bi.c_created_time,
        -- Flag live consolidada (bets_info + bet_details)
        CASE WHEN bi.c_bet_type = 'Live' OR bi.c_is_live = true
             THEN true ELSE false END AS is_live,
        ROW_NUMBER() OVER (
            PARTITION BY bi.c_bet_slip_id
            ORDER BY bi.c_created_time DESC
        ) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info bi
    WHERE bi.c_created_time >= TIMESTAMP '2025-10-01'
      AND bi.c_transaction_type = 'P'
)

SELECT
    CAST(sb.c_created_time AT TIME ZONE 'UTC'
         AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
    bs.sport_name,

    COUNT(DISTINCT sb.c_bet_slip_id) AS qty_bets,
    COUNT(DISTINCT sb.c_customer_id) AS qty_players,

    -- Financeiro (BRL real, vendor_ec2 nao usa centavos)
    SUM(COALESCE(sb.c_total_stake, 0)) AS turnover,
    SUM(COALESCE(sb.c_total_return, 0)) AS total_return,
    SUM(COALESCE(sb.c_total_stake, 0))
        - SUM(COALESCE(sb.c_total_return, 0)) AS ggr,

    -- Margem
    CASE WHEN SUM(COALESCE(sb.c_total_stake, 0)) > 0
         THEN (SUM(COALESCE(sb.c_total_stake, 0))
               - SUM(COALESCE(sb.c_total_return, 0)))
              * 100.0 / SUM(COALESCE(sb.c_total_stake, 0))
         ELSE 0 END AS margin_pct,

    -- Ticket medio e odds media
    AVG(sb.c_total_stake) AS avg_ticket,
    AVG(sb.total_odds) AS avg_odds,

    -- Pre-match vs Live (contagem)
    COUNT(DISTINCT CASE WHEN NOT sb.is_live AND bs.has_live_leg = 0
        THEN sb.c_bet_slip_id END) AS qty_pre_match,
    COUNT(DISTINCT CASE WHEN sb.is_live OR bs.has_live_leg = 1
        THEN sb.c_bet_slip_id END) AS qty_live,

    -- Turnover pre/live REAL (calculado por stake, nao por aproximacao)
    SUM(CASE WHEN NOT sb.is_live AND bs.has_live_leg = 0
             THEN COALESCE(sb.c_total_stake, 0) ELSE 0 END) AS turnover_pre_match,
    SUM(CASE WHEN sb.is_live OR bs.has_live_leg = 1
             THEN COALESCE(sb.c_total_stake, 0) ELSE 0 END) AS turnover_live,

    -- % pre/live
    CASE WHEN SUM(COALESCE(sb.c_total_stake, 0)) > 0
         THEN SUM(CASE WHEN NOT sb.is_live AND bs.has_live_leg = 0
                       THEN COALESCE(sb.c_total_stake, 0) ELSE 0 END)
              * 100.0 / SUM(COALESCE(sb.c_total_stake, 0))
         ELSE 0 END AS pct_pre_match,
    CASE WHEN SUM(COALESCE(sb.c_total_stake, 0)) > 0
         THEN SUM(CASE WHEN sb.is_live OR bs.has_live_leg = 1
                       THEN COALESCE(sb.c_total_stake, 0) ELSE 0 END)
              * 100.0 / SUM(COALESCE(sb.c_total_stake, 0))
         ELSE 0 END AS pct_live

FROM settled_bets sb
JOIN bet_sport bs ON sb.c_bet_slip_id = bs.c_bet_slip_id
JOIN valid_players vp ON CAST(sb.c_customer_id AS VARCHAR) = vp.ext_id
WHERE sb.rn = 1
GROUP BY 1, 2
ORDER BY 1 DESC, ggr DESC
"""


def setup_table():
    log.info("Criando tabela fact_sports_bets_by_sport...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    try:
        execute_supernova(DDL_INDEX)
    except Exception:
        pass
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Sports Bets por Esporte REAL)...")
    log.info("Fontes: vendor_ec2.tbl_sports_book_bets_info + tbl_sports_book_bet_details")
    df = query_athena(QUERY_ATHENA, database="vendor_ec2")
    log.info(f"{len(df)} linhas obtidas (esportes x dias).")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    # Turnover pre/live agora calculado diretamente no SQL (nao mais por aproximacao)

    insert_sql = """
        INSERT INTO multibet.fact_sports_bets_by_sport
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
        with conn:  # auto-commit on success, rollback on exception
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE multibet.fact_sports_bets_by_sport;")
                psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
    finally:
        try:
            conn.close()
        finally:
            ssh.close()

    total_ggr = sum(r[6] for r in records)
    total_turnover = sum(r[4] for r in records)
    sports = sorted(set(r[1] for r in records))
    log.info(f"{len(records)} linhas inseridas | {len(sports)} esportes")
    log.info(f"  Esportes: {', '.join(sports[:10])}{'...' if len(sports) > 10 else ''}")
    log.info(f"  Turnover: R$ {total_turnover:,.2f}")
    log.info(f"  GGR: R$ {total_ggr:,.2f}")
    if total_turnover > 0:
        log.info(f"  Margin: {total_ggr / total_turnover * 100:.2f}%")


if __name__ == "__main__":
    try:
        log.info("=== Iniciando pipeline fact_sports_bets_by_sport ===")
        setup_table()
        refresh()
        log.info("=== Pipeline concluido ===")
    except Exception as e:
        log.error(f"Pipeline falhou: {e}", exc_info=True)
        sys.exit(1)
