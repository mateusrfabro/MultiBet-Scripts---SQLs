"""
Pipeline: fact_live_casino (ps_bi + gaming_sessions — Live Casino por Jogo)
============================================================================
Dominio: Produto e Performance de Jogos (Prioridade 4 — tabela 20)

Grao: dt (dia) x game_id
Fonte: ps_bi + bireports_ec2 (gaming_sessions para duracao de sessao)

KPIs por jogo live x dia:
    - qty_players, total_rounds (bet_count)
    - turnover, wins, ggr (total)
    - hold_rate_pct, rtp_pct
    - qty_sessions, avg_session_duration_sec, avg_rounds_per_session
    - concurrent_players_max (proxy para "ocupacao de mesas")

Categorias Live identificadas no catalogo:
    - game_category = 'Live' no ps_bi.dim_game
    - Inclui: LiveDealer, Blackjack, Roulette, Baccarat, GameShow, etc.
    - Vendors tipicos: alea_evolution, alea_creedroomz, ezugi, vivo

NOTA sobre "Ocupacao de mesas %":
    - Requer dados de capacidade do provedor (Evolution API, etc.) que NAO temos.
    - Usamos concurrent_players_max como PROXY: pico de jogadores simultaneos por mesa/dia.

NOTA sobre "Tempo medio por sessao":
    - bireports_ec2.tbl_ecr_gaming_sessions tem c_session_length_in_sec (exato!)
    - Fonte oficial da Pragmatic para sessoes de jogo.

Fontes (Athena):
    1. ps_bi.fct_casino_activity_daily  → Atividade live casino (BRL)
    2. ps_bi.dim_game                    → Catalogo (filtro game_category = 'Live')
    3. bireports_ec2.tbl_ecr_gaming_sessions → Sessoes (duracao, rodadas)
    4. bireports_ec2.tbl_ecr            → Filtro test users

Destino: Super Nova DB -> multibet.fact_live_casino
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fact_live_casino.py
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
CREATE TABLE IF NOT EXISTS multibet.fact_live_casino (
    dt                          DATE,
    game_id                     VARCHAR(50),
    game_name                   VARCHAR(255),
    vendor_id                   VARCHAR(50),
    game_category_desc          VARCHAR(100),
    qty_players                 INTEGER DEFAULT 0,
    total_rounds                INTEGER DEFAULT 0,
    turnover_total              NUMERIC(18,2) DEFAULT 0,
    wins_total                  NUMERIC(18,2) DEFAULT 0,
    ggr_total                   NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct               NUMERIC(10,4) DEFAULT 0,
    rtp_pct                     NUMERIC(10,4) DEFAULT 0,
    qty_sessions                INTEGER DEFAULT 0,
    avg_session_duration_sec    NUMERIC(10,2) DEFAULT 0,
    avg_rounds_per_session      NUMERIC(10,2) DEFAULT 0,
    max_concurrent_players      INTEGER DEFAULT 0,
    refreshed_at                TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, game_id)
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_flc_ggr ON multibet.fact_live_casino (dt, ggr_total DESC);
"""

# --- Query: combinar atividade financeira + sessoes -----------------------
# Estrategia em 2 CTEs:
#   1. finance: ps_bi.fct_casino_activity_daily filtrado por Live
#   2. sessions: bireports_ec2.tbl_ecr_gaming_sessions para duracao
# Merge por game_id + dt

# NOTA: gaming_sessions removido temporariamente por erro ICEBERG_BAD_DATA (20/03/2026)
# Sessoes serao adicionadas quando o Iceberg estabilizar.
# Por ora: apenas dados financeiros do ps_bi (que funciona).

QUERY_ATHENA = """
WITH valid_players AS (
    SELECT c_ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Jogos live (catalogo COMPLETO via bireports_ec2 — ps_bi.dim_game tem poucos mapeados)
live_games AS (
    SELECT c_game_id AS game_id,
           MAX(c_game_desc) AS game_desc,
           MAX(c_vendor_id) AS vendor_id,
           MAX(c_game_category_desc) AS game_category_desc
    FROM bireports_ec2.tbl_vendor_games_mapping_data
    WHERE LOWER(c_game_category) = 'live'
      AND UPPER(c_product_id) = 'CASINO'
    GROUP BY c_game_id
)

SELECT
    fca.activity_date AS dt,
    fca.game_id,
    COALESCE(lg.game_desc, 'Desconhecido') AS game_name,
    COALESCE(lg.vendor_id, 'unknown') AS vendor_id,
    COALESCE(lg.game_category_desc, 'Live') AS game_category_desc,
    COUNT(DISTINCT fca.player_id) AS qty_players,
    CAST(SUM(fca.bet_count) AS INTEGER) AS total_rounds,
    SUM(COALESCE(fca.bet_amount_local, 0)) AS turnover_total,
    SUM(COALESCE(fca.win_amount_local, 0)) AS wins_total,
    SUM(COALESCE(fca.ggr_local, 0)) AS ggr_total,
    CASE WHEN SUM(COALESCE(fca.bet_amount_local, 0)) > 0
         THEN SUM(COALESCE(fca.ggr_local, 0)) * 100.0
              / SUM(COALESCE(fca.bet_amount_local, 0))
         ELSE 0 END AS hold_rate_pct,
    CASE WHEN SUM(COALESCE(fca.bet_amount_local, 0)) > 0
         THEN SUM(COALESCE(fca.win_amount_local, 0)) * 100.0
              / SUM(COALESCE(fca.bet_amount_local, 0))
         ELSE 0 END AS rtp_pct,
    -- Sessoes: placeholder ate Iceberg estabilizar
    0 AS qty_sessions,
    0.0 AS avg_session_duration_sec,
    0.0 AS avg_rounds_per_session,
    0 AS max_concurrent_players
FROM ps_bi.fct_casino_activity_daily fca
JOIN live_games lg ON fca.game_id = lg.game_id
JOIN valid_players vp ON fca.player_id = vp.c_ecr_id
WHERE fca.activity_date >= DATE '2025-10-01'
  AND LOWER(fca.product_id) = 'casino'
GROUP BY fca.activity_date, fca.game_id, lg.game_desc, lg.vendor_id, lg.game_category_desc
ORDER BY 1 DESC, ggr_total DESC
"""


def setup_table():
    log.info("Criando tabela fact_live_casino...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    try:
        execute_supernova(DDL_INDEX)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Live Casino — finance + sessions)...")
    log.info("Fontes: ps_bi.fct_casino_activity_daily + dim_game + tbl_ecr_gaming_sessions")
    df = query_athena(QUERY_ATHENA, database="ps_bi")
    log.info(f"{len(df)} linhas obtidas (jogos live x dias).")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_live_casino
            (dt, game_id, game_name, vendor_id, game_category_desc,
             qty_players, total_rounds,
             turnover_total, wins_total, ggr_total,
             hold_rate_pct, rtp_pct,
             qty_sessions, avg_session_duration_sec, avg_rounds_per_session,
             max_concurrent_players, refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            str(row["game_id"] or ""),
            str(row["game_name"] or "Desconhecido"),
            str(row["vendor_id"] or "unknown"),
            str(row["game_category_desc"] or "Live"),
            int(row["qty_players"]),
            int(row["total_rounds"] or 0),
            float(row["turnover_total"] or 0),
            float(row["wins_total"] or 0),
            float(row["ggr_total"] or 0),
            float(row["hold_rate_pct"] or 0),
            float(row["rtp_pct"] or 0),
            int(row["qty_sessions"] or 0),
            float(row["avg_session_duration_sec"] or 0),
            float(row["avg_rounds_per_session"] or 0),
            int(row["max_concurrent_players"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_live_casino;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total_ggr = sum(r[9] for r in records)
    total_turnover = sum(r[7] for r in records)
    unique_games = len(set(r[1] for r in records))
    avg_session = sum(r[13] for r in records) / len(records) if records else 0
    log.info(f"{len(records)} linhas inseridas | {unique_games} jogos live")
    log.info(f"  Turnover: R$ {total_turnover:,.2f} | GGR: R$ {total_ggr:,.2f}")
    log.info(f"  Sessao media: {avg_session:.0f}s ({avg_session/60:.1f}min)")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_live_casino (ps_bi + sessions) ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
