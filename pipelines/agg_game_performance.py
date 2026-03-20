"""
Pipeline: agg_game_performance (ps_bi — Performance Semanal de Jogos)
======================================================================
Dominio: Produto e Performance de Jogos (Opcional — tabela 22)

Grao: week_start x game_id
Fonte: ps_bi.fct_casino_activity_daily (agregado por semana)

KPIs por jogo por semana:
    - ggr_rank            → Ranking semanal de NGR por jogo
    - dau                 → Jogadores unicos por jogo (media diaria na semana)
    - ggr, turnover       → Receita e volume
    - concentration_pct   → % GGR deste jogo vs GGR total da semana
    - is_new_game         → Se o jogo estreou nessa semana (1a vez com atividade)

Views derivadas (calculadas no dashboard):
    - Receita por jogo (rank) → ORDER BY ggr DESC
    - DAU por jogo            → dau
    - Concentracao top 10%    → SUM(ggr) WHERE ggr_rank <= top_10pct / SUM total
    - Jogos estreantes vs legados → is_new_game

Fontes (Athena):
    1. ps_bi.fct_casino_activity_daily → Atividade por player/game/dia
    2. ps_bi.dim_game                   → Catalogo
    3. bireports_ec2.tbl_ecr           → Filtro test users

Destino: Super Nova DB -> multibet.agg_game_performance
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/agg_game_performance.py
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
CREATE TABLE IF NOT EXISTS multibet.agg_game_performance (
    week_start          DATE,
    game_id             VARCHAR(50),
    game_name           VARCHAR(255),
    vendor_id           VARCHAR(50),
    game_category       VARCHAR(100),
    qty_active_days     INTEGER DEFAULT 0,
    dau_avg             NUMERIC(10,2) DEFAULT 0,
    total_players       INTEGER DEFAULT 0,
    total_rounds        INTEGER DEFAULT 0,
    turnover            NUMERIC(18,2) DEFAULT 0,
    ggr                 NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct       NUMERIC(10,4) DEFAULT 0,
    ggr_rank            INTEGER,
    concentration_pct   NUMERIC(10,4) DEFAULT 0,
    first_activity_date DATE,
    is_new_game         BOOLEAN DEFAULT FALSE,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (week_start, game_id)
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_agp_rank ON multibet.agg_game_performance (week_start, ggr_rank);
"""

# --- Query Athena ---------------------------------------------------------
# Agregar por semana (date_trunc('week')) e jogo
# Window functions: ggr_rank, concentration_pct, first_activity

QUERY_ATHENA = """
WITH valid_players AS (
    SELECT c_ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Primeira atividade de cada jogo (para detectar estreantes)
first_seen AS (
    SELECT game_id,
           MIN(activity_date) AS first_activity_date
    FROM ps_bi.fct_casino_activity_daily
    WHERE LOWER(product_id) = 'casino'
    GROUP BY game_id
),

-- Agregacao semanal por jogo
weekly AS (
    SELECT
        date_trunc('week', fca.activity_date) AS week_start,
        fca.game_id,
        COALESCE(dg.game_desc, 'Desconhecido') AS game_name,
        COALESCE(dg.vendor_id, 'unknown') AS vendor_id,
        COALESCE(dg.game_category, 'Outros') AS game_category,

        -- Dias ativos na semana
        COUNT(DISTINCT fca.activity_date) AS qty_active_days,

        -- DAU medio (jogadores unicos por dia, media na semana)
        CAST(COUNT(DISTINCT CONCAT(CAST(fca.activity_date AS VARCHAR), '-', CAST(fca.player_id AS VARCHAR))) AS DOUBLE)
            / NULLIF(COUNT(DISTINCT fca.activity_date), 0) AS dau_avg,

        COUNT(DISTINCT fca.player_id) AS total_players,
        CAST(SUM(fca.bet_count) AS INTEGER) AS total_rounds,
        SUM(COALESCE(fca.bet_amount_local, 0)) AS turnover,
        SUM(COALESCE(fca.ggr_local, 0)) AS ggr,

        CASE WHEN SUM(COALESCE(fca.bet_amount_local, 0)) > 0
             THEN SUM(COALESCE(fca.ggr_local, 0)) * 100.0 / SUM(COALESCE(fca.bet_amount_local, 0))
             ELSE 0 END AS hold_rate_pct,

        fs.first_activity_date

    FROM ps_bi.fct_casino_activity_daily fca
    LEFT JOIN ps_bi.dim_game dg ON fca.game_id = dg.game_id
    LEFT JOIN first_seen fs ON fca.game_id = fs.game_id
    JOIN valid_players vp ON fca.player_id = vp.c_ecr_id
    WHERE fca.activity_date >= DATE '2025-10-01'
      AND LOWER(fca.product_id) = 'casino'
    GROUP BY 1, 2, 3, 4, 5, fs.first_activity_date
)

-- Calcular rank e concentracao
SELECT
    w.week_start,
    w.game_id,
    w.game_name,
    w.vendor_id,
    w.game_category,
    w.qty_active_days,
    w.dau_avg,
    w.total_players,
    w.total_rounds,
    w.turnover,
    w.ggr,
    w.hold_rate_pct,

    -- Ranking por GGR (descendente) na semana
    ROW_NUMBER() OVER (PARTITION BY w.week_start ORDER BY w.ggr DESC) AS ggr_rank,

    -- Concentracao: % do GGR total da semana
    CASE WHEN SUM(w.ggr) OVER (PARTITION BY w.week_start) > 0
         THEN w.ggr * 100.0 / SUM(w.ggr) OVER (PARTITION BY w.week_start)
         ELSE 0 END AS concentration_pct,

    w.first_activity_date,

    -- Estreante: primeira atividade nessa mesma semana
    CASE WHEN w.first_activity_date >= w.week_start
          AND w.first_activity_date < date_add('week', 1, w.week_start)
         THEN true ELSE false END AS is_new_game

FROM weekly w
ORDER BY w.week_start DESC, w.ggr DESC
"""


def setup_table():
    log.info("Criando tabela agg_game_performance...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    try:
        execute_supernova(DDL_INDEX)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Performance Semanal de Jogos)...")
    df = query_athena(QUERY_ATHENA, database="ps_bi")
    log.info(f"{len(df)} linhas obtidas (jogos x semanas).")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.agg_game_performance
            (week_start, game_id, game_name, vendor_id, game_category,
             qty_active_days, dau_avg, total_players, total_rounds,
             turnover, ggr, hold_rate_pct,
             ggr_rank, concentration_pct,
             first_activity_date, is_new_game, refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["week_start"],
            str(row["game_id"] or ""),
            str(row["game_name"] or "Desconhecido"),
            str(row["vendor_id"] or "unknown"),
            str(row["game_category"] or "Outros"),
            int(row["qty_active_days"] or 0),
            float(row["dau_avg"] or 0),
            int(row["total_players"] or 0),
            int(row["total_rounds"] or 0),
            float(row["turnover"] or 0),
            float(row["ggr"] or 0),
            float(row["hold_rate_pct"] or 0),
            int(row["ggr_rank"]),
            float(row["concentration_pct"] or 0),
            row["first_activity_date"],
            bool(row["is_new_game"]),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.agg_game_performance;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total_ggr = sum(r[10] for r in records)
    unique_games = len(set(r[1] for r in records))
    weeks = len(set(r[0] for r in records))
    new_games = sum(1 for r in records if r[15])
    log.info(f"{len(records)} linhas inseridas | {unique_games} jogos | {weeks} semanas")
    log.info(f"  GGR total: R$ {total_ggr:,.2f} | Jogos estreantes: {new_games}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline agg_game_performance ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
