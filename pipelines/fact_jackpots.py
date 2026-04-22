"""
Pipeline: fact_jackpots (ps_bi — Jackpots por Jogo/Mes)
=========================================================
Dominio: Produto e Performance de Jogos (Opcional — tabela 23)

Grao: month x game_id
Fonte: ps_bi.fct_casino_activity_daily (colunas jackpot_win/contribution)

KPIs:
    - jackpots_count     → Quantidade de jackpots disparados (estimativa: wins > 0)
    - jackpot_total_paid → Total pago em jackpots
    - avg_jackpot_value  → Valor medio por jackpot
    - contribution_total → Total contribuido para jackpots
    - ggr_total          → GGR total do jogo no periodo
    - jackpot_impact_pct → Jackpots pagos / GGR * 100

NOTA: Nao existe tabela dedicada de jackpots no Athena.
Dados de jackpot vivem como COLUNAS nas tabelas existentes:
    - ps_bi.fct_casino_activity_daily: jackpot_win_amount_local, jackpot_contribution_local
    - vendor_ec2.tbl_vendor_games_mapping_mst: c_has_jackpot (flag por jogo)

Limitacao: nao temos contagem exata de "triggers" de jackpot.
Estimamos pela contagem de player-game-days com jackpot_win > 0.

Fontes (Athena):
    1. ps_bi.fct_casino_activity_daily → Jackpot wins e contributions (BRL)
    2. ps_bi.dim_game                   → Catalogo
    3. bireports_ec2.tbl_ecr           → Filtro test users

Destino: Super Nova DB -> multibet.fact_jackpots
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fact_jackpots.py
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
CREATE TABLE IF NOT EXISTS multibet.fact_jackpots (
    month_start         DATE,
    game_id             VARCHAR(50),
    game_name           VARCHAR(255),
    vendor_id           VARCHAR(50),
    jackpots_count      INTEGER DEFAULT 0,
    jackpot_total_paid  NUMERIC(18,2) DEFAULT 0,
    avg_jackpot_value   NUMERIC(18,2) DEFAULT 0,
    max_jackpot_value   NUMERIC(18,2) DEFAULT 0,
    contribution_total  NUMERIC(18,2) DEFAULT 0,
    ggr_total           NUMERIC(18,2) DEFAULT 0,
    jackpot_impact_pct  NUMERIC(10,4) DEFAULT 0,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (month_start, game_id)
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_fj_month ON multibet.fact_jackpots (month_start);
CREATE INDEX IF NOT EXISTS idx_fj_impact ON multibet.fact_jackpots (month_start, jackpot_impact_pct DESC);
"""

# --- Query Athena ---------------------------------------------------------
# Agregar por mes e jogo, apenas jogos com alguma atividade de jackpot

QUERY_ATHENA = """
WITH valid_players AS (
    SELECT c_ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Atividade diaria com jackpot
daily AS (
    SELECT
        fca.activity_date,
        fca.game_id,
        fca.player_id,
        COALESCE(fca.jackpot_win_amount_local, 0) AS jackpot_win,
        COALESCE(fca.jackpot_contribution_local, 0) AS jackpot_contribution,
        COALESCE(fca.ggr_local, 0) AS ggr
    FROM ps_bi.fct_casino_activity_daily fca
    JOIN valid_players vp ON fca.player_id = vp.c_ecr_id
    WHERE fca.activity_date >= DATE '2025-10-01'
      AND LOWER(fca.product_id) = 'casino'
      -- Filtrar apenas jogos que tem alguma atividade de jackpot
      AND (COALESCE(fca.jackpot_win_amount_local, 0) > 0
           OR COALESCE(fca.jackpot_contribution_local, 0) > 0)
)

SELECT
    date_trunc('month', d.activity_date) AS month_start,
    d.game_id,
    COALESCE(dg.game_desc, 'Desconhecido') AS game_name,
    COALESCE(dg.vendor_id, 'unknown') AS vendor_id,

    -- Contagem de "eventos" de jackpot: player-game-days com win > 0
    COUNT_IF(d.jackpot_win > 0) AS jackpots_count,

    SUM(d.jackpot_win) AS jackpot_total_paid,

    -- Valor medio dos jackpots (apenas quando houve win)
    CASE WHEN COUNT_IF(d.jackpot_win > 0) > 0
         THEN SUM(d.jackpot_win) / COUNT_IF(d.jackpot_win > 0)
         ELSE 0 END AS avg_jackpot_value,

    MAX(d.jackpot_win) AS max_jackpot_value,

    SUM(d.jackpot_contribution) AS contribution_total,
    SUM(d.ggr) AS ggr_total,

    -- Impacto no GGR: jackpots pagos / GGR * 100
    CASE WHEN SUM(d.ggr) > 0
         THEN SUM(d.jackpot_win) * 100.0 / SUM(d.ggr)
         ELSE 0 END AS jackpot_impact_pct

FROM daily d
LEFT JOIN ps_bi.dim_game dg ON d.game_id = dg.game_id
GROUP BY 1, 2, 3, 4
HAVING SUM(d.jackpot_win) > 0 OR SUM(d.jackpot_contribution) > 0
ORDER BY month_start DESC, jackpot_total_paid DESC
"""


def setup_table():
    log.info("Criando tabela fact_jackpots...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    try:
        execute_supernova(DDL_INDEX)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Jackpots por Jogo/Mes)...")
    log.info("Fonte: ps_bi.fct_casino_activity_daily (colunas jackpot)")
    df = query_athena(QUERY_ATHENA, database="ps_bi")
    log.info(f"{len(df)} linhas obtidas (jogos x meses com jackpot).")

    if df.empty:
        log.warning("Nenhum dado de jackpot retornado. Pode nao haver jackpots no periodo.")
        return

    # Sanitiza NULLs de texto ANTES do insert (ver memory/feedback_pandas_nan_or_default_bug.md).
    str_defaults = {
        "game_id":   "",
        "game_name": "Desconhecido",
        "vendor_id": "unknown",
    }
    df = df.fillna(str_defaults)
    for col, default in str_defaults.items():
        df[col] = df[col].astype(str).replace({"nan": default, "NaN": default, "None": default})

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_jackpots
            (month_start, game_id, game_name, vendor_id,
             jackpots_count, jackpot_total_paid, avg_jackpot_value, max_jackpot_value,
             contribution_total, ggr_total, jackpot_impact_pct, refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["month_start"],
            str(row["game_id"] or ""),
            str(row["game_name"] or "Desconhecido"),
            str(row["vendor_id"] or "unknown"),
            int(row["jackpots_count"] or 0),
            float(row["jackpot_total_paid"] or 0),
            float(row["avg_jackpot_value"] or 0),
            float(row["max_jackpot_value"] or 0),
            float(row["contribution_total"] or 0),
            float(row["ggr_total"] or 0),
            float(row["jackpot_impact_pct"] or 0),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_jackpots;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    total_paid = sum(r[5] for r in records)
    total_count = sum(r[4] for r in records)
    unique_games = len(set(r[1] for r in records))
    months = len(set(r[0] for r in records))
    log.info(f"{len(records)} linhas inseridas | {unique_games} jogos | {months} meses")
    log.info(f"  Jackpots disparados: {total_count} | Total pago: R$ {total_paid:,.2f}")
    if total_count > 0:
        log.info(f"  Valor medio: R$ {total_paid/total_count:,.2f}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_jackpots ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
