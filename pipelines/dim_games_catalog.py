"""
Pipeline: dim_games_catalog (ps_bi + vendor_ec2 — Catalogo de Jogos)
=====================================================================
Dominio: Produto e Performance de Jogos (Opcional — tabela 21)

Grao: game_id (snapshot do catalogo)
Fonte: ps_bi.dim_game + vendor_ec2.tbl_vendor_games_mapping_mst (flags)

KPIs / metricas:
    - Jogos ativos vs inativos
    - Mix por categoria % (Slots, Live, Outros)
    - Coverage de esportes (via sportsbook)
    - Flags: has_jackpot, free_spin_game, feature_trigger_game

Fontes (Athena):
    1. ps_bi.dim_game                           → Catalogo base (11 cols)
    2. vendor_ec2.tbl_vendor_games_mapping_mst  → Flags (has_jackpot, free_spin, etc.)
    3. casino_ec2.tbl_casino_game_category_mst  → Categorias detalhadas

Destino: Super Nova DB -> multibet.dim_games_catalog
Estrategia: TRUNCATE + INSERT (snapshot)

Execucao:
    python pipelines/dim_games_catalog.py
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
CREATE TABLE IF NOT EXISTS multibet.dim_games_catalog (
    game_id                 VARCHAR(50) PRIMARY KEY,
    game_name               VARCHAR(255),
    vendor_id               VARCHAR(50),
    sub_vendor_id           VARCHAR(50),
    product_id              VARCHAR(30),
    game_category           VARCHAR(100),
    game_category_desc      VARCHAR(100),
    game_type_id            INTEGER,
    game_type_desc          VARCHAR(255),
    status                  VARCHAR(30),
    game_technology         VARCHAR(30),
    has_jackpot             BOOLEAN DEFAULT FALSE,
    free_spin_game          BOOLEAN DEFAULT FALSE,
    feature_trigger_game    BOOLEAN DEFAULT FALSE,
    snapshot_dt             DATE DEFAULT CURRENT_DATE,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW()
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_dgc_vendor ON multibet.dim_games_catalog (vendor_id);
CREATE INDEX IF NOT EXISTS idx_dgc_category ON multibet.dim_games_catalog (game_category);
CREATE INDEX IF NOT EXISTS idx_dgc_status ON multibet.dim_games_catalog (status);
"""

# --- Query Athena ---------------------------------------------------------
# Combinar dim_game (principal) com vendor_games_mapping_mst (flags)
# Deduplicar por game_id (pegar H5/WEB como preferencia)

QUERY_ATHENA = """
WITH game_flags AS (
    -- Flags de jackpot/freespin por game_id (deduplicar plataformas)
    SELECT
        c_game_id,
        MAX(c_sub_vendor_id) AS sub_vendor_id,
        MAX(c_game_technology) AS game_technology,
        MAX(CASE WHEN c_has_jackpot = '1' OR c_has_jackpot = 'true' THEN true ELSE false END) AS has_jackpot,
        MAX(c_free_spin_game) AS free_spin_game,
        MAX(c_feature_trigger_game) AS feature_trigger_game
    FROM vendor_ec2.tbl_vendor_games_mapping_mst
    GROUP BY c_game_id
)

SELECT
    dg.game_id,
    dg.game_desc AS game_name,
    dg.vendor_id,
    COALESCE(gf.sub_vendor_id, '') AS sub_vendor_id,
    dg.product_id,
    dg.game_category,
    dg.game_category_desc,
    dg.game_type_id,
    dg.game_type_desc,
    dg.status,
    COALESCE(gf.game_technology, 'H5') AS game_technology,
    COALESCE(gf.has_jackpot, false) AS has_jackpot,
    COALESCE(gf.free_spin_game, false) AS free_spin_game,
    COALESCE(gf.feature_trigger_game, false) AS feature_trigger_game,
    CURRENT_DATE AS snapshot_dt
FROM ps_bi.dim_game dg
LEFT JOIN game_flags gf ON dg.game_id = gf.c_game_id
ORDER BY dg.vendor_id, dg.game_category, dg.game_desc
"""


def setup_table():
    log.info("Criando tabela dim_games_catalog...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    try:
        execute_supernova(DDL_INDEX)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    log.info("Tabela pronta.")


def refresh():
    log.info("Executando query no Athena (Catalogo de Jogos)...")
    log.info("Fontes: ps_bi.dim_game + vendor_ec2.tbl_vendor_games_mapping_mst")
    df = query_athena(QUERY_ATHENA, database="ps_bi")
    log.info(f"{len(df)} jogos obtidos do catalogo.")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    # Sanitiza NULLs de texto ANTES do insert (ver memory/feedback_pandas_nan_or_default_bug.md).
    str_defaults = {
        "game_id":            "",
        "game_name":          "",
        "vendor_id":          "",
        "sub_vendor_id":      "",
        "product_id":         "",
        "game_category":      "",
        "game_category_desc": "",
        "game_type_desc":     "",
        "status":             "",
        "game_technology":    "H5",
    }
    df = df.fillna(str_defaults)
    for col, default in str_defaults.items():
        df[col] = df[col].astype(str).replace({"nan": default, "NaN": default, "None": default})

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.dim_games_catalog
            (game_id, game_name, vendor_id, sub_vendor_id, product_id,
             game_category, game_category_desc, game_type_id, game_type_desc,
             status, game_technology,
             has_jackpot, free_spin_game, feature_trigger_game,
             snapshot_dt, refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            str(row["game_id"] or ""),
            str(row["game_name"] or ""),
            str(row["vendor_id"] or ""),
            str(row["sub_vendor_id"] or ""),
            str(row["product_id"] or ""),
            str(row["game_category"] or ""),
            str(row["game_category_desc"] or ""),
            int(row["game_type_id"]) if row["game_type_id"] is not None else 0,
            str(row["game_type_desc"] or ""),
            str(row["status"] or ""),
            str(row["game_technology"] or "H5"),
            bool(row["has_jackpot"]),
            bool(row["free_spin_game"]),
            bool(row["feature_trigger_game"]),
            row["snapshot_dt"],
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.dim_games_catalog;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    # Resumo
    total = len(records)
    active = sum(1 for r in records if r[9] == 'active')
    inactive = total - active
    categories = {}
    for r in records:
        cat = r[5] or 'Outros'
        categories[cat] = categories.get(cat, 0) + 1
    jackpot_games = sum(1 for r in records if r[11])

    log.info(f"{total} jogos inseridos | {active} ativos | {inactive} inativos")
    log.info(f"  Jogos com jackpot: {jackpot_games}")
    log.info(f"  Categorias: {dict(sorted(categories.items(), key=lambda x: -x[1]))}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline dim_games_catalog ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
