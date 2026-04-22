"""
Pipeline: fact_casino_rounds (ps_bi — Performance de Jogos Casino)
===================================================================
Dominio: Produto e Performance de Jogos (Prioridade 4 — tabela 18)

Grao: dt (dia) x game_id
Fonte PRIMARIA: ps_bi.fct_casino_activity_daily (pre-agregado, BRL, real/bonus split)
  - Vantagem: dados ja validados pelo dbt/Pragmatic, custo Athena muito menor
  - Baseline de validacao: fct_casino_activity.py (fund_ec2, sub-fund isolation Mauro)

KPIs por jogo por dia:
    - qty_players, total_rounds (bet_count), rounds_per_player
    - turnover (real/bonus/total), wins, ggr
    - hold_rate_pct = GGR / Turnover * 100
    - rtp_pct = Wins / Turnover * 100
    - jackpot_win, jackpot_contribution
    - free_spins_bet, free_spins_win

Fontes (Athena ps_bi):
    1. ps_bi.fct_casino_activity_daily  → Atividade casino player/game/dia (BRL)
    2. ps_bi.dim_game                    → Catalogo de jogos (nome, vendor, categoria)
    3. bireports_ec2.tbl_ecr            → Filtro test users (c_test_user = false)

Destino: Super Nova DB -> multibet.fact_casino_rounds
Estrategia: TRUNCATE + INSERT
Backfill: desde 2025-10-01

Execucao:
    python pipelines/fact_casino_rounds.py
"""

import sys, os, logging
import pandas as pd
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
CREATE TABLE IF NOT EXISTS multibet.fact_casino_rounds (
    dt                      DATE,
    game_id                 VARCHAR(50),
    game_name               VARCHAR(255),
    vendor_id               VARCHAR(50),
    sub_vendor_id           VARCHAR(50),
    game_category           VARCHAR(100),
    qty_players             INTEGER DEFAULT 0,
    total_rounds            INTEGER DEFAULT 0,
    rounds_per_player       NUMERIC(10,2) DEFAULT 0,
    turnover_real           NUMERIC(18,2) DEFAULT 0,
    wins_real               NUMERIC(18,2) DEFAULT 0,
    ggr_real                NUMERIC(18,2) DEFAULT 0,
    turnover_bonus          NUMERIC(18,2) DEFAULT 0,
    wins_bonus              NUMERIC(18,2) DEFAULT 0,
    ggr_bonus               NUMERIC(18,2) DEFAULT 0,
    turnover_total          NUMERIC(18,2) DEFAULT 0,
    wins_total              NUMERIC(18,2) DEFAULT 0,
    ggr_total               NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct           NUMERIC(10,4) DEFAULT 0,
    rtp_pct                 NUMERIC(10,4) DEFAULT 0,
    jackpot_win             NUMERIC(18,2) DEFAULT 0,
    jackpot_contribution    NUMERIC(18,2) DEFAULT 0,
    free_spins_bet          NUMERIC(18,2) DEFAULT 0,
    free_spins_win          NUMERIC(18,2) DEFAULT 0,
    provider_display_name   VARCHAR(50),                 -- v4.2: vem de game_image_mapping
    game_category_front     VARCHAR(20),                 -- v4.2: vem de game_image_mapping
    refreshed_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, game_id)
);
"""

# Idempotente: garante que tabelas pre-existentes (pre-v4.2) ganhem as 2 colunas.
DDL_ALTER_V42 = """
ALTER TABLE multibet.fact_casino_rounds
    ADD COLUMN IF NOT EXISTS provider_display_name VARCHAR(50),
    ADD COLUMN IF NOT EXISTS game_category_front   VARCHAR(20);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_fcr_vendor ON multibet.fact_casino_rounds (vendor_id, dt);
CREATE INDEX IF NOT EXISTS idx_fcr_ggr ON multibet.fact_casino_rounds (dt, ggr_total DESC);
CREATE INDEX IF NOT EXISTS idx_fcr_category ON multibet.fact_casino_rounds (game_category, dt);
"""

# --- Query Athena (ps_bi — pre-agregado, BRL, validado) -------------------
# ps_bi.fct_casino_activity_daily: grao = player_id x game_id x activity_date
# Agregamos por game_id x activity_date (removendo dimensao player)
# Valores ja em BRL (sem divisao por 100)
# Filtro test users via bireports_ec2.tbl_ecr
#
# FIX Gusta bloqueador #1 (10/04/2026):
#   ps_bi.dim_game e INCOMPLETO (cobre 0.2% do turnover, PG Soft/Spribe/Evolution ausentes).
#   Troca para bireports_ec2.tbl_vendor_games_mapping_data (catalogo completo, validado).
#   Tratamento de game_ids compostos (ex: '7617_164515') via SPLIT_PART.
#   Mantem ps_bi.dim_game SOMENTE como fallback para game_category (coluna nao existe
#   em tbl_vendor_games_mapping_data).

QUERY_ATHENA = """
WITH valid_players AS (
    SELECT c_ecr_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Catalogo principal: bireports (cobertura 99%+, inclui PG Soft, Spribe, Evolution, etc.)
-- Deduplicado por game_id: prioriza registro com client_platform = 'WEB'
--
-- FIX Gusta v4.1 (10/04/2026): puxa TAMBEM c_game_category direto do bireports
-- (coberura 97.9%: 2010 Slots + 641 Live + 58 NULL). Elimina dependencia do
-- ps_bi.dim_game (que so cobre 0.2% do turnover) como fallback de categoria.
-- Antes: 2.349 jogos caiam em 'Outros'. Depois: 97.9% classificados.
game_catalog AS (
    SELECT c_game_id, c_game_desc, c_vendor_id, c_game_category, c_game_type_desc
    FROM (
        SELECT
            c_game_id, c_game_desc, c_vendor_id,
            c_game_category, c_game_type_desc,
            ROW_NUMBER() OVER (
                PARTITION BY c_game_id
                ORDER BY CASE WHEN c_client_platform = 'WEB' THEN 0 ELSE 1 END
            ) AS rn
        FROM bireports_ec2.tbl_vendor_games_mapping_data
        WHERE c_status = 'active'
          AND c_game_id IS NOT NULL
          AND c_game_desc IS NOT NULL
    )
    WHERE rn = 1
)

SELECT
    fca.activity_date AS dt,
    fca.game_id,
    -- Primeiro tenta match direto, depois com SPLIT_PART (game_ids compostos '7617_164515')
    COALESCE(gc.c_game_desc, gc2.c_game_desc, 'Desconhecido') AS game_name,
    COALESCE(gc.c_vendor_id, gc2.c_vendor_id,
             COALESCE(MAX(fca.sub_vendor_id), 'unknown')) AS vendor_id,
    COALESCE(MAX(fca.sub_vendor_id), '') AS sub_vendor_id,
    -- v4.1: categoria direto do bireports (97.9% cobertura). Fallback 'Outros' reduzido a ~2%.
    COALESCE(gc.c_game_category, gc2.c_game_category, 'Outros') AS game_category,

    COUNT(DISTINCT fca.player_id) AS qty_players,
    CAST(SUM(fca.bet_count) AS INTEGER) AS total_rounds,
    CAST(SUM(fca.bet_count) AS DOUBLE) / NULLIF(COUNT(DISTINCT fca.player_id), 0) AS rounds_per_player,

    SUM(COALESCE(fca.real_bet_amount_local, 0)) AS turnover_real,
    SUM(COALESCE(fca.real_win_amount_local, 0)) AS wins_real,
    SUM(COALESCE(fca.real_bet_amount_local, 0)) - SUM(COALESCE(fca.real_win_amount_local, 0)) AS ggr_real,

    SUM(COALESCE(fca.bonus_bet_amount_local, 0)) AS turnover_bonus,
    SUM(COALESCE(fca.bonus_win_amount_local, 0)) AS wins_bonus,
    SUM(COALESCE(fca.bonus_bet_amount_local, 0)) - SUM(COALESCE(fca.bonus_win_amount_local, 0)) AS ggr_bonus,

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

    SUM(COALESCE(fca.jackpot_win_amount_local, 0)) AS jackpot_win,
    SUM(COALESCE(fca.jackpot_contribution_local, 0)) AS jackpot_contribution,

    SUM(COALESCE(fca.free_spins_bet_amount_local, 0)) AS free_spins_bet,
    SUM(COALESCE(fca.free_spins_win_amount_local, 0)) AS free_spins_win

FROM ps_bi.fct_casino_activity_daily fca
-- Match direto: game_id exato no catalogo bireports
LEFT JOIN game_catalog gc
    ON fca.game_id = gc.c_game_id
-- Fallback: game_ids compostos ('7617_164515' -> '7617')
LEFT JOIN game_catalog gc2
    ON (CASE WHEN STRPOS(fca.game_id, '_') > 0
             THEN SPLIT_PART(fca.game_id, '_', 1)
             ELSE fca.game_id END) = gc2.c_game_id
    AND gc.c_game_id IS NULL
JOIN valid_players vp ON fca.player_id = vp.c_ecr_id
WHERE fca.activity_date >= DATE '2025-10-01'
  AND LOWER(fca.product_id) = 'casino'
GROUP BY fca.activity_date, fca.game_id,
         gc.c_game_desc, gc.c_vendor_id, gc.c_game_category,
         gc2.c_game_desc, gc2.c_vendor_id, gc2.c_game_category
ORDER BY 1 DESC, ggr_total DESC
"""


def setup_table():
    log.info("Criando tabela fact_casino_rounds...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_ALTER_V42)  # v4.2: idempotente p/ tabelas antigas
    try:
        execute_supernova(DDL_INDEX)
    except Exception as e:
        log.warning(f"Indices ja existem ou erro menor: {e}")
    log.info("Tabela pronta.")


def load_enrichment_lookup():
    """
    Carrega mapping da game_image_mapping p/ enriquecer a fact.
    Retorna 2 Series indexadas (p/ match por game_id e por game_name_upper).
    """
    log.info("Carregando game_image_mapping p/ enrichment (v4.2)...")
    sql = """
        SELECT provider_game_id,
               UPPER(TRIM(game_name)) AS game_name_upper,
               provider_display_name,
               game_category_front
        FROM multibet.game_image_mapping
        WHERE provider_display_name IS NOT NULL
           OR game_category_front IS NOT NULL
    """
    ssh, conn = get_supernova_connection()
    try:
        lookup = pd.read_sql(sql, conn)
    finally:
        conn.close()
        if ssh:
            ssh.close()
    log.info(f"  lookup: {len(lookup):,} linhas em game_image_mapping")
    return lookup


def enrich_df(df: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece o df do Athena com provider_display_name e game_category_front.
    Prioriza match por game_id; fallback por UPPER(TRIM(game_name)).
    """
    # Match 1: game_id exato
    by_gid = lookup.dropna(subset=["provider_game_id"]).drop_duplicates("provider_game_id").set_index("provider_game_id")
    df["provider_display_name"] = df["game_id"].map(by_gid["provider_display_name"])
    df["game_category_front"]   = df["game_id"].map(by_gid["game_category_front"])

    # Match 2: fallback por game_name (pra game_ids compostos que nao batem)
    mask = df["provider_display_name"].isna() | df["game_category_front"].isna()
    if mask.any():
        by_name = lookup.dropna(subset=["game_name_upper"]).drop_duplicates("game_name_upper").set_index("game_name_upper")
        keys = df.loc[mask, "game_name"].str.upper().str.strip()
        df.loc[mask & df["provider_display_name"].isna(), "provider_display_name"] = keys.map(by_name["provider_display_name"])
        df.loc[mask & df["game_category_front"].isna(),   "game_category_front"]   = keys.map(by_name["game_category_front"])

    cobertura_prov = 100 * df["provider_display_name"].notna().mean()
    cobertura_cat  = 100 * df["game_category_front"].notna().mean()
    log.info(f"  Enrichment: display {cobertura_prov:.1f}% | category_front {cobertura_cat:.1f}%")
    return df


def refresh():
    log.info("Executando query no Athena (ps_bi — Casino por Jogo)...")
    log.info("Fonte: ps_bi.fct_casino_activity_daily + dim_game + filtro test users")
    df = query_athena(QUERY_ATHENA, database="ps_bi")
    log.info(f"{len(df)} linhas obtidas (jogos x dias).")

    if df.empty:
        log.warning("Nenhum dado retornado. Abortando.")
        return

    # Sanitiza NULLs de texto ANTES do insert.
    # Motivo: pandas carrega NULL do Athena como numpy.nan (float). Expressao
    # `str(row["x"] or default)` NAO funciona: NaN e truthy (float != 0), entao
    # retorna NaN e `str(NaN)` -> "nan"/"NaN" — vira string literal no Postgres
    # e escapa do COALESCE do dashboard (vw_casino_by_provider / app.py Gusta).
    str_defaults = {
        "game_id":       "",
        "game_name":     "Desconhecido",
        "vendor_id":     "unknown",
        "sub_vendor_id": "",
        "game_category": "Outros",
    }
    df = df.fillna(str_defaults)
    for col, default in str_defaults.items():
        df[col] = df[col].astype(str).replace({"nan": default, "NaN": default, "None": default})

    # v4.2: enriquece com provider_display_name + game_category_front (lookup em game_image_mapping)
    df = enrich_df(df, load_enrichment_lookup())

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_casino_rounds
            (dt, game_id, game_name, vendor_id, sub_vendor_id, game_category,
             qty_players, total_rounds, rounds_per_player,
             turnover_real, wins_real, ggr_real,
             turnover_bonus, wins_bonus, ggr_bonus,
             turnover_total, wins_total, ggr_total,
             hold_rate_pct, rtp_pct,
             jackpot_win, jackpot_contribution,
             free_spins_bet, free_spins_win,
             provider_display_name, game_category_front,
             refreshed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    records = []
    for _, row in df.iterrows():
        records.append((
            row["dt"],
            str(row["game_id"] or ""),
            str(row["game_name"] or "Desconhecido"),
            str(row["vendor_id"] or "unknown"),
            str(row["sub_vendor_id"] or ""),
            str(row["game_category"] or "Outros"),
            int(row["qty_players"]),
            int(row["total_rounds"] or 0),
            float(row["rounds_per_player"] or 0),
            float(row["turnover_real"] or 0),
            float(row["wins_real"] or 0),
            float(row["ggr_real"] or 0),
            float(row["turnover_bonus"] or 0),
            float(row["wins_bonus"] or 0),
            float(row["ggr_bonus"] or 0),
            float(row["turnover_total"] or 0),
            float(row["wins_total"] or 0),
            float(row["ggr_total"] or 0),
            float(row["hold_rate_pct"] or 0),
            float(row["rtp_pct"] or 0),
            float(row["jackpot_win"] or 0),
            float(row["jackpot_contribution"] or 0),
            float(row["free_spins_bet"] or 0),
            float(row["free_spins_win"] or 0),
            # v4.2: enrichment cols (NaN -> None p/ virar NULL no Postgres)
            (row["provider_display_name"] if pd.notna(row["provider_display_name"]) else None),
            (row["game_category_front"]   if pd.notna(row["game_category_front"])   else None),
            now_utc,
        ))

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.fact_casino_rounds;")
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    # Resumo
    total_ggr = sum(r[17] for r in records)
    total_turnover = sum(r[15] for r in records)
    total_rounds = sum(r[7] for r in records)
    total_jackpot = sum(r[20] for r in records)
    unique_games = len(set(r[1] for r in records))
    unique_vendors = len(set(r[3] for r in records))
    avg_hold = (total_ggr / total_turnover * 100) if total_turnover > 0 else 0

    log.info(f"{len(records)} linhas inseridas | {unique_games} jogos | {unique_vendors} vendors")
    log.info(f"  Turnover: R$ {total_turnover:,.2f} | GGR: R$ {total_ggr:,.2f}")
    log.info(f"  Hold Rate medio: {avg_hold:.2f}% | Rodadas: {total_rounds:,}")
    log.info(f"  Jackpot Wins: R$ {total_jackpot:,.2f}")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline fact_casino_rounds (ps_bi — por Jogo) ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluido ===")
