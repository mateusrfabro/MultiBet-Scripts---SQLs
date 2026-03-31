"""
Pipeline: Grandes Ganhos do Dia
================================
Origem 1: BigQuery (Smartico DW)  — ganhos, jogadores, nomes de jogos
Origem 2: Super Nova DB           — mapeamento de imagens (multibet.game_image_mapping)
Destino : Super Nova DB (PostgreSQL) — tabela multibet.grandes_ganhos

Execução:
    python pipelines/grandes_ganhos.py

Frequência: 1x/dia às 00:30 BRT via cron na EC2.
Pré-requisito: rodar game_image_mapper.py 1x/dia antes para manter mapeamento atualizado.
"""

import sys
import os
import re
import unicodedata
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.bigquery import query_bigquery
from db.supernova import execute_supernova, get_supernova_connection

import pandas as pd
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── DDL ──────────────────────────────────────────────────────────────────────
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.grandes_ganhos (
    id                  SERIAL PRIMARY KEY,

    -- Jogo
    game_name           VARCHAR(255),
    provider_name       VARCHAR(100),
    game_slug           VARCHAR(200),   -- Path de acesso ao jogo no site (ex: /pb/gameplay/aviator/real-game)
    game_image_url      VARCHAR(500),   -- URL do thumbnail no CDN do provedor

    -- Player (hasheado — LGPD)
    player_name_hashed  VARCHAR(50),
    smr_user_id         BIGINT,         -- ID interno Smartico — NÃO expor no front

    -- Ganho
    win_amount          NUMERIC(15, 2),

    -- Controle
    event_time          TIMESTAMPTZ,
    refreshed_at        TIMESTAMPTZ
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_gg_event_time
    ON multibet.grandes_ganhos (event_time DESC);
"""

# ─── SQL BigQuery ──────────────────────────────────────────────────────────────
QUERY_BIGQUERY = """
SELECT
    g.game_name                                              AS game_name,
    p.provider_name                                         AS provider_name,
    CONCAT(
        SUBSTR(u.core_username, 1, 2), '***', RIGHT(u.core_username, 1)
    )                                                        AS player_name_hashed,
    w.user_id                                               AS smr_user_id,
    ROUND(CAST(w.casino_last_win_amount_real AS FLOAT64), 2) AS win_amount,
    w.event_time

FROM `smartico-bq6.dwh_ext_24105.tr_casino_win` w

LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_casino_game_name` g
    ON CAST(w.casino_last_bet_game_name AS INT64) = g.smr_game_id
    AND g.label_id = 24105

LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_casino_provider_name` p
    ON CAST(w.casino_last_bet_game_provider AS INT64) = p.smr_provider_id
    AND p.label_id = 24105

LEFT JOIN `smartico-bq6.dwh_ext_24105.j_user` u
    ON w.user_id = u.user_id

WHERE
    DATE(w.event_time) = CURRENT_DATE()
    AND w.label_id = 24105
    AND CAST(w.casino_last_win_amount_real AS FLOAT64) > 0
    AND u.core_username IS NOT NULL
    AND g.game_name IS NOT NULL

ORDER BY win_amount DESC
LIMIT 50
"""

# ─── SQL Super Nova DB ─────────────────────────────────────────────────────────
# Busca mapeamento de imagens da tabela populada pelo game_image_mapper.py
QUERY_MAPPING = """
SELECT
    game_name_upper,
    game_image_url,
    game_slug
FROM multibet.game_image_mapping
WHERE game_image_url IS NOT NULL
"""


def slugify(name: str) -> str:
    """Converte nome do jogo em slug de URL.

    Ex: 'FORTUNE SNAKE' → 'fortune-snake'
        'AVIATOR'       → 'aviator'
    """
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s_]+", "-", name)
    name = re.sub(r"-+", "-", name)
    return name


def build_game_url(game_name: str) -> str | None:
    """Retorna o path de acesso ao jogo no site da MultiBet.

    Padrão: /pb/gameplay/{slug}/real-game
    Ex: 'AVIATOR' → '/pb/gameplay/aviator/real-game'
    """
    if not game_name:
        return None
    return f"/pb/gameplay/{slugify(game_name)}/real-game"


# ─── Funções principais ────────────────────────────────────────────────────────

def setup_table():
    """Cria schema, tabela e índice no Super Nova DB (idempotente).
    Também aplica migrations para colunas novas caso a tabela já exista."""
    log.info("Verificando/criando tabela multibet.grandes_ganhos...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX)
    # Migration: garante que game_slug existe e remove game_url (substituída pelo game_slug)
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS game_slug VARCHAR(200);")
    execute_supernova("ALTER TABLE multibet.grandes_ganhos DROP COLUMN IF EXISTS game_url;")
    log.info("Tabela pronta.")


def refresh():
    """
    Estratégia: TRUNCATE RESTART IDENTITY + INSERT.
    Cada execução substitui o snapshot completo do dia atual.
    IDs sempre começam em 1.
    """
    # 1. BigQuery — ganhos do dia
    log.info("Buscando maiores ganhos no BigQuery (Smartico)...")
    df = query_bigquery(QUERY_BIGQUERY)
    log.info(f"{len(df)} registros obtidos do BigQuery.")

    if df.empty:
        log.warning("Nenhum ganho encontrado hoje. Abortando refresh.")
        return

    # 2. Super Nova DB — mapeamento de imagens (populado pelo game_image_mapper.py)
    log.info("Buscando mapeamento de imagens no Super Nova DB...")
    try:
        rows = execute_supernova(QUERY_MAPPING, fetch=True) or []
        df_mapping = pd.DataFrame(rows, columns=["game_name_upper", "game_image_url", "game_slug"])
        log.info(f"{len(df_mapping)} jogos com imagem no mapeamento.")
    except Exception as e:
        log.warning(f"Falha ao buscar mapeamento (continuando sem imagens): {e}")
        df_mapping = pd.DataFrame(columns=["game_name_upper", "game_image_url", "game_slug"])

    # 3. Join: BigQuery x Mapeamento via nome do jogo (case-insensitive)
    df["game_name_upper"] = df["game_name"].str.upper().str.strip()

    # Remove duplicatas no mapeamento (não deveria ter, mas por segurança)
    df_mapping_dedup = df_mapping.drop_duplicates(subset="game_name_upper", keep="first")

    df = df.merge(
        df_mapping_dedup[["game_name_upper", "game_image_url", "game_slug"]],
        on="game_name_upper",
        how="left",
    )

    # Fallback para variantes (ex: "ZEUS VS HADES – GODS OF WAR 250"):
    # Remove sufixos numéricos (250, 1000, etc.) e tenta match com o jogo base.
    mask_no_img = df["game_image_url"].isna()
    if mask_no_img.any():
        mapping_lookup = df_mapping_dedup.set_index("game_name_upper")
        for idx in df[mask_no_img].index:
            name = df.at[idx, "game_name_upper"]
            # Remove sufixo numerico (ex: " 250", " 1000") para tentar match com base
            base_name = re.sub(r"\s+\d+$", "", name).strip()
            if base_name != name and base_name in mapping_lookup.index:
                row = mapping_lookup.loc[base_name]
                df.at[idx, "game_image_url"] = row["game_image_url"]
                df.at[idx, "game_slug"] = row["game_slug"]
                log.info(f"  Fallback variante: '{name}' → imagem de '{base_name}'")

    # Fallback: gera game_slug para jogos que não estão no mapeamento
    mask_no_slug = df["game_slug"].isna()
    if mask_no_slug.any():
        df.loc[mask_no_slug, "game_slug"] = df.loc[mask_no_slug, "game_name"].apply(build_game_url)

    with_img = df["game_image_url"].notna().sum()
    with_slug = df["game_slug"].notna().sum()
    log.info(f"Join concluído: {with_img}/{len(df)} com image_url | {with_slug}/{len(df)} com game_slug.")

    # 4. Inserir no Super Nova DB
    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.grandes_ganhos
            (game_name, provider_name, game_slug, game_image_url,
             player_name_hashed, smr_user_id, win_amount, event_time, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            row["game_name"],
            row["provider_name"],
            row.get("game_slug"),
            row.get("game_image_url") if not isinstance(row.get("game_image_url"), float) else None,
            row["player_name_hashed"],
            int(row["smr_user_id"]),
            float(row["win_amount"]),
            row["event_time"].to_pydatetime(),
            now_utc,
        )
        for _, row in df.iterrows()
    ]

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # RESTART IDENTITY garante que os IDs sempre comecem em 1
            cur.execute("TRUNCATE TABLE multibet.grandes_ganhos RESTART IDENTITY;")
            psycopg2.extras.execute_batch(cur, insert_sql, records)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    log.info(f"{len(records)} registros inseridos em multibet.grandes_ganhos.")


if __name__ == "__main__":
    log.info("=== Iniciando pipeline Grandes Ganhos ===")
    setup_table()
    refresh()
    log.info("=== Pipeline concluído ===")
