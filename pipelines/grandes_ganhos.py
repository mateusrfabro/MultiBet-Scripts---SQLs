"""
Pipeline: Grandes Ganhos do Dia
================================
Origem 1: Athena (fund_ec2)       — ganhos casino (c_txn_type=45 CASINO_WIN)
Origem 2: Athena (bireports_ec2)  — nomes de jogos e providers
Origem 3: Athena (ecr_ec2)        — nomes de jogadores (hash LGPD)
Origem 4: Super Nova DB           — mapeamento enriquecido (multibet.game_image_mapping)
Destino : Super Nova DB (PostgreSQL) — tabela multibet.grandes_ganhos

Execução:
    python pipelines/grandes_ganhos.py

Frequência: 4x/dia (a cada 4h) via cron na EC2 — alinhado ao game_image_mapper.
    Sugestao crontab: 30 0,4,8,12,16,20 * * * (BRT, ajustar TZ da EC2)
    Mudanca de 1x/dia -> 4h aprovada em 22/04/2026.
Pré-requisito: rodar game_image_mapper.py antes para manter mapeamento atualizado.

Histórico:
    - v1: BigQuery como fonte principal (até 06/04/2026)
    - v2: Migrado para Athena (fund_ec2 + bireports_ec2 + ecr_ec2) — 06/04/2026
    - v4: + provider_display_name, game_category, game_category_front (JOIN com mapping) — 22/04/2026
          + cron 4h
"""

import sys
import os
import re
import unicodedata
import logging
from datetime import datetime, timezone, timedelta
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
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
    ecr_id              BIGINT,         -- ID interno ECR (Athena) — NÃO expor no front

    -- Ganho
    win_amount          NUMERIC(15, 2),

    -- Controle
    event_time          TIMESTAMPTZ,
    refreshed_at        TIMESTAMPTZ,

    -- v4: enriquecimento do catalogo (vem do JOIN com game_image_mapping)
    provider_display_name VARCHAR(50),  -- ex: "PG Soft" (vendor amigavel)
    game_category         VARCHAR(30),  -- nativo Pragmatic: slots | live | drawgames
    game_category_front   VARCHAR(20)   -- bucket front: Live | Crash | TV Shows | ...
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_gg_event_time
    ON multibet.grandes_ganhos (event_time DESC);
"""

# ─── SQL Athena ───────────────────────────────────────────────────────────────
# Template com placeholder {date_start} e {date_end} (preenchidos em runtime)
QUERY_ATHENA_TEMPLATE = """
WITH game_catalog AS (
    -- Dedup: 1 registro por c_game_id (prioriza plataforma WEB)
    SELECT c_game_id, c_game_desc, c_vendor_id
    FROM (
        SELECT
            c_game_id, c_game_desc, c_vendor_id,
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
),
wins AS (
    SELECT
        f.c_ecr_id,
        f.c_game_id AS game_id_raw,
        -- Extrai parte base do game_id (antes do '_' quando composto, ex: '3008_157309' -> '3008')
        CASE
            WHEN STRPOS(f.c_game_id, '_') > 0
            THEN SPLIT_PART(f.c_game_id, '_', 1)
            ELSE f.c_game_id
        END AS game_id_base,
        f.c_sub_vendor_id AS fund_vendor,
        f.c_amount_in_ecr_ccy / 100.0 AS win_amount,
        f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS event_time_brt,
        f.c_start_time AS event_time_utc
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_txn_type = 45                        -- CASINO_WIN
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_product_id = 'CASINO'
      AND f.c_start_time >= TIMESTAMP '{date_start}'
      AND f.c_start_time <  TIMESTAMP '{date_end}'
      AND f.c_amount_in_ecr_ccy > 0
)
SELECT
    COALESCE(g.c_game_desc, g2.c_game_desc)                 AS game_name,
    COALESCE(g.c_vendor_id, g2.c_vendor_id, w.fund_vendor)  AS provider_name,
    CONCAT(SUBSTR(p.c_fname, 1, 2), '***')                  AS player_name_hashed,
    w.c_ecr_id                                               AS ecr_id,
    w.win_amount,
    w.event_time_brt,
    w.event_time_utc
FROM wins w
-- Match exato com game_id original
LEFT JOIN game_catalog g
    ON w.game_id_raw = g.c_game_id
-- Fallback: match com parte base do game_id (antes do '_')
LEFT JOIN game_catalog g2
    ON w.game_id_base = g2.c_game_id
    AND g.c_game_id IS NULL
-- Nome do jogador para hash LGPD
LEFT JOIN ecr_ec2.tbl_ecr_profile p
    ON w.c_ecr_id = p.c_ecr_id
WHERE COALESCE(g.c_game_desc, g2.c_game_desc) IS NOT NULL
  AND p.c_fname IS NOT NULL
ORDER BY w.win_amount DESC
LIMIT 50
"""

# ─── SQL Super Nova DB ─────────────────────────────────────────────────────────
# Busca mapeamento enriquecido (imagem + slug + provider amigavel + categoria).
# v4 (22/04): adiciona provider_display_name, game_category, game_category_front
QUERY_MAPPING = """
SELECT
    game_name_upper,
    game_image_url,
    game_slug,
    provider_display_name,
    game_category,
    game_category_front
FROM multibet.game_image_mapping
WHERE game_image_url IS NOT NULL
"""


def slugify(name: str) -> str:
    """Converte nome do jogo em slug de URL.

    Ex: 'FORTUNE SNAKE' → 'fortune_snake'
        'AVIATOR'       → 'aviator'
    """
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s-]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name


def build_game_url(game_name: str) -> str | None:
    """Retorna o path de acesso ao jogo no site da MultiBet.

    Padrão: /pb/gameplay/{slug}/real-game
    Ex: 'AVIATOR' → '/pb/gameplay/aviator/real-game'
    """
    if not game_name:
        return None
    return f"/pb/gameplay/{slugify(game_name)}/real-game"


# ─── CDN auto-discovery (validador pós-refresh) ─────────────────────────────
CDN_BASE_URL = "https://multi.bet.br/uploads/games/MUL"

# Vendor → prefixo CDN (validados empiricamente 14/04/2026)
VENDOR_CDN_PREFIX = {
    "pragmaticplay": "pp",
    "pragmaticexternal": "pp",
    "evolution": "alea_evo",
    "tadagaming": "alea_tad",
}


def check_url_exists(url: str, timeout: int = 5) -> bool:
    """HEAD request para verificar se URL do CDN existe (HTTP 200)."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        req.add_header("User-Agent", "Mozilla/5.0")
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status == 200
    except Exception:
        return False


def try_resolve_cdn_url(game_id: str, vendor_id: str) -> str | None:
    """Tenta construir URL CDN baseado em vendor + game_id e verifica com HEAD.

    Padrão CDN: https://multi.bet.br/uploads/games/MUL/{key}/{key}.webp
    Retorna a URL se encontrada, None caso contrário.
    """
    if not game_id or not vendor_id:
        return None

    vendor_lower = str(vendor_id).lower().strip()
    game_id_str = str(game_id).strip()
    candidates = []

    # 1. Prefixo conhecido do vendor
    prefix = VENDOR_CDN_PREFIX.get(vendor_lower)
    if prefix:
        key = f"{prefix}{game_id_str}"
        candidates.append(f"{CDN_BASE_URL}/{key}/{key}.webp")

    # 2. Padrão genérico alea_{vendor_3chars}{game_id}
    vendor_short = vendor_lower[:3]
    key_generic = f"alea_{vendor_short}{game_id_str}"
    if not any(key_generic in c for c in candidates):
        candidates.append(f"{CDN_BASE_URL}/{key_generic}/{key_generic}.webp")

    for url in candidates:
        if check_url_exists(url):
            return url

    return None


# ─── Funções principais ────────────────────────────────────────────────────────

def setup_table():
    """Cria schema, tabela e índice no Super Nova DB (idempotente).
    Também aplica migrations para colunas novas caso a tabela já exista."""
    log.info("Verificando/criando tabela multibet.grandes_ganhos...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX)
    # Migrations
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS game_slug VARCHAR(200);")
    execute_supernova("ALTER TABLE multibet.grandes_ganhos DROP COLUMN IF EXISTS game_url;")
    # v2: migra smr_user_id -> ecr_id (Athena)
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS ecr_id BIGINT;")
    # v4: enriquecimento de catalogo (vem via JOIN com game_image_mapping)
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS provider_display_name VARCHAR(50);")
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS game_category VARCHAR(30);")
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS game_category_front VARCHAR(20);")
    log.info("Tabela pronta.")


def refresh():
    """
    Estratégia: TRUNCATE RESTART IDENTITY + INSERT.
    Cada execução substitui o snapshot completo do dia atual.
    IDs sempre começam em 1.

    v2: usa Athena (fund_ec2 + bireports_ec2 + ecr_ec2) em vez de BigQuery.
    """
    # 1. Athena — ganhos casino do dia (CASINO_WIN, c_txn_type=45)
    # Roda às 00:30 BRT, então pega o dia anterior completo
    today_brt = datetime.now(timezone(timedelta(hours=-3))).date()
    date_start = today_brt.strftime("%Y-%m-%d")
    date_end = (today_brt + timedelta(days=1)).strftime("%Y-%m-%d")

    query = QUERY_ATHENA_TEMPLATE.format(date_start=date_start, date_end=date_end)
    log.info(f"Buscando maiores ganhos no Athena (fund_ec2) para {date_start}...")
    df = query_athena(query, database="fund_ec2")
    log.info(f"{len(df)} registros obtidos do Athena.")

    if df.empty:
        log.warning("Nenhum ganho encontrado hoje. Abortando refresh.")
        return

    # 2. Super Nova DB — mapeamento enriquecido (populado pelo game_image_mapper.py)
    # v4: alem de imagem/slug, tras provider_display_name + game_category + game_category_front
    log.info("Buscando mapeamento enriquecido no Super Nova DB...")
    mapping_cols = [
        "game_name_upper", "game_image_url", "game_slug",
        "provider_display_name", "game_category", "game_category_front",
    ]
    try:
        rows = execute_supernova(QUERY_MAPPING, fetch=True) or []
        df_mapping = pd.DataFrame(rows, columns=mapping_cols)
        log.info(f"{len(df_mapping)} jogos com imagem no mapeamento.")
    except Exception as e:
        log.warning(f"Falha ao buscar mapeamento (continuando sem imagens): {e}")
        df_mapping = pd.DataFrame(columns=mapping_cols)

    # 3. Join: Athena x Mapeamento via nome do jogo (case-insensitive)
    df["game_name_upper"] = df["game_name"].str.upper().str.strip()

    df_mapping_dedup = df_mapping.drop_duplicates(subset="game_name_upper", keep="first")

    df = df.merge(
        df_mapping_dedup[mapping_cols],
        on="game_name_upper",
        how="left",
    )

    # Fallback para variantes (ex: "ZEUS VS HADES – GODS OF WAR 250"):
    # Remove sufixos numéricos (250, 1000, etc.) e tenta match com o jogo base.
    enrichment_cols = ["game_image_url", "game_slug", "provider_display_name",
                       "game_category", "game_category_front"]
    mask_no_img = df["game_image_url"].isna()
    if mask_no_img.any():
        mapping_lookup = df_mapping_dedup.set_index("game_name_upper")
        for idx in df[mask_no_img].index:
            name = df.at[idx, "game_name_upper"]
            base_name = re.sub(r"\s+\d+$", "", name).strip()
            if base_name != name and base_name in mapping_lookup.index:
                row = mapping_lookup.loc[base_name]
                # Propaga TODOS os campos enriquecidos (imagem + categoria + provider)
                for col in enrichment_cols:
                    df.at[idx, col] = row[col]
                log.info(f"  Fallback variante: '{name}' → enriquecimento de '{base_name}'")

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
             player_name_hashed, ecr_id, win_amount, event_time, refreshed_at,
             provider_display_name, game_category, game_category_front)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    def _nan_to_none(v):
        """pd NaN -> None (psycopg2 nao converte NaN automaticamente)."""
        if v is None:
            return None
        if isinstance(v, float) and pd.isna(v):
            return None
        return v

    records = [
        (
            row["game_name"],
            row["provider_name"],
            _nan_to_none(row.get("game_slug")),
            _nan_to_none(row.get("game_image_url")),
            row["player_name_hashed"],
            int(row["ecr_id"]),
            float(row["win_amount"]),
            row["event_time_utc"],
            now_utc,
            _nan_to_none(row.get("provider_display_name")),
            _nan_to_none(row.get("game_category")),
            _nan_to_none(row.get("game_category_front")),
        )
        for _, row in df.iterrows()
    ]

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.grandes_ganhos RESTART IDENTITY;")
            psycopg2.extras.execute_batch(cur, insert_sql, records)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    log.info(f"{len(records)} registros inseridos em multibet.grandes_ganhos.")


def validate_and_fix_images():
    """Validador pós-refresh: verifica se há jogos sem game_image_url na grandes_ganhos.

    Se houver, consulta o catálogo Athena para obter game_id/vendor, tenta descobrir
    a URL CDN automaticamente (HEAD request), e atualiza game_image_mapping + grandes_ganhos.

    Só roda quando há jogos com game_image_url NULL — se tudo estiver OK, pula.
    """
    log.info("--- Validando game_image_url nos registros inseridos ---")

    # 1. Busca jogos sem imagem na grandes_ganhos
    rows_missing = execute_supernova(
        """
        SELECT DISTINCT game_name
        FROM multibet.grandes_ganhos
        WHERE game_image_url IS NULL
           OR TRIM(game_image_url) = ''
        """,
        fetch=True,
    ) or []

    if not rows_missing:
        log.info("Validacao OK: todos os jogos possuem game_image_url.")
        return

    # Mapeia UPPER → nome original (preserva casing)
    missing_map = {r[0].upper().strip(): r[0] for r in rows_missing}
    missing_names = list(missing_map.keys())
    log.warning(f"{len(missing_names)} jogos sem game_image_url: {list(missing_map.values())}")

    # 2. Busca game_id + vendor no catálogo Athena (bireports)
    names_escaped = [n.replace("'", "''") for n in missing_names]
    names_sql = ", ".join([f"'{n}'" for n in names_escaped])

    query_catalog = f"""
    SELECT
        UPPER(TRIM(c_game_desc))  AS game_name_upper,
        c_game_id,
        c_vendor_id
    FROM (
        SELECT
            c_game_desc, c_game_id, c_vendor_id,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(c_game_desc))
                ORDER BY CASE WHEN c_client_platform = 'WEB' THEN 0 ELSE 1 END
            ) AS rn
        FROM bireports_ec2.tbl_vendor_games_mapping_data
        WHERE c_status = 'active'
          AND UPPER(TRIM(c_game_desc)) IN ({names_sql})
    )
    WHERE rn = 1
    """

    try:
        df_catalog = query_athena(query_catalog, database="bireports_ec2")
        log.info(f"Catalogo Athena: {len(df_catalog)} jogos encontrados para resolucao.")
    except Exception as e:
        log.error(f"Falha ao buscar catalogo Athena: {e}")
        return

    if df_catalog.empty:
        log.warning("Nenhum jogo faltante no catalogo Athena. Correcao manual necessaria (fix_missing_game_images.py).")
        return

    # 3. Tenta descobrir URL CDN para cada jogo (HEAD request)
    fixed = []
    for _, row in df_catalog.iterrows():
        game_name_upper = row["game_name_upper"]
        game_id = row["c_game_id"]
        vendor_id = row["c_vendor_id"]

        url = try_resolve_cdn_url(game_id, vendor_id)
        if url:
            log.info(f"  CDN encontrado: {game_name_upper} -> {url}")
            original_name = missing_map.get(game_name_upper, game_name_upper.title())
            fixed.append({
                "game_name": original_name,
                "game_name_upper": game_name_upper,
                "game_id": str(game_id),
                "vendor_id": vendor_id,
                "url": url,
            })
        else:
            log.warning(f"  CDN NAO encontrado: {game_name_upper} (vendor={vendor_id}, game_id={game_id})")

    if not fixed:
        log.warning("Nenhuma URL CDN descoberta automaticamente. Correcao manual necessaria (fix_missing_game_images.py).")
        return

    # 4. Atualiza game_image_mapping + grandes_ganhos
    now_utc = datetime.now(timezone.utc)
    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for f in fixed:
                slug = build_game_url(f["game_name"])

                # Upsert no game_image_mapping (persiste para próximas execuções)
                cur.execute("""
                    INSERT INTO multibet.game_image_mapping
                        (game_name, game_name_upper, provider_game_id, vendor_id,
                         game_image_url, game_slug, source, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, 'auto_fix', %s)
                    ON CONFLICT (game_name_upper) DO UPDATE SET
                        game_image_url = EXCLUDED.game_image_url,
                        game_slug      = EXCLUDED.game_slug,
                        source         = EXCLUDED.source,
                        updated_at     = EXCLUDED.updated_at
                """, (f["game_name"], f["game_name_upper"], f["game_id"],
                      f["vendor_id"], f["url"], slug, now_utc))

                # Update direto na grandes_ganhos (corrige o dado que o front lê)
                cur.execute("""
                    UPDATE multibet.grandes_ganhos
                    SET game_image_url = %s,
                        game_slug     = COALESCE(game_slug, %s)
                    WHERE UPPER(TRIM(game_name)) = %s
                      AND (game_image_url IS NULL OR TRIM(game_image_url) = '')
                """, (f["url"], slug, f["game_name_upper"]))

        conn.commit()
        log.info(f"Auto-fix concluido: {len(fixed)} jogos corrigidos em game_image_mapping + grandes_ganhos.")
    finally:
        conn.close()
        ssh.close()


if __name__ == "__main__":
    log.info("=== Iniciando pipeline Grandes Ganhos ===")
    setup_table()
    refresh()
    validate_and_fix_images()
    log.info("=== Pipeline concluído ===")
