"""
Pipeline: Game Image Mapper
============================
Popula multibet.game_image_mapping com URLs de imagem para todos os jogos.

Fontes:
  1. CSV gerado pelo scraper (capturar_jogos_pc.py) — nome + game_image_url
  2. Redshift (Pragmatic Solutions)                 — provider_game_id + vendor_id

O CSV é a fonte principal de imagens (extraídas direto do site multi.bet.br).
O Redshift complementa com metadados do catálogo (vendor, game_id).

Execução:
    python pipelines/game_image_mapper.py [--scraper]

    --scraper : roda o scraper antes para atualizar o CSV (requer Playwright)

Frequência: 1x por dia, antes do grandes_ganhos.
"""

import sys
import os
import re
import csv
import unicodedata
import logging
import argparse
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.redshift import query_redshift
from db.supernova import execute_supernova, get_supernova_connection

import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Caminhos ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_PATH = os.path.join(PROJECT_ROOT, "pipelines", "jogos.csv")

# ─── DDL ──────────────────────────────────────────────────────────────────────
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.game_image_mapping (
    id                  SERIAL PRIMARY KEY,
    game_name           VARCHAR(255) NOT NULL,
    game_name_upper     VARCHAR(255) NOT NULL,
    provider_game_id    VARCHAR(50),
    vendor_id           VARCHAR(100),
    game_image_url      VARCHAR(500),
    game_slug           VARCHAR(200),
    source              VARCHAR(50) DEFAULT 'scraper',
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_game_name_upper UNIQUE (game_name_upper)
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_gim_game_name_upper
    ON multibet.game_image_mapping (game_name_upper);
"""

# ─── SQL Redshift ─────────────────────────────────────────────────────────────
# Catálogo completo de jogos ativos para enriquecer com provider_game_id e vendor_id
QUERY_REDSHIFT_GAMES = """
SELECT
    UPPER(TRIM(c_game_desc))  AS game_name_upper,
    c_vendor_id               AS vendor_id,
    c_game_id                 AS provider_game_id
FROM lake.vw_bireports_vendor_games_mapping_data
WHERE c_status = 'active'
  AND c_game_id IS NOT NULL
  AND c_game_desc IS NOT NULL
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def slugify(name: str) -> str:
    """Converte nome do jogo em slug de URL.
    Ex: 'Fortune Ox' → 'fortune-ox'
    """
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s_]+", "-", name)
    name = re.sub(r"-+", "-", name)
    return name


def load_csv(path: str) -> list[dict]:
    """Lê o CSV do scraper e retorna lista de {nome, url}."""
    if not os.path.exists(path):
        log.warning(f"CSV não encontrado: {path}")
        log.warning("Rode o scraper primeiro: python pipelines/capturar_jogos_pc.py")
        return []

    jogos = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            nome = row.get("nome", "").strip()
            url = row.get("url", "").strip()
            if nome and url:
                # Ignora placeholder
                if "placeholder" in url.lower():
                    continue
                jogos.append({"nome": nome, "url": url})

    return jogos


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def setup_table():
    """Cria schema, tabela e índice (idempotente)."""
    log.info("Verificando/criando tabela multibet.game_image_mapping...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX)
    log.info("Tabela pronta.")


def run_scraper():
    """Executa o scraper Playwright para atualizar o CSV."""
    log.info("Executando scraper para atualizar jogos.csv...")
    import subprocess
    python_exe = sys.executable
    result = subprocess.run(
        [python_exe, os.path.join(PROJECT_ROOT, "pipelines", "capturar_jogos_pc.py")],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=600,  # 10 min máximo
    )
    if result.returncode != 0:
        log.error(f"Scraper falhou:\n{result.stderr}")
        raise RuntimeError("Scraper falhou")
    log.info("Scraper concluído com sucesso.")


def refresh():
    """
    Lê CSV + Redshift, faz upsert no Super Nova DB.
    Estratégia: UPSERT (INSERT ON CONFLICT UPDATE).
    """
    # 1. Lê CSV do scraper
    log.info(f"Lendo CSV: {CSV_PATH}")
    jogos_csv = load_csv(CSV_PATH)
    log.info(f"{len(jogos_csv)} jogos no CSV.")

    if not jogos_csv:
        log.error("Nenhum jogo no CSV. Abortando.")
        return

    # 2. Monta dicionário nome_upper → {url, nome_original}
    csv_map = {}
    for j in jogos_csv:
        key = j["nome"].upper().strip()
        if key not in csv_map:  # Primeiro match ganha (evita duplicatas)
            csv_map[key] = {"url": j["url"], "nome": j["nome"]}

    log.info(f"{len(csv_map)} jogos únicos no CSV (por nome).")

    # 3. Redshift — catálogo de jogos (metadados)
    log.info("Buscando catálogo de jogos no Redshift...")
    try:
        df_games = query_redshift(QUERY_REDSHIFT_GAMES)
        log.info(f"{len(df_games)} jogos no catálogo Redshift.")

        # Monta dicionário nome_upper → {vendor_id, provider_game_id}
        # Prioriza pragmaticplay em caso de duplicatas
        redshift_map = {}
        for _, row in df_games.iterrows():
            key = row["game_name_upper"]
            if key not in redshift_map:
                redshift_map[key] = {
                    "vendor_id": row["vendor_id"],
                    "provider_game_id": str(row["provider_game_id"]),
                }
            elif row["vendor_id"] == "pragmaticplay":
                # Pragmatic tem prioridade
                redshift_map[key] = {
                    "vendor_id": row["vendor_id"],
                    "provider_game_id": str(row["provider_game_id"]),
                }
    except Exception as e:
        log.warning(f"Falha ao consultar Redshift (continuando só com CSV): {e}")
        redshift_map = {}

    # 4. Merge: CSV (imagens) + Redshift (metadados)
    # Todos os jogos únicos (união dos dois conjuntos)
    all_names = set(csv_map.keys()) | set(redshift_map.keys())
    log.info(f"{len(all_names)} jogos únicos no total (CSV + Redshift).")

    now_utc = datetime.now(timezone.utc)
    records = []

    for name_upper in all_names:
        csv_entry = csv_map.get(name_upper, {})
        rs_entry = redshift_map.get(name_upper, {})

        game_name = csv_entry.get("nome", name_upper.title())
        game_image_url = csv_entry.get("url")
        vendor_id = rs_entry.get("vendor_id")
        provider_game_id = rs_entry.get("provider_game_id")
        game_slug = f"/pb/gameplay/{slugify(game_name)}/real-game"

        # Determina a fonte
        if csv_entry and rs_entry:
            source = "scraper+redshift"
        elif csv_entry:
            source = "scraper"
        else:
            source = "redshift"

        records.append((
            game_name,
            name_upper,
            provider_game_id,
            vendor_id,
            game_image_url,
            game_slug,
            source,
            now_utc,
        ))

    # 5. Upsert no Super Nova DB
    upsert_sql = """
        INSERT INTO multibet.game_image_mapping
            (game_name, game_name_upper, provider_game_id, vendor_id,
             game_image_url, game_slug, source, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_name_upper) DO UPDATE SET
            game_name        = EXCLUDED.game_name,
            provider_game_id = COALESCE(EXCLUDED.provider_game_id, multibet.game_image_mapping.provider_game_id),
            vendor_id        = COALESCE(EXCLUDED.vendor_id, multibet.game_image_mapping.vendor_id),
            game_image_url   = COALESCE(EXCLUDED.game_image_url, multibet.game_image_mapping.game_image_url),
            game_slug        = EXCLUDED.game_slug,
            source           = EXCLUDED.source,
            updated_at       = EXCLUDED.updated_at
    """

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, records, page_size=100)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    # 6. Relatório
    with_img = sum(1 for r in records if r[4] is not None)
    without_img = len(records) - with_img
    log.info(f"Upsert concluído: {len(records)} jogos processados.")
    log.info(f"  Com imagem:  {with_img}")
    log.info(f"  Sem imagem:  {without_img}")

    if without_img > 0:
        missing = [r[0] for r in records if r[4] is None][:20]
        log.warning(f"  Jogos sem imagem (primeiros 20): {', '.join(missing)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Game Image Mapper")
    parser.add_argument("--scraper", action="store_true",
                        help="Roda o scraper antes para atualizar o CSV")
    args = parser.parse_args()

    log.info("=== Iniciando pipeline Game Image Mapper ===")
    setup_table()

    if args.scraper:
        run_scraper()

    refresh()
    log.info("=== Pipeline concluído ===")