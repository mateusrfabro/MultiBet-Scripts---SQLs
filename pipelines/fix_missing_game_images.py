"""
Fix: Inserir game_image_url faltantes na tabela multibet.game_image_mapping
============================================================================
Jogos identificados sem imagem no pipeline grandes_ganhos.
URLs extraídas do site multi.bet.br via scraper Playwright ou DevTools.

Historico:
    - 18/03/2026: Speed Blackjack 47, Speed Roleta Brasileira
    - 26/03/2026: Zeus vs Hades - Gods of War 250 (variante 250x, mesma imagem do base)

Execução:
    python pipelines/fix_missing_game_images.py
"""

import sys
import os
import re
import unicodedata
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def slugify(name: str) -> str:
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s_]+", "-", name)
    name = re.sub(r"-+", "-", name)
    return name


# ─── Jogos faltantes (extraídos do site multi.bet.br via DevTools) ────────────
MISSING_GAMES = [
    {
        "game_name": "Speed Blackjack 47",
        "game_image_url": "https://multi.bet.br/uploads/games/MUL//pp775/pp775.webp",
    },
    {
        "game_name": "Speed Roleta Brasileira",
        "game_image_url": "https://multi.bet.br/uploads/games/MUL//alea_pla18974/alea_pla18974.webp",
    },
    {
        # Variante 250x do Zeus vs Hades - mesmo jogo, mesma imagem do base (ppvs15godsofwar)
        # Site multi.bet.br nao tem variante separada - 26/03/2026
        "game_name": "Zeus vs Hades \u2013 Gods of War 250",
        "game_image_url": "https://multi.bet.br//uploads/games/MUL//ppvs15godsofwar/ppvs15godsofwar.webp",
    },
]


def fix_missing():
    now_utc = datetime.now(timezone.utc)

    records = []
    for g in MISSING_GAMES:
        name = g["game_name"]
        name_upper = name.upper().strip()
        slug = f"/pb/gameplay/{slugify(name)}/real-game"
        records.append((
            name,
            name_upper,
            None,  # provider_game_id
            None,  # vendor_id
            g["game_image_url"],
            slug,
            "manual",
            now_utc,
        ))

    upsert_sql = """
        INSERT INTO multibet.game_image_mapping
            (game_name, game_name_upper, provider_game_id, vendor_id,
             game_image_url, game_slug, source, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_name_upper) DO UPDATE SET
            game_image_url = EXCLUDED.game_image_url,
            game_slug      = EXCLUDED.game_slug,
            source         = EXCLUDED.source,
            updated_at     = EXCLUDED.updated_at
    """

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, records, page_size=100)
        conn.commit()
        log.info(f"{len(records)} jogos inseridos/atualizados com sucesso.")
    finally:
        conn.close()
        ssh.close()

    # Validação: busca os registros inseridos
    for g in MISSING_GAMES:
        name_upper = g["game_name"].upper().strip()
        rows = execute_supernova(
            "SELECT game_name, game_image_url FROM multibet.game_image_mapping WHERE game_name_upper = %s",
            params=(name_upper,),
            fetch=True,
        )
        if rows:
            log.info(f"  ✓ {rows[0][0]} → {rows[0][1]}")
        else:
            log.error(f"  ✗ {g['game_name']} NÃO encontrado após upsert!")


if __name__ == "__main__":
    log.info("=== Fix: Inserir game_image_url faltantes ===")
    fix_missing()
    log.info("=== Concluído ===")
