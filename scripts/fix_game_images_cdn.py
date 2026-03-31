"""
Fix: Descobrir game_image_url via CDN multi.bet para jogos sem imagem.

Lógica:
  1. Busca jogos sem imagem no Super Nova DB (game_image_mapping)
  2. Infere o prefixo URL a partir do vendor_id (padrão: alea_ + 3 primeiras letras)
  3. Constrói URL candidata: https://multi.bet.br//uploads/games/MUL//{prefix}{game_id}/{prefix}{game_id}.webp
  4. Testa com HTTP HEAD se a URL existe (status 200)
  5. Atualiza no banco os que retornarem 200

Execução:
    python scripts/fix_game_images_cdn.py [--dry-run]
"""

import sys
import os
import re
import logging
import argparse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CDN_BASE = "https://multi.bet.br//uploads/games/MUL//"

# Prefixos conhecidos (confirmados via dados existentes no banco)
KNOWN_PREFIXES = {
    "alea_bigtimegaming": "alea_big",
    "alea_caleta": "alea_cal",
    "alea_evolution": "alea_evo",
    "alea_ezugi": "alea_ezu",
    "alea_gamingcorps": "alea_gam",
    "alea_hypetechgames": "alea_hyp",
    "alea_netent": "alea_net",
    "alea_netgaming": "alea_neg",
    "alea_pgsoft": "alea_pg",
    "alea_playngo": "alea_play",
    "alea_playtech": "alea_pla",
    "alea_popok": "alea_pop",
    "alea_redrake": "alea_rer",
    "alea_redtiger": "alea_red",
    "alea_rubyplay": "alea_rub",
    "alea_skywind": "alea_skw",
    "alea_skywindlive": "alea_sky",
    "alea_spinoro": "alea_spi",
    "alea_spribe": "alea_spr",
    "alea_tadagaming": "alea_tad",
    "alea_wazdan": "alea_waz",
    "pragmaticplay": "pp",
}

# Prefixos inferidos (padrão alea_ + 3 primeiras letras do sufixo)
INFERRED_PREFIXES = {
    "alea_hacksawgaming": "alea_hac",
    "alea_creedroomz": "alea_cre",
    "alea_3oaksgaming": "alea_3oa",
    "alea_platipus": "alea_plt",
    "alea_7777gaming": "alea_777",
    "alea_galaxsys": "alea_gal",
    "alea_1x2gaming": "alea_1x2",
}


def get_prefix(vendor_id: str) -> str | None:
    """Retorna o prefixo URL para um vendor_id."""
    if not vendor_id:
        return None
    if vendor_id in KNOWN_PREFIXES:
        return KNOWN_PREFIXES[vendor_id]
    if vendor_id in INFERRED_PREFIXES:
        return INFERRED_PREFIXES[vendor_id]
    # Tenta inferir: alea_ + 3 primeiras letras
    if vendor_id.startswith("alea_"):
        suffix = vendor_id[5:]
        return f"alea_{suffix[:3]}"
    return None


def build_url(prefix: str, game_id: str) -> str:
    code = f"{prefix}{game_id}"
    return f"{CDN_BASE}{code}/{code}.webp"


def check_url(url: str, timeout: float = 5.0) -> bool:
    """Testa se a URL existe via HEAD request."""
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.status_code == 200
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="Fix game images via CDN discovery")
    parser.add_argument("--dry-run", action="store_true", help="Apenas testa, não atualiza o banco")
    args = parser.parse_args()

    # 1. Busca jogos sem imagem
    log.info("Buscando jogos sem imagem no Super Nova DB...")
    rows = execute_supernova(
        "SELECT game_name_upper, vendor_id, provider_game_id FROM multibet.game_image_mapping WHERE game_image_url IS NULL AND provider_game_id IS NOT NULL",
        fetch=True,
    )
    log.info(f"{len(rows)} jogos sem imagem com game_id.")

    # 2. Constrói URLs candidatas
    candidates = []
    skipped_vendors = set()
    for name_upper, vendor_id, game_id in rows:
        prefix = get_prefix(vendor_id)
        if not prefix:
            skipped_vendors.add(vendor_id or "NULL")
            continue
        url = build_url(prefix, game_id)
        candidates.append({"name_upper": name_upper, "url": url, "vendor_id": vendor_id})

    log.info(f"{len(candidates)} URLs candidatas geradas.")
    if skipped_vendors:
        log.warning(f"Vendors sem prefixo (pulados): {skipped_vendors}")

    # 3. Testa URLs em paralelo (10 threads)
    log.info("Testando URLs via HTTP HEAD (10 threads)...")
    found = []

    def _check(c):
        ok = check_url(c["url"])
        return c, ok

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_check, c): c for c in candidates}
        done = 0
        for f in as_completed(futures):
            done += 1
            c, ok = f.result()
            if ok:
                found.append(c)
            if done % 50 == 0:
                log.info(f"  Progresso: {done}/{len(candidates)} testados, {len(found)} encontrados")

    log.info(f"Resultado: {len(found)}/{len(candidates)} URLs válidas (HTTP 200).")

    if not found:
        log.info("Nenhuma URL nova encontrada.")
        return

    # 4. Mostra encontrados
    for f in found[:20]:
        log.info(f"  OK: {f['name_upper'][:40]:<40} -> {f['url'][:70]}")
    if len(found) > 20:
        log.info(f"  ... e mais {len(found) - 20}")

    # 5. Atualiza no banco
    if args.dry_run:
        log.info(f"DRY RUN: {len(found)} jogos seriam atualizados. Use sem --dry-run para persistir.")
        return

    log.info(f"Atualizando {len(found)} jogos no Super Nova DB...")
    update_sql = """
        UPDATE multibet.game_image_mapping
        SET game_image_url = %s,
            source = source || '+cdn_fix',
            updated_at = NOW()
        WHERE game_name_upper = %s
          AND game_image_url IS NULL
    """
    records = [(f["url"], f["name_upper"]) for f in found]

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, update_sql, records, page_size=100)
        conn.commit()
        log.info(f"Atualizado {len(records)} jogos com sucesso.")
    finally:
        conn.close()
        ssh.close()


if __name__ == "__main__":
    main()
