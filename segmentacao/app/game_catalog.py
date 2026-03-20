"""
Catalogo de jogos — resolve nome do jogo para game_id (Redshift) e smr_game_id (BigQuery).

Usa busca fuzzy por nome no catalogo do Redshift e BigQuery.
Mantém cache em memoria para evitar consultas repetidas.

Uso:
    from segmentacao_app.game_catalog import resolve_game
    game = resolve_game("Sweet Bonanza")
    # game = {"redshift_game_id": "vs20fruitsw", "smartico_game_id": 45883879, "game_name": "Sweet Bonanza"}
"""

import os
import sys
import logging
from dataclasses import dataclass
from dotenv import load_dotenv

# Garantir imports do projeto e carregar .env
PROJECT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, PROJECT_DIR)
load_dotenv(os.path.join(PROJECT_DIR, '.env'))

log = logging.getLogger(__name__)

# Cache em memoria: nome_normalizado -> GameInfo
_cache: dict = {}


@dataclass
class GameInfo:
    redshift_game_id: str
    smartico_game_id: int | None
    game_name: str
    vendor: str


def _normalize(name: str) -> str:
    """Normaliza nome do jogo para busca."""
    return name.strip().lower()


def _search_redshift(nome_jogo: str) -> list:
    """Busca no catalogo do Redshift por nome do jogo."""
    from db.redshift import query_redshift

    # Busca por LIKE — pega variantes
    nome_like = f"%{nome_jogo}%"
    sql = f"""
    SELECT c_game_id, c_game_desc, c_vendor_id
    FROM bireports.tbl_vendor_games_mapping_data
    WHERE LOWER(c_game_desc) LIKE LOWER('{nome_like}')
    ORDER BY c_game_desc
    """
    df = query_redshift(sql)
    return df.to_dict('records') if not df.empty else []


def _search_smartico(nome_jogo: str) -> list:
    """Busca no catalogo do BigQuery Smartico por nome do jogo."""
    from db.bigquery import query_bigquery

    nome_like = f"%{nome_jogo.upper()}%"
    sql = f"""
    SELECT smr_game_id, game_name
    FROM `smartico-bq6.dwh_ext_24105.dm_casino_game_name`
    WHERE UPPER(game_name) LIKE '{nome_like}'
    ORDER BY game_name
    """
    df = query_bigquery(sql)
    return df.to_dict('records') if not df.empty else []


def _best_match_redshift(nome_jogo: str, results: list) -> dict | None:
    """
    Seleciona o melhor match do Redshift.
    Prioriza match exato, depois o mais curto (evita variantes como "1000", "Xmas", etc).
    """
    nome_lower = nome_jogo.strip().lower()

    # Match exato primeiro
    for r in results:
        if r['c_game_desc'].strip().lower() == nome_lower:
            return r

    # Menor nome (mais proximo do original, sem sufixos)
    if results:
        return min(results, key=lambda r: len(r['c_game_desc']))

    return None


def _best_match_smartico(nome_jogo: str, results: list) -> dict | None:
    """Seleciona o melhor match do Smartico."""
    nome_upper = nome_jogo.strip().upper()

    # Match exato
    for r in results:
        if r['game_name'].strip().upper() == nome_upper:
            return r

    # Menor nome
    if results:
        return min(results, key=lambda r: len(r['game_name']))

    return None


def resolve_game(nome_jogo: str) -> GameInfo | None:
    """
    Resolve nome do jogo para IDs do Redshift e Smartico.

    Args:
        nome_jogo: Nome do jogo (ex: "Sweet Bonanza", "Gates of Olympus")

    Returns:
        GameInfo com IDs ou None se nao encontrado
    """
    key = _normalize(nome_jogo)

    # Cache hit
    if key in _cache:
        log.info(f"Cache hit: {nome_jogo} -> {_cache[key].redshift_game_id}")
        return _cache[key]

    log.info(f"Buscando jogo '{nome_jogo}' nos catalogos...")

    # Buscar no Redshift
    rs_results = _search_redshift(nome_jogo)
    rs_match = _best_match_redshift(nome_jogo, rs_results)

    if not rs_match:
        log.error(f"Jogo '{nome_jogo}' nao encontrado no catalogo Redshift")
        return None

    redshift_id = rs_match['c_game_id'].strip()
    vendor = rs_match.get('c_vendor_id', '').strip()
    game_name = rs_match['c_game_desc'].strip()

    log.info(f"Redshift: {nome_jogo} -> game_id={redshift_id}, vendor={vendor}")

    # Buscar no Smartico
    smr_results = _search_smartico(nome_jogo)
    smr_match = _best_match_smartico(nome_jogo, smr_results)
    smartico_id = int(smr_match['smr_game_id']) if smr_match else None

    if smartico_id:
        log.info(f"Smartico: {nome_jogo} -> smr_game_id={smartico_id}")
    else:
        log.warning(f"Jogo '{nome_jogo}' nao encontrado no catalogo Smartico (validacao cruzada indisponivel)")

    # Salvar no cache
    info = GameInfo(
        redshift_game_id=redshift_id,
        smartico_game_id=smartico_id,
        game_name=game_name,
        vendor=vendor,
    )
    _cache[key] = info
    return info


def list_variants(nome_jogo: str) -> dict:
    """Lista todas as variantes encontradas nos dois catalogos (para debug)."""
    rs = _search_redshift(nome_jogo)
    smr = _search_smartico(nome_jogo)
    return {
        "redshift": [{"game_id": r['c_game_id'].strip(), "name": r['c_game_desc'].strip()} for r in rs],
        "smartico": [{"smr_game_id": r['smr_game_id'], "name": r['game_name'].strip()} for r in smr],
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    for jogo in ["Sweet Bonanza", "Gates of Olympus", "Fortune Rabbit", "Fortune Tiger"]:
        print(f"\n--- {jogo} ---")
        info = resolve_game(jogo)
        if info:
            print(f"  Redshift: {info.redshift_game_id} ({info.vendor})")
            print(f"  Smartico: {info.smartico_game_id}")
            print(f"  Nome oficial: {info.game_name}")
        else:
            print("  NAO ENCONTRADO")
