"""
Pipeline: Game Image Mapper (v2 - enriquecido para views do front)
====================================================================
Popula multibet.game_image_mapping com tudo que o front precisa para
montar carrosseis (Mais jogados, Cassino ao vivo, Pragmatic, Jackpots).

Fontes:
  1. CSV gerado pelo scraper (capturar_jogos_pc.py)  — nome + game_image_url
  2. Athena bireports_ec2.tbl_vendor_games_mapping_data — categoria, vendor,
                                                          tipo, jackpot, status
  3. Athena vendor_ec2.tbl_vendor_games_mapping_mst   — sub_vendor (pgsoft, etc)
  4. PostgreSQL multibet.silver_game_15min            — rounds/players ULTIMAS 24h

Colunas adicionadas em v2 (DDL: pipelines/ddl/ddl_game_image_mapping_v2.sql):
  - product_id, sub_vendor_id
  - game_category, game_category_desc, game_type_desc
  - live_subtype (Roleta/Blackjack/Baccarat/GameShow/Outros — regex)
  - has_jackpot, is_active
  - rounds_24h, players_24h, popularity_rank_24h, popularity_window_end

Execução:
    python pipelines/game_image_mapper.py [--scraper]
    --scraper : roda o scraper antes para atualizar o CSV (requer Playwright)

Frequência: a cada 4h (cron EC2) — alimenta views vw_front_*.
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

from db.athena import query_athena
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

# ─── SQL Athena ──────────────────────────────────────────────────────────────
# v2: Catalogo enriquecido — categoria, tipo, jackpot, sub_vendor (via JOIN com mst)
# Estrategia: bireports_ec2 = view oficial (publica), vendor_ec2 = master (sub_vendor)
QUERY_ATHENA_GAMES = """
WITH catalogo_publico AS (
    -- View oficial (sem sub_vendor, mas com tudo agregado/limpo)
    SELECT
        UPPER(TRIM(c_game_desc))   AS game_name_upper,
        c_vendor_id                AS vendor_id,
        c_game_id                  AS provider_game_id,
        UPPER(c_product_id)        AS product_id,
        LOWER(c_game_category)     AS game_category,
        c_game_category_desc       AS game_category_desc,
        c_game_type_desc           AS game_type_desc,
        c_status                   AS status,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(c_game_desc))
            ORDER BY CASE WHEN c_vendor_id = 'pragmaticplay' THEN 0 ELSE 1 END,
                     c_game_id
        ) AS rn
    FROM bireports_ec2.tbl_vendor_games_mapping_data
    WHERE c_game_id IS NOT NULL
      AND c_game_desc IS NOT NULL
),
mst_flags AS (
    -- vendor_ec2 master: sub_vendor + has_jackpot (deduplicar por game_id+vendor)
    SELECT
        c_vendor_id,
        c_game_id,
        MAX(c_sub_vendor_id) AS sub_vendor_id,
        MAX(CASE WHEN c_has_jackpot IN ('1', 'true', 'TRUE') THEN TRUE ELSE FALSE END) AS has_jackpot
    FROM vendor_ec2.tbl_vendor_games_mapping_mst
    GROUP BY c_vendor_id, c_game_id
)
SELECT
    cp.game_name_upper,
    cp.vendor_id,
    cp.provider_game_id,
    cp.product_id,
    cp.game_category,
    cp.game_category_desc,
    cp.game_type_desc,
    cp.status,
    COALESCE(mf.sub_vendor_id, '')   AS sub_vendor_id,
    COALESCE(mf.has_jackpot, FALSE)  AS has_jackpot
FROM catalogo_publico cp
LEFT JOIN mst_flags mf
    ON cp.vendor_id = mf.c_vendor_id
   AND cp.provider_game_id = mf.c_game_id
WHERE cp.rn = 1  -- 1 registro por nome (Pragmatic ganha em duplicatas)
"""

# ─── Query PostgreSQL: rounds/players ULTIMAS 24h ROLANTES ───────────────────
# silver_game_15min e atualizado em janelas de 15min — fonte ideal para 24h rolante
# Janela: (now_utc - 24h) ate now_utc
QUERY_PG_RANK_24H = """
WITH base_24h AS (
    SELECT
        UPPER(TRIM(game_name)) AS game_name_upper,
        SUM(total_bets)        AS rounds_24h,
        SUM(unique_players)    AS players_24h,
        SUM(total_bet)         AS total_bet_24h,
        SUM(total_win)         AS total_wins_24h
    FROM multibet.silver_game_15min
    WHERE janela_fim >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
      AND total_bets > 0
      AND game_name IS NOT NULL
      AND game_name <> 'altenar'
    GROUP BY UPPER(TRIM(game_name))
),
ranked AS (
    SELECT
        game_name_upper,
        rounds_24h,
        players_24h,
        total_bet_24h,
        total_wins_24h,
        ROW_NUMBER() OVER (ORDER BY rounds_24h DESC) AS popularity_rank_24h
    FROM base_24h
)
SELECT game_name_upper, rounds_24h, players_24h,
       total_bet_24h, total_wins_24h, popularity_rank_24h
FROM ranked
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────

# Overrides manuais de game_slug — casos em que a URL de gameplay no front
# NAO segue o padrao lowercase/underscore do slugify().
# Chave: game_name_upper (UPPER TRIM) | Valor: game_slug literal a gravar.
SLUG_OVERRIDES = {
    "FORTUNE SNAKE": "/pb/gameplay/Fortune_Snake/real-game",
    # Jogos "+" — slugify() remove o "+" por ser char especial.
    # Padrao no front multi.bet.br: <nome>_plus (validado 16/04/2026).
    "CHICKEN+":      "/pb/gameplay/chicken_plus/real-game",
    "MINES+":        "/pb/gameplay/mines_plus/real-game",
}


def slugify(name: str) -> str:
    """Converte nome do jogo em slug de URL.
    Ex: 'Fortune Ox' → 'fortune_ox'
    """
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s-]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name


def build_game_slug(game_name: str, name_upper: str) -> str:
    """Retorna slug final — aplica override manual antes do slugify padrao."""
    if name_upper in SLUG_OVERRIDES:
        return SLUG_OVERRIDES[name_upper]
    return f"/pb/gameplay/{slugify(game_name)}/real-game"


# ─── Provider display name (alea_pgsoft -> "PG Soft") ───────────────────────
# Dict estatico — espelha o providerDisplayName da categories-multibet-api.
# Adicionar novos vendors aqui quando entrar provider novo no catalogo.
PROVIDER_DISPLAY_NAMES = {
    "dummy":              "Dummy",
    "pragmaticplay":      "Pragmatic Play",
    "pragmaticexternal":  "Pragmatic Play",
    "alea_pgsoft":        "PG Soft",
    "alea_pg_soft":       "PG Soft",
    "alea_hypetechgames": "Hypetech",
    "alea_hacksawgaming": "Hacksaw Gaming",
    "alea_galaxsys":      "Galaxsys",
    "alea_7777gaming":    "7777 Gaming",
    "alea_3oaksgaming":   "3 Oaks Gaming",
    "alea_evolution":     "Evolution",
    "alea_creedroomz":    "Creedroomz",
    "alea_tadagaming":    "TaDa Gaming",
    "alea_1x2gaming":     "1x2 Gaming",
    "alea_platipus":      "Platipus",
    "alea_spribe":        "Spribe",
    "alea_caleta":        "Caleta",
    "alea_redtiger":      "Red Tiger Gaming",
    "alea_popok":         "PopOk",
    "alea_netent":        "NetEnt",
    "alea_wazdan":        "Wazdan",
    "alea_playngo":       "PlayNGo",
    "alea_redrake":       "Red Rake Gaming",
    "alea_rubyplay":      "Ruby Play",
    "alea_playtech":      "Playtech",
    "alea_netgaming":     "NetGaming",
    "alea_ezugi":         "Ezugi",
    "alea_gamingcorps":   "Gaming Corps",
    "alea_skywind":       "Skywind",
    "alea_spinoro":       "Spinoro",
    "alea_bigtimegaming": "Big Time Gaming",
    "alea_skywindlive":   "Skywind Live",
    "alea_pragmaticplay": "Pragmatic Play",
}


def display_provider_name(vendor_id: str | None) -> str | None:
    """Retorna nome amigavel do vendor (ex: 'alea_pgsoft' -> 'PG Soft').
    Se vendor desconhecido, retorna o vendor_id cru truncado a 50 chars
    (coluna provider_display_name e VARCHAR(50) — defesa contra overflow).
    """
    if not vendor_id:
        return None
    return PROVIDER_DISPLAY_NAMES.get(str(vendor_id).strip().lower(), str(vendor_id)[:50])


# ─── Categoria do front (8 buckets para carrosseis) ─────────────────────────
# Replica a logica da categories-multibet-api (game.queries.ts).
# Prioridade: Live (game_category nativo) -> Crash -> TV Shows -> Bac Bo
#             -> Baccarat -> Blackjack -> Fortune -> Slots (fallback).
# Usa game_category nativo (Athena) para Live; resto e regex no nome.
CRASH_GAME_IDS_FRONT = {"8369", "1301", "1320"}  # Aviator, Spaceman, Big Bass Crash

TV_SHOWS_PATTERNS_FRONT = [
    "money time", "football blitz", "football studio", "monopoly",
    "crazy time", "mega ball", "deal or no deal", "dream catcher",
    "gonzo", "mega wheel", "funky time", "top card",
]


def categorize_front(
    game_name: str | None,
    game_category: str | None,
    provider_game_id: str | None,
) -> str:
    """Classifica o jogo em 1 de 8 buckets do front.

    DIVERGE da categories-multibet-api em 1 ponto: a gente da prioridade ao
    REGEX DE NOME para jogos classicos de mesa (roulette/blackjack/baccarat/
    bacbo) ANTES de confiar em `game_category` nativo. Motivo: o Athena
    classifica varios jogos de roleta como `slots` (ex: 27 jogos da
    Creedroomz, Speed Roleta Brasileira da Playtech) — confiar cego no
    fonte cascateia o erro pro front. Validado empiricamente em 22/04/2026.
    """
    name_lower = (game_name or "").strip().lower()
    cat_lower  = (game_category or "").strip().lower()
    gid        = str(provider_game_id or "").strip()

    # 1. Crash (nome ou IDs hardcoded — Aviator, Spaceman, Big Bass Crash)
    #    Vem PRIMEIRO porque "crash" no nome e sinal forte (e Aviator nao
    #    tem game_category nativo no Athena — ficaria sem classificacao).
    #    Inclui plinko e mines (mecanicas de risk-game tipicas do segmento Crash).
    if "crash" in name_lower or "plinko" in name_lower or "mines" in name_lower \
            or gid in CRASH_GAME_IDS_FRONT:
        return "Crash"

    # 2. Mesa classica por NOME (override ao game_category fonte que erra)
    #    Ordem importa: Bac Bo antes de Baccarat (pra nao roubar match).
    if any(p in name_lower for p in ("bac bo", "bac-bo", "bacbo")):
        return "Bac Bo"
    if "baccarat" in name_lower:
        return "Baccarat"
    if "blackjack" in name_lower:
        return "Blackjack"
    if "roleta" in name_lower or "roulette" in name_lower:
        return "Live"  # roletas vao em Live (sub-bucket via live_subtype='Roleta')

    # 3. Live nativo (depois do override de mesa — pega LIVEs sem palavra-chave)
    if cat_lower == "live":
        return "Live"

    # 4. TV Shows
    for pattern in TV_SHOWS_PATTERNS_FRONT:
        if pattern in name_lower:
            return "TV Shows"

    # 5. Fortune (PT-BR + EN)
    if "fortune" in name_lower or "fortuna" in name_lower:
        return "Fortune"

    # 6. Slots (fallback)
    return "Slots"


def derive_game_category(
    game_category_raw: str | None,
    category_front: str | None,
) -> str | None:
    """Preenche game_category quando o Athena nao classificou (ex: Aviator/Spribe)
    OU quando classificou errado (ex: roleta como slots).

    Regra: bucket_front Live -> 'live'; Crash -> 'crash'. Resto preserva o
    valor original (ou None). Garante coerencia entre as 2 colunas.
    """
    fronted = (category_front or "").strip().lower()
    raw     = (game_category_raw or "").strip().lower() or None

    if fronted == "live":
        return "live"
    if fronted == "crash":
        return "crash"
    if fronted in ("bac bo", "baccarat", "blackjack"):
        return "live"  # mesa classica = live
    # Followup auditor 22/04: cobertura 98% -> 99.9% — buckets que ficavam NULL
    # quando Athena nao classificava o catalogo nativamente.
    if fronted == "tv shows":
        return "live"  # Crazy Time, Mega Ball, Monopoly Live = game shows ao vivo
    if fronted in ("slots", "fortune"):
        return "slots"  # Fortune Tiger/Ox sao slots tematicos da PG Soft
    return raw


# ─── Normalizacao de subtipo Live (regex v1) ─────────────────────────────────
# Categoriza c_game_type_desc do bireports em 5 grupos para o front.
# Validar manualmente apos primeiro refresh — pode precisar mais regras.
LIVE_SUBTYPE_RULES = [
    # (regex, label) — ordem importa (primeiro match vence)
    # Nao usar \b apos a palavra-chave: pega "BlackjackX", "RouletteLive", etc.
    (r"\b(roulette|roleta)",                     "Roleta"),
    (r"\bblackjack",                             "Blackjack"),  # BlackjackX, BlackjackLive...
    (r"\b(baccarat|punto banco)",                "Baccarat"),   # Punto Banco = variante Baccarat
    (r"\b(crazy time|monopoly|mega ball|game show|teen patti|spin a win|"
     r"dragon tiger|sic ?bo|andar bahar|dream catcher|fan tan|"
     r"deal or no deal|cards show|wheel of|poker)\b",
                                                 "GameShow"),
]


def classify_live_subtype(game_type_desc: str | None, game_category: str | None) -> str | None:
    """
    Normaliza c_game_type_desc -> Roleta | Blackjack | Baccarat | GameShow | Outros
    Aplica somente para jogos LIVE (game_category = 'live').
    """
    if not game_category or game_category.lower() != "live":
        return None
    if not game_type_desc:
        return "Outros"

    raw = game_type_desc.lower()
    for pattern, label in LIVE_SUBTYPE_RULES:
        if re.search(pattern, raw, flags=re.IGNORECASE):
            return label
    return "Outros"


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
    """Cria schema, tabela e índice (idempotente).
    Aplica migrations das colunas v2/v3/v4 caso a tabela ja exista sem elas.
    """
    log.info("Verificando/criando tabela multibet.game_image_mapping...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX)
    # v4 migrations (idempotentes — seguro rodar mesmo se DDL ja foi aplicado)
    execute_supernova(
        "ALTER TABLE multibet.game_image_mapping "
        "ADD COLUMN IF NOT EXISTS provider_display_name VARCHAR(50);"
    )
    execute_supernova(
        "ALTER TABLE multibet.game_image_mapping "
        "ADD COLUMN IF NOT EXISTS game_category_front VARCHAR(20);"
    )
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
    Le CSV (imagens) + Athena (catalogo enriquecido) + PostgreSQL (rank 24h),
    faz upsert no Super Nova DB com COALESCE preservando valores anteriores
    quando a fonte vier NULL (regra: "se nao atualizou, mantem o antigo").
    """
    now_utc = datetime.now(timezone.utc)

    # ─── 1. CSV do scraper (imagens) ─────────────────────────────────────────
    log.info(f"Lendo CSV: {CSV_PATH}")
    jogos_csv = load_csv(CSV_PATH)
    log.info(f"{len(jogos_csv)} jogos no CSV.")

    if not jogos_csv:
        log.error("Nenhum jogo no CSV. Abortando.")
        return

    csv_map = {}
    for j in jogos_csv:
        key = j["nome"].upper().strip()
        if key not in csv_map:
            csv_map[key] = {"url": j["url"], "nome": j["nome"]}
    log.info(f"{len(csv_map)} jogos unicos no CSV.")

    # ─── 2. Athena: catalogo enriquecido (categoria, vendor, jackpot, etc) ──
    log.info("Buscando catalogo enriquecido no Athena (bireports + vendor mst)...")
    athena_map = {}
    try:
        df_games = query_athena(QUERY_ATHENA_GAMES, database="bireports_ec2")
        log.info(f"{len(df_games)} jogos no catalogo Athena (1 por nome, Pragmatic prioritario).")
        for _, row in df_games.iterrows():
            key = row["game_name_upper"]
            athena_map[key] = {
                "vendor_id":          row["vendor_id"],
                "provider_game_id":   str(row["provider_game_id"]) if row["provider_game_id"] is not None else None,
                "product_id":         row["product_id"],
                "game_category":      row["game_category"],
                "game_category_desc": row["game_category_desc"],
                "game_type_desc":     row["game_type_desc"],
                "status":             row["status"],
                "sub_vendor_id":      row.get("sub_vendor_id") or None,
                "has_jackpot":        bool(row.get("has_jackpot")) if row.get("has_jackpot") is not None else False,
            }
    except Exception as e:
        log.warning(f"Falha ao consultar Athena (continuando so com CSV): {e}")

    # ─── 3. PostgreSQL: rounds/players ULTIMAS 24h rolantes (silver_game_15min) ──
    log.info("Calculando rank de popularidade (ultimas 24h rolantes)...")
    rank_map = {}
    try:
        rank_rows = execute_supernova(QUERY_PG_RANK_24H, fetch=True)
        log.info(f"{len(rank_rows)} jogos com atividade nas ultimas 24h.")
        for row in rank_rows:
            game_name_upper, rounds_24h, players_24h, total_bet, total_wins, rank = row
            rank_map[game_name_upper] = {
                "rounds_24h":         int(rounds_24h or 0),
                "players_24h":        int(players_24h or 0),
                "total_bet_24h":      float(total_bet or 0),
                "total_wins_24h":     float(total_wins or 0),
                "popularity_rank_24h": int(rank),
            }
    except Exception as e:
        log.warning(f"Falha ao calcular rank 24h (continuando sem ranking): {e}")

    # ─── 4. Merge: CSV + Athena + Rank 24h ───────────────────────────────────
    all_names = set(csv_map.keys()) | set(athena_map.keys())
    log.info(f"{len(all_names)} jogos unicos no total.")

    records = []
    for name_upper in all_names:
        csv_entry    = csv_map.get(name_upper, {})
        athena_entry = athena_map.get(name_upper, {})
        rank_entry   = rank_map.get(name_upper, {})

        game_name        = csv_entry.get("nome") or name_upper.title()
        game_image_url   = csv_entry.get("url")
        game_slug        = build_game_slug(game_name, name_upper)

        vendor_id          = athena_entry.get("vendor_id")
        provider_game_id   = athena_entry.get("provider_game_id")
        product_id         = athena_entry.get("product_id")
        sub_vendor_id      = athena_entry.get("sub_vendor_id")
        game_category      = athena_entry.get("game_category")
        game_category_desc = athena_entry.get("game_category_desc")
        game_type_desc     = athena_entry.get("game_type_desc")
        status             = athena_entry.get("status")
        has_jackpot        = athena_entry.get("has_jackpot", False)
        is_active          = (status == "active") if status else None

        # v4: Provider display name + bucket de categoria do front (8 buckets)
        # ORDEM IMPORTA: categorize_front -> derive_game_category -> classify_live_subtype
        # (live_subtype precisa do game_category JA derivado pra pegar roletas
        # que vieram como 'slots' do Athena e foram corrigidas pra 'live')
        provider_display_name = display_provider_name(vendor_id)
        game_category_front   = categorize_front(game_name, game_category, provider_game_id)
        # v4.1: derivar game_category quando Athena nao classificou ou classificou errado
        game_category         = derive_game_category(game_category, game_category_front)
        # Subtipo Live normalizado (Roleta/Blackjack/Baccarat/GameShow/Outros)
        live_subtype          = classify_live_subtype(game_type_desc, game_category)

        # Source flag
        if csv_entry and athena_entry:
            source = "scraper+athena"
        elif csv_entry:
            source = "scraper"
        else:
            source = "athena"

        # Popularity (24h rolantes)
        rounds_24h          = rank_entry.get("rounds_24h", 0)
        players_24h         = rank_entry.get("players_24h", 0)
        total_bet_24h       = rank_entry.get("total_bet_24h", 0.0)
        total_wins_24h      = rank_entry.get("total_wins_24h", 0.0)
        popularity_rank_24h = rank_entry.get("popularity_rank_24h")  # NULL se sem atividade

        records.append((
            game_name, name_upper, provider_game_id, vendor_id,
            game_image_url, game_slug, source, now_utc,
            product_id, sub_vendor_id, game_category, game_category_desc,
            game_type_desc, live_subtype, has_jackpot, is_active,
            rounds_24h, players_24h, popularity_rank_24h, now_utc,
            total_bet_24h, total_wins_24h,
            provider_display_name, game_category_front,
        ))

    # ─── 5. Upsert no Super Nova DB ──────────────────────────────────────────
    # Regra "preserva valor antigo se novo for NULL" via COALESCE
    upsert_sql = """
        INSERT INTO multibet.game_image_mapping
            (game_name, game_name_upper, provider_game_id, vendor_id,
             game_image_url, game_slug, source, updated_at,
             product_id, sub_vendor_id, game_category, game_category_desc,
             game_type_desc, live_subtype, has_jackpot, is_active,
             rounds_24h, players_24h, popularity_rank_24h, popularity_window_end,
             total_bet_24h, total_wins_24h,
             provider_display_name, game_category_front)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s, %s,%s)
        ON CONFLICT (game_name_upper) DO UPDATE SET
            game_name             = EXCLUDED.game_name,
            provider_game_id      = COALESCE(EXCLUDED.provider_game_id, multibet.game_image_mapping.provider_game_id),
            vendor_id             = COALESCE(EXCLUDED.vendor_id, multibet.game_image_mapping.vendor_id),
            game_image_url        = COALESCE(EXCLUDED.game_image_url, multibet.game_image_mapping.game_image_url),
            game_slug             = EXCLUDED.game_slug,
            source                = EXCLUDED.source,
            updated_at            = EXCLUDED.updated_at,
            product_id            = COALESCE(EXCLUDED.product_id, multibet.game_image_mapping.product_id),
            sub_vendor_id         = COALESCE(EXCLUDED.sub_vendor_id, multibet.game_image_mapping.sub_vendor_id),
            game_category         = COALESCE(EXCLUDED.game_category, multibet.game_image_mapping.game_category),
            game_category_desc    = COALESCE(EXCLUDED.game_category_desc, multibet.game_image_mapping.game_category_desc),
            game_type_desc        = COALESCE(EXCLUDED.game_type_desc, multibet.game_image_mapping.game_type_desc),
            live_subtype          = COALESCE(EXCLUDED.live_subtype, multibet.game_image_mapping.live_subtype),
            has_jackpot           = COALESCE(EXCLUDED.has_jackpot, multibet.game_image_mapping.has_jackpot),
            is_active             = COALESCE(EXCLUDED.is_active, multibet.game_image_mapping.is_active),
            -- Rank/valores 24h: SEMPRE atualiza (valor "fresco" da janela 24h atual).
            -- Se EXCLUDED.popularity_rank_24h for NULL = jogo nao teve atividade nas ultimas 24h,
            -- intencionalmente zeramos metricas e limpamos rank.
            rounds_24h            = EXCLUDED.rounds_24h,
            players_24h           = EXCLUDED.players_24h,
            total_bet_24h         = EXCLUDED.total_bet_24h,
            total_wins_24h        = EXCLUDED.total_wins_24h,
            popularity_rank_24h   = EXCLUDED.popularity_rank_24h,
            popularity_window_end = EXCLUDED.popularity_window_end,
            -- v4: provider amigavel + bucket front (sempre derivados, atualiza)
            provider_display_name = EXCLUDED.provider_display_name,
            game_category_front   = EXCLUDED.game_category_front
    """

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, upsert_sql, records, page_size=100)
        conn.commit()
    finally:
        conn.close()
        ssh.close()

    # ─── 6. Relatorio ────────────────────────────────────────────────────────
    with_img      = sum(1 for r in records if r[4] is not None)
    without_img   = len(records) - with_img
    with_rank     = sum(1 for r in records if r[18] is not None)
    live_count    = sum(1 for r in records if r[10] == "live")
    slot_count    = sum(1 for r in records if r[10] == "slots")
    jackpot_count = sum(1 for r in records if r[14])
    active_count  = sum(1 for r in records if r[15] is True)

    total_bet_sum  = sum(float(r[20] or 0) for r in records)
    total_wins_sum = sum(float(r[21] or 0) for r in records)

    # v4: distribuicao por bucket de categoria do front
    from collections import Counter
    cat_front_counter = Counter(r[23] for r in records if r[23])
    providers_named   = sum(1 for r in records if r[22])

    log.info(f"Upsert concluido: {len(records)} jogos processados.")
    log.info(f"  Com imagem:        {with_img}  | Sem imagem: {without_img}")
    log.info(f"  Categoria slots:   {slot_count}")
    log.info(f"  Categoria live:    {live_count}")
    log.info(f"  Com jackpot:       {jackpot_count}")
    log.info(f"  Status active:     {active_count}")
    log.info(f"  Com rank 24h:      {with_rank} (jogos com atividade nas ultimas 24h)")
    log.info(f"  Turnover 24h:      R$ {total_bet_sum:>15,.2f}")
    log.info(f"  Wins 24h:          R$ {total_wins_sum:>15,.2f}")
    log.info(f"  Janela 24h ate:    {now_utc.isoformat()} UTC")
    log.info(f"  Provider amigavel: {providers_named}/{len(records)} mapeados")
    log.info(f"  Categoria front:   {dict(cat_front_counter.most_common())}")

    # S1: alerta vendors desconhecidos — orienta atualizar PROVIDER_DISPLAY_NAMES
    # quando provider novo entrar no catalogo (em vez de aparecer ID cru no front).
    unknown_vendors = sorted({
        r[3] for r in records
        if r[3] and str(r[3]).strip().lower() not in PROVIDER_DISPLAY_NAMES
    })
    if unknown_vendors:
        log.warning(
            f"  Vendors SEM display name ({len(unknown_vendors)}) — "
            f"atualizar PROVIDER_DISPLAY_NAMES: {unknown_vendors}"
        )

    if without_img > 0:
        missing = [r[0] for r in records if r[4] is None][:10]
        log.warning(f"  Jogos sem imagem (primeiros 10): {', '.join(missing)}")


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