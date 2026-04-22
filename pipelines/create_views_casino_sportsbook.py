"""
Pipeline: Views Gold — Casino & Sportsbook (diferenciadas por produto)
======================================================================
Cria 7 views no SuperNova DB (schema multibet) para consumo do front-end.

RESUMO (financeiro diario, base para cards e graficos de tendencia):
    1. vw_casino_kpis          — KPIs diarios casino (GGR/Jogador, Hold Rate)
    2. vw_sportsbook_kpis      — KPIs diarios sportsbook (GGR/Jogador, Margin)

CASINO-ESPECIFICAS (dimensoes unicas do casino):
    3. vw_casino_by_provider   — Performance por provedor (Pragmatic, PG Soft, Evolution...)
    4. vw_casino_by_category   — Performance por categoria (Slots vs Live vs Crash)
    5. vw_casino_top_games     — Ranking de jogos por GGR (com RTP, Hold Rate, Rounds)

SPORTSBOOK-ESPECIFICAS (dimensoes unicas do sportsbook):
    6. vw_sportsbook_by_sport  — Performance por esporte (Futebol, Basquete, Tennis...)
    7. vw_sportsbook_exposure  — Apostas abertas / exposicao por esporte

Fontes (silver tables):
    - fct_casino_activity (dia, sub-fund isolation)
    - fct_sports_activity (dia, sub-fund isolation)
    - fact_casino_rounds (dia x jogo, ps_bi)
    - fact_sports_bets_by_sport (dia x esporte, vendor_ec2)
    - fact_sports_open_bets (snapshot, vendor_ec2)

Execucao:
    python pipelines/create_views_casino_sportsbook.py

Nota: Usa DROP RESTRICT + CREATE (nao CASCADE) para evitar destruir views dependentes.
Se uma view tiver dependentes, o DROP falhara — isso e intencional (seguranca).
"""

import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# =========================================================================
# 1. vw_casino_kpis — Resumo diario casino
#    Fonte: fct_casino_activity (sub-fund isolation Mauro)
# =========================================================================
VW_CASINO_KPIS = """
CREATE OR REPLACE VIEW multibet.vw_casino_kpis AS
SELECT
    c.dt,
    c.qty_players,
    -- Fix Gusta #3: total_rounds (sessoes) via fact_casino_rounds
    COALESCE(r.total_rounds, 0) AS total_rounds,
    c.casino_real_bet,
    c.casino_bonus_bet,
    c.casino_total_bet,
    c.casino_real_win,
    c.casino_bonus_win,
    c.casino_total_win,
    c.casino_real_ggr,
    c.casino_bonus_ggr,
    c.casino_total_ggr,
    -- GGR / Jogador (KPI pedido pelo Castrin)
    CASE WHEN c.qty_players > 0
         THEN ROUND(c.casino_real_ggr / c.qty_players, 2)
         ELSE NULL END AS ggr_per_player,
    -- Hold Rate (house edge = GGR / Bet * 100)
    CASE WHEN c.casino_real_bet > 0
         THEN ROUND(c.casino_real_ggr / c.casino_real_bet * 100, 2)
         ELSE NULL END AS hold_rate_pct,
    c.refreshed_at
FROM multibet.fct_casino_activity c
LEFT JOIN (
    SELECT dt, SUM(total_rounds) AS total_rounds
    FROM multibet.fact_casino_rounds
    GROUP BY dt
) r ON c.dt = r.dt;
"""

# =========================================================================
# 2. vw_sportsbook_kpis — Resumo diario sportsbook
#    Fonte: fct_sports_activity (sub-fund isolation)
# =========================================================================
VW_SPORTSBOOK_KPIS = """
CREATE OR REPLACE VIEW multibet.vw_sportsbook_kpis AS
SELECT
    dt,
    qty_players,
    -- FIX Gusta gap menor #4 (10/04/2026): qty_bets + avg_ticket pra paridade com casino
    -- Fonte: SB_BUYIN no fct_sports_activity (mesma fonte do GGR, bate 1-pra-1)
    qty_bets,
    sports_real_bet,
    sports_bonus_bet,
    sports_total_bet,
    sports_real_win,
    sports_bonus_win,
    sports_total_win,
    sports_real_ggr,
    sports_bonus_ggr,
    sports_total_ggr,
    -- GGR / Jogador (KPI pedido pelo Castrin)
    CASE WHEN qty_players > 0
         THEN ROUND(sports_real_ggr / qty_players, 2)
         ELSE NULL END AS ggr_per_player,
    -- Ticket medio (turnover real / qty_bets) — pedido do Gusta gap #4
    CASE WHEN qty_bets > 0
         THEN ROUND(sports_real_bet / qty_bets, 2)
         ELSE NULL END AS avg_ticket,
    -- Margin (GGR / Bet * 100 — conceito equivalente ao hold rate)
    CASE WHEN sports_real_bet > 0
         THEN ROUND(sports_real_ggr / sports_real_bet * 100, 2)
         ELSE NULL END AS margin_pct,
    refreshed_at
FROM multibet.fct_sports_activity;
"""

# =========================================================================
# 3. vw_casino_by_provider — Performance por provedor
#    Fonte: fact_casino_rounds (ps_bi, dia x jogo)
#    Agrega por: dt, sub_vendor_id (provedor)
# =========================================================================
VW_CASINO_BY_PROVIDER = """
CREATE OR REPLACE VIEW multibet.vw_casino_by_provider AS
WITH provider_lookup AS (
    -- v4.2 (22/04/2026): mapping vendor_key -> provider_display_name a partir
    -- da game_image_mapping enriquecida (dict estatico em pipelines/game_image_mapper.py).
    -- DISTINCT ON garante 1 linha por vendor_key.
    SELECT DISTINCT ON (vendor_key)
           vendor_key,
           provider_display_name
    FROM (
        SELECT COALESCE(NULLIF(TRIM(sub_vendor_id), ''), NULLIF(TRIM(vendor_id), '')) AS vendor_key,
               provider_display_name
        FROM multibet.game_image_mapping
        WHERE provider_display_name IS NOT NULL
          AND COALESCE(NULLIF(TRIM(sub_vendor_id), ''), NULLIF(TRIM(vendor_id), '')) IS NOT NULL
    ) sub
    ORDER BY vendor_key, provider_display_name
)
SELECT
    -- ORDEM IDENTICA A V4.1 (retro-compat: CREATE OR REPLACE VIEW nao permite reordenar)
    fcr.dt,
    COALESCE(NULLIF(TRIM(fcr.sub_vendor_id), ''), NULLIF(TRIM(fcr.vendor_id), ''), 'Nao identificado') AS provider,
    COUNT(DISTINCT fcr.game_id)    AS qty_games,
    SUM(fcr.qty_players)           AS qty_players,
    SUM(fcr.total_rounds)          AS total_rounds,
    SUM(fcr.turnover_real)         AS turnover_real,
    SUM(fcr.wins_real)             AS wins_real,
    SUM(fcr.ggr_real)              AS ggr_real,
    SUM(fcr.turnover_total)        AS turnover_total,
    SUM(fcr.ggr_total)             AS ggr_total,
    CASE WHEN SUM(fcr.turnover_real) > 0
         THEN ROUND(SUM(fcr.ggr_real) / SUM(fcr.turnover_real) * 100, 2)
         ELSE NULL END             AS hold_rate_pct,
    CASE WHEN SUM(fcr.turnover_real) > 0
         THEN ROUND(SUM(fcr.wins_real) / SUM(fcr.turnover_real) * 100, 2)
         ELSE NULL END             AS rtp_pct,
    SUM(fcr.jackpot_win)           AS jackpot_win,
    SUM(fcr.jackpot_contribution)  AS jackpot_contribution,
    SUM(fcr.free_spins_bet)        AS free_spins_bet,
    SUM(fcr.free_spins_win)        AS free_spins_win,
    -- v4.2: NOVA coluna no FINAL (CREATE OR REPLACE so aceita adicoes)
    -- Nome amigavel p/ o front (PG Soft, Pragmatic Play, ...) com fallback p/ o id
    COALESCE(
        pl.provider_display_name,
        NULLIF(TRIM(fcr.sub_vendor_id), ''),
        NULLIF(TRIM(fcr.vendor_id), ''),
        'Nao identificado'
    ) AS provider_name
FROM multibet.fact_casino_rounds fcr
LEFT JOIN provider_lookup pl
    ON pl.vendor_key = COALESCE(NULLIF(TRIM(fcr.sub_vendor_id), ''), NULLIF(TRIM(fcr.vendor_id), ''))
GROUP BY
    fcr.dt,
    COALESCE(NULLIF(TRIM(fcr.sub_vendor_id), ''), NULLIF(TRIM(fcr.vendor_id), ''), 'Nao identificado'),
    COALESCE(pl.provider_display_name,
             NULLIF(TRIM(fcr.sub_vendor_id), ''),
             NULLIF(TRIM(fcr.vendor_id), ''),
             'Nao identificado');
"""

# =========================================================================
# 4. vw_casino_by_category — Performance por categoria de jogo
#    Fonte: fact_casino_rounds
#    Agrega por: dt, category (canonica)
#
#    FIX Gusta v4.1 (10/04/2026): o bireports fornece 97.9% dos jogos
#    categorizados como Slots ou Live, mas crash/instant games (Aviator,
#    Spaceman, Mines, Plinko, High Flyer, Big Bass Crash, etc) caem como
#    NULL → Outros. Sozinho o Aviator vale R$ 850K de GGR em Mar/2026,
#    entao vale classificar explicitamente.
#
#    Override por nome: crash/instant games conhecidos → 'Crash/Instant'.
#    Lista baseada em analise empirica Mar/2026 dos top jogos em 'Outros'.
# =========================================================================
VW_CASINO_BY_CATEGORY = """
CREATE OR REPLACE VIEW multibet.vw_casino_by_category AS
SELECT
    dt,
    CASE
        WHEN game_category = 'Live' THEN 'Live Casino'
        ELSE 'Casino'
    END AS category,
    COUNT(DISTINCT game_id) AS qty_games,
    SUM(qty_players) AS qty_players,
    SUM(total_rounds) AS total_rounds,
    SUM(turnover_real) AS turnover_real,
    SUM(wins_real) AS wins_real,
    SUM(ggr_real) AS ggr_real,
    SUM(ggr_total) AS ggr_total,
    -- Hold Rate por categoria
    CASE WHEN SUM(turnover_real) > 0
         THEN ROUND(SUM(ggr_real) / SUM(turnover_real) * 100, 2)
         ELSE NULL END AS hold_rate_pct,
    -- RTP por categoria
    CASE WHEN SUM(turnover_real) > 0
         THEN ROUND(SUM(wins_real) / SUM(turnover_real) * 100, 2)
         ELSE NULL END AS rtp_pct,
    SUM(jackpot_win) AS jackpot_win
FROM multibet.fact_casino_rounds
GROUP BY dt,
    CASE
        WHEN game_category = 'Live' THEN 'Live Casino'
        ELSE 'Casino'
    END;
"""

# =========================================================================
# 5. vw_casino_top_games — Ranking de jogos por GGR
#    Fonte: fact_casino_rounds
#    Grao: dt x game_id (sem agregacao — expoe o detalhe)
# =========================================================================
VW_CASINO_TOP_GAMES = """
CREATE OR REPLACE VIEW multibet.vw_casino_top_games AS
SELECT
    -- ORDEM IDENTICA A V4.1 (retro-compat: CREATE OR REPLACE nao permite reordenar)
    fcr.dt,
    fcr.game_id,
    CASE WHEN fcr.game_name = 'Desconhecido'
         THEN 'Desconhecido (' || fcr.game_id || ')'
         ELSE fcr.game_name END AS game_name,
    COALESCE(NULLIF(TRIM(fcr.sub_vendor_id), ''), NULLIF(TRIM(fcr.vendor_id), ''), 'Nao identificado') AS provider,
    CASE
        WHEN fcr.game_category = 'Live' THEN 'Live Casino'
        ELSE 'Casino'
    END AS category,
    fcr.qty_players,
    fcr.total_rounds,
    fcr.rounds_per_player,
    fcr.turnover_real,
    fcr.wins_real,
    fcr.ggr_real,
    fcr.hold_rate_pct,
    fcr.rtp_pct,
    fcr.jackpot_win,
    fcr.free_spins_bet,
    fcr.free_spins_win,
    -- v4.2: NOVAS colunas no FINAL (ver comentario em VW_CASINO_BY_PROVIDER)
    -- provider_name: nome amigavel p/ front (PG Soft, Pragmatic Play, ...)
    COALESCE(
        gim.provider_display_name,
        NULLIF(TRIM(fcr.sub_vendor_id), ''),
        NULLIF(TRIM(fcr.vendor_id), ''),
        'Nao identificado'
    ) AS provider_name,
    -- category_front: bucket amigavel da v4 (Fortune, Crash, TV Shows, Blackjack...)
    COALESCE(
        gim.game_category_front,
        CASE WHEN fcr.game_category = 'Live' THEN 'Live Casino' ELSE 'Casino' END
    ) AS category_front,
    -- game_image_url: permite o front montar o card direto da view
    gim.game_image_url
FROM multibet.fact_casino_rounds fcr
-- LATERAL garante 1 match (prioriza game_id exato, fallback por game_name_upper)
LEFT JOIN LATERAL (
    SELECT provider_display_name, game_category_front, game_image_url
    FROM multibet.game_image_mapping g
    WHERE g.provider_game_id = fcr.game_id
       OR UPPER(TRIM(g.game_name)) = UPPER(TRIM(fcr.game_name))
    ORDER BY CASE WHEN g.provider_game_id = fcr.game_id THEN 0 ELSE 1 END
    LIMIT 1
) gim ON TRUE;
"""

# =========================================================================
# 6. vw_sportsbook_by_sport — Performance por esporte
#    Fonte: fact_sports_bets_by_sport (NOVA silver com breakdown real)
#    Agrega por: dt, sport_name
#
#    FIX Gusta gap #5 (10/04/2026): normalizacao de 46 esportes fragmentados.
#    46 sport_names distintos (idiomas pt/en/es + virtuais + provedores diferentes).
#    Solucao: coluna `sport_category` canonica hardcoded na view. Mantem
#    `sport_name` original pra drill-down. Mapping baseado em analise
#    empirica do Mar/2026 (todos os 46 esportes com volume).
# =========================================================================
VW_SPORTSBOOK_BY_SPORT = """
CREATE OR REPLACE VIEW multibet.vw_sportsbook_by_sport AS
SELECT
    dt,
    -- Fix Gusta #6: limpar tabs/espacos em sport_name ("E-sports +\t\t")
    regexp_replace(sport_name, '(^[[:space:]]+|[[:space:]]+$)', '', 'g') AS sport_name,
    -- Fix Gusta #5: categoria canonica pra graficos agregados (evita fragmentacao)
    CASE
        -- Futebol real (portugues, ingles, espanhol + eventos oficiais)
        WHEN sport_name IN ('Futebol', 'Football', 'Soccer', 'Football Cup - World')
            THEN 'Futebol'
        -- Futebol virtual (Kiron + variantes)
        WHEN sport_name IN ('KironFootball', 'Virtual Football Cup', 'Virtual Football League')
            THEN 'Futebol Virtual'
        -- Futebol Americano (3 variantes)
        WHEN sport_name IN ('Futebol Americano', 'American Football', 'AmericanFootballH2H')
            THEN 'Futebol Americano'
        -- Basquete (pt + en)
        WHEN sport_name IN ('Basquete', 'Basketball', 'Baloncesto')
            THEN 'Basquete'
        -- Tenis (pt + en, sem acento — consistente com resto)
        WHEN sport_name IN ('Tenis', 'Tênis', 'Tennis')
            THEN 'Tenis'
        WHEN sport_name IN ('Tenis de mesa', 'Tênis de mesa', 'Table Tennis')
            THEN 'Tenis de Mesa'
        -- Volei + variantes
        WHEN sport_name IN ('Volei', 'Vôlei', 'Volleyball')
            THEN 'Volei'
        WHEN sport_name IN ('Volei de Praia', 'Vôlei de Praia')
            THEN 'Volei de Praia'
        -- Beisebol (pt + en)
        WHEN sport_name IN ('Beisebol', 'Baseball')
            THEN 'Beisebol'
        -- Hoquei no Gelo (pt + en)
        WHEN sport_name IN ('Hoquei no Gelo', 'Hóquei no Gelo', 'Ice Hockey')
            THEN 'Hoquei no Gelo'
        WHEN sport_name IN ('Hoquei em campo', 'Hóquei em campo')
            THEN 'Hoquei em Campo'
        -- Handebol (pt + en)
        WHEN sport_name IN ('Handebol', 'Handball')
            THEN 'Handebol'
        -- Boxe (pt + en)
        WHEN sport_name IN ('Boxe', 'Boxing')
            THEN 'Boxe'
        -- Dardos (pt + en)
        WHEN sport_name IN ('Dardos', 'Darts')
            THEN 'Dardos'
        -- Rugby (todas variantes)
        WHEN sport_name IN ('Rugby', 'Rugby League', 'Rugby Union')
            THEN 'Rugby'
        -- Outros sem variantes detectadas (mantem o nome)
        WHEN sport_name IN ('MMA', 'Futsal', 'Cricket', 'Ciclismo', 'Badminton',
                            'Floorball', 'E-sports +', 'Especiais',
                            'Esportes Motorizados', 'Sinuca internacional',
                            'Jogos Olimpicos', 'Jogos Olímpicos')
            THEN regexp_replace(sport_name, '(^[[:space:]]+|[[:space:]]+$)', '', 'g')
        ELSE 'Outros'
    END AS sport_category,
    qty_bets,
    qty_players,
    turnover,
    total_return,
    ggr,
    margin_pct,
    avg_ticket,
    avg_odds,
    qty_pre_match,
    qty_live,
    turnover_pre_match,
    turnover_live,
    pct_pre_match,
    pct_live,
    -- GGR / Jogador por esporte
    CASE WHEN qty_players > 0
         THEN ROUND(ggr / qty_players, 2)
         ELSE NULL END AS ggr_per_player,
    refreshed_at
FROM multibet.fact_sports_bets_by_sport;
"""

# =========================================================================
# 7. vw_sportsbook_exposure — Apostas abertas / risco por esporte
#    Fonte: fact_sports_open_bets (snapshot pontual)
# =========================================================================
VW_SPORTSBOOK_EXPOSURE = """
CREATE OR REPLACE VIEW multibet.vw_sportsbook_exposure AS
SELECT
    snapshot_dt,
    -- Fix Gusta #6: limpar tabs/espacos em sport_name
    regexp_replace(sport_name, '(^[[:space:]]+|[[:space:]]+$)', '', 'g') AS sport_name,
    qty_open_bets,
    total_stake_open,
    avg_odds_open,
    projected_liability,
    projected_ggr,
    -- % do total de stake aberto
    CASE WHEN SUM(total_stake_open) OVER () > 0
         THEN ROUND((total_stake_open * 100.0
                     / SUM(total_stake_open) OVER ())::NUMERIC, 2)
         ELSE NULL END AS pct_stake_total,
    refreshed_at
FROM multibet.fact_sports_open_bets;
"""


# =========================================================================
# Mapa de views e nomes para log
# =========================================================================
VIEWS = [
    ("vw_casino_kpis",          VW_CASINO_KPIS,          "Resumo casino"),
    ("vw_sportsbook_kpis",      VW_SPORTSBOOK_KPIS,      "Resumo sportsbook"),
    ("vw_casino_by_provider",   VW_CASINO_BY_PROVIDER,   "Casino por provedor"),
    ("vw_casino_by_category",   VW_CASINO_BY_CATEGORY,   "Casino por categoria"),
    ("vw_casino_top_games",     VW_CASINO_TOP_GAMES,     "Casino jogos (detalhe)"),
    ("vw_sportsbook_by_sport",  VW_SPORTSBOOK_BY_SPORT,  "Sportsbook por esporte"),
    ("vw_sportsbook_exposure",  VW_SPORTSBOOK_EXPOSURE,  "Sportsbook exposicao"),
]


def create_views():
    """Cria ou recria todas as views gold.
    Usa DROP + CREATE para permitir mudanca de colunas (PostgreSQL nao permite
    remover colunas com CREATE OR REPLACE VIEW).
    """
    for name, sql, desc in VIEWS:
        log.info(f"Criando multibet.{name} ({desc})...")
        try:
            # DROP RESTRICT (nao CASCADE) para nao destruir views dependentes
            execute_supernova(f"DROP VIEW IF EXISTS multibet.{name} RESTRICT;")
            # Trocar CREATE OR REPLACE por CREATE
            create_sql = sql.replace("CREATE OR REPLACE VIEW", "CREATE VIEW")
            execute_supernova(create_sql)
            log.info(f"  OK")
        except Exception as e:
            log.error(f"  ERRO ao criar {name}: {e}")
            raise


def validate_views():
    """Valida que as views retornam dados."""
    log.info("--- Validacao ---")
    for name, _, desc in VIEWS:
        try:
            # Verifica se a silver table fonte existe e tem dados
            rows = execute_supernova(
                f"SELECT COUNT(*) FROM multibet.{name};",
                fetch=True,
            )
            n = rows[0][0]
            log.info(f"  {name}: {n:,} linhas")
        except Exception as e:
            log.warning(f"  {name}: ERRO (silver table pode nao existir ainda) — {e}")

    # Sanity checks especificos
    log.info("--- Sanity Checks ---")

    # GGR/Player casino
    rows = execute_supernova(
        """SELECT dt, ggr_per_player, qty_players
           FROM multibet.vw_casino_kpis
           WHERE ggr_per_player > 500
           ORDER BY dt DESC LIMIT 3;""",
        fetch=True,
    )
    log.info(f"  Casino GGR/Player > R$500: {len(rows)} dias" +
             (" (dias de alta variancia)" if rows else " — OK"))

    # Hold Rate casino
    rows = execute_supernova(
        """SELECT dt, hold_rate_pct
           FROM multibet.vw_casino_kpis
           WHERE hold_rate_pct IS NOT NULL
             AND (hold_rate_pct > 10 OR hold_rate_pct < -5)
           ORDER BY dt DESC LIMIT 3;""",
        fetch=True,
    )
    log.info(f"  Casino Hold Rate fora de -5..10%: {len(rows)} dias" +
             (" (verificar rollbacks)" if rows else " — OK"))

    # Providers casino
    rows = execute_supernova(
        """SELECT provider, SUM(ggr_real) AS ggr
           FROM multibet.vw_casino_by_provider
           GROUP BY provider
           ORDER BY ggr DESC LIMIT 5;""",
        fetch=True,
    )
    log.info(f"  Top 5 providers casino: {', '.join(f'{r[0]}(R${r[1]:,.0f})' for r in rows)}")

    # Categorias casino
    rows = execute_supernova(
        """SELECT category, SUM(ggr_real) AS ggr
           FROM multibet.vw_casino_by_category
           GROUP BY category
           ORDER BY ggr DESC;""",
        fetch=True,
    )
    log.info(f"  Categorias casino: {', '.join(f'{r[0]}(R${r[1]:,.0f})' for r in rows)}")

    # Esportes sportsbook (se disponivel)
    try:
        rows = execute_supernova(
            """SELECT sport_name, SUM(ggr) AS ggr
               FROM multibet.vw_sportsbook_by_sport
               GROUP BY sport_name
               ORDER BY ggr DESC LIMIT 5;""",
            fetch=True,
        )
        log.info(f"  Top 5 esportes SB: {', '.join(f'{r[0]}(R${r[1]:,.0f})' for r in rows)}")
    except Exception:
        log.warning("  Sportsbook by sport: silver table ainda nao populada")

    # Exposure sportsbook (se disponivel)
    try:
        rows = execute_supernova(
            """SELECT SUM(total_stake_open), SUM(projected_liability)
               FROM multibet.vw_sportsbook_exposure;""",
            fetch=True,
        )
        stake, liab = rows[0]
        log.info(f"  Exposure SB: Stake R$ {stake:,.2f} | Liability R$ {liab:,.2f}")
    except Exception:
        log.warning("  Sportsbook exposure: silver table ainda nao populada")


if __name__ == "__main__":
    try:
        log.info("=== Criando views gold Casino & Sportsbook (7 views) ===")
        create_views()
        validate_views()
        log.info("=== Concluido ===")
    except Exception as e:
        log.error(f"Pipeline falhou: {e}", exc_info=True)
        sys.exit(1)
