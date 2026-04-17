-- ============================================================
-- DDL: Views vw_front_* — consumidas pelo time de produto/front
-- Banco: Super Nova DB (PostgreSQL)
-- Data: 2026-04-17
-- Demanda: CTO Gabriel Barbosa (via Castrin)
--
-- Padrao multibet:
--   - Prefixo vw_front_*
--   - Nunca expoe FK/colunas internas (id, source, updated_at)
--   - Sempre retorna game_image_url e game_slug (usados pelo front)
--   - Sempre filtra is_active = TRUE e game_image_url IS NOT NULL
--   - Front consome SO PostgreSQL (Athena fica fora do caminho critico)
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- 1. vw_front_top_24h — "Mais jogados" (carrossel principal)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW multibet.vw_front_top_24h AS
SELECT
    popularity_rank_24h    AS rank,
    game_name,
    vendor_id              AS vendor,
    sub_vendor_id          AS sub_vendor,
    game_category          AS category,
    live_subtype,
    game_image_url         AS image_url,
    game_slug              AS slug,
    rounds_24h,
    players_24h,
    popularity_window_end  AS window_end_utc
FROM multibet.game_image_mapping
WHERE is_active = TRUE
  AND game_image_url IS NOT NULL
  AND popularity_rank_24h IS NOT NULL
  AND popularity_rank_24h <= 50  -- top 50 (front pode paginar)
ORDER BY popularity_rank_24h;


-- ────────────────────────────────────────────────────────────
-- 2. vw_front_live_casino — Cassino ao vivo (com subtipo)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW multibet.vw_front_live_casino AS
SELECT
    game_name,
    vendor_id              AS vendor,
    sub_vendor_id          AS sub_vendor,
    live_subtype,                       -- Roleta | Blackjack | Baccarat | GameShow | Outros
    game_type_desc         AS subtipo_raw,
    game_image_url         AS image_url,
    game_slug              AS slug,
    rounds_24h,
    players_24h,
    popularity_rank_24h    AS rank
FROM multibet.game_image_mapping
WHERE is_active = TRUE
  AND game_image_url IS NOT NULL
  AND game_category = 'live'
ORDER BY
    live_subtype,
    COALESCE(popularity_rank_24h, 999999),
    game_name;


-- ────────────────────────────────────────────────────────────
-- 3. vw_front_by_vendor — "Jogos Pragmatic", "Jogos PG Soft", etc.
--    Front filtra com WHERE vendor = 'pragmaticplay'
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW multibet.vw_front_by_vendor AS
SELECT
    vendor_id              AS vendor,
    sub_vendor_id          AS sub_vendor,
    game_name,
    game_category          AS category,
    live_subtype,
    game_image_url         AS image_url,
    game_slug              AS slug,
    rounds_24h,
    popularity_rank_24h    AS rank,
    has_jackpot
FROM multibet.game_image_mapping
WHERE is_active = TRUE
  AND game_image_url IS NOT NULL
  AND vendor_id IS NOT NULL
ORDER BY
    vendor_id,
    COALESCE(popularity_rank_24h, 999999),
    game_name;


-- ────────────────────────────────────────────────────────────
-- 4. vw_front_by_category — Slots | Live | DrawGames (filtros macro)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW multibet.vw_front_by_category AS
SELECT
    game_category          AS category,
    game_category_desc     AS category_desc,
    game_name,
    vendor_id              AS vendor,
    live_subtype,
    game_image_url         AS image_url,
    game_slug              AS slug,
    rounds_24h,
    popularity_rank_24h    AS rank,
    has_jackpot
FROM multibet.game_image_mapping
WHERE is_active = TRUE
  AND game_image_url IS NOT NULL
  AND game_category IS NOT NULL
ORDER BY
    game_category,
    COALESCE(popularity_rank_24h, 999999),
    game_name;


-- ────────────────────────────────────────────────────────────
-- 5. vw_front_jackpot — Carrossel "Jackpots"
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW multibet.vw_front_jackpot AS
SELECT
    game_name,
    vendor_id              AS vendor,
    game_category          AS category,
    game_image_url         AS image_url,
    game_slug              AS slug,
    rounds_24h,
    popularity_rank_24h    AS rank
FROM multibet.game_image_mapping
WHERE is_active = TRUE
  AND game_image_url IS NOT NULL
  AND has_jackpot = TRUE
ORDER BY COALESCE(popularity_rank_24h, 999999), game_name;


-- ============================================================
-- LEGENDA / GLOSSARIO (para o time de front)
-- ============================================================
-- Todas as views retornam:
--   game_name      → nome amigavel para exibicao
--   image_url      → URL da imagem (CDN multi.bet)
--   slug           → path para abrir o jogo (ex: /pb/gameplay/fortune_ox/real-game)
--   rounds_24h     → rodadas nas ULTIMAS 24h rolantes (refresh a cada 4h)
--   rank           → 1 = mais jogado nas ultimas 24h
--
-- Filtros padrao em todas:
--   is_active = TRUE         → catalogo Pragmatic ativo
--   game_image_url NOT NULL  → so jogos com imagem (evita placeholder no front)
--
-- Validacao anti-erro (caso slot vai para Live ao vivo):
--   vw_front_live_casino filtra game_category = 'live' (origem: catalogo Pragmatic)
--   Se um slot estiver classificado como live no catalogo, vai aparecer aqui.
--   A correcao deve ser feita na ORIGEM (catalogo Pragmatic via BackOffice),
--   nao em workaround na view.
-- ============================================================
