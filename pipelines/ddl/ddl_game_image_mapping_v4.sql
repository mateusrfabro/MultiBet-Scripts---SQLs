-- ============================================================
-- DDL v4: multibet.game_image_mapping + vw_front_api_games
-- Data: 2026-04-22
-- Demanda: Head (via analise do categories-multibet-api)
--
-- Adicoes em v4 (sobre v3):
--   - provider_display_name  (nome amigavel do vendor, ex: "PG Soft")
--   - game_category_front    (bucket do front, ex: "Crash", "Fortune", "TV Shows")
--
-- Mantem retro-compat: ALTER ADD COLUMN IF NOT EXISTS (idempotente).
-- View vw_front_api_games recriada para expor as novas colunas como
-- "providerDisplayName" e "categoryFront" (CamelCase, simetria com DTO).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS multibet;

ALTER TABLE multibet.game_image_mapping
    ADD COLUMN IF NOT EXISTS provider_display_name VARCHAR(50),
    ADD COLUMN IF NOT EXISTS game_category_front   VARCHAR(20);

COMMENT ON COLUMN multibet.game_image_mapping.provider_display_name IS
    'Nome amigavel do vendor (ex: alea_pgsoft -> "PG Soft"). Dict estatico no pipeline.';
COMMENT ON COLUMN multibet.game_image_mapping.game_category_front IS
    'Bucket de categoria exposto ao front: Live | Crash | TV Shows | Bac Bo | Baccarat | Blackjack | Fortune | Slots. Derivado em prioridade: game_category nativo -> regex no game_name.';

-- Indice para filtros por categoria do front (carrosseis de destaque)
CREATE INDEX IF NOT EXISTS idx_gim_category_front
    ON multibet.game_image_mapping (game_category_front, is_active);

-- ============================================================
-- VIEW: vw_front_api_games (v4 — expoe as 2 colunas novas)
--
-- IMPORTANTE: CREATE OR REPLACE VIEW no Postgres NAO permite renomear
-- nem reordenar colunas existentes — so adicionar colunas no FINAL.
-- Por isso: 1) mantemos a ordem identica a v3 nos primeiros 16 campos
--           2) adicionamos providerDisplayName e categoryFront APENAS no final
-- Se a v3 nao existir, o CREATE simples cria do zero.
-- ============================================================
CREATE OR REPLACE VIEW multibet.vw_front_api_games AS
SELECT
    -- ⬇️ ordem identica a v3 (NAO alterar) ⬇️
    provider_game_id        AS "gameId",
    game_name               AS "name",
    game_slug               AS "gameSlug",
    game_slug               AS "gamePath",
    game_image_url          AS "image",
    vendor_id               AS "provider",
    game_category           AS "category",
    game_category_desc      AS "categoryDescription",
    COALESCE(rounds_24h,      0) AS "totalBets",
    COALESCE(players_24h,     0) AS "uniquePlayers",
    COALESCE(total_bet_24h,   0) AS "totalBet",
    COALESCE(total_wins_24h,  0) AS "totalWins",
    popularity_rank_24h     AS "rank",
    live_subtype,
    has_jackpot,
    popularity_window_end   AS "windowEndUtc",
    -- ⬇️ v4: novas colunas (sempre no FINAL para manter compat com CREATE OR REPLACE) ⬇️
    provider_display_name   AS "providerDisplayName",
    game_category_front     AS "categoryFront"
FROM multibet.game_image_mapping
WHERE is_active = TRUE
  AND game_image_url IS NOT NULL
  AND game_image_url <> '';

COMMENT ON VIEW multibet.vw_front_api_games IS
'Shape 1:1 com GameResponseDto + campos providerDisplayName e categoryFront
 (derivados, equivalentes ao que a categories-multibet-api calcula em runtime).
 Refresh: pipeline game_image_mapper a cada 4h (cron EC2 ETL).';
