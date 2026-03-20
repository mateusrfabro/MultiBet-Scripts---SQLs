-- ============================================================
-- DDL: multibet.game_image_mapping
-- Banco: Super Nova DB (supernova_db)
-- Criado por: Mateus Fabro
-- Descrição: Mapeamento de jogos → URL de imagem + slug.
--   Origem: CSV gerado pelo scraper (capturar_jogos_pc.py)
--           + catálogo Redshift (vendor_games_mapping_data)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.game_image_mapping (
    id                  SERIAL PRIMARY KEY,

    -- Identificação do jogo
    game_name           VARCHAR(255) NOT NULL,       -- Nome original do jogo (ex: Fortune Ox)
    game_name_upper     VARCHAR(255) NOT NULL,        -- Nome normalizado UPPER para joins
    provider_game_id    VARCHAR(50),                  -- c_game_id do Redshift (ex: 4776)
    vendor_id           VARCHAR(100),                 -- c_vendor_id do Redshift (ex: alea_pgsoft)

    -- URLs
    game_image_url      VARCHAR(500),                 -- URL do thumbnail (CDN multi.bet ou provedor)
    game_slug           VARCHAR(200),                 -- Path de acesso ao jogo (ex: /pb/gameplay/fortune-ox/real-game)

    -- Controle
    source              VARCHAR(50) DEFAULT 'scraper', -- scraper, manual, redshift
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    -- Constraint: um registro por game_name normalizado
    CONSTRAINT uq_game_name_upper UNIQUE (game_name_upper)
);

CREATE INDEX IF NOT EXISTS idx_gim_game_name_upper
    ON multibet.game_image_mapping (game_name_upper);

-- ============================================================
-- Consulta para join com grandes_ganhos:
-- ============================================================
/*
SELECT
    m.game_image_url,
    m.game_slug
FROM multibet.game_image_mapping m
WHERE m.game_name_upper = UPPER(TRIM('Fortune Ox'));
*/