-- ============================================================
-- DDL: multibet.grandes_ganhos
-- Banco: Super Nova DB (supernova_db)
-- Criado por: Mateus Fabro
-- Descrição: Maiores ganhos do dia por jogo.
--   Origem 1: BigQuery Smartico (ganhos, jogadores, nomes)
--   Origem 2: Redshift Pragmatic (slug e image_url do jogo)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.grandes_ganhos (
    id                  SERIAL PRIMARY KEY,

    -- Jogo
    game_name           VARCHAR(255),       -- ex: FORTUNE SNAKE
    provider_name       VARCHAR(100),       -- ex: PRAGMATICPLAY
    game_slug           VARCHAR(200),       -- Path de acesso ao jogo no site (ex: /pb/gameplay/aviator/real-game)
    game_image_url      VARCHAR(500),       -- URL do thumbnail no CDN do provedor

    -- Player (LGPD: nome hasheado, ex: "Ri***s")
    player_name_hashed  VARCHAR(50),
    smr_user_id         BIGINT,             -- ID interno Smartico — NÃO expor no front

    -- Ganho
    win_amount          NUMERIC(15, 2),     -- valor em BRL

    -- Controle
    event_time          TIMESTAMPTZ,        -- momento do ganho (UTC)
    refreshed_at        TIMESTAMPTZ         -- última atualização
);

CREATE INDEX IF NOT EXISTS idx_gg_event_time
    ON multibet.grandes_ganhos (event_time DESC);

-- ============================================================
-- Consulta que o front-end/API deve usar:
-- ============================================================
/*
SELECT
    game_name,
    provider_name,
    game_slug,
    game_image_url,
    player_name_hashed,
    win_amount,
    event_time
FROM multibet.grandes_ganhos
ORDER BY win_amount DESC
LIMIT 20;
*/

-- ============================================================
-- Padrões de URL de imagem por provedor (para referência do dev):
-- PRAGMATICPLAY: https://cdn.pragmaticplay.net/game-icons/{slug}/game_pic/square2/{slug}.png
-- ============================================================
