-- ============================================================
-- DDL: multibet.grandes_ganhos
-- Banco: Super Nova DB (supernova_db)
-- Criado por: Mateus Fabro
-- Descrição: Maiores ganhos do dia por jogo.
--   Origem 1: Athena fund_ec2 (ganhos casino, c_txn_type=45)
--   Origem 2: Athena bireports_ec2 (nomes jogos/providers)
--   Origem 3: Athena ecr_ec2 (nomes jogadores, hash LGPD)
--   Origem 4: Super Nova DB (mapeamento imagens)
-- Histórico: v1 BigQuery (até 06/04/2026) → v2 Athena
-- ============================================================

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.grandes_ganhos (
    id                  SERIAL PRIMARY KEY,

    -- Jogo
    game_name           VARCHAR(255),       -- ex: FORTUNE SNAKE
    provider_name       VARCHAR(100),       -- ex: pragmaticplay, alea_evolution
    game_slug           VARCHAR(200),       -- Path de acesso ao jogo no site (ex: /pb/gameplay/aviator/real-game)
    game_image_url      VARCHAR(500),       -- URL do thumbnail no CDN do provedor

    -- Player (LGPD: nome hasheado, ex: "Ri***")
    player_name_hashed  VARCHAR(50),
    ecr_id              BIGINT,             -- ID interno ECR (Athena) — NÃO expor no front

    -- Ganho
    win_amount          NUMERIC(15, 2),     -- valor em BRL

    -- Controle
    event_time          TIMESTAMPTZ,        -- momento do ganho (UTC)
    refreshed_at        TIMESTAMPTZ         -- última atualização
);

CREATE INDEX IF NOT EXISTS idx_gg_event_time
    ON multibet.grandes_ganhos (event_time DESC);

-- ============================================================
-- v4 (2026-04-22): enriquecimento de catalogo (JOIN com game_image_mapping)
-- Mantem retro-compat — front continua lendo os mesmos campos.
-- Novas colunas alimentadas no refresh do pipeline grandes_ganhos.py.
-- ============================================================
ALTER TABLE multibet.grandes_ganhos
    ADD COLUMN IF NOT EXISTS provider_display_name VARCHAR(50),
    ADD COLUMN IF NOT EXISTS game_category         VARCHAR(30),
    ADD COLUMN IF NOT EXISTS game_category_front   VARCHAR(20);

COMMENT ON COLUMN multibet.grandes_ganhos.provider_display_name IS
    'Nome amigavel do vendor (ex: alea_pgsoft -> "PG Soft"). Vem do game_image_mapping.';
COMMENT ON COLUMN multibet.grandes_ganhos.game_category IS
    'Categoria nativa Pragmatic: slots | live | drawgames. Vem do game_image_mapping.';
COMMENT ON COLUMN multibet.grandes_ganhos.game_category_front IS
    'Bucket front: Live | Crash | TV Shows | Bac Bo | Baccarat | Blackjack | Fortune | Slots.';

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
