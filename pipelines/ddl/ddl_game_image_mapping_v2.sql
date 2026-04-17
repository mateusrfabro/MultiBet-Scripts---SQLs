-- ============================================================
-- DDL v2: multibet.game_image_mapping (enriquecido)
-- Banco: Super Nova DB (PostgreSQL)
-- Data: 2026-04-17
-- Demanda: CTO Gabriel Barbosa (via Castrin) — views do front
--
-- Adiciona 11 colunas ao schema original v1 para suportar:
--   - Categorizacao Casino vs Live vs DrawGames
--   - Subtipo Live normalizado (Roleta/Blackjack/Baccarat/GameShow/Outros)
--   - Vendor + sub-vendor (filtros tipo "Jogos Pragmatic")
--   - Flag has_jackpot (carrossel "Jackpots")
--   - Flag is_active (status do catalogo)
--   - Ranking de popularidade nas ultimas 24h rolantes
-- ============================================================

CREATE SCHEMA IF NOT EXISTS multibet;

-- v1 ja existe, so adiciona colunas (idempotente via IF NOT EXISTS)
ALTER TABLE multibet.game_image_mapping
    ADD COLUMN IF NOT EXISTS product_id            VARCHAR(20),
    ADD COLUMN IF NOT EXISTS sub_vendor_id         VARCHAR(50),
    ADD COLUMN IF NOT EXISTS game_category         VARCHAR(30),
    ADD COLUMN IF NOT EXISTS game_category_desc    VARCHAR(50),
    ADD COLUMN IF NOT EXISTS game_type_desc        VARCHAR(100),
    ADD COLUMN IF NOT EXISTS live_subtype          VARCHAR(30),
    ADD COLUMN IF NOT EXISTS has_jackpot           BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_active             BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS rounds_24h            BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS players_24h           INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS popularity_rank_24h   INTEGER,
    ADD COLUMN IF NOT EXISTS popularity_window_end TIMESTAMPTZ;

-- Indices para as queries do front (filtros mais usados)
CREATE INDEX IF NOT EXISTS idx_gim_category
    ON multibet.game_image_mapping (game_category, is_active);

CREATE INDEX IF NOT EXISTS idx_gim_vendor_active
    ON multibet.game_image_mapping (vendor_id, is_active);

CREATE INDEX IF NOT EXISTS idx_gim_rank_24h
    ON multibet.game_image_mapping (popularity_rank_24h)
    WHERE popularity_rank_24h IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gim_live_subtype
    ON multibet.game_image_mapping (live_subtype)
    WHERE live_subtype IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gim_jackpot
    ON multibet.game_image_mapping (has_jackpot)
    WHERE has_jackpot = TRUE;

-- ============================================================
-- LEGENDA DE COLUNAS
-- ============================================================
COMMENT ON COLUMN multibet.game_image_mapping.product_id            IS 'CASINO | SPORTS_BOOK';
COMMENT ON COLUMN multibet.game_image_mapping.sub_vendor_id         IS 'pgsoft, betsoft, etc (granularidade abaixo de vendor_id)';
COMMENT ON COLUMN multibet.game_image_mapping.game_category         IS 'Categoria do catalogo Pragmatic: slots | live | (drawgames)';
COMMENT ON COLUMN multibet.game_image_mapping.game_category_desc    IS 'ClassicSlots | VideoSlots | LiveDealer | DrawGames';
COMMENT ON COLUMN multibet.game_image_mapping.game_type_desc        IS 'Tipo bruto do provedor (ex: European Roulette, Speed Blackjack 1)';
COMMENT ON COLUMN multibet.game_image_mapping.live_subtype          IS 'Normalizado para front: Roleta | Blackjack | Baccarat | GameShow | Outros';
COMMENT ON COLUMN multibet.game_image_mapping.has_jackpot           IS 'Jogo possui jackpot ativo';
COMMENT ON COLUMN multibet.game_image_mapping.is_active             IS 'Status do catalogo (c_status = active)';
COMMENT ON COLUMN multibet.game_image_mapping.rounds_24h            IS 'Total de rodadas (bets) nas ULTIMAS 24h ROLANTES (a partir do refresh)';
COMMENT ON COLUMN multibet.game_image_mapping.players_24h           IS 'Jogadores unicos nas ultimas 24h rolantes';
COMMENT ON COLUMN multibet.game_image_mapping.popularity_rank_24h   IS 'Ranking 1=mais jogado nas ultimas 24h rolantes (NULL = sem atividade)';
COMMENT ON COLUMN multibet.game_image_mapping.popularity_window_end IS 'Timestamp UTC do fim da janela 24h (= momento do refresh)';
