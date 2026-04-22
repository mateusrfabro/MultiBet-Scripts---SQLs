-- DDL: multibet.fact_casino_rounds
-- Pipeline: pipelines/fact_casino_rounds.py
-- Fonte: ps_bi.fct_casino_activity_daily + ps_bi.dim_game
-- Grao: dt x game_id
-- Dominio: Produto e Performance de Jogos (tabela 18)

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.fact_casino_rounds (
    dt                      DATE,
    game_id                 VARCHAR(50),
    game_name               VARCHAR(255),
    vendor_id               VARCHAR(50),
    sub_vendor_id           VARCHAR(50),
    game_category           VARCHAR(100),
    qty_players             INTEGER DEFAULT 0,
    total_rounds            INTEGER DEFAULT 0,
    rounds_per_player       NUMERIC(10,2) DEFAULT 0,
    turnover_real           NUMERIC(18,2) DEFAULT 0,
    wins_real               NUMERIC(18,2) DEFAULT 0,
    ggr_real                NUMERIC(18,2) DEFAULT 0,
    turnover_bonus          NUMERIC(18,2) DEFAULT 0,
    wins_bonus              NUMERIC(18,2) DEFAULT 0,
    ggr_bonus               NUMERIC(18,2) DEFAULT 0,
    turnover_total          NUMERIC(18,2) DEFAULT 0,
    wins_total              NUMERIC(18,2) DEFAULT 0,
    ggr_total               NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct           NUMERIC(10,4) DEFAULT 0,
    rtp_pct                 NUMERIC(10,4) DEFAULT 0,
    jackpot_win             NUMERIC(18,2) DEFAULT 0,
    jackpot_contribution    NUMERIC(18,2) DEFAULT 0,
    free_spins_bet          NUMERIC(18,2) DEFAULT 0,
    free_spins_win          NUMERIC(18,2) DEFAULT 0,
    provider_display_name   VARCHAR(50),                 -- v4.2: enriquecido via game_image_mapping
    game_category_front     VARCHAR(20),                 -- v4.2: bucket do front (Fortune/Crash/Live/...)
    refreshed_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, game_id)
);

-- v4.2 idempotente: garante que instalacoes anteriores ganhem as 2 colunas
ALTER TABLE multibet.fact_casino_rounds
    ADD COLUMN IF NOT EXISTS provider_display_name VARCHAR(50),
    ADD COLUMN IF NOT EXISTS game_category_front   VARCHAR(20);

CREATE INDEX IF NOT EXISTS idx_fcr_vendor ON multibet.fact_casino_rounds (vendor_id, dt);
CREATE INDEX IF NOT EXISTS idx_fcr_ggr ON multibet.fact_casino_rounds (dt, ggr_total DESC);
CREATE INDEX IF NOT EXISTS idx_fcr_category ON multibet.fact_casino_rounds (game_category, dt);
