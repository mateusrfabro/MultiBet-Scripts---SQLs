-- DDL: multibet.agg_game_performance
-- Pipeline: pipelines/agg_game_performance.py
-- Fonte: ps_bi.fct_casino_activity_daily (agregado semanal)
-- Grao: week_start x game_id
-- Dominio: Produto e Performance de Jogos (tabela 22)

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.agg_game_performance (
    week_start          DATE,
    game_id             VARCHAR(50),
    game_name           VARCHAR(255),
    vendor_id           VARCHAR(50),
    game_category       VARCHAR(100),
    qty_active_days     INTEGER DEFAULT 0,
    dau_avg             NUMERIC(10,2) DEFAULT 0,
    total_players       INTEGER DEFAULT 0,
    total_rounds        INTEGER DEFAULT 0,
    turnover            NUMERIC(18,2) DEFAULT 0,
    ggr                 NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct       NUMERIC(10,4) DEFAULT 0,
    ggr_rank            INTEGER,
    concentration_pct   NUMERIC(10,4) DEFAULT 0,
    first_activity_date DATE,
    is_new_game         BOOLEAN DEFAULT FALSE,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (week_start, game_id)
);

CREATE INDEX IF NOT EXISTS idx_agp_rank ON multibet.agg_game_performance (week_start, ggr_rank);
