-- DDL: multibet.fact_live_casino
-- Pipeline: pipelines/fact_live_casino.py
-- Fonte: ps_bi.fct_casino_activity_daily + tbl_ecr_gaming_sessions
-- Grao: dt x game_id (apenas jogos live)
-- Dominio: Produto e Performance de Jogos (tabela 20)

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.fact_live_casino (
    dt                          DATE,
    game_id                     VARCHAR(50),
    game_name                   VARCHAR(255),
    vendor_id                   VARCHAR(50),
    game_category_desc          VARCHAR(100),
    qty_players                 INTEGER DEFAULT 0,
    total_rounds                INTEGER DEFAULT 0,
    turnover_total              NUMERIC(18,2) DEFAULT 0,
    wins_total                  NUMERIC(18,2) DEFAULT 0,
    ggr_total                   NUMERIC(18,2) DEFAULT 0,
    hold_rate_pct               NUMERIC(10,4) DEFAULT 0,
    rtp_pct                     NUMERIC(10,4) DEFAULT 0,
    qty_sessions                INTEGER DEFAULT 0,
    avg_session_duration_sec    NUMERIC(10,2) DEFAULT 0,
    avg_rounds_per_session      NUMERIC(10,2) DEFAULT 0,
    max_concurrent_players      INTEGER DEFAULT 0,
    refreshed_at                TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, game_id)
);

CREATE INDEX IF NOT EXISTS idx_flc_ggr ON multibet.fact_live_casino (dt, ggr_total DESC);
