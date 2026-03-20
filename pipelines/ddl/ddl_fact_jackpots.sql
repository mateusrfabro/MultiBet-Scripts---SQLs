-- DDL: multibet.fact_jackpots
-- Pipeline: pipelines/fact_jackpots.py
-- Fonte: ps_bi.fct_casino_activity_daily (colunas jackpot)
-- Grao: month_start x game_id
-- Dominio: Produto e Performance de Jogos (tabela 23)

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.fact_jackpots (
    month_start         DATE,
    game_id             VARCHAR(50),
    game_name           VARCHAR(255),
    vendor_id           VARCHAR(50),
    jackpots_count      INTEGER DEFAULT 0,
    jackpot_total_paid  NUMERIC(18,2) DEFAULT 0,
    avg_jackpot_value   NUMERIC(18,2) DEFAULT 0,
    max_jackpot_value   NUMERIC(18,2) DEFAULT 0,
    contribution_total  NUMERIC(18,2) DEFAULT 0,
    ggr_total           NUMERIC(18,2) DEFAULT 0,
    jackpot_impact_pct  NUMERIC(10,4) DEFAULT 0,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (month_start, game_id)
);

CREATE INDEX IF NOT EXISTS idx_fj_month ON multibet.fact_jackpots (month_start);
CREATE INDEX IF NOT EXISTS idx_fj_impact ON multibet.fact_jackpots (month_start, jackpot_impact_pct DESC);
