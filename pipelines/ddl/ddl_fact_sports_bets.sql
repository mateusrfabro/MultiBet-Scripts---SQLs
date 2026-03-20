-- DDL: multibet.fact_sports_bets + multibet.fact_sports_open_bets
-- Pipeline: pipelines/fact_sports_bets.py
-- Fonte: vendor_ec2.tbl_sports_book_bets_info + bet_details
-- Grao: dt x sport_name
-- Dominio: Produto e Performance de Jogos (tabela 19)

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.fact_sports_bets (
    dt                  DATE,
    sport_name          VARCHAR(255),
    qty_bets            INTEGER DEFAULT 0,
    qty_players         INTEGER DEFAULT 0,
    turnover            NUMERIC(18,2) DEFAULT 0,
    total_return        NUMERIC(18,2) DEFAULT 0,
    ggr                 NUMERIC(18,2) DEFAULT 0,
    margin_pct          NUMERIC(10,4) DEFAULT 0,
    avg_ticket          NUMERIC(18,2) DEFAULT 0,
    avg_odds            NUMERIC(10,4) DEFAULT 0,
    qty_pre_match       INTEGER DEFAULT 0,
    qty_live            INTEGER DEFAULT 0,
    turnover_pre_match  NUMERIC(18,2) DEFAULT 0,
    turnover_live       NUMERIC(18,2) DEFAULT 0,
    pct_pre_match       NUMERIC(10,4) DEFAULT 0,
    pct_live            NUMERIC(10,4) DEFAULT 0,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, sport_name)
);

CREATE TABLE IF NOT EXISTS multibet.fact_sports_open_bets (
    snapshot_dt         DATE,
    sport_name          VARCHAR(255),
    qty_open_bets       INTEGER DEFAULT 0,
    total_stake_open    NUMERIC(18,2) DEFAULT 0,
    avg_odds_open       NUMERIC(10,4) DEFAULT 0,
    projected_liability NUMERIC(18,2) DEFAULT 0,
    projected_ggr       NUMERIC(18,2) DEFAULT 0,
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (snapshot_dt, sport_name)
);

CREATE INDEX IF NOT EXISTS idx_fsb_ggr ON multibet.fact_sports_bets (dt, ggr DESC);
