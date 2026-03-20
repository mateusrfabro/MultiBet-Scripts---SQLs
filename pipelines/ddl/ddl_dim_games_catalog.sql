-- DDL: multibet.dim_games_catalog
-- Pipeline: pipelines/dim_games_catalog.py
-- Fonte: ps_bi.dim_game + vendor_ec2.tbl_vendor_games_mapping_mst
-- Grao: game_id (snapshot)
-- Dominio: Produto e Performance de Jogos (tabela 21)

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.dim_games_catalog (
    game_id                 VARCHAR(50) PRIMARY KEY,
    game_name               VARCHAR(255),
    vendor_id               VARCHAR(50),
    sub_vendor_id           VARCHAR(50),
    product_id              VARCHAR(30),
    game_category           VARCHAR(100),
    game_category_desc      VARCHAR(100),
    game_type_id            INTEGER,
    game_type_desc          VARCHAR(255),
    status                  VARCHAR(30),
    game_technology         VARCHAR(30),
    has_jackpot             BOOLEAN DEFAULT FALSE,
    free_spin_game          BOOLEAN DEFAULT FALSE,
    feature_trigger_game    BOOLEAN DEFAULT FALSE,
    snapshot_dt             DATE DEFAULT CURRENT_DATE,
    refreshed_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dgc_vendor ON multibet.dim_games_catalog (vendor_id);
CREATE INDEX IF NOT EXISTS idx_dgc_category ON multibet.dim_games_catalog (game_category);
CREATE INDEX IF NOT EXISTS idx_dgc_status ON multibet.dim_games_catalog (status);
