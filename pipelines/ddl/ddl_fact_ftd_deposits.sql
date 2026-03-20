-- DDL: multibet.fact_ftd_deposits
-- Origem: Athena cashier_ec2.tbl_cashier_deposit
-- Domínio 2 — KPIs: Qtd FTD, Valor FTD, Atribuição básica
-- Criado: 2026-03-18

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.fact_ftd_deposits (
    id                  SERIAL PRIMARY KEY,

    -- Player
    c_ecr_id            BIGINT NOT NULL,

    -- Transação FTD
    ftd_txn_id          BIGINT NOT NULL,        -- c_txn_id do primeiro depósito
    ftd_amount          NUMERIC(15, 2) NOT NULL, -- valor em BRL (centavos / 100)

    -- Datas
    ftd_date            DATE NOT NULL,           -- data do FTD em BRT
    ftd_time            TIMESTAMPTZ NOT NULL,    -- timestamp original (UTC)

    -- Partição lógica
    dt                  DATE NOT NULL,           -- = ftd_date

    -- Controle
    refreshed_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_fftd_dt ON multibet.fact_ftd_deposits (dt);
CREATE INDEX IF NOT EXISTS idx_fftd_ecr_id ON multibet.fact_ftd_deposits (c_ecr_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fftd_ecr_unique ON multibet.fact_ftd_deposits (c_ecr_id);
