-- DDL: multibet.fact_registrations
-- Origem: Athena ecr_ec2.tbl_ecr
-- Domínio 2 — KPIs: Regs, Trackers, UTMs (se disponíveis)
-- Criado: 2026-03-18

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.fact_registrations (
    id                  SERIAL PRIMARY KEY,

    -- Player
    c_ecr_id            BIGINT NOT NULL,
    c_external_id       BIGINT,
    c_tracker_id        VARCHAR(255),
    c_country_code      VARCHAR(50),

    -- Datas
    registration_date   DATE NOT NULL,          -- c_signup_time convertido para BRT e truncado
    registration_time   TIMESTAMPTZ NOT NULL,   -- c_signup_time original (UTC)

    -- Partição lógica (para queries otimizadas)
    dt                  DATE NOT NULL,          -- = registration_date

    -- Controle
    refreshed_at        TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para queries frequentes
CREATE INDEX IF NOT EXISTS idx_fr_dt ON multibet.fact_registrations (dt);
CREATE INDEX IF NOT EXISTS idx_fr_ecr_id ON multibet.fact_registrations (c_ecr_id);
CREATE INDEX IF NOT EXISTS idx_fr_tracker ON multibet.fact_registrations (c_tracker_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_fr_ecr_unique ON multibet.fact_registrations (c_ecr_id);
