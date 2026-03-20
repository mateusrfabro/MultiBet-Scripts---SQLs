-- View: multibet.agg_financial_monthly
-- Domínio 1 (P2) — Visão Mensal consolidada
-- Depende de: multibet.fact_ftd_deposits
-- Criado: 2026-03-18

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE OR REPLACE VIEW multibet.agg_financial_monthly AS
SELECT
    TO_CHAR(dt, 'YYYY-MM')                          AS month_reference,
    COUNT(DISTINCT c_ecr_id)                         AS total_ftds,
    TO_CHAR(SUM(ftd_amount), 'FM999,999,999.00')     AS ftd_volume_brl
FROM multibet.fact_ftd_deposits
GROUP BY TO_CHAR(dt, 'YYYY-MM')
ORDER BY month_reference DESC;
