-- =============================================================================
-- DDL: multibet.fact_crm_daily_performance
-- =============================================================================
-- Tabela fato para performance diaria de campanhas CRM.
-- Usa colunas JSONB para armazenar blocos de metricas (funil, financeiro,
-- comparativo) sem inchar o schema a cada nova metrica.
--
-- Grao: 1 linha por campanha + periodo (BEFORE / DURING / AFTER)
-- Estrategia: UPSERT diario — DURING acumula conforme campanha avanca.
--
-- Destino: supernova_db.multibet
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS multibet;

CREATE TABLE IF NOT EXISTS multibet.fact_crm_daily_performance (
    id                  SERIAL PRIMARY KEY,

    -- Identificacao da campanha
    campanha_id         VARCHAR(100)  NOT NULL,   -- ex: 'RETEM_2026_02', 'MULTIVERSO_2026_03'
    campanha_name       VARCHAR(255),             -- ex: 'Campanha RETEM Fevereiro'
    campanha_start      DATE          NOT NULL,
    campanha_end        DATE          NOT NULL,

    -- Periodo de analise
    period              VARCHAR(10)   NOT NULL,   -- 'BEFORE', 'DURING', 'AFTER'
    period_start        DATE          NOT NULL,   -- inicio efetivo do periodo
    period_end          DATE          NOT NULL,   -- fim efetivo do periodo (dinamico p/ DURING)

    -- Metricas em blocos JSONB
    -- -----------------------------------------------------------------------
    -- funil: metricas do funil CRM (comunicacao e engajamento)
    -- Exemplo:
    --   {
    --     "comunicacoes_enviadas": 15000,
    --     "comunicacoes_entregues": 14200,
    --     "comunicacoes_abertas": 8500,
    --     "comunicacoes_clicadas": 3200,
    --     "depositos_pos_click": 1800,
    --     "canais": {"whatsapp": 5000, "sms": 4000, "push": 6000}
    --   }
    funil               JSONB         DEFAULT '{}'::JSONB,

    -- financeiro: metricas financeiras (GGR, BTR, NGR, depositos)
    -- Exemplo:
    --   {
    --     "total_users": 12500,
    --     "depositos_brl": 850000.00,
    --     "depositos_qtd": 25000,
    --     "ggr_brl": 320000.00,
    --     "btr_brl": 45000.00,
    --     "rca_brl": 12000.00,
    --     "ngr_brl": 263000.00,
    --     "avg_play_days": 3.45,
    --     "total_sessions": 95000
    --   }
    financeiro          JSONB         DEFAULT '{}'::JSONB,

    -- comparativo: deltas, custos e ROI
    -- Exemplo:
    --   {
    --     "ngr_incremental": 50000.00,
    --     "ngr_variacao_pct": 23.5,
    --     "custo_whatsapp": 2400.00,
    --     "custo_sms": 1080.00,
    --     "custo_push": 360.00,
    --     "custo_total": 3840.00,
    --     "roi": 13.02
    --   }
    comparativo         JSONB         DEFAULT '{}'::JSONB,

    -- Controle
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   DEFAULT NOW(),

    -- Constraint de unicidade: 1 linha por campanha + periodo
    CONSTRAINT uq_campanha_period UNIQUE (campanha_id, period)
);

-- Indices para consultas frequentes
CREATE INDEX IF NOT EXISTS idx_fact_crm_campanha
    ON multibet.fact_crm_daily_performance (campanha_id);

CREATE INDEX IF NOT EXISTS idx_fact_crm_period
    ON multibet.fact_crm_daily_performance (campanha_id, period);

-- Indice GIN para queries dentro dos JSONBs (ex: WHERE financeiro->>'ngr_brl' > ...)
CREATE INDEX IF NOT EXISTS idx_fact_crm_financeiro_gin
    ON multibet.fact_crm_daily_performance USING GIN (financeiro);

CREATE INDEX IF NOT EXISTS idx_fact_crm_comparativo_gin
    ON multibet.fact_crm_daily_performance USING GIN (comparativo);