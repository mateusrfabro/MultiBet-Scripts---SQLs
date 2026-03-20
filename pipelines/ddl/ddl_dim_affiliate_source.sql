-- ═══════════════════════════════════════════════════════════════
-- multibet.dim_affiliate_source
-- Tabela dimensao: mapeamento affiliate_id -> fonte de trafego
-- Alimentada via pipeline Python (Athena -> Super Nova DB)
--
-- Validado com arquiteto em 18/03/2026
-- Apenas campos de MAPEAMENTO — metricas ficam nas tabelas fato
-- ═══════════════════════════════════════════════════════════════

-- Dropar tabela antiga (tinha metricas e sinais que nao pertencem a dimensao)
DROP TABLE IF EXISTS multibet.dim_affiliate_source;

CREATE TABLE multibet.dim_affiliate_source (
    affiliate_id        VARCHAR(50)     NOT NULL,
    affiliate_name      VARCHAR(200),
    source_id           VARCHAR(200),
    fonte_trafego       VARCHAR(50)     NOT NULL DEFAULT 'Direct/Organic',
    utm_source          VARCHAR(200),
    utm_medium          VARCHAR(200),
    utm_campaign        VARCHAR(200),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW(),

    PRIMARY KEY (affiliate_id)
);

CREATE INDEX IF NOT EXISTS idx_das_fonte
    ON multibet.dim_affiliate_source (fonte_trafego);

COMMENT ON TABLE multibet.dim_affiliate_source IS
    'DE-PARA de affiliate_id para fonte de trafego. '
    'Classificacao inferida por click IDs (gclid, fbclid, ttclid) '
    'da ecr_ec2.tbl_ecr_banner. Atualizada por pipelines/de_para_affiliates.py. '
    'Pendente: replicacao das tabelas master da Pragmatic para classificacao oficial.';
