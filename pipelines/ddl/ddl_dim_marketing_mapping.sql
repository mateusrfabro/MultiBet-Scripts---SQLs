-- ═══════════════════════════════════════════════════════════════
-- multibet.dim_marketing_mapping — MIGRACAO SEGURA
-- Tabela mestra de atribuicao: mapeia affiliate/tracker -> fonte
--
-- Evolucao da arquitetura de atribuicao (19/03/2026):
-- - Schema original (v1): tracker_id, campaign_name, source, confidence, mapping_logic
-- - Schema novo (v2): + affiliate_id, source_name, partner_name, evidence_logic, is_validated
-- - Estrategia: ALTER TABLE + migrar dados (NUNCA dropar a tabela existente)
--
-- Consumidores:
--   fact_attribution.py        -> load_mapping() -> tracker_id, source
--   agg_cohort_acquisition.py  -> load_mapping() -> tracker_id, source
--   fact_player_engagement.py  -> load_mapping() -> tracker_id, source
--   vw_cohort_roi              -> JOIN m.tracker_id = a.c_tracker_id
-- ═══════════════════════════════════════════════════════════════

-- =============================================================
-- PASSO 1: Adicionar novas colunas (idempotente com IF NOT EXISTS)
-- =============================================================
ALTER TABLE multibet.dim_marketing_mapping
    ADD COLUMN IF NOT EXISTS affiliate_id     VARCHAR(50),
    ADD COLUMN IF NOT EXISTS source_name      VARCHAR(100),
    ADD COLUMN IF NOT EXISTS partner_name     VARCHAR(200),
    ADD COLUMN IF NOT EXISTS evidence_logic   TEXT,
    ADD COLUMN IF NOT EXISTS is_validated     BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS created_at       TIMESTAMPTZ DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at       TIMESTAMPTZ DEFAULT NOW();

-- =============================================================
-- PASSO 2: Migrar dados das colunas antigas para as novas
-- =============================================================

-- source_name <- source (coluna original)
UPDATE multibet.dim_marketing_mapping
SET source_name = source
WHERE source_name IS NULL AND source IS NOT NULL;

-- partner_name <- campaign_name (coluna original)
UPDATE multibet.dim_marketing_mapping
SET partner_name = campaign_name
WHERE partner_name IS NULL AND campaign_name IS NOT NULL;

-- evidence_logic <- mapping_logic (coluna original)
UPDATE multibet.dim_marketing_mapping
SET evidence_logic = mapping_logic
WHERE evidence_logic IS NULL AND mapping_logic IS NOT NULL;

-- affiliate_id <- tracker_id (default: mesmo valor, sera corrigido depois)
UPDATE multibet.dim_marketing_mapping
SET affiliate_id = tracker_id
WHERE affiliate_id IS NULL;

-- is_validated: TRUE para confidence que contem 'Official', FALSE para o resto
UPDATE multibet.dim_marketing_mapping
SET is_validated = CASE
    WHEN confidence LIKE '%Official%' THEN TRUE
    ELSE FALSE
END
WHERE is_validated IS NULL OR is_validated = FALSE;

-- Garantir NOT NULL nas novas colunas
UPDATE multibet.dim_marketing_mapping SET affiliate_id  = tracker_id WHERE affiliate_id IS NULL;
UPDATE multibet.dim_marketing_mapping SET source_name   = COALESCE(source, 'unmapped') WHERE source_name IS NULL;
UPDATE multibet.dim_marketing_mapping SET is_validated  = FALSE WHERE is_validated IS NULL;

-- =============================================================
-- PASSO 3: Indices nas novas colunas
-- =============================================================
CREATE INDEX IF NOT EXISTS idx_dmm_source_name ON multibet.dim_marketing_mapping (source_name);
CREATE INDEX IF NOT EXISTS idx_dmm_validated   ON multibet.dim_marketing_mapping (is_validated);
CREATE INDEX IF NOT EXISTS idx_dmm_aff_id      ON multibet.dim_marketing_mapping (affiliate_id);

-- =============================================================
-- PASSO 4: Comentarios atualizados
-- =============================================================
COMMENT ON TABLE multibet.dim_marketing_mapping IS
    'Tabela mestra de atribuicao de marketing (v2). '
    'Mapeia tracker_id para fonte de trafego. '
    'Colunas legado (campaign_name, source, confidence, mapping_logic) mantidas para historico. '
    'Novas colunas: affiliate_id, source_name, partner_name, evidence_logic, is_validated. '
    'IDs oficiais (is_validated=TRUE) confirmados pelo Marketing. '
    'IDs forenses (is_validated=FALSE) inferidos por click IDs. '
    'Pipeline: pipelines/dim_marketing_mapping.py';

-- =============================================================
-- NOTA: As colunas legado (campaign_name, source, confidence, mapping_logic)
-- NAO sao removidas. Ficam para historico e para que os pipelines existentes
-- que fazem SELECT tracker_id, source continuem funcionando sem alteracao.
-- =============================================================
