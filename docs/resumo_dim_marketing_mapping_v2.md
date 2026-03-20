# dim_marketing_mapping v2 — Resumo para Arquiteto

**Data:** 19/03/2026
**Autor:** Mateus F. (Squad 3 — Intelligence Engine)
**Destino:** Super Nova DB → `multibet.dim_marketing_mapping`

---

## Contexto

A `dim_marketing_mapping` eh a tabela mestra de atribuicao de marketing do MultiBet.
Ela mapeia `tracker_id` (e agora `affiliate_id`) para a fonte de trafego (`source_name`)
e eh consumida por 4+ pipelines:

- `fact_attribution.py` — distribuicao de spend por source
- `agg_cohort_acquisition.py` — cohort analysis por canal
- `fact_player_engagement_daily.py` — engagement por canal
- `vw_cohort_roi` (view) — ROI por cohort

## O que existia (v1)

Tabela com 30 registros mapeados manualmente + forense, schema:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| tracker_id | VARCHAR(255) PK | Identificador do tracker ou affiliate |
| campaign_name | VARCHAR(200) | Nome descritivo do canal |
| source | VARCHAR(100) | Classificacao (google_ads, meta_ads, etc.) |
| confidence | VARCHAR(50) | Nivel de confianca (High, Medium, Low) |
| mapping_logic | TEXT | Logica/evidencia do mapeamento |

### Categorias mapeadas (v1)
- **google_ads** — 468114, 297657, 445431, google_ads, google
- **meta_ads** — 464673, 467185, 53194, fb, affbrgeov
- **organic** — 0, sem_tracker
- **instagram** — ig
- **tiktok_kwai** — qxvideo
- **portais_midia** — 449235, gazeta-tp-boca, lance, siapesbr
- **influencers** — 522633, MECAAP
- **affiliate_performance** — 469069, 488468, 476724, 452351, 452808, 489203, 502638
- **affiliate_unknown** — 474045, 509759, 522402

## O que foi feito (v2)

### Estrategia: ALTER TABLE (migracao segura)

**NAO fizemos DROP TABLE.** Apenas adicionamos colunas novas e migramos dados:

| Coluna nova | Tipo | Origem (migrada de) |
|-------------|------|---------------------|
| affiliate_id | VARCHAR(50) | ← tracker_id (default) |
| source_name | VARCHAR(100) | ← source |
| partner_name | VARCHAR(200) | ← campaign_name |
| evidence_logic | TEXT | ← mapping_logic |
| is_validated | BOOLEAN | ← confidence LIKE '%Official%' |
| created_at | TIMESTAMPTZ | NOW() |
| updated_at | TIMESTAMPTZ | NOW() |

As colunas legado (`campaign_name`, `source`, `confidence`, `mapping_logic`) **NAO foram removidas** — pipelines existentes que fazem `SELECT tracker_id, source` continuam funcionando sem alteracao.

### Carga automatica de IDs forenses

O pipeline `dim_marketing_mapping.py` agora roda uma query no Athena (`ecr_ec2.tbl_ecr_banner`) que:

1. Agrupa registros por `c_affiliate_id` + `c_tracker_id`
2. Conta sinais de click ID (gclid, fbclid, ttclid, kwai) nas URLs
3. Classifica a fonte pelo sinal predominante
4. Insere apenas os que **NAO existem** na tabela (`ON CONFLICT DO NOTHING`)

### Relatorio Unmapped

O pipeline gera automaticamente um Excel com:
- **Resumo** — metricas de cobertura (% GGR sem atribuicao)
- **Unmapped por GGR** — todos os trackers sem mapeamento, ordenados por GGR desc
- **Top 20 Prioridade** — os 20 de maior impacto financeiro
- **Mapeamento Atual** — snapshot da tabela para referencia

Saida: `output/unmapped_trackers_para_marketing_YYYY-MM-DD.xlsx`

## Fluxo de uso

```
1. Rodar pipeline:
   python pipelines/dim_marketing_mapping.py

2. Pipeline faz:
   ALTER TABLE (add colunas v2)
   → Migra dados legados
   → Descobre novos IDs forenses no Athena
   → Insere novos (ON CONFLICT DO NOTHING)
   → Gera Excel de unmapped

3. Enviar Excel ao Marketing para validacao

4. Marketing retorna com classificacao dos unmapped

5. Inserir manualmente no banco (ou via pipeline futuro)
```

## Retrocompatibilidade

| Pipeline | Interface usada | Status |
|----------|-----------------|--------|
| fact_attribution.py | `SELECT tracker_id, source` | OK, coluna `source` mantida |
| agg_cohort_acquisition.py | `SELECT tracker_id, source` | OK |
| fact_player_engagement.py | `SELECT tracker_id, source` | OK |
| vw_cohort_roi (view) | `JOIN m.tracker_id` | OK |
| de_para_affiliates.py | Tabela separada (dim_affiliate_source) | Independente |

## Riscos e pendencias

1. **PK continua em `tracker_id` (single column)** — nao alteramos para PK composta (affiliate_id, tracker_id) para nao quebrar FKs existentes. Quando o Marketing fornecer o DE-PARA completo com tracker_id separado de affiliate_id, podemos migrar.

2. **Afiliados sem click ID** — trackers que usam tracking proprietario (sem gclid/fbclid) nao sao detectaveis automaticamente. Dependem de input do Marketing.

3. **Tabelas master nao replicadas** — `tbl_affiliate_mst_config` e `tbl_affiliate_tracker_mapping` da Pragmatic Solutions NAO estao no Data Lake. Se fossem replicadas, teriamos mapeamento oficial completo.

## Arquivos

| Arquivo | Proposito |
|---------|-----------|
| `pipelines/ddl_dim_marketing_mapping.sql` | DDL de migracao (ALTER TABLE) |
| `pipelines/dim_marketing_mapping.py` | Pipeline completo (migracao + forense + relatorio) |
| `output/unmapped_trackers_para_marketing_*.xlsx` | Relatorio para Marketing |
