# Indice de Documentacao — MultiBet

Mapa de navegacao dos documentos em `docs/`. Organizado por tema.
Complementa `memory/INDEX_schemas.md` (que referencia tanto docs versionados como auto-memory).

---

## Guias operacionais de banco

| Arquivo | Cobertura | Data |
|---|---|---|
| [athena_pragmatic_guide.md](athena_pragmatic_guide.md) | Guia completo Athena/Pragmatic: schemas, regras SQL, receitas, 51 colunas fund validadas | 31/03/2026 |
| [bigquery_smartico_guide.md](bigquery_smartico_guide.md) | Guia BigQuery/Smartico (102 views) — **banco desativado em 19/04** | 31/03/2026 |
| [supernova_bet_guide.md](supernova_bet_guide.md) | Guia Super Nova Bet / Play4Tune (68 tabelas, dados granulares) | 24/04/2026 |
| [mapeamento_bancos_athena_bigquery.md](mapeamento_bancos_athena_bigquery.md) | Mapeamento cruzado Athena x BigQuery (referencia historica) | 20/04/2026 |

## Schemas canonicos

| Arquivo | Cobertura | Data |
|---|---|---|
| [schema_multibet_database_v2.0.md](schema_multibet_database_v2.0.md) | Super Nova DB (PostgreSQL) — schema canonico 2.168 linhas | 20/04/2026 |
| [schema_multibet_database_v1.0.md](schema_multibet_database_v1.0.md) | Super Nova DB v1.0 (historico) | - |
| [schema_play4_supernova.md](schema_play4_supernova.md) | Play4Tune — DDL 9 foreign tables | 20/04/2026 |
| [_migration/schema_bronze_multibet_v1.0.md](_migration/schema_bronze_multibet_v1.0.md) | Bronze (camada replicacao) | - |
| [inventario_schema_multibet.md](inventario_schema_multibet.md) | Inventario completo schema multibet | 19/04/2026 |

## Validacoes empiricas (bronze)

| Arquivo | Cobertura |
|---|---|
| [bronze_correcoes_mauro_v1.md](bronze_correcoes_mauro_v1.md) | 69 correcoes empiricas validadas pelo Mauro (20/03/2026) |
| [bronze_selects_kpis_CORRIGIDO_v2.md](bronze_selects_kpis_CORRIGIDO_v2.md) | Queries validadas por KPI |
| [bronze_selects_produto_performance_jogos.md](bronze_selects_produto_performance_jogos.md) | Queries performance jogos |
| [changelog_bronze_v1_vs_v2.md](changelog_bronze_v1_vs_v2.md) | Mudancas entre versoes |
| [diagnostico_bronze_v2.md](diagnostico_bronze_v2.md) | Diagnostico bronze v2 |

## CRM / Reports / Dashboards

| Arquivo | Cobertura |
|---|---|
| [handoff_crm_dashboard_v1.md](handoff_crm_dashboard_v1.md) | Handoff dashboard CRM |
| [documentacao_dashboard_crm_v0.md](documentacao_dashboard_crm_v0.md) | Documentacao dashboard CRM v0 |
| [roteiro_apresentacao_crm_v0.md](roteiro_apresentacao_crm_v0.md) | Roteiro apresentacao CRM |
| [draft_report_crm_diario.md](draft_report_crm_diario.md) | Draft report CRM diario |
| [racional_report_crm_promocoes.md](racional_report_crm_promocoes.md) | Racional report promocoes |
| [report_crm_performance_entrega.md](report_crm_performance_entrega.md) | Report performance CRM |
| [validacao_smartico_push_abr2026.md](validacao_smartico_push_abr2026.md) | Validacao push Smartico abril/2026 |

## Aquisicao / Trafego / Affiliates

| Arquivo | Cobertura |
|---|---|
| [handoff_aquisicao_trafego.md](handoff_aquisicao_trafego.md) | Handoff pipeline aquisicao/trafego |
| [handoff_etl_retention_weekly.md](handoff_etl_retention_weekly.md) | Handoff ETL retention semanal |
| [resumo_dim_marketing_mapping_v2.md](resumo_dim_marketing_mapping_v2.md) | dim_marketing_mapping v2 |

## Risco / Anti-fraude

| Arquivo | Cobertura |
|---|---|
| [notion_risk_matrix_v2.md](notion_risk_matrix_v2.md) | Matriz de risco v2 (privada) |
| [notion_risk_matrix_PUBLICO.md](notion_risk_matrix_PUBLICO.md) | Matriz de risco v2 (publica) |
| [notion_risk_matrix_v2_IMPORT.md](notion_risk_matrix_v2_IMPORT.md) | Versao para importar no Notion |
| [casos_fraude_observados_multibet.md](casos_fraude_observados_multibet.md) | Casos observados de fraude |
| [auditoria_sql_matriz_risco_20260420.md](auditoria_sql_matriz_risco_20260420.md) | Auditoria SQL matriz risco (20/04) |

## PCR (Player Credit Rating)

| Arquivo | Cobertura |
|---|---|
| [pcr_player_credit_rating.md](pcr_player_credit_rating.md) | Proposta PCR rating D-AAA |
| [proposta_pcr_rating_new_20260420.md](proposta_pcr_rating_new_20260420.md) | Proposta PCR nova (20/04) |
| [notion_pcr_v1.md](notion_pcr_v1.md) | PCR v1 (privado) |
| [notion_pcr_PUBLICO.md](notion_pcr_PUBLICO.md) | PCR v1 (publico) |
| [auditoria_sql_pcr_20260420.md](auditoria_sql_pcr_20260420.md) | Auditoria SQL PCR (20/04) |
| [testes_pcr_migration_20260421/README.md](testes_pcr_migration_20260421/README.md) | Testes migracao PCR (21/04) |

## Views Casino / Sportsbook

| Arquivo | Cobertura |
|---|---|
| [handoff_views_casino_sportsbook.md](handoff_views_casino_sportsbook.md) | Handoff 7 views gold (08/04) |
| [handoff_views_casino_sportsbook_v4.md](handoff_views_casino_sportsbook_v4.md) | Versao v4 |
| [deep_dive_live_delay_764_777.md](deep_dive_live_delay_764_777.md) | Deep dive live delay |
| [procedimento_teste_manual_altenar.md](procedimento_teste_manual_altenar.md) | Teste manual Altenar |

## Bot analytics e segmentacao

| Arquivo | Cobertura |
|---|---|
| [arquitetura_bot_analytics_mvp.md](arquitetura_bot_analytics_mvp.md) | Arquitetura MVP bot analytics |
| [catalogo_intents_bot_analytics.md](catalogo_intents_bot_analytics.md) | Catalogo de intents |
| [feedback_segmentacao_castrin.md](feedback_segmentacao_castrin.md) | Feedback segmentacao (Castrin) |

## Outros guias

| Arquivo | Cobertura |
|---|---|
| [guia_supernova_dashboard_usuario_FINAL.md](guia_supernova_dashboard_usuario_FINAL.md) | Guia usuario dashboard Super Nova |
| [multiverso_analise_tecnica.md](multiverso_analise_tecnica.md) | Analise tecnica Multiverso |
| [README_auditoria_sql_20260420.md](README_auditoria_sql_20260420.md) | README auditoria SQL (20/04) |

## Legado / migracao

Arquivos em [_migration/](_migration/) sao referencias historicas da migracao anterior. Consultar apenas para contexto.

---

**Como usar este indice:**
1. Ao receber uma task, identificar o tema (banco / metrica / entrega / risco / CRM etc.)
2. Verificar tambem `memory/INDEX_schemas.md` se for sobre estrutura de banco
3. Arquivos com sufixo `_FINAL` ou versao mais alta sao os vigentes
