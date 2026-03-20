# MultiBet Analytics

Repositorio de pipelines, analises e ferramentas de dados para a operacao **MultiBet** — gerenciado pelo squad de Intelligence Engine da Super Nova Gaming.

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Data Lake | AWS Athena (Iceberg) — databases `*_ec2`, `ps_bi`, `silver` |
| CRM | BigQuery (Smartico) |
| Persistencia | Super Nova DB (PostgreSQL via SSH tunnel) |
| Linguagens | Python 3.12, SQL (Presto/Trino) |
| Dashboards | Flask + HTML/CSS |
| Versionamento | GitHub |

## Estrutura do Repositorio

```
multibet-analytics/
|
|-- db/                          # Conectores de banco de dados
|   |-- athena.py                #   AWS Athena (Iceberg Data Lake)
|   |-- bigquery.py              #   Google BigQuery (Smartico CRM)
|   |-- supernova.py             #   Super Nova DB (PostgreSQL + SSH)
|   +-- redshift.py              #   Redshift (legado, descontinuado)
|
|-- pipelines/                   # Pipelines de producao
|   |-- ddl/                     #   DDLs das tabelas no Super Nova DB
|   |-- fact_*.py                #   Tabelas fato (transacoes, bets, depositos)
|   |-- dim_*.py                 #   Tabelas dimensao (jogos, marketing)
|   |-- agg_*.py                 #   Agregacoes (cohort, game performance)
|   |-- anti_abuse_multiverso.py #   Deteccao de abuso em campanhas CRM
|   |-- crm_daily_performance.py #   KPIs diarios CRM
|   +-- grandes_ganhos.py        #   Maiores ganhos do dia
|
|-- segmentacao/                 # Segmentacoes CRM por jogo/campanha
|   |-- app/                     #   App Flask de segmentacao automatizada
|   |-- fortune_tiger/           #   Segmentacao Fortune Tiger
|   |-- fortune_ox/              #   Segmentacao Fortune Ox
|   |-- fortune_rabbit/          #   Segmentacao Fortune Rabbit
|   |-- gates_of_olympus/        #   Segmentacao Gates of Olympus
|   |-- sweet_bonanza/           #   Segmentacao Sweet Bonanza
|   |-- sugar_rush/              #   Segmentacao Sugar Rush
|   +-- ratinho_sortudo_fat_panda/ # Segmentacao Ratinho Sortudo
|
|-- scripts/                     # Ferramentas e utilitarios
|   |-- validar_bronze_*.py      #   Validacao de schemas bronze
|   |-- athena_schema_discovery.py # Discovery de schemas Athena
|   |-- md_to_pdf.py             #   Conversao Markdown para PDF
|   +-- update_excel_*.py        #   Atualizacao de planilhas
|
|-- analysis/                    # Analises ad-hoc
|   +-- campanhas_retem_*.py     #   Analise campanhas RETEM
|
|-- validacoes/                  # Validacoes de dados e status
|   +-- validar_status_conta.py  #   Validacao status conta (Athena vs Smartico)
|
|-- docs/                        # Documentacao tecnica
|   |-- schema_multibet_*.md     #   Schema completo do banco
|   |-- mapeamento_bancos_*.md   #   Mapeamento Athena/BigQuery
|   |-- bronze_selects_*.md      #   Queries bronze layer
|   +-- multiverso_analise_*.md  #   Analise tecnica Multiverso
|
|-- crm_dashboard/               # Dashboard CRM (Flask)
|   +-- app.py                   #   Servidor Flask + templates
|
|-- anti_abuse_deploy/           # Deploy EC2 — anti-abuse
|-- ec2_deploy/                  # Deploy EC2 — pipelines gerais
|-- matriz_de_risco/             # Mapeamento matriz de risco (Smartico)
|-- anotacoes/                   # Notas de reunioes
|-- solicitacoes_pontuais/       # Demandas pontuais do time
|-- temp/                        # Scripts exploratorios (nao producao)
+-- output/                      # Outputs gerados (gitignored)
```

## Pipelines Disponiveis

### Tabelas Fato

| Pipeline | Descricao | Fonte |
|----------|-----------|-------|
| `fact_casino_rounds` | GGR, Hold Rate, RTP por jogo/dia | ps_bi |
| `fact_sports_bets` | Apostas esportivas por esporte/dia | vendor_ec2 |
| `fact_live_casino` | Live casino com sessoes | ps_bi |
| `fact_ftd_deposits` | First-time deposits | ps_bi |
| `fact_registrations` | Registros de novos jogadores | ps_bi |
| `fact_redeposits` | Redepositos por cohort | ps_bi |
| `fact_player_activity` | Atividade geral do jogador | ps_bi |
| `fact_player_engagement_daily` | Engajamento diario | ps_bi |
| `fact_gaming_activity_daily` | Atividade de gaming agregada | ps_bi |
| `fact_attribution` | Atribuicao de fonte de trafego | ecr_ec2 |
| `fact_crm_daily_performance` | KPIs CRM diarios | BigQuery |
| `fct_casino_activity` | Atividade casino consolidada | ps_bi |
| `fct_sports_activity` | Atividade sports consolidada | vendor_ec2 |

### Tabelas Dimensao

| Pipeline | Descricao | Fonte |
|----------|-----------|-------|
| `dim_games_catalog` | Catalogo de jogos com flags | ps_bi + vendor_ec2 |
| `dim_marketing_mapping` | Mapeamento de atribuicao | ecr_ec2 + Super Nova DB |
| `dim_marketing_mapping_canonical` | Versao canonica com auditoria | Multi-source |

### Agregacoes e Reports

| Pipeline | Descricao | Fonte |
|----------|-----------|-------|
| `agg_game_performance` | Performance semanal com ranking | ps_bi |
| `agg_cohort_acquisition` | Cohort de aquisicao | ps_bi |
| `crm_daily_performance` | Dashboard KPIs CRM | BigQuery |
| `anti_abuse_multiverso` | Deteccao de abuso em campanhas | BigQuery |
| `grandes_ganhos` | Maiores ganhos do dia | BigQuery |
| `report_multiverso_campanha` | Report campanha Multiverso | BigQuery |

## Pre-requisitos

1. **Python 3.12+**
2. **Arquivo `.env`** na raiz com as credenciais:
   ```
   ATHENA_AWS_ACCESS_KEY_ID=...
   ATHENA_AWS_SECRET_ACCESS_KEY=...
   SUPERNOVA_DB_PASSWORD=...
   ```
3. **`bigquery_credentials.json`** na raiz (service account Smartico)
4. Dependencias: `pip install boto3 google-cloud-bigquery paramiko pandas openpyxl`

## Regras Importantes

- **Athena opera em UTC** — sempre converter para BRT: `AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'`
- **Valores em centavos** nos databases `_ec2` — dividir por 100
- **Camada `ps_bi`** ja tem valores em BRL — preferir para analises
- **Sempre filtrar test users** (`c_test_user = false`)
- **Nunca `SELECT *`** — Athena cobra por dados escaneados

## Squad

| Nome | Funcao |
|------|--------|
| Castrin (Caio) | Head de Dados |
| Mauro | Analista Senior (Analytics) |
| Gusta | Analista Senior (Infra) |
| Mateus | Analista de Dados |

---

*Mantido pelo squad Intelligence Engine — Super Nova Gaming*
