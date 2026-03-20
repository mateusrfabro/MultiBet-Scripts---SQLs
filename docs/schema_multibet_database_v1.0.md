# Schema do Banco de Dados MultiBet
## Documento de Schema — Super Nova DB

**Schema:** `multibet`
**Banco:** Super Nova DB (PostgreSQL — AWS RDS)
**Versao:** 1.1
**Data de Criacao:** 18-19/03/2026
**Ultima Atualizacao:** 19/03/2026 — Dominio Produto e Performance de Jogos
**Responsavel:** Mateus Fabro — Squad Intelligence Engine
**Empresa:** Super Nova Gaming

---

## Sumario

1. [Resumo](#1-resumo)
2. [Arquitetura do Schema](#2-arquitetura-do-schema)
3. [Dicionario de Dados — Dimensoes](#3-dicionario-de-dados--dimensoes)
   - 3.1. [dim_marketing_mapping](#31-tabela-dim_marketing_mapping)
   - 3.2. [dim_affiliate_source](#32-tabela-dim_affiliate_source)
   - 3.3. [dim_crm_friendly_names](#33-tabela-dim_crm_friendly_names)
   - 3.4. [game_image_mapping](#34-tabela-game_image_mapping)
4. [Dicionario de Dados — Fatos de Aquisicao](#4-dicionario-de-dados--fatos-de-aquisicao)
   - 4.1. [fact_registrations](#41-tabela-fact_registrations)
   - 4.2. [fact_ftd_deposits](#42-tabela-fact_ftd_deposits)
   - 4.3. [fact_redeposits](#43-tabela-fact_redeposits)
   - 4.4. [fact_attribution](#44-tabela-fact_attribution)
   - 4.5. [agg_cohort_acquisition](#45-tabela-agg_cohort_acquisition)
5. [Dicionario de Dados — Fatos de Atividade e Performance](#5-dicionario-de-dados--fatos-de-atividade-e-performance)
   - 5.1. [fact_player_activity](#51-tabela-fact_player_activity)
   - 5.2. [fact_player_engagement_daily](#52-tabela-fact_player_engagement_daily)
   - 5.3. [fact_gaming_activity_daily](#53-tabela-fact_gaming_activity_daily)
   - 5.4. [fact_casino_rounds](#54-tabela-fact_casino_rounds)
   - 5.5. [fct_casino_activity](#55-tabela-fct_casino_activity)
   - 5.6. [fct_sports_activity](#56-tabela-fct_sports_activity)
   - 5.7. [fact_sports_bets](#57-tabela-fact_sports_bets)
   - 5.8. [fact_sports_open_bets](#58-tabela-fact_sports_open_bets)
   - 5.9. [fact_live_casino](#59-tabela-fact_live_casino)
   - 5.10. [fact_jackpots](#510-tabela-fact_jackpots)
   - 5.11. [agg_game_performance](#511-tabela-agg_game_performance)
   - 5.12. [dim_games_catalog](#512-tabela-dim_games_catalog)
6. [Dicionario de Dados — CRM e Campanhas](#6-dicionario-de-dados--crm-e-campanhas)
   - 6.1. [fact_crm_daily_performance](#61-tabela-fact_crm_daily_performance)
7. [Dicionario de Dados — Produto e Front-End](#7-dicionario-de-dados--produto-e-front-end)
   - 7.1. [grandes_ganhos](#71-tabela-grandes_ganhos)
8. [Views Derivadas](#8-views-derivadas)
   - 8.1. [agg_financial_monthly](#81-view-agg_financial_monthly)
   - 8.2. [vw_cohort_roi](#82-view-vw_cohort_roi)
   - 8.3. [vw_acquisition_channel](#83-view-vw_acquisition_channel)
   - 8.4. [vw_attribution_metrics](#84-view-vw_attribution_metrics)
9. [Indices e Constraints](#9-indices-e-constraints)

---

## 1. Resumo

Este documento descreve o schema `multibet` no banco Super Nova DB (PostgreSQL).
O schema contem as tabelas canonicas criadas pelo time de dados da MultiBet para
centralizar, transformar e disponibilizar informacoes de jogadores, transacoes,
aquisicao, marketing e performance de jogos.

As tabelas sao alimentadas por **pipelines Python** que extraem dados de duas
fontes principais:

| Fonte               | Tecnologia                 | Uso Principal                          |
|----------------------|----------------------------|----------------------------------------|
| Athena (Iceberg)     | AWS Athena / Trino-Presto  | Dados transacionais, jogos, financeiro |
| BigQuery (Smartico)  | Google BigQuery            | CRM, campanhas, bonus, comunicacao     |

Os dados sao transformados e persistidos no Super Nova DB, que serve como
**camada analitica centralizada** — pronta para consumo por dashboards,
APIs e relatorios.

**Convencoes adotadas:**
- Valores monetarios em **BRL (reais)** — ja convertidos de centavos na origem
- Timestamps convertidos para **BRT** (America/Sao_Paulo) antes de persistir
- Test users **excluidos** em todos os pipelines
- Nomenclatura: `fact_` (fatos), `dim_` (dimensoes), `agg_` (agregacoes), `fct_` (fatos agregados diarios), `vw_` (views)

**Estrategia de carga:**
- Maioria das tabelas: **TRUNCATE + INSERT** (full refresh)
- Tabelas de dimensao: **UPSERT** (merge incremental)
- Views: **calculadas em tempo de consulta** (sem persistencia)

---

## 2. Arquitetura do Schema

```
                    ┌─────────────────────────────────┐
                    │        FONTES DE DADOS           │
                    │  Athena (Iceberg)  │  BigQuery   │
                    └────────┬──────────┬──────────────┘
                             │          │
                    ┌────────▼──────────▼──────────────┐
                    │     PIPELINES PYTHON (ETL)        │
                    │  pipelines/*.py  (Transform)      │
                    └────────────────┬─────────────────┘
                                     │
                    ┌────────────────▼─────────────────┐
                    │   SUPER NOVA DB (PostgreSQL)      │
                    │   Schema: multibet                │
                    │                                   │
                    │  ┌─────────────┐ ┌─────────────┐  │
                    │  │ DIMENSOES   │ │   FATOS     │  │
                    │  │ dim_*       │ │ fact_*/fct_* │  │
                    │  └─────────────┘ └─────────────┘  │
                    │  ┌─────────────┐ ┌─────────────┐  │
                    │  │ AGREGACOES  │ │   VIEWS     │  │
                    │  │ agg_*       │ │ vw_*        │  │
                    │  └─────────────┘ └─────────────┘  │
                    └──────────────────────────────────┘
                                     │
                    ┌────────────────▼─────────────────┐
                    │        CONSUMIDORES               │
                    │  Dashboards │ APIs │ Relatorios   │
                    └──────────────────────────────────┘
```

### Lista de Objetos do Schema

| #  | Tipo     | Nome                          | Grao                        | Pipeline                              |
|----|----------|-------------------------------|-----------------------------|---------------------------------------|
| 1  | Dimensao | dim_marketing_mapping         | tracker_id                  | dim_marketing_mapping_canonical.py    |
| 2  | Dimensao | dim_affiliate_source          | affiliate_id                | de_para_affiliates.py                 |
| 3  | Dimensao | dim_crm_friendly_names        | entity_id                   | crm_daily_performance.py              |
| 4  | Dimensao | game_image_mapping            | game_name_upper             | game_image_mapper.py                  |
| 5  | Fato     | fact_registrations            | c_ecr_id                    | fact_registrations.py                 |
| 6  | Fato     | fact_ftd_deposits             | c_ecr_id                    | fact_ftd_deposits.py                  |
| 7  | Fato     | fact_redeposits               | c_ecr_id                    | fact_redeposits.py                    |
| 8  | Fato     | fact_attribution              | dt x c_tracker_id           | fact_attribution.py                   |
| 9  | Agregacao| agg_cohort_acquisition        | c_ecr_id                    | agg_cohort_acquisition.py             |
| 10 | Fato     | fact_player_activity          | dt                          | fact_player_activity.py               |
| 11 | Fato     | fact_player_engagement_daily  | c_ecr_id                    | fact_player_engagement_daily.py       |
| 12 | Fato     | fact_gaming_activity_daily    | dt x c_tracker_id           | fact_gaming_activity_daily.py         |
| 13 | Fato     | fact_casino_rounds            | dt x game_id                | fact_casino_rounds.py                 |
| 14 | Fato     | fct_casino_activity           | dt                          | fct_casino_activity.py                |
| 15 | Fato     | fct_sports_activity           | dt                          | fct_sports_activity.py                |
| 16 | Fato     | fact_sports_bets              | dt x sport_name             | fact_sports_bets.py                   |
| 17 | Fato     | fact_sports_open_bets         | snapshot_dt x sport_name    | fact_sports_bets.py                   |
| 18 | Fato     | fact_live_casino              | dt x game_id (live)         | fact_live_casino.py                   |
| 19 | Fato     | fact_jackpots                 | month_start x game_id       | fact_jackpots.py                      |
| 20 | Agregacao| agg_game_performance          | week_start x game_id        | agg_game_performance.py               |
| 21 | Dimensao | dim_games_catalog             | game_id                     | dim_games_catalog.py                  |
| 22 | Fato     | fact_crm_daily_performance    | campanha_id x period        | crm_daily_performance.py              |
| 19 | Fato     | grandes_ganhos                | id (serial)                 | grandes_ganhos.py                     |
| 20 | View     | agg_financial_monthly         | month_reference             | DDL: ddl_agg_financial_monthly.sql    |
| 21 | View     | vw_cohort_roi                 | month_of_ftd x source       | agg_cohort_acquisition.py             |
| 22 | View     | vw_acquisition_channel        | dt x source                 | temp_audit_and_views.py               |
| 23 | View     | vw_attribution_metrics        | dt x c_tracker_id           | fact_attribution.py                   |

---

## 3. Dicionario de Dados — Dimensoes

### 3.1. Tabela: dim_marketing_mapping

**Descricao:** Tabela mestra de atribuicao de marketing. Mapeia cada `tracker_id`
para a fonte de trafego correspondente (Google Ads, Meta Ads, organico, afiliados, etc.).
Combina IDs oficiais validados pelo Marketing com IDs inferidos por analise forense
de click IDs (gclid, fbclid, ttclid).

**Pipeline:** `pipelines/dim_marketing_mapping_canonical.py`
**Estrategia de carga:** DROP + CREATE + INSERT (rebuild canonical)
**Backfill:** Desde inicio da operacao

| Coluna           | Tipo           | Nulo | Default       | Descricao                                                    |
|------------------|----------------|------|---------------|--------------------------------------------------------------|
| tracker_id       | VARCHAR(255)   | NAO  | —             | **PK.** ID do tracker Pragmatic (chave de join principal)    |
| campaign_name    | VARCHAR(255)   | SIM  | —             | Nome da campanha (legado v1 — mantido para historico)        |
| source           | VARCHAR(100)   | SIM  | —             | Fonte de trafego (legado v1 — ex: 'google_ads', 'organic')  |
| confidence       | VARCHAR(100)   | SIM  | —             | Nivel de confianca (legado v1 — ex: 'Official', 'Forensic') |
| mapping_logic    | TEXT           | SIM  | —             | Logica de mapeamento (legado v1)                             |
| affiliate_id     | VARCHAR(50)    | SIM  | —             | ID do afiliado Pragmatic (v2)                                |
| source_name      | VARCHAR(100)   | SIM  | —             | Fonte de trafego normalizada (v2)                            |
| partner_name     | VARCHAR(200)   | SIM  | —             | Nome do parceiro / campanha (v2)                             |
| evidence_logic   | TEXT           | SIM  | —             | Evidencia da classificacao (v2)                              |
| is_validated     | BOOLEAN        | SIM  | FALSE         | TRUE = confirmado pelo Marketing; FALSE = inferido           |
| created_at       | TIMESTAMPTZ    | SIM  | NOW()         | Data de criacao do registro                                  |
| updated_at       | TIMESTAMPTZ    | SIM  | NOW()         | Data da ultima atualizacao                                   |

**Indices:**
- `idx_dmm_source_name` — B-tree em `source_name`
- `idx_dmm_validated` — B-tree em `is_validated`
- `idx_dmm_aff_id` — B-tree em `affiliate_id`

**Observacoes:**
- As colunas legado (campaign_name, source, confidence, mapping_logic) NAO foram removidas para manter retrocompatibilidade com pipelines existentes que fazem `SELECT tracker_id, source`.
- Consumida por: `fact_attribution.py`, `agg_cohort_acquisition.py`, `fact_player_engagement_daily.py`, `vw_cohort_roi`, `vw_acquisition_channel`.

---

### 3.2. Tabela: dim_affiliate_source

**Descricao:** Tabela dimensao que mapeia cada `affiliate_id` para sua fonte de
trafego inferida. A classificacao e feita por analise de click IDs (gclid, fbclid,
ttclid) presentes na tabela `ecr_ec2.tbl_ecr_banner` do Athena.

**Pipeline:** `pipelines/de_para_affiliates.py`
**Estrategia de carga:** DROP + CREATE + INSERT
**Backfill:** Desde inicio da operacao

| Coluna           | Tipo           | Nulo | Default              | Descricao                                                |
|------------------|----------------|------|----------------------|----------------------------------------------------------|
| affiliate_id     | VARCHAR(50)    | NAO  | —                    | **PK.** ID do afiliado Pragmatic                         |
| affiliate_name   | VARCHAR(200)   | SIM  | —                    | Nome do afiliado (quando disponivel)                     |
| source_id        | VARCHAR(200)   | SIM  | —                    | ID da fonte (tracker principal associado)                |
| fonte_trafego    | VARCHAR(50)    | NAO  | 'Direct/Organic'     | Classificacao da fonte (Google, Meta, TikTok, etc.)      |
| utm_source       | VARCHAR(200)   | SIM  | —                    | UTM source (quando disponivel)                           |
| utm_medium       | VARCHAR(200)   | SIM  | —                    | UTM medium (quando disponivel)                           |
| utm_campaign     | VARCHAR(200)   | SIM  | —                    | UTM campaign (quando disponivel)                         |
| updated_at       | TIMESTAMP      | NAO  | NOW()                | Data da ultima atualizacao                               |

**Indices:**
- `idx_das_fonte` — B-tree em `fonte_trafego`

**Observacoes:**
- Decisao confirmada com arquiteto em 19/03/2026: tabelas master de affiliates da Pragmatic NAO serao replicadas no Athena.
- Esta tabela serve como alternativa inferida enquanto nao ha acesso direto as tabelas master.

---

### 3.3. Tabela: dim_crm_friendly_names

**Descricao:** Tabela de DE-PARA para nomes amigaveis de entidades CRM
(campanhas, automacoes, templates de bonus). Permite traduzir IDs tecnicos
em nomes legiveis para dashboards e relatorios.

**Pipeline:** `pipelines/crm_daily_performance.py`
**Estrategia de carga:** INSERT manual / futuro UPSERT via CRM Leader (Raphael)

| Coluna          | Tipo           | Nulo | Default | Descricao                                          |
|-----------------|----------------|------|---------|----------------------------------------------------|
| entity_id       | VARCHAR(100)   | NAO  | —       | **PK.** ID da entidade CRM (campanha, automacao)   |
| friendly_name   | VARCHAR(255)   | NAO  | —       | Nome amigavel para exibicao                        |
| categoria       | VARCHAR(100)   | SIM  | —       | Categoria (ex: 'RETEM', 'MULTIVERSO', 'WELCOME')  |
| responsavel     | VARCHAR(100)   | SIM  | —       | Responsavel pela entidade (ex: 'Raphael')          |
| created_at      | TIMESTAMPTZ    | SIM  | NOW()   | Data de criacao                                    |
| updated_at      | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                         |

---

### 3.4. Tabela: game_image_mapping

**Descricao:** Mapeamento de jogos para URLs de imagem (thumbnail) e slug de
acesso. Combina dados do scraper do site multi.bet com o catalogo de jogos do
Athena (`bireports_ec2.tbl_vendor_games_mapping_data`).

**Pipeline:** `pipelines/game_image_mapper.py`
**Estrategia de carga:** UPSERT (ON CONFLICT game_name_upper)

| Coluna            | Tipo           | Nulo | Default    | Descricao                                                |
|-------------------|----------------|------|------------|----------------------------------------------------------|
| id                | SERIAL         | NAO  | auto       | PK auto-incremento                                       |
| game_name         | VARCHAR(255)   | NAO  | —          | Nome original do jogo (ex: 'Fortune Ox')                 |
| game_name_upper   | VARCHAR(255)   | NAO  | —          | **UNIQUE.** Nome normalizado UPPER para joins            |
| provider_game_id  | VARCHAR(50)    | SIM  | —          | `c_game_id` do catalogo Pragmatic (ex: '4776')           |
| vendor_id         | VARCHAR(100)   | SIM  | —          | `c_vendor_id` do catalogo (ex: 'alea_pgsoft')            |
| game_image_url    | VARCHAR(500)   | SIM  | —          | URL do thumbnail (CDN multi.bet ou provedor)             |
| game_slug         | VARCHAR(200)   | SIM  | —          | Path de acesso ao jogo (ex: /pb/gameplay/fortune-ox/real-game) |
| source            | VARCHAR(50)    | SIM  | 'scraper'  | Origem do dado ('scraper', 'manual', 'redshift')         |
| updated_at        | TIMESTAMPTZ    | SIM  | NOW()      | Data da ultima atualizacao                               |

**Indices:**
- `idx_gim_game_name_upper` — B-tree em `game_name_upper`

**Constraint:** `uq_game_name_upper` — UNIQUE em `game_name_upper`

---

## 4. Dicionario de Dados — Fatos de Aquisicao

### 4.1. Tabela: fact_registrations

**Descricao:** Registros de jogadores (sign-up). Uma linha por jogador registrado.
Inclui tracker de origem para atribuicao de canal.

**Pipeline:** `pipelines/fact_registrations.py`
**Fonte:** Athena `ecr_ec2.tbl_ecr`
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna             | Tipo           | Nulo | Default | Descricao                                            |
|--------------------|----------------|------|---------|------------------------------------------------------|
| id                 | SERIAL         | NAO  | auto    | PK auto-incremento                                   |
| c_ecr_id           | BIGINT         | NAO  | —       | **UNIQUE.** ID interno do jogador (18 digitos)       |
| c_external_id      | BIGINT         | SIM  | —       | ID externo (= Smartico `user_ext_id`)                |
| c_tracker_id       | VARCHAR(255)   | SIM  | —       | Tracker de origem (canal de aquisicao)               |
| c_country_code     | VARCHAR(50)    | SIM  | —       | Codigo do pais do jogador                            |
| registration_date  | DATE           | NAO  | —       | Data do registro em BRT (truncada)                   |
| registration_time  | TIMESTAMPTZ    | NAO  | —       | Timestamp original do registro (UTC)                 |
| dt                 | DATE           | NAO  | —       | Particao logica (= registration_date)                |
| refreshed_at       | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao do pipeline               |

**Indices:**
- `idx_fr_dt` — B-tree em `dt`
- `idx_fr_ecr_id` — B-tree em `c_ecr_id`
- `idx_fr_tracker` — B-tree em `c_tracker_id`
- `idx_fr_ecr_unique` — UNIQUE em `c_ecr_id`

---

### 4.2. Tabela: fact_ftd_deposits

**Descricao:** First-Time Deposits (FTD). Uma linha por jogador que realizou
pelo menos um deposito confirmado. Registra o valor e data do primeiro deposito.

**Pipeline:** `pipelines/fact_ftd_deposits.py`
**Fonte:** Athena `cashier_ec2.tbl_cashier_deposit`
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna       | Tipo           | Nulo | Default | Descricao                                         |
|--------------|----------------|------|---------|----------------------------------------------------|
| id           | SERIAL         | NAO  | auto    | PK auto-incremento                                 |
| c_ecr_id     | BIGINT         | NAO  | —       | **UNIQUE.** ID interno do jogador                  |
| ftd_txn_id   | BIGINT         | NAO  | —       | ID da transacao do primeiro deposito               |
| ftd_amount   | NUMERIC(15,2)  | NAO  | —       | Valor do FTD em BRL (centavos / 100)               |
| ftd_date     | DATE           | NAO  | —       | Data do FTD em BRT                                 |
| ftd_time     | TIMESTAMPTZ    | NAO  | —       | Timestamp original do FTD (UTC)                    |
| dt           | DATE           | NAO  | —       | Particao logica (= ftd_date)                       |
| refreshed_at | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao do pipeline             |

**Indices:**
- `idx_fftd_dt` — B-tree em `dt`
- `idx_fftd_ecr_id` — B-tree em `c_ecr_id`
- `idx_fftd_ecr_unique` — UNIQUE em `c_ecr_id`

---

### 4.3. Tabela: fact_redeposits

**Descricao:** Metricas de redeposito por jogador. Calcula frequencia, ticket
medio, intervalo entre depositos e flag de redeposito em 7 dias. Complementa
a `fact_ftd_deposits` com visao de retencao financeira.

**Pipeline:** `pipelines/fact_redeposits.py`
**Fonte:** Athena `cashier_ec2.tbl_cashier_deposit` + `bireports_ec2.tbl_ecr`
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna                      | Tipo           | Nulo | Default | Descricao                                           |
|-----------------------------|----------------|------|---------|-----------------------------------------------------|
| c_ecr_id                    | BIGINT         | NAO  | —       | **PK.** ID interno do jogador                       |
| c_tracker_id                | VARCHAR(255)   | SIM  | —       | Tracker de origem                                   |
| ftd_date                    | DATE           | SIM  | —       | Data do primeiro deposito (BRT)                     |
| ftd_amount                  | NUMERIC(18,2)  | SIM  | —       | Valor do FTD em BRL                                 |
| total_deposits              | INTEGER        | SIM  | 0       | Total de depositos confirmados                      |
| redeposit_count             | INTEGER        | SIM  | 0       | Depositos apos o FTD (total - 1)                    |
| is_redepositor_d7           | SMALLINT       | SIM  | 0       | 1 se fez 2o deposito em ate 7 dias, 0 caso contrario|
| second_deposit_date         | DATE           | SIM  | —       | Data do segundo deposito                            |
| days_to_second_deposit      | INTEGER        | SIM  | —       | Dias entre FTD e 2o deposito                        |
| avg_redeposit_amount        | NUMERIC(18,2)  | SIM  | —       | Ticket medio dos redepositos (excluindo FTD)        |
| total_redeposit_amount      | NUMERIC(18,2)  | SIM  | 0       | Valor total dos redepositos em BRL                  |
| avg_days_between_deposits   | NUMERIC(10,2)  | SIM  | —       | Intervalo medio entre depositos consecutivos (dias) |
| deposits_per_month          | NUMERIC(10,2)  | SIM  | —       | Frequencia mensal de depositos                      |
| refreshed_at                | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                          |

---

### 4.4. Tabela: fact_attribution

**Descricao:** Atribuicao de marketing por dia e tracker. Consolida registros,
FTDs, GGR e spend de marketing para calculo de CPA, CAC e ROAS.
O spend e carregado de planilha Google Ads exportada.

**Pipeline:** `pipelines/fact_attribution.py`
**Fontes:** Athena (regs, FTDs, GGR) + Google Sheets (spend)
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna             | Tipo           | Nulo | Default | Descricao                                     |
|--------------------|----------------|------|---------|-----------------------------------------------|
| dt                 | DATE           | NAO  | —       | **PK (composta).** Data em BRT                |
| c_tracker_id       | VARCHAR(255)   | NAO  | —       | **PK (composta).** Tracker de origem          |
| qty_registrations  | INTEGER        | SIM  | 0       | Quantidade de registros no dia                |
| qty_ftds           | INTEGER        | SIM  | 0       | Quantidade de FTDs no dia                     |
| ggr                | NUMERIC(18,2)  | SIM  | 0       | GGR total dos jogadores deste tracker no dia  |
| marketing_spend    | NUMERIC(18,2)  | SIM  | 0       | Investimento em marketing (BRL)               |
| refreshed_at       | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                    |

**Observacoes:**
- Derivadas calculadas na view `vw_attribution_metrics`: CPA, CAC, ROAS, ROI%.

---

### 4.5. Tabela: agg_cohort_acquisition

**Descricao:** Analise de cohort por safra de FTD. Calcula GGR acumulado em
janelas D0, D7 e D30 por jogador, permitindo analise de LTV e ROI de longo
prazo por fonte de trafego.

**Pipeline:** `pipelines/agg_cohort_acquisition.py`
**Fontes:** Athena (bireports_ec2, cashier_ec2, fund_ec2) + Super Nova DB (dim_marketing_mapping, fact_attribution)
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna          | Tipo           | Nulo | Default               | Descricao                                         |
|-----------------|----------------|------|-----------------------|---------------------------------------------------|
| c_ecr_id        | BIGINT         | NAO  | —                     | **PK.** ID interno do jogador                     |
| month_of_ftd    | VARCHAR(7)     | NAO  | —                     | Safra do FTD no formato YYYY-MM                   |
| source          | VARCHAR(100)   | SIM  | 'unmapped_orphans'    | Fonte de trafego (via dim_marketing_mapping)       |
| c_tracker_id    | VARCHAR(255)   | SIM  | —                     | Tracker de origem                                 |
| ftd_date        | DATE           | SIM  | —                     | Data do FTD                                       |
| ftd_amount      | NUMERIC(18,2)  | SIM  | —                     | Valor do FTD em BRL                               |
| ggr_d0          | NUMERIC(18,2)  | SIM  | 0                     | GGR acumulado no dia do FTD (D0)                  |
| ggr_d7          | NUMERIC(18,2)  | SIM  | 0                     | GGR acumulado D0 a D7                             |
| ggr_d30         | NUMERIC(18,2)  | SIM  | 0                     | GGR acumulado D0 a D30                            |
| is_2nd_depositor| SMALLINT       | SIM  | 0                     | 1 se fez segundo deposito, 0 caso contrario       |
| refreshed_at    | TIMESTAMPTZ    | SIM  | NOW()                 | Data da ultima atualizacao                        |

**View derivada:** `vw_cohort_roi` — agrega por safra x source com ROI D30.

---

## 5. Dicionario de Dados — Fatos de Atividade e Performance

### 5.1. Tabela: fact_player_activity

**Descricao:** Metricas de atividade de jogadores por dia. Calcula DAU
(Daily Active Users), WAU (Weekly), MAU (Monthly), stickiness e GGR por
jogador ativo.

**Pipeline:** `pipelines/fact_player_activity.py`
**Fonte:** Athena `fund_ec2.tbl_real_fund_txn`
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna              | Tipo           | Nulo | Default | Descricao                                         |
|---------------------|----------------|------|---------|---------------------------------------------------|
| dt                  | DATE           | NAO  | —       | **PK.** Data em BRT                               |
| dau                 | INTEGER        | SIM  | 0       | Jogadores unicos ativos no dia                    |
| wau                 | INTEGER        | SIM  | 0       | Jogadores unicos ativos nos ultimos 7 dias        |
| mau                 | INTEGER        | SIM  | 0       | Jogadores unicos ativos nos ultimos 30 dias       |
| stickiness_pct      | NUMERIC(10,4)  | SIM  | 0       | DAU / MAU * 100 (engajamento)                     |
| total_bets          | INTEGER        | SIM  | 0       | Total de apostas no dia                           |
| avg_bets_per_player | NUMERIC(10,2)  | SIM  | 0       | Media de apostas por jogador ativo                |
| total_ggr           | NUMERIC(18,2)  | SIM  | 0       | GGR total do dia em BRL                           |
| ggr_per_dau         | NUMERIC(18,2)  | SIM  | 0       | GGR / DAU (receita por jogador ativo)             |
| refreshed_at        | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                        |

---

### 5.2. Tabela: fact_player_engagement_daily

**Descricao:** Metricas de engajamento por jogador. Calcula dias ativos,
frequencia de apostas, GGR acumulado e flag de churn. Usado para
segmentacao e retencao.

**Pipeline:** `pipelines/fact_player_engagement_daily.py`
**Fontes:** Athena (fund_ec2, cashier_ec2, bireports_ec2) + Super Nova DB (dim_marketing_mapping)
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna                 | Tipo           | Nulo | Default | Descricao                                          |
|------------------------|----------------|------|---------|----------------------------------------------------|
| c_ecr_id               | BIGINT         | NAO  | —       | **PK.** ID interno do jogador                      |
| c_tracker_id           | VARCHAR(255)   | SIM  | —       | Tracker de origem                                  |
| source                 | VARCHAR(100)   | SIM  | —       | Fonte de trafego (via dim_marketing_mapping)        |
| ftd_date               | DATE           | SIM  | —       | Data do primeiro deposito                          |
| first_active_date      | DATE           | SIM  | —       | Primeira data com atividade de jogo                |
| last_active_date       | DATE           | SIM  | —       | Ultima data com atividade de jogo                  |
| days_active_since_ftd  | INTEGER        | SIM  | 0       | Dias ativos desde o FTD                            |
| total_active_days      | INTEGER        | SIM  | 0       | Total de dias distintos com atividade              |
| total_bets_count       | INTEGER        | SIM  | 0       | Total de apostas realizadas                        |
| avg_bets_per_day       | NUMERIC(10,2)  | SIM  | 0       | Media de apostas por dia ativo                     |
| total_ggr              | NUMERIC(18,2)  | SIM  | 0       | GGR total acumulado em BRL                         |
| days_since_last_active | INTEGER        | SIM  | —       | Dias desde a ultima atividade                      |
| is_churned             | SMALLINT       | SIM  | 0       | 1 se inativo ha mais de 30 dias, 0 caso contrario  |
| refreshed_at           | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                         |

---

### 5.3. Tabela: fact_gaming_activity_daily

**Descricao:** GGR, NGR e metricas de jogo por dia e tracker. Utiliza **Sub-Fund
Isolation** (separacao dinheiro real vs bonus) para calculo preciso de receita.
Tabela base para `fact_attribution`.

**Pipeline:** `pipelines/fact_gaming_activity_daily.py`
**Fontes:** Athena — Sub-Fund Isolation (tbl_real_fund_txn + tbl_realcash_sub_fund_txn + tbl_bonus_sub_fund_txn + tbl_real_fund_txn_type_mst + tbl_ecr_flags)
**Metodo:** Validado com Mauro em 19/03/2026 — diff R$ 13,98 em R$ 270M (0.000%)
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna              | Tipo           | Nulo | Default | Descricao                                         |
|---------------------|----------------|------|---------|---------------------------------------------------|
| dt                  | DATE           | NAO  | —       | **PK (composta).** Data em BRT                    |
| c_tracker_id        | VARCHAR(255)   | NAO  | —       | **PK (composta).** Tracker de origem              |
| qty_players         | INTEGER        | SIM  | 0       | Jogadores unicos                                  |
| total_bets          | NUMERIC(18,2)  | SIM  | 0       | Turnover total em BRL                             |
| total_wins          | NUMERIC(18,2)  | SIM  | 0       | Pagamentos totais em BRL                          |
| ggr                 | NUMERIC(18,2)  | SIM  | 0       | GGR = Bets - Wins (dinheiro real + DRP)           |
| bonus_cost          | NUMERIC(18,2)  | SIM  | 0       | Custo de bonus (CRP + WRP + RRP)                  |
| ngr                 | NUMERIC(18,2)  | SIM  | 0       | NGR = GGR - Bonus Cost                            |
| margin_pct          | NUMERIC(10,4)  | SIM  | 0       | GGR / Turnover * 100                              |
| ggr_casino          | NUMERIC(18,2)  | SIM  | 0       | GGR somente de casino                             |
| ggr_sports          | NUMERIC(18,2)  | SIM  | 0       | GGR somente de sports                             |
| max_single_win_val  | NUMERIC(18,2)  | SIM  | —       | Maior ganho individual no dia (monitoramento)     |
| rollback_count      | INTEGER        | SIM  | 0       | Quantidade de rollbacks (cancelamentos)           |
| rollback_total      | NUMERIC(18,2)  | SIM  | 0       | Valor total de rollbacks em BRL                   |
| refreshed_at        | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                        |

**Nota tecnica — Sub-Fund Isolation:**
- Real = `realcash_sub_fund` + `DRP` (Deposit Reward Points — classificado como dinheiro real)
- Bonus = `CRP` + `WRP` + `RRP` (Conversion/Winnings/Referral Reward Points)
- Tipo 36 NAO e bonus (validado com AWS Console e Mauro)

---

### 5.4. Tabela: fact_casino_rounds

**Descricao:** Performance de jogos de casino por dia e jogo. Calcula turnover,
wins, GGR, Hold Rate, RTP, jackpot e free spins por jogo, com separacao
real vs bonus. Fonte primaria: `ps_bi.fct_casino_activity_daily` (pre-agregado,
BRL, validado pelo dbt/Pragmatic).

**Pipeline:** `pipelines/fact_casino_rounds.py`
**Fontes:** Athena `ps_bi.fct_casino_activity_daily` + `ps_bi.dim_game` + filtro test users
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna               | Tipo           | Nulo | Default | Descricao                                         |
|----------------------|----------------|------|---------|---------------------------------------------------|
| dt                   | DATE           | NAO  | —       | **PK (composta).** Data                           |
| game_id              | VARCHAR(50)    | NAO  | —       | **PK (composta).** ID do jogo (c_game_id)         |
| game_name            | VARCHAR(255)   | SIM  | —       | Nome do jogo (game_desc do catalogo)              |
| vendor_id            | VARCHAR(50)    | SIM  | —       | ID do provedor (ex: 'alea_pgsoft')                |
| sub_vendor_id        | VARCHAR(50)    | SIM  | —       | Sub-provedor (quando aplicavel)                   |
| game_category        | VARCHAR(100)   | SIM  | —       | Categoria: 'Slots', 'Live', 'Outros'             |
| qty_players          | INTEGER        | SIM  | 0       | Jogadores unicos no dia                           |
| total_rounds         | INTEGER        | SIM  | 0       | Total de rodadas (bet_count)                      |
| rounds_per_player    | NUMERIC(10,2)  | SIM  | 0       | Rodadas / jogador                                 |
| turnover_real        | NUMERIC(18,2)  | SIM  | 0       | Turnover dinheiro real em BRL                     |
| wins_real            | NUMERIC(18,2)  | SIM  | 0       | Pagamentos dinheiro real em BRL                   |
| ggr_real             | NUMERIC(18,2)  | SIM  | 0       | GGR dinheiro real em BRL                          |
| turnover_bonus       | NUMERIC(18,2)  | SIM  | 0       | Turnover bonus em BRL                             |
| wins_bonus           | NUMERIC(18,2)  | SIM  | 0       | Pagamentos bonus em BRL                           |
| ggr_bonus            | NUMERIC(18,2)  | SIM  | 0       | GGR bonus em BRL                                  |
| turnover_total       | NUMERIC(18,2)  | SIM  | 0       | Turnover total (real + bonus) em BRL              |
| wins_total           | NUMERIC(18,2)  | SIM  | 0       | Pagamentos totais em BRL                          |
| ggr_total            | NUMERIC(18,2)  | SIM  | 0       | GGR total em BRL                                  |
| hold_rate_pct        | NUMERIC(10,4)  | SIM  | 0       | Hold Rate = GGR / Turnover * 100                  |
| rtp_pct              | NUMERIC(10,4)  | SIM  | 0       | RTP = Wins / Turnover * 100 (= 100 - Hold Rate)  |
| jackpot_win          | NUMERIC(18,2)  | SIM  | 0       | Jackpots pagos em BRL                             |
| jackpot_contribution | NUMERIC(18,2)  | SIM  | 0       | Contribuicoes para jackpot em BRL                 |
| free_spins_bet       | NUMERIC(18,2)  | SIM  | 0       | Turnover de free spins em BRL                     |
| free_spins_win       | NUMERIC(18,2)  | SIM  | 0       | Pagamentos de free spins em BRL                   |
| refreshed_at         | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                        |

**Indices:**
- `idx_fcr_vendor` — B-tree em `(vendor_id, dt)`
- `idx_fcr_ggr` — B-tree em `(dt, ggr_total DESC)`
- `idx_fcr_category` — B-tree em `(game_category, dt)`

---

### 5.5. Tabela: fct_casino_activity

**Descricao:** Atividade diaria consolidada de casino. Agrega turnover, wins e
GGR separados em real vs bonus via Sub-Fund Isolation. Visao macro do
produto casino por dia.

**Pipeline:** `pipelines/fct_casino_activity.py`
**Fontes:** Athena — Sub-Fund Isolation (mesma logica do Mauro/Redshift, adaptada para Presto)
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna            | Tipo           | Nulo | Default | Descricao                                   |
|-------------------|----------------|------|---------|---------------------------------------------|
| dt                | DATE           | NAO  | —       | **PK.** Data em BRT                         |
| qty_players       | INTEGER        | SIM  | 0       | Jogadores unicos de casino no dia           |
| casino_real_bet   | NUMERIC(18,2)  | SIM  | 0       | Turnover dinheiro real em BRL               |
| casino_real_win   | NUMERIC(18,2)  | SIM  | 0       | Pagamentos dinheiro real em BRL             |
| casino_real_ggr   | NUMERIC(18,2)  | SIM  | 0       | GGR dinheiro real em BRL                    |
| casino_bonus_bet  | NUMERIC(18,2)  | SIM  | 0       | Turnover bonus em BRL                       |
| casino_bonus_win  | NUMERIC(18,2)  | SIM  | 0       | Pagamentos bonus em BRL                     |
| casino_bonus_ggr  | NUMERIC(18,2)  | SIM  | 0       | GGR bonus em BRL                            |
| casino_total_bet  | NUMERIC(18,2)  | SIM  | 0       | Turnover total em BRL                       |
| casino_total_win  | NUMERIC(18,2)  | SIM  | 0       | Pagamentos totais em BRL                    |
| casino_total_ggr  | NUMERIC(18,2)  | SIM  | 0       | GGR total em BRL                            |
| refreshed_at      | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                  |

---

### 5.6. Tabela: fct_sports_activity

**Descricao:** Atividade diaria consolidada de esportes (sportsbook). Mesma
estrutura da `fct_casino_activity`, separando real vs bonus via Sub-Fund Isolation.

**Pipeline:** `pipelines/fct_sports_activity.py`
**Fontes:** Athena — Sub-Fund Isolation (filtro `c_product_id = 'SPORTS_BOOK'`)
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna            | Tipo           | Nulo | Default | Descricao                                   |
|-------------------|----------------|------|---------|---------------------------------------------|
| dt                | DATE           | NAO  | —       | **PK.** Data em BRT                         |
| qty_players       | INTEGER        | SIM  | 0       | Jogadores unicos de sports no dia           |
| sports_real_bet   | NUMERIC(18,2)  | SIM  | 0       | Turnover dinheiro real em BRL               |
| sports_real_win   | NUMERIC(18,2)  | SIM  | 0       | Pagamentos dinheiro real em BRL             |
| sports_real_ggr   | NUMERIC(18,2)  | SIM  | 0       | GGR dinheiro real em BRL                    |
| sports_bonus_bet  | NUMERIC(18,2)  | SIM  | 0       | Turnover bonus em BRL                       |
| sports_bonus_win  | NUMERIC(18,2)  | SIM  | 0       | Pagamentos bonus em BRL                     |
| sports_bonus_ggr  | NUMERIC(18,2)  | SIM  | 0       | GGR bonus em BRL                            |
| sports_total_bet  | NUMERIC(18,2)  | SIM  | 0       | Turnover total em BRL                       |
| sports_total_win  | NUMERIC(18,2)  | SIM  | 0       | Pagamentos totais em BRL                    |
| sports_total_ggr  | NUMERIC(18,2)  | SIM  | 0       | GGR total em BRL                            |
| refreshed_at      | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                  |

---

### 5.7. Tabela: fact_sports_bets

**Descricao:** Apostas esportivas agregadas por dia e esporte. Inclui turnover,
GGR, margem, ticket medio, odds media e split pre-jogo vs ao vivo.

**Pipeline:** `pipelines/fact_sports_bets.py`
**Fontes:** Athena `vendor_ec2.tbl_sports_book_bets_info` + `tbl_sports_book_bet_details`
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna              | Tipo           | Nulo | Default | Descricao                                          |
|---------------------|----------------|------|---------|----------------------------------------------------|
| dt                  | DATE           | NAO  | —       | **PK (composta).** Data em BRT                     |
| sport_name          | VARCHAR(255)   | NAO  | —       | **PK (composta).** Nome do esporte                 |
| qty_bets            | INTEGER        | SIM  | 0       | Quantidade de bilhetes                             |
| qty_players         | INTEGER        | SIM  | 0       | Jogadores unicos                                   |
| turnover            | NUMERIC(18,2)  | SIM  | 0       | Total apostado em BRL                              |
| total_return        | NUMERIC(18,2)  | SIM  | 0       | Total pago em BRL                                  |
| ggr                 | NUMERIC(18,2)  | SIM  | 0       | GGR = Turnover - Return                            |
| margin_pct          | NUMERIC(10,4)  | SIM  | 0       | Margem = GGR / Turnover * 100                      |
| avg_ticket          | NUMERIC(18,2)  | SIM  | 0       | Ticket medio por bilhete                           |
| avg_odds            | NUMERIC(10,4)  | SIM  | 0       | Odds media dos bilhetes                            |
| qty_pre_match       | INTEGER        | SIM  | 0       | Bilhetes pre-jogo                                  |
| qty_live            | INTEGER        | SIM  | 0       | Bilhetes ao vivo                                   |
| turnover_pre_match  | NUMERIC(18,2)  | SIM  | 0       | Turnover pre-jogo em BRL                           |
| turnover_live       | NUMERIC(18,2)  | SIM  | 0       | Turnover ao vivo em BRL                            |
| pct_pre_match       | NUMERIC(10,4)  | SIM  | 0       | % dos bilhetes que sao pre-jogo                    |
| pct_live            | NUMERIC(10,4)  | SIM  | 0       | % dos bilhetes que sao ao vivo                     |
| refreshed_at        | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                         |

**Indices:**
- `idx_fact_sports_bets_ggr` — B-tree em `(dt, ggr DESC)`

**Nota:** Valores no `vendor_ec2` sportsbook sao em BRL real (NAO centavos).

---

### 5.8. Tabela: fact_sports_open_bets

**Descricao:** Snapshot diario de apostas esportivas abertas (nao liquidadas).
Permite projecao de passivo e GGR esperado.

**Pipeline:** `pipelines/fact_sports_bets.py`
**Fonte:** Athena `vendor_ec2.tbl_sports_book_bets_info` (filtro `bet_state = 'open'`)
**Estrategia de carga:** TRUNCATE + INSERT (snapshot diario)

| Coluna              | Tipo           | Nulo | Default | Descricao                                         |
|---------------------|----------------|------|---------|---------------------------------------------------|
| snapshot_dt         | DATE           | NAO  | —       | **PK (composta).** Data do snapshot               |
| sport_name          | VARCHAR(255)   | NAO  | —       | **PK (composta).** Nome do esporte                |
| qty_open_bets       | INTEGER        | SIM  | 0       | Quantidade de apostas abertas                     |
| total_stake_open    | NUMERIC(18,2)  | SIM  | 0       | Valor total em aberto (BRL)                       |
| avg_odds_open       | NUMERIC(10,4)  | SIM  | 0       | Odds media das apostas abertas                    |
| projected_liability | NUMERIC(18,2)  | SIM  | 0       | Passivo projetado = stake * avg_odds              |
| projected_ggr       | NUMERIC(18,2)  | SIM  | 0       | GGR projetado (estimativa)                        |
| refreshed_at        | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                        |

---

### 5.9. Tabela: fact_live_casino

**Descricao:** Performance de jogos de casino ao vivo por dia e jogo. Combina
metricas financeiras (ps_bi) com dados de sessao (gaming_sessions) para
calcular duracao media, rodadas por sessao e pico de jogadores.

**Pipeline:** `pipelines/fact_live_casino.py`
**Fontes:** Athena `ps_bi.fct_casino_activity_daily` + `ps_bi.dim_game` (filtro `game_category = 'Live'`) + `bireports_ec2.tbl_ecr_gaming_sessions`
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna                      | Tipo           | Nulo | Default | Descricao                                         |
|-----------------------------|----------------|------|---------|---------------------------------------------------|
| dt                          | DATE           | NAO  | —       | **PK (composta).** Data                           |
| game_id                     | VARCHAR(50)    | NAO  | —       | **PK (composta).** ID do jogo live                |
| game_name                   | VARCHAR(255)   | SIM  | —       | Nome do jogo (ex: 'Lightning Roulette')           |
| vendor_id                   | VARCHAR(50)    | SIM  | —       | ID do provedor (ex: 'alea_evolution')             |
| game_category_desc          | VARCHAR(100)   | SIM  | —       | Subcategoria (LiveDealer, Blackjack, Roulette)    |
| qty_players                 | INTEGER        | SIM  | 0       | Jogadores unicos no dia                           |
| total_rounds                | INTEGER        | SIM  | 0       | Total de rodadas (bet_count)                      |
| turnover_total              | NUMERIC(18,2)  | SIM  | 0       | Turnover total em BRL                             |
| wins_total                  | NUMERIC(18,2)  | SIM  | 0       | Pagamentos totais em BRL                          |
| ggr_total                   | NUMERIC(18,2)  | SIM  | 0       | GGR total em BRL                                  |
| hold_rate_pct               | NUMERIC(10,4)  | SIM  | 0       | Hold Rate %                                       |
| rtp_pct                     | NUMERIC(10,4)  | SIM  | 0       | RTP %                                             |
| qty_sessions                | INTEGER        | SIM  | 0       | Sessoes de jogo (via gaming_sessions)             |
| avg_session_duration_sec    | NUMERIC(10,2)  | SIM  | 0       | Duracao media da sessao em segundos               |
| avg_rounds_per_session      | NUMERIC(10,2)  | SIM  | 0       | Rodadas medias por sessao                         |
| max_concurrent_players      | INTEGER        | SIM  | 0       | Pico de jogadores simultaneos (proxy ocupacao)    |
| refreshed_at                | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                        |

**Indices:**
- `idx_flc_ggr` — B-tree em `(dt, ggr_total DESC)`

**Nota:** "Ocupacao de mesas %" requer dados de capacidade do provedor (Evolution API)
que nao temos. `max_concurrent_players` e um proxy baseado em jogadores por hora.

---

### 5.10. Tabela: fact_jackpots

**Descricao:** Jackpots pagos e contribuicoes por jogo e mes. Dados extraidos
das colunas de jackpot em `ps_bi.fct_casino_activity_daily`. Calcula impacto
dos jackpots no GGR.

**Pipeline:** `pipelines/fact_jackpots.py`
**Fontes:** Athena `ps_bi.fct_casino_activity_daily` (colunas `jackpot_win_amount_local`, `jackpot_contribution_local`)
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna              | Tipo           | Nulo | Default | Descricao                                          |
|---------------------|----------------|------|---------|----------------------------------------------------|
| month_start         | DATE           | NAO  | —       | **PK (composta).** Primeiro dia do mes             |
| game_id             | VARCHAR(50)    | NAO  | —       | **PK (composta).** ID do jogo                      |
| game_name           | VARCHAR(255)   | SIM  | —       | Nome do jogo                                       |
| vendor_id           | VARCHAR(50)    | SIM  | —       | ID do provedor                                     |
| jackpots_count      | INTEGER        | SIM  | 0       | Eventos de jackpot (player-game-days com win > 0)  |
| jackpot_total_paid  | NUMERIC(18,2)  | SIM  | 0       | Total pago em jackpots (BRL)                       |
| avg_jackpot_value   | NUMERIC(18,2)  | SIM  | 0       | Valor medio por jackpot                            |
| max_jackpot_value   | NUMERIC(18,2)  | SIM  | 0       | Maior jackpot do mes                               |
| contribution_total  | NUMERIC(18,2)  | SIM  | 0       | Total contribuido para jackpots (BRL)              |
| ggr_total           | NUMERIC(18,2)  | SIM  | 0       | GGR total do jogo no mes (BRL)                     |
| jackpot_impact_pct  | NUMERIC(10,4)  | SIM  | 0       | Impacto = jackpot_paid / GGR * 100                 |
| refreshed_at        | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                         |

**Indices:**
- `idx_fj_month` — B-tree em `month_start`
- `idx_fj_impact` — B-tree em `(month_start, jackpot_impact_pct DESC)`

**Limitacao:** Nao existe tabela dedicada de jackpots no Athena. Contagem de
"triggers" e estimada por player-game-days com `jackpot_win > 0`.

---

### 5.11. Tabela: agg_game_performance

**Descricao:** Performance semanal de jogos casino com ranking, concentracao
de receita e deteccao de jogos estreantes. Fonte para analise de portfilio
de jogos e decisoes de curadoria.

**Pipeline:** `pipelines/agg_game_performance.py`
**Fontes:** Athena `ps_bi.fct_casino_activity_daily` + `ps_bi.dim_game`
**Estrategia de carga:** TRUNCATE + INSERT
**Backfill:** Desde 01/10/2025

| Coluna              | Tipo           | Nulo | Default | Descricao                                          |
|---------------------|----------------|------|---------|----------------------------------------------------|
| week_start          | DATE           | NAO  | —       | **PK (composta).** Inicio da semana                |
| game_id             | VARCHAR(50)    | NAO  | —       | **PK (composta).** ID do jogo                      |
| game_name           | VARCHAR(255)   | SIM  | —       | Nome do jogo                                       |
| vendor_id           | VARCHAR(50)    | SIM  | —       | ID do provedor                                     |
| game_category       | VARCHAR(100)   | SIM  | —       | Categoria (Slots, Live, Outros)                    |
| qty_active_days     | INTEGER        | SIM  | 0       | Dias com atividade na semana                       |
| dau_avg             | NUMERIC(10,2)  | SIM  | 0       | DAU medio (jogadores/dia)                          |
| total_players       | INTEGER        | SIM  | 0       | Jogadores unicos na semana                         |
| total_rounds        | INTEGER        | SIM  | 0       | Total de rodadas                                   |
| turnover            | NUMERIC(18,2)  | SIM  | 0       | Turnover em BRL                                    |
| ggr                 | NUMERIC(18,2)  | SIM  | 0       | GGR em BRL                                         |
| hold_rate_pct       | NUMERIC(10,4)  | SIM  | 0       | Hold Rate %                                        |
| ggr_rank            | INTEGER        | SIM  | —       | Ranking por GGR (1 = mais receita na semana)       |
| concentration_pct   | NUMERIC(10,4)  | SIM  | 0       | % do GGR total da semana                           |
| first_activity_date | DATE           | SIM  | —       | Primeira atividade historica do jogo                |
| is_new_game         | BOOLEAN        | SIM  | FALSE   | TRUE se estreou nessa semana                       |
| refreshed_at        | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                         |

**Indices:**
- `idx_agp_rank` — B-tree em `(week_start, ggr_rank)`

---

### 5.12. Tabela: dim_games_catalog

**Descricao:** Catalogo completo de jogos (snapshot). Combina `ps_bi.dim_game`
com flags de jackpot e free spins de `vendor_ec2.tbl_vendor_games_mapping_mst`.

**Pipeline:** `pipelines/dim_games_catalog.py`
**Fontes:** Athena `ps_bi.dim_game` + `vendor_ec2.tbl_vendor_games_mapping_mst`
**Estrategia de carga:** TRUNCATE + INSERT (snapshot)

| Coluna               | Tipo           | Nulo | Default | Descricao                                       |
|----------------------|----------------|------|---------|-------------------------------------------------|
| game_id              | VARCHAR(50)    | NAO  | —       | **PK.** ID do jogo                              |
| game_name            | VARCHAR(255)   | SIM  | —       | Nome do jogo                                    |
| vendor_id            | VARCHAR(50)    | SIM  | —       | Provedor principal                              |
| sub_vendor_id        | VARCHAR(50)    | SIM  | —       | Sub-provedor                                    |
| product_id           | VARCHAR(30)    | SIM  | —       | 'casino' ou 'sports_book'                       |
| game_category        | VARCHAR(100)   | SIM  | —       | Categoria: 'Slots', 'Live'                      |
| game_category_desc   | VARCHAR(100)   | SIM  | —       | Descricao: 'VideoSlots', 'LiveDealer', etc.     |
| game_type_id         | INTEGER        | SIM  | —       | ID do tipo de jogo                              |
| game_type_desc       | VARCHAR(255)   | SIM  | —       | Nome do tipo de jogo                            |
| status               | VARCHAR(30)    | SIM  | —       | 'active' ou 'inactive'                          |
| game_technology      | VARCHAR(30)    | SIM  | —       | 'H5' (HTML5), 'F' (Flash)                       |
| has_jackpot          | BOOLEAN        | SIM  | FALSE   | Jogo tem jackpot progressivo                    |
| free_spin_game       | BOOLEAN        | SIM  | FALSE   | Jogo suporta free spins                         |
| feature_trigger_game | BOOLEAN        | SIM  | FALSE   | Jogo tem feature trigger                        |
| snapshot_dt          | DATE           | SIM  | TODAY   | Data do snapshot                                |
| refreshed_at         | TIMESTAMPTZ    | SIM  | NOW()   | Data da ultima atualizacao                      |

**Indices:**
- `idx_dgc_vendor` — B-tree em `vendor_id`
- `idx_dgc_category` — B-tree em `game_category`
- `idx_dgc_status` — B-tree em `status`

**Distribuicao atual (19/03/2026):** 2.718 jogos: 1.989 Slots, 670 Live, 58 sem categoria, 1 Sports.

---

## 6. Dicionario de Dados — CRM e Campanhas

### 6.1. Tabela: fact_crm_daily_performance

**Descricao:** Performance de campanhas CRM. Armazena metricas de funil
(comunicacoes enviadas, abertas, clicadas), financeiras (GGR, NGR, depositos)
e comparativas (ROI, custo, incremento) em colunas JSONB flexiveis.

**Pipeline:** `pipelines/crm_daily_performance.py`
**Fontes:** BigQuery Smartico (funil CRM) + Athena (metricas financeiras)
**Estrategia de carga:** UPSERT (1 linha por campanha + periodo)

| Coluna          | Tipo           | Nulo | Default         | Descricao                                                      |
|-----------------|----------------|------|-----------------|----------------------------------------------------------------|
| id              | SERIAL         | NAO  | auto            | PK auto-incremento                                             |
| campanha_id     | VARCHAR(100)   | NAO  | —               | ID da campanha (ex: 'RETEM_2026_02')                           |
| campanha_name   | VARCHAR(255)   | SIM  | —               | Nome da campanha                                               |
| campanha_start  | DATE           | NAO  | —               | Data de inicio da campanha                                     |
| campanha_end    | DATE           | NAO  | —               | Data de fim da campanha                                        |
| period          | VARCHAR(10)    | NAO  | —               | Periodo de analise: 'BEFORE', 'DURING' ou 'AFTER'             |
| period_start    | DATE           | NAO  | —               | Inicio efetivo do periodo analisado                            |
| period_end      | DATE           | NAO  | —               | Fim efetivo do periodo (dinamico para DURING)                  |
| funil           | JSONB          | SIM  | '{}'::JSONB     | Metricas de funil CRM (ver estrutura abaixo)                   |
| financeiro      | JSONB          | SIM  | '{}'::JSONB     | Metricas financeiras (ver estrutura abaixo)                    |
| comparativo     | JSONB          | SIM  | '{}'::JSONB     | Deltas, custos e ROI (ver estrutura abaixo)                    |
| created_at      | TIMESTAMPTZ    | SIM  | NOW()           | Data de criacao                                                |
| updated_at      | TIMESTAMPTZ    | SIM  | NOW()           | Data da ultima atualizacao                                     |

**Constraint:** `uq_campanha_period` — UNIQUE em `(campanha_id, period)`

**Indices:**
- `idx_fact_crm_campanha` — B-tree em `campanha_id`
- `idx_fact_crm_financeiro_gin` — GIN em `financeiro` (para queries JSONB)
- `idx_fact_crm_comparativo_gin` — GIN em `comparativo` (para queries JSONB)

**Estrutura do JSONB `funil`:**
```json
{
  "comunicacoes_enviadas": 15000,
  "comunicacoes_entregues": 14200,
  "comunicacoes_abertas": 8500,
  "comunicacoes_clicadas": 3200,
  "depositos_pos_click": 1800,
  "canais": {"whatsapp": 5000, "sms": 4000, "push": 6000}
}
```

**Estrutura do JSONB `financeiro`:**
```json
{
  "total_users": 12500,
  "depositos_brl": 850000.00,
  "depositos_qtd": 25000,
  "ggr_brl": 320000.00,
  "btr_brl": 45000.00,
  "rca_brl": 12000.00,
  "ngr_brl": 263000.00,
  "avg_play_days": 3.45,
  "total_sessions": 95000
}
```

**Estrutura do JSONB `comparativo`:**
```json
{
  "ngr_incremental": 50000.00,
  "ngr_variacao_pct": 23.5,
  "custo_whatsapp": 2400.00,
  "custo_sms": 1080.00,
  "custo_push": 360.00,
  "custo_total": 3840.00,
  "roi": 13.02
}
```

---

## 7. Dicionario de Dados — Produto e Front-End

### 7.1. Tabela: grandes_ganhos

**Descricao:** Maiores ganhos do dia, consumidos pelo front-end/API do site
multi.bet. Dados extraidos do BigQuery Smartico e enriquecidos com URLs
de imagem do catalogo de jogos.

**Pipeline:** `pipelines/grandes_ganhos.py`
**Fontes:** BigQuery Smartico (ganhos, jogadores) + Super Nova DB (game_image_mapping)
**Estrategia de carga:** TRUNCATE + INSERT (refresh diario)

| Coluna             | Tipo           | Nulo | Default | Descricao                                                    |
|--------------------|----------------|------|---------|--------------------------------------------------------------|
| id                 | SERIAL         | NAO  | auto    | PK auto-incremento                                           |
| game_name          | VARCHAR(255)   | SIM  | —       | Nome do jogo (ex: 'FORTUNE SNAKE')                           |
| provider_name      | VARCHAR(100)   | SIM  | —       | Nome do provedor (ex: 'PRAGMATICPLAY')                       |
| game_slug          | VARCHAR(200)   | SIM  | —       | Path de acesso ao jogo no site                               |
| game_image_url     | VARCHAR(500)   | SIM  | —       | URL do thumbnail no CDN                                      |
| player_name_hashed | VARCHAR(50)    | SIM  | —       | Nome do jogador hasheado (LGPD, ex: 'Ri***s')               |
| smr_user_id        | BIGINT         | SIM  | —       | ID interno Smartico — NAO expor no front-end                 |
| win_amount         | NUMERIC(15,2)  | SIM  | —       | Valor do ganho em BRL                                        |
| event_time         | TIMESTAMPTZ    | SIM  | —       | Momento do ganho (UTC)                                       |
| refreshed_at       | TIMESTAMPTZ    | SIM  | —       | Data da ultima atualizacao                                   |

**Indices:**
- `idx_gg_event_time` — B-tree em `event_time DESC`

**Query sugerida para API/front-end:**
```sql
SELECT game_name, provider_name, game_slug, game_image_url,
       player_name_hashed, win_amount, event_time
FROM multibet.grandes_ganhos
ORDER BY win_amount DESC
LIMIT 20;
```

---

## 8. Views Derivadas

### 8.1. View: agg_financial_monthly

**Descricao:** Visao mensal consolidada de FTDs. Agrega quantidade e volume
total de FTDs por mes.

**DDL:** `pipelines/ddl_agg_financial_monthly.sql`
**Depende de:** `multibet.fact_ftd_deposits`

```sql
CREATE OR REPLACE VIEW multibet.agg_financial_monthly AS
SELECT
    TO_CHAR(dt, 'YYYY-MM')                          AS month_reference,
    COUNT(DISTINCT c_ecr_id)                         AS total_ftds,
    TO_CHAR(SUM(ftd_amount), 'FM999,999,999.00')     AS ftd_volume_brl
FROM multibet.fact_ftd_deposits
GROUP BY TO_CHAR(dt, 'YYYY-MM')
ORDER BY month_reference DESC;
```

| Coluna           | Tipo    | Descricao                            |
|------------------|---------|--------------------------------------|
| month_reference  | VARCHAR | Mes no formato YYYY-MM               |
| total_ftds       | BIGINT  | Quantidade de FTDs unicos no mes     |
| ftd_volume_brl   | VARCHAR | Volume total em BRL (formatado)      |

---

### 8.2. View: vw_cohort_roi

**Descricao:** ROI por safra de FTD e fonte de trafego. Calcula LTV medio (D30),
taxa de 2o deposito, spend e ROI/payback por canal.

**Depende de:** `multibet.agg_cohort_acquisition` + `multibet.fact_attribution`

| Coluna          | Tipo           | Descricao                                          |
|-----------------|----------------|----------------------------------------------------|
| month_of_ftd    | VARCHAR(7)     | Safra (YYYY-MM)                                    |
| source          | VARCHAR(100)   | Fonte de trafego                                   |
| qty_players     | BIGINT         | Jogadores na safra/fonte                           |
| avg_ftd_amount  | NUMERIC        | FTD medio em BRL                                   |
| total_ggr_d0    | NUMERIC        | GGR total D0                                       |
| total_ggr_d7    | NUMERIC        | GGR total D7                                       |
| total_ggr_d30   | NUMERIC        | GGR total D30                                      |
| avg_ltv_d30     | NUMERIC        | LTV medio D30 por jogador                          |
| pct_2nd_deposit | NUMERIC        | % jogadores com 2o deposito                        |
| monthly_spend   | NUMERIC        | Spend mensal da fonte                              |
| roi_d30_pct     | NUMERIC        | ROI D30 = GGR_D30 / Spend * 100                   |
| payback_ratio   | NUMERIC        | Payback = Spend / GGR_D30                          |

---

### 8.3. View: vw_acquisition_channel

**Descricao:** Visao de canais de aquisicao com tiering automatico.
Agrupa fontes em Direct/Organic, Paid Media, Partnerships e Unmapped.

**Depende de:** `multibet.fact_attribution` + `multibet.dim_marketing_mapping`

| Coluna             | Tipo           | Descricao                                       |
|--------------------|----------------|-------------------------------------------------|
| dt                 | DATE           | Data                                            |
| channel_tier       | VARCHAR        | Tier do canal (Direct/Organic, Paid Media, etc.)|
| source             | VARCHAR(100)   | Fonte de trafego                                |
| qty_registrations  | BIGINT         | Registros                                       |
| qty_ftds           | BIGINT         | FTDs                                            |
| ggr                | NUMERIC        | GGR total em BRL                                |
| marketing_spend    | NUMERIC        | Spend em BRL                                    |
| ftd_rate           | NUMERIC        | Taxa de conversao Reg -> FTD (%)                |
| roas               | NUMERIC        | Return on Ad Spend (GGR / Spend)                |

**Regra de tiering:**
| Fonte                                                    | Tier              |
|----------------------------------------------------------|-------------------|
| organic                                                  | Direct / Organic  |
| google_ads, meta_ads, tiktok_kwai, instagram             | Paid Media        |
| influencers, portais_midia, affiliate_performance        | Partnerships      |
| Qualquer outra                                           | Unmapped          |

---

### 8.4. View: vw_attribution_metrics

**Descricao:** Metricas derivadas de atribuicao. Calcula CPA, CAC, ROAS e ROI%
a partir dos dados da `fact_attribution`.

**Depende de:** `multibet.fact_attribution`

| Coluna             | Tipo           | Descricao                                          |
|--------------------|----------------|----------------------------------------------------|
| dt                 | DATE           | Data                                               |
| c_tracker_id       | VARCHAR(255)   | Tracker de origem                                  |
| qty_registrations  | INTEGER        | Registros                                          |
| qty_ftds           | INTEGER        | FTDs                                               |
| ggr                | NUMERIC(18,2)  | GGR em BRL                                         |
| marketing_spend    | NUMERIC(18,2)  | Spend em BRL                                       |
| cpa                | NUMERIC        | Custo por Aquisicao = Spend / FTDs                 |
| cac                | NUMERIC        | Custo de Aquisicao por Cliente = Spend / Regs      |
| roas               | NUMERIC        | Return on Ad Spend = GGR / Spend                   |
| roi_pct            | NUMERIC        | ROI % = (GGR - Spend) / Spend * 100               |
| refreshed_at       | TIMESTAMPTZ    | Data da ultima atualizacao da tabela base          |

---

## 9. Indices e Constraints

### Resumo de Chaves Primarias

| Tabela                        | Chave Primaria                        | Tipo           |
|-------------------------------|---------------------------------------|----------------|
| dim_marketing_mapping         | tracker_id                            | Natural        |
| dim_affiliate_source          | affiliate_id                          | Natural        |
| dim_crm_friendly_names        | entity_id                             | Natural        |
| game_image_mapping            | id (serial)                           | Surrogate      |
| fact_registrations            | id (serial)                           | Surrogate      |
| fact_ftd_deposits             | id (serial)                           | Surrogate      |
| fact_redeposits               | c_ecr_id                              | Natural        |
| fact_attribution              | (dt, c_tracker_id)                    | Composta       |
| agg_cohort_acquisition        | c_ecr_id                              | Natural        |
| fact_player_activity          | dt                                    | Natural        |
| fact_player_engagement_daily  | c_ecr_id                              | Natural        |
| fact_gaming_activity_daily    | (dt, c_tracker_id)                    | Composta       |
| fact_casino_rounds            | (dt, game_id)                         | Composta       |
| fct_casino_activity           | dt                                    | Natural        |
| fct_sports_activity           | dt                                    | Natural        |
| fact_sports_bets              | (dt, sport_name)                      | Composta       |
| fact_sports_open_bets         | (snapshot_dt, sport_name)             | Composta       |
| fact_live_casino              | (dt, game_id)                         | Composta       |
| fact_jackpots                 | (month_start, game_id)                | Composta       |
| agg_game_performance          | (week_start, game_id)                 | Composta       |
| dim_games_catalog             | game_id                               | Natural        |
| fact_crm_daily_performance    | id (serial)                           | Surrogate      |
| grandes_ganhos                | id (serial)                           | Surrogate      |

### Resumo de Constraints UNIQUE

| Tabela                     | Constraint                    | Colunas                   |
|----------------------------|-------------------------------|---------------------------|
| game_image_mapping         | uq_game_name_upper            | game_name_upper           |
| fact_registrations         | idx_fr_ecr_unique             | c_ecr_id                  |
| fact_ftd_deposits          | idx_fftd_ecr_unique           | c_ecr_id                  |
| fact_crm_daily_performance | uq_campanha_period            | (campanha_id, period)     |

### Resumo de Indices

| Tabela                        | Indice                          | Tipo   | Colunas                    |
|-------------------------------|---------------------------------|--------|----------------------------|
| dim_marketing_mapping         | idx_dmm_source_name             | B-tree | source_name                |
| dim_marketing_mapping         | idx_dmm_validated               | B-tree | is_validated               |
| dim_marketing_mapping         | idx_dmm_aff_id                  | B-tree | affiliate_id               |
| dim_affiliate_source          | idx_das_fonte                   | B-tree | fonte_trafego              |
| game_image_mapping            | idx_gim_game_name_upper         | B-tree | game_name_upper            |
| fact_registrations            | idx_fr_dt                       | B-tree | dt                         |
| fact_registrations            | idx_fr_ecr_id                   | B-tree | c_ecr_id                   |
| fact_registrations            | idx_fr_tracker                  | B-tree | c_tracker_id               |
| fact_ftd_deposits             | idx_fftd_dt                     | B-tree | dt                         |
| fact_ftd_deposits             | idx_fftd_ecr_id                 | B-tree | c_ecr_id                   |
| fact_attribution              | —                               | PK     | (dt, c_tracker_id)         |
| fact_casino_rounds            | idx_fcr_vendor                  | B-tree | (vendor_id, dt)            |
| fact_casino_rounds            | idx_fcr_ggr                     | B-tree | (dt, ggr_total DESC)       |
| fact_casino_rounds            | idx_fcr_category                | B-tree | (game_category, dt)        |
| fact_sports_bets              | idx_fsb_ggr                     | B-tree | (dt, ggr DESC)             |
| fact_live_casino              | idx_flc_ggr                     | B-tree | (dt, ggr_total DESC)       |
| fact_jackpots                 | idx_fj_month                    | B-tree | month_start                |
| fact_jackpots                 | idx_fj_impact                   | B-tree | (month_start, jackpot_impact_pct DESC) |
| agg_game_performance          | idx_agp_rank                    | B-tree | (week_start, ggr_rank)     |
| dim_games_catalog             | idx_dgc_vendor                  | B-tree | vendor_id                  |
| dim_games_catalog             | idx_dgc_category                | B-tree | game_category              |
| dim_games_catalog             | idx_dgc_status                  | B-tree | status                     |
| fact_crm_daily_performance    | idx_fact_crm_campanha           | B-tree | campanha_id                |
| fact_crm_daily_performance    | idx_fact_crm_financeiro_gin     | GIN    | financeiro                 |
| fact_crm_daily_performance    | idx_fact_crm_comparativo_gin    | GIN    | comparativo                |
| grandes_ganhos                | idx_gg_event_time               | B-tree | event_time DESC            |

---

**Fim do Documento**

*Schema MultiBet Database v1.1 — Super Nova Gaming*
*Criado em 18-19/03/2026 por Mateus Fabro — Squad Intelligence Engine*
*Atualizado em 19/03/2026 — Dominio Produto e Performance de Jogos (6 tabelas novas/atualizadas)*
