# Mapeamento Bronze - SELECTs para KPIs
## ETL SuperNovaDB - Colunas Necessarias (Validadas 20/03/2026)

**DOMINIO: PRODUTO E PERFORMANCE DE JOGOS (Tabelas 18-23)**
**Responsavel:** Mateus Fabro - Squad Intelligence Engine
**Referencia:** bronze_selects_kpis_FINAL.pdf (Mauro K.)

---

## Arquitetura de Camadas

```
ATHENA (Iceberg)                    SUPER NOVA DB (PostgreSQL)
+----------------------------+      +---------------------------+
| ps_bi                      |      |                           |
|   fct_casino_activity_daily| ---> | multibet_bronze (RAW)     |
|   dim_game                 |      | Sem calculos, auditavel   |
|                            |      | Dados brutos replicados   |
| bireports_ec2              |      +---------------------------+
|   tbl_vendor_games_mapping |      |
|   tbl_ecr_gaming_sessions  |      | 7 tabelas novas           |
|   tbl_ecr (test users)     |      | + 5 reutilizadas (Mauro)  |
|                            |      |                           |
| vendor_ec2                 |      +---------------------------+
|   tbl_sports_book_bets_info|
|   tbl_sports_book_bet_dtls |
|   tbl_vendor_games_mapping |
+----------------------------+
```

**Fonte unica: Athena (Iceberg Data Lake).** BigQuery nao se aplica a este dominio.
**Principio:** Dados brutos primeiro, sem calculos. Toda coluna rastreavel ate a origem.

---

## BRONZE 1: Atividade Casino por Jogo/Dia (ps_bi)

**Fonte Athena:** `ps_bi.fct_casino_activity_daily`
**Destino Bronze:** `multibet_bronze.fct_casino_activity_daily`
**Alimenta:** fact_casino_rounds, fact_live_casino, fact_jackpots, agg_game_performance
**Colunas validadas:** 53 colunas (discovery 19/03/2026)

```sql
-- Bronze: multibet_bronze.fct_casino_activity_daily
-- Fonte: ps_bi.fct_casino_activity_daily (Athena, BRL, pre-agregado dbt)
-- Grao: player_id x game_id x activity_date
SELECT
    player_id,                          -- bigint, = c_ecr_id (18 digitos)
    partner_id,                         -- varchar, = 'multibet'
    product_id,                         -- varchar, 'casino' ou 'sports_book'
    game_id,                            -- varchar, ID do jogo (join com dim_game)
    activity_date,                      -- date, data UTC

    -- Financeiro TOTAL (BRL, ja convertido)
    bet_amount_local,                   -- double, turnover total
    win_amount_local,                   -- double, pagamentos totais
    ggr_local,                          -- double, GGR pre-calculado (bet - win)

    -- Separacao Real vs Bonus (BRL)
    real_bet_amount_local,              -- double, apostas dinheiro real
    real_win_amount_local,              -- double, ganhos dinheiro real
    bonus_bet_amount_local,             -- double, apostas com bonus
    bonus_win_amount_local,             -- double, ganhos com bonus

    -- Contagem de rodadas
    bet_count,                          -- bigint, total de apostas
    real_bet_count,                     -- bigint, apostas real
    bonus_bet_count,                    -- bigint, apostas bonus

    -- Cancelamentos
    realbet_canceled_amount_local,      -- double
    bonusbet_canceled_amount_local,     -- double
    bet_canceled_count,                 -- bigint
    cancel_real_bet_count,              -- bigint
    cancel_bonus_bet_count,             -- bigint

    -- Jackpot
    jackpot_win_amount_local,           -- double, jackpots pagos
    jackpot_contribution_local,         -- double, contribuicoes ao pot

    -- Free Spins
    free_spins_bet_amount_local,        -- double
    free_spins_win_amount_local,        -- double
    free_spins_bet_count,               -- bigint

    -- Metadados
    sub_vendor_id,                      -- varchar, sub-provedor
    game_type,                          -- integer, tipo jogo
    sub_product_id,                     -- varchar
    session_status                      -- integer

FROM ps_bi.fct_casino_activity_daily
WHERE activity_date >= DATE '2025-10-01';
```

**Volume estimado:** ~2.5M linhas (102K jogos x dias, expandido por player)
**Frequencia de carga:** Diaria (TRUNCATE + INSERT ou incremental por activity_date)

---

## BRONZE 2: Catalogo de Jogos (ps_bi)

**Fonte Athena:** `ps_bi.dim_game`
**Destino Bronze:** `multibet_bronze.dim_game`
**Alimenta:** fact_casino_rounds, fact_live_casino, agg_game_performance, dim_games_catalog
**Colunas validadas:** 11 colunas

```sql
-- Bronze: multibet_bronze.dim_game
-- Fonte: ps_bi.dim_game (Athena, catalogo dbt)
-- Grao: game_id (1 linha por jogo)
SELECT
    game_id,                            -- varchar, PK
    vendor_id,                          -- varchar, provedor (alea_pgsoft, pragmaticplay, etc.)
    product_id,                         -- varchar, 'casino' ou 'sports_book'
    game_desc,                          -- varchar, nome do jogo
    game_type_id,                       -- integer, ID tipo
    game_type_desc,                     -- varchar, descricao tipo
    game_category,                      -- varchar, 'Slots' | 'Live' | 'altenar-category'
    game_category_desc,                 -- varchar, 'VideoSlots' | 'LiveDealer' | etc.
    status,                             -- varchar, 'active' | 'inactive'
    updated_time                        -- timestamp+tz
FROM ps_bi.dim_game;
```

**Volume estimado:** ~380 linhas (jogos ativos)
**NOTA:** ps_bi.dim_game tem apenas ~380 jogos. Para catalogo completo (2.718),
usar tambem `bireports_ec2.tbl_vendor_games_mapping_data` (Bronze 3).

---

## BRONZE 3: Catalogo Completo + Flags (vendor_ec2 + bireports_ec2)

**Fonte Athena:** `vendor_ec2.tbl_vendor_games_mapping_mst` + `bireports_ec2.tbl_vendor_games_mapping_data`
**Destino Bronze:** `multibet_bronze.tbl_vendor_games_mapping`
**Alimenta:** dim_games_catalog (flags jackpot, freespin), fact_live_casino (categorias Live)

```sql
-- Bronze: multibet_bronze.tbl_vendor_games_mapping
-- Fonte: vendor_ec2.tbl_vendor_games_mapping_mst (Athena, catalogo master)
-- Grao: c_game_id x c_vendor_id x c_client_platform (deduplicar depois)
SELECT
    c_id,                               -- bigint, auto-increment
    c_vendor_id,                        -- varchar, provedor
    c_sub_vendor_id,                    -- varchar, sub-provedor
    c_game_id,                          -- varchar, ID jogo
    c_game_desc,                        -- varchar, nome jogo
    c_game_cat_id,                      -- integer, ID categoria
    c_game_type_id,                     -- integer, ID tipo
    c_game_category_desc,               -- varchar, descricao categoria
    c_game_type_desc,                   -- varchar, descricao tipo
    c_product_id,                       -- varchar, 'CASINO' ou 'SPORTS_BOOK'
    c_game_technology,                  -- varchar, 'H5' | 'F'
    c_client_platform,                  -- varchar, 'WEB' | 'MOBILE'
    c_status,                           -- varchar, 'active' | 'inactive'
    c_has_jackpot,                      -- varchar, '0' | '1'
    c_free_spin_game,                   -- boolean
    c_feature_trigger_game,             -- boolean
    c_flexible_game,                    -- boolean
    c_updated_time                      -- timestamp+tz
FROM vendor_ec2.tbl_vendor_games_mapping_mst;
```

**Volume estimado:** ~2.700 linhas
**Complementar com bireports para categorias:**

```sql
-- Bronze complementar: multibet_bronze.tbl_vendor_games_bireports
-- Fonte: bireports_ec2.tbl_vendor_games_mapping_data (Athena)
-- Mais completo em categorias (Slots/Live/etc.)
SELECT
    c_id,
    c_vendor_id,
    c_game_id,
    c_game_desc,
    c_game_cat_id,
    c_game_type_id,
    c_game_category_desc,
    c_game_type_desc,
    c_product_id,
    c_game_technology,
    c_client_platform,
    c_status,
    c_game_category                     -- varchar, 'Slots' | 'Live' | etc.
FROM bireports_ec2.tbl_vendor_games_mapping_data;
```

---

## BRONZE 4: Apostas Esportivas - Header (vendor_ec2)

**Fonte Athena:** `vendor_ec2.tbl_sports_book_bets_info`
**Destino Bronze:** `multibet_bronze.tbl_sports_book_bets_info`
**Alimenta:** fact_sports_bets, fact_sports_open_bets
**Colunas validadas:** 22 colunas (discovery 19/03/2026)

```sql
-- Bronze: multibet_bronze.tbl_sports_book_bets_info
-- Fonte: vendor_ec2.tbl_sports_book_bets_info (Athena, BRL real)
-- Grao: c_bet_slip_id x c_transaction_type
-- ATENCAO: c_total_odds e VARCHAR, c_bet_slip_state e BOOLEAN
SELECT
    c_customer_id,                      -- bigint, = ECR External ID
    c_bet_slip_id,                      -- varchar, PK bilhete
    c_bet_type,                         -- varchar, 'PreLive' | 'Live' | 'Mixed'
    c_bet_slip_state,                   -- boolean (NAO 'C'/'O')
    c_bet_id,                           -- varchar
    c_bet_state,                        -- varchar, 'O'|'C'|'W'|'L'
    c_is_free,                          -- boolean, aposta gratis
    c_is_live,                          -- boolean, ao vivo
    c_total_return,                     -- decimal(10,2), BRL real
    c_total_stake,                      -- decimal(10,2), BRL real
    c_total_odds,                       -- VARCHAR (precisa TRY_CAST!)
    c_unit_stake,                       -- decimal(10,2)
    c_bonus_amount,                     -- decimal(10,2)
    c_transaction_type,                 -- varchar, 'M'(place) | 'P'(settle)
    c_transaction_id,                   -- varchar
    c_fees,                             -- varchar
    c_fees_type,                        -- varchar
    c_created_time,                     -- timestamp+tz, data criacao
    c_bet_closure_time,                 -- timestamp+tz, data liquidacao
    c_updated_time                      -- timestamp+tz
FROM vendor_ec2.tbl_sports_book_bets_info
WHERE c_created_time >= TIMESTAMP '2025-10-01';
```

**Volume estimado:** ~500K linhas
**NOTA:** Tabela SEM particao no Athena — query leva 15-20 min. Considerar carga incremental.

---

## BRONZE 5: Apostas Esportivas - Detalhes/Legs (vendor_ec2)

**Fonte Athena:** `vendor_ec2.tbl_sports_book_bet_details`
**Destino Bronze:** `multibet_bronze.tbl_sports_book_bet_details`
**Alimenta:** fact_sports_bets (nome esporte, torneio, pre/live por leg)
**Colunas validadas:** 36 colunas (discovery 19/03/2026)

```sql
-- Bronze: multibet_bronze.tbl_sports_book_bet_details
-- Fonte: vendor_ec2.tbl_sports_book_bet_details (Athena)
-- Grao: c_bet_slip_id x c_selection_id (multiplos legs por bilhete)
-- ATENCAO: esporte = c_sport_type_name (NAO c_sport_name!)
SELECT
    c_customer_id,                      -- bigint
    c_bet_slip_id,                      -- varchar, JOIN com bets_info
    c_bet_id,                           -- varchar
    c_event_id,                         -- varchar
    c_event_name,                       -- varchar, ex: 'FC Midtjylland vs. Forest'
    c_market_id,                        -- varchar
    c_market_name,                      -- varchar, ex: 'Vencedor do encontro'
    c_selection_id,                     -- varchar
    c_selection_name,                   -- varchar, ex: 'Empate'
    c_sport_id,                         -- varchar, ID numerico esporte
    c_sport_type_id,                    -- varchar
    c_sport_type_name,                  -- varchar, NOME ESPORTE ('Futebol')
    c_tournament_id,                    -- varchar
    c_tournament_name,                  -- varchar, liga/torneio
    c_odds,                             -- decimal(10,2), odds do leg
    c_is_live,                          -- boolean, leg ao vivo
    c_leg_status,                       -- varchar, 'L'(lost)|'O'(open)|'W'(won)
    c_leg_settlement_date,              -- timestamp+tz
    c_vs_participant_home,              -- varchar, time casa
    c_vs_participant_away,              -- varchar, time fora
    c_is_4in_running,                   -- boolean
    c_ts_realstart,                     -- varchar, inicio evento
    c_ts_realend,                       -- varchar, fim evento
    c_created_time,                     -- timestamp+tz
    c_bet_slip_closure_time             -- timestamp+tz
FROM vendor_ec2.tbl_sports_book_bet_details
WHERE c_created_time >= TIMESTAMP '2025-10-01';
```

**Volume estimado:** ~1-2M linhas (multiplos legs por bilhete)
**NOTA:** Tabela PESADA, sem particao. Carga incremental recomendada.

---

## BRONZE 6: Sessoes de Jogo (bireports_ec2)

**Fonte Athena:** `bireports_ec2.tbl_ecr_gaming_sessions`
**Destino Bronze:** `multibet_bronze.tbl_ecr_gaming_sessions`
**Alimenta:** fact_live_casino (duracao sessao, rodadas por sessao)
**Colunas validadas:** 23 colunas (discovery 19/03/2026)

```sql
-- Bronze: multibet_bronze.tbl_ecr_gaming_sessions
-- Fonte: bireports_ec2.tbl_ecr_gaming_sessions (Athena)
-- Grao: c_game_session_id (1 linha por sessao)
-- NOTA: ICEBERG_BAD_DATA em 20/03/2026 — monitorar estabilidade
SELECT
    c_game_session_id,                  -- bigint, PK
    c_login_session_id,                 -- varchar, sessao login (UUID)
    c_ecr_id,                           -- bigint, player ID
    c_partner_id,                       -- varchar
    c_product_id,                       -- varchar, 'CASINO'
    c_game_id,                          -- varchar, ID jogo
    c_game_desc,                        -- varchar, nome jogo
    c_game_type,                        -- integer
    c_game_category,                    -- integer (ID, NAO nome!)
    c_vendor_id,                        -- varchar
    c_game_played_count,                -- integer, rodadas na sessao
    c_session_start_time,               -- timestamp+tz, inicio UTC
    c_session_end_time,                 -- timestamp+tz, fim UTC
    c_last_bet_end_time,                -- timestamp+tz
    c_session_active,                   -- boolean, sessao ativa?
    c_session_length_in_sec,            -- integer, duracao EXATA em segundos
    c_comments                          -- varchar, motivo encerramento
FROM bireports_ec2.tbl_ecr_gaming_sessions
WHERE c_session_start_time >= TIMESTAMP '2025-10-01';
```

**Volume estimado:** ~3-5M linhas
**STATUS (20/03/2026):** Erro ICEBERG_BAD_DATA — infra do Iceberg instavel.
Carga pendente ate estabilizar.

---

## BRONZE 7: Filtro Test Users (ecr_ec2)

**Fonte Athena:** `bireports_ec2.tbl_ecr`
**Destino Bronze:** `multibet_bronze.tbl_ecr`
**Alimenta:** TODAS as tabelas (filtro universal c_test_user = false)
**Ja mapeado por Mauro (item 25 do doc dele)** — reutilizar.

```sql
-- Bronze: multibet_bronze.tbl_ecr
-- Fonte: bireports_ec2.tbl_ecr (Athena)
-- Grao: c_ecr_id (1 linha por player)
SELECT
    c_ecr_id,                           -- bigint, PK interno
    c_external_id,                      -- bigint, = Smartico user_ext_id
    c_test_user,                        -- boolean, FILTRO OBRIGATORIO
    c_sign_up_time,                     -- timestamp+tz
    c_last_login_time,                  -- timestamp+tz
    c_tracker_id,                       -- varchar
    c_affiliate_id,                     -- bigint
    c_country_code                      -- varchar
FROM bireports_ec2.tbl_ecr;
```

---

## RESUMO IMPLEMENTACAO

### ATHENA SOURCES (7 tabelas Bronze para este dominio):

| # | Database | Tabela Origem | Destino Bronze | KPIs que alimenta |
|---|----------|---------------|----------------|-------------------|
| 1 | ps_bi | fct_casino_activity_daily | multibet_bronze.fct_casino_activity_daily | GGR/jogo, Hold Rate, RTP, rodadas, jackpots |
| 2 | ps_bi | dim_game | multibet_bronze.dim_game | Catalogo jogos, categorias Slots/Live |
| 3 | vendor_ec2 | tbl_vendor_games_mapping_mst | multibet_bronze.tbl_vendor_games_mapping | Flags jackpot, freespin, feature trigger |
| 4 | bireports_ec2 | tbl_vendor_games_mapping_data | multibet_bronze.tbl_vendor_games_bireports | Categorias Live/Slots completas |
| 5 | vendor_ec2 | tbl_sports_book_bets_info | multibet_bronze.tbl_sports_book_bets_info | Turnover, GGR, margin, pre/live, odds |
| 6 | vendor_ec2 | tbl_sports_book_bet_details | multibet_bronze.tbl_sports_book_bet_details | Esporte, torneio, legs, odds por selecao |
| 7 | bireports_ec2 | tbl_ecr_gaming_sessions | multibet_bronze.tbl_ecr_gaming_sessions | Sessoes live casino, duracao, rodadas |

**Reutilizados do mapeamento do Mauro (ja existentes):**
- `multibet_bronze.tbl_ecr` (bireports_ec2.tbl_ecr) — filtro test users
- `multibet_bronze.tbl_ecr_flags` (ecr_ec2.tbl_ecr_flags) — flags
- `multibet_bronze.tbl_real_fund_txn` (fund_ec2) — transacoes gaming
- `multibet_bronze.tbl_realcash_sub_fund_txn` (fund_ec2) — sub-fund real
- `multibet_bronze.tbl_bonus_sub_fund_txn` (fund_ec2) — sub-fund bonus

### TOTAL: 7 tabelas Bronze novas + 5 reutilizadas do Mauro = 12 tabelas Bronze

**Objetivo: Ingestao de dados brutos sem calculos — 100% auditavel**

---

## PENDENCIAS E VALIDACOES

| Item | Status | Nota |
|------|--------|------|
| ps_bi.fct_casino_activity_daily | OK | Validado, 53 cols |
| ps_bi.dim_game | PARCIAL | Apenas ~380 jogos (catalogo completo em bireports_ec2) |
| vendor_ec2 sportsbook | LENTO | Sem particao, 15-20 min por query. Carga incremental necessaria |
| bireports_ec2.gaming_sessions | ERRO | ICEBERG_BAD_DATA (20/03/2026). Aguardar estabilizacao |
| Validacao GGR ps_bi vs fund_ec2 | PENDENTE | Comparar ggr_local do ps_bi com calculo sub-fund isolation |
| Sport name por esporte | PENDENTE | Requer bet_details (pesado). Implementar incremental |
| Jackpots | MINIMO | ps_bi mostra dados minimos de jackpot. Verificar se ha atividade real |

---

*Mapeamento Bronze - Produto e Performance de Jogos v1.0*
*Criado em 20/03/2026 por Mateus Fabro - Squad Intelligence Engine*
*Baseado no padrao bronze_selects_kpis_FINAL.pdf (Mauro K.)*
