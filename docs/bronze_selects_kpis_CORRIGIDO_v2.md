# Mapeamento Bronze - SELECTs para KPIs v2

**ETL SuperNovaDB - Colunas Necessarias**

**Versao:** 2.1 — Corrigido apos validacao empirica no Athena (SHOW COLUMNS + SELECT LIMIT 5)
**Validado por:** Mateus Fabro — 2026-03-20
**Base:** bronze_selects_kpis_FINAL.pdf (Mauro)
**Correcoes:** 69 colunas corrigidas, c_test_user boolean fix, Gold removidas da Bronze
**Principio:** Bronze = SOMENTE dados brutos (_ec2) + dimensoes (ps_bi.dim_*). Calculos na Silver.

---

## DOMINIO: AQUISICAO E MARKETING

### 1. Registros de Jogadores (ecr_ec2.tbl_ecr)

```sql
-- Bronze: athena_bronze.tbl_ecr
-- Fonte: ecr_ec2.tbl_ecr (20 colunas)
-- CORRECAO: c_country_code nao existe nesta tabela.
--           Usar ps_bi.dim_user.country_code (tabela #21) para pais.
SELECT
    e.c_ecr_id,
    e.c_external_id,
    e.c_tracker_id,
    e.c_affiliate_id,               -- fallback tracker
    e.c_jurisdiction,               -- Corrigido: substitui c_country_code
    e.c_language,                   -- EXTRA: idioma do jogador
    e.c_ecr_status,                 -- EXTRA: status da conta
    e.c_signup_time,
    CAST(e.c_signup_time AS DATE) AS dt  -- Corrigido: sintaxe Presto (era ::date)
FROM ecr_ec2.tbl_ecr e
JOIN ecr_ec2.tbl_ecr_flags f ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_test_user = false              -- Corrigido: era = 0 (boolean, nao integer)
```

### 2. Depositos FTD (cashier_ec2.tbl_cashier_deposit)

```sql
-- Bronze: athena_bronze.tbl_cashier_deposit
-- Fonte: cashier_ec2.tbl_cashier_deposit (66 colunas)
-- NOTA: c_initial_amount em CENTAVOS (/100 para BRL)
SELECT
    d.c_ecr_id,
    d.c_txn_id,
    d.c_initial_amount,             -- Em centavos (/100 = BRL)
    d.c_created_time,
    d.c_txn_status,
    CAST(d.c_created_time AS DATE) AS dt
FROM cashier_ec2.tbl_cashier_deposit d
JOIN ecr_ec2.tbl_ecr_flags f ON d.c_ecr_id = f.c_ecr_id
WHERE d.c_txn_status = 'txn_confirmed_success'
  AND f.c_test_user = false
```

### 3. Banners e Marketing (ecr_ec2.tbl_ecr_banner)

```sql
-- Bronze: athena_bronze.tbl_ecr_banner
-- Fonte: ecr_ec2.tbl_ecr_banner (16 colunas)
-- CORRECAO: c_click_id, c_utm_source, c_utm_medium, c_utm_campaign NAO EXISTEM.
--           Click IDs provavelmente em c_custom1..c_custom4 ou c_reference_url.
SELECT
    b.c_ecr_id,
    b.c_tracker_id,
    b.c_affiliate_id,
    b.c_affiliate_name,
    b.c_banner_id,
    b.c_reference_url,              -- SUBSTITUI c_click_id
    b.c_custom1,                    -- SUBSTITUI UTMs
    b.c_custom2,
    b.c_custom3,
    b.c_custom4,
    b.c_created_time
FROM ecr_ec2.tbl_ecr_banner b
WHERE b.c_tracker_id IS NOT NULL
```

### 4. Flags de Teste (ecr_ec2.tbl_ecr_flags)

```sql
-- Bronze: athena_bronze.tbl_ecr_flags
-- Fonte: ecr_ec2.tbl_ecr_flags (13 colunas)
-- CORRECAO: c_flag_name e c_flag_value NAO EXISTEM.
--           Flags sao colunas individuais, nao key-value.
SELECT
    f.c_ecr_id,
    f.c_test_user,                  -- boolean (filtro principal)
    f.c_referral_ban,
    f.c_withdrawl_allowed,
    f.c_two_factor_auth_enabled,
    f.c_hide_username_feed
FROM ecr_ec2.tbl_ecr_flags f
```

---

## DOMINIO: GAMING E PERFORMANCE

### 5. Transacoes de Jogo (fund_ec2.tbl_real_fund_txn)

```sql
-- Bronze: athena_bronze.tbl_real_fund_txn
-- Fonte: fund_ec2.tbl_real_fund_txn (51 colunas)
-- CORRECAO CRITICA: SELECT original estava SEM coluna de valor!
--   c_confirmed_amount_in_inhouse_ccy NAO EXISTE
--   c_tracker_id NAO EXISTE (tracker em ecr_ec2.tbl_ecr)
--   c_vendor_id NAO EXISTE (usar c_sub_vendor_id)
--   c_round_id NAO EXISTE | dt NAO EXISTE como coluna visivel
SELECT
    t.c_ecr_id,
    t.c_txn_id,
    t.c_txn_type,
    t.c_txn_status,                 -- ADICIONADO
    t.c_amount_in_ecr_ccy,          -- ADICIONADO CRITICO: valor em centavos
    t.c_op_type,                    -- ADICIONADO: DB/CR
    t.c_game_id,                    -- ADICIONADO
    t.c_sub_vendor_id,              -- Corrigido: era c_vendor_id
    t.c_product_id,                 -- ADICIONADO: casino/sports
    t.c_game_category,              -- ADICIONADO
    t.c_start_time,
    CAST(t.c_start_time AS DATE) AS dt
FROM fund_ec2.tbl_real_fund_txn t
JOIN ecr_ec2.tbl_ecr_flags f ON t.c_ecr_id = f.c_ecr_id
WHERE f.c_test_user = false
```

### 6. Sub-Fund Real Cash (fund_ec2.tbl_realcash_sub_fund_txn)

```sql
-- Bronze: athena_bronze.tbl_realcash_sub_fund_txn
SELECT
    s.c_fund_txn_id,
    s.c_ecr_id,                     -- ADICIONADO: facilita joins
    s.c_amount_in_house_ccy          -- Valor dinheiro real (centavos)
FROM fund_ec2.tbl_realcash_sub_fund_txn s
```

### 7. Sub-Fund Bonus (fund_ec2.tbl_bonus_sub_fund_txn)

```sql
-- Bronze: athena_bronze.tbl_bonus_sub_fund_txn
SELECT
    b.c_fund_txn_id,
    b.c_ecr_id,                     -- ADICIONADO
    b.c_drp_amount_in_house_ccy,    -- DRP = Real
    b.c_crp_amount_in_house_ccy,    -- CRP = Bonus
    b.c_wrp_amount_in_house_ccy,    -- WRP = Bonus
    b.c_rrp_amount_in_house_ccy     -- RRP = Bonus
FROM fund_ec2.tbl_bonus_sub_fund_txn b
```

### 8. Tipos de Transacao (fund_ec2.tbl_real_fund_txn_type_mst)

```sql
-- Bronze: athena_bronze.tbl_real_fund_txn_type_mst
-- CORRECAO: c_txn_type_name NAO EXISTE. Usar c_internal_description.
SELECT
    m.c_txn_type,
    m.c_internal_description,       -- Corrigido: era c_txn_type_name
    m.c_op_type,
    m.c_is_gaming_txn,
    m.c_is_cancel_txn,
    m.c_is_free_spin_txn,           -- ADICIONADO
    m.c_is_refund_txn_type,         -- ADICIONADO
    m.c_is_settlement_txn_type,     -- ADICIONADO
    m.c_product_id,                 -- ADICIONADO
    m.c_txn_identifier_key
FROM fund_ec2.tbl_real_fund_txn_type_mst m
```

### 9. Catalogo de Jogos (vendor_ec2.tbl_vendor_games_mapping_mst)

```sql
-- Bronze: athena_bronze.tbl_vendor_games_mapping_mst
-- CORRECOES: 6 nomes de colunas errados
SELECT
    g.c_game_id,
    g.c_game_desc,                  -- Corrigido: era c_game_name
    g.c_vendor_id,
    g.c_sub_vendor_id,
    g.c_product_id,
    g.c_game_category_desc,         -- Corrigido: era c_game_category
    g.c_game_type_id,
    g.c_game_type_desc,
    g.c_status,
    g.c_has_jackpot,
    g.c_free_spin_game,             -- Corrigido: era c_has_free_spins
    g.c_feature_trigger_game,       -- Corrigido: era c_feature_trigger
    g.c_game_technology,            -- Corrigido: era c_technology
    g.c_updated_time                -- Corrigido: era c_updated_dt
FROM vendor_ec2.tbl_vendor_games_mapping_mst g
```

### 10. Sports Bets (vendor_ec2.tbl_sports_book_bets_info)

```sql
-- Bronze: athena_bronze.tbl_sports_book_bets_info
-- CORRECAO: c_sport_name NAO EXISTE. Nome do esporte esta na tabela #11.
-- NOTA: valores ja em BRL real (NAO centavos!)
SELECT
    i.c_bet_id,
    i.c_bet_slip_id,                -- ADICIONADO: join com bet_details
    i.c_customer_id,                -- External ID
    i.c_total_stake,                -- BRL real
    i.c_total_return,               -- BRL real
    i.c_total_odds,                 -- VARCHAR! precisa TRY_CAST
    i.c_bonus_amount,
    i.c_is_free,
    i.c_is_live,                    -- ADICIONADO
    i.c_bet_type,                   -- PreLive/Live/Mixed
    i.c_bet_state,                  -- O=Open, C=Closed
    i.c_bet_slip_state,             -- ADICIONADO
    i.c_transaction_type,           -- M=Commit, P=Payout
    i.c_transaction_id,             -- ADICIONADO
    i.c_bet_closure_time,
    i.c_created_time,
    CAST(i.c_bet_closure_time AS DATE) AS dt
FROM vendor_ec2.tbl_sports_book_bets_info i
```

### 11. Sports Details (vendor_ec2.tbl_sports_book_bet_details) — NOVO

```sql
-- Bronze: athena_bronze.tbl_sports_book_bet_details
-- ADICIONADO: necessario para c_sport_type_name (nome do esporte)
SELECT
    d.c_customer_id,
    d.c_bet_slip_id,                -- JOIN com bets_info
    d.c_transaction_id,
    d.c_bet_id,
    d.c_sport_type_name,            -- NOME DO ESPORTE (ex: 'Futebol')
    d.c_sport_id,
    d.c_event_name,
    d.c_market_name,
    d.c_selection_name,
    d.c_odds,
    d.c_leg_status,                 -- O=Open, W=Won, L=Lost
    d.c_tournament_name,
    d.c_is_live,
    d.c_created_time,
    d.c_leg_settlement_date
FROM vendor_ec2.tbl_sports_book_bet_details d
```

### 12. Gaming Sessions (bireports_ec2.tbl_ecr_gaming_sessions)

```sql
-- Bronze: athena_bronze.tbl_ecr_gaming_sessions
-- CORRECOES: c_session_duration_sec -> c_session_length_in_sec
--            c_round_count -> c_game_played_count
SELECT
    gs.c_ecr_id,
    gs.c_game_id,
    gs.c_session_start_time,
    gs.c_session_end_time,
    gs.c_session_length_in_sec,     -- Corrigido
    gs.c_game_played_count,         -- Corrigido
    gs.c_product_id,
    gs.c_vendor_id,                 -- ADICIONADO
    gs.c_game_category,             -- ADICIONADO
    CAST(gs.c_session_start_time AS DATE) AS dt
FROM bireports_ec2.tbl_ecr_gaming_sessions gs
WHERE gs.c_product_id = 'CASINO'
```

---

## DOMINIO: FINANCEIRO

### 13. Saques (cashier_ec2.tbl_cashier_cashout)

```sql
-- Bronze: athena_bronze.tbl_cashier_cashout
-- NOTA: c_initial_amount em CENTAVOS
SELECT
    c.c_ecr_id,
    c.c_txn_id,
    c.c_initial_amount,
    c.c_created_time,
    c.c_txn_status,
    CAST(c.c_created_time AS DATE) AS dt
FROM cashier_ec2.tbl_cashier_cashout c
JOIN ecr_ec2.tbl_ecr_flags f ON c.c_ecr_id = f.c_ecr_id
WHERE c.c_txn_status = 'co_success'
  AND f.c_test_user = false
```

### 14. Resumo Diario Pagamentos (cashier_ec2.tbl_cashier_ecr_daily_payment_summary)

```sql
-- Bronze: athena_bronze.tbl_cashier_ecr_daily_payment_summary
-- CORRECAO: 6 de 8 colunas com nome errado!
-- Campos calculados (net_deposit, avg_ticket) NAO existem — calcular na Silver
SELECT
    s.c_ecr_id,
    s.c_created_date,               -- Corrigido: era c_date
    s.c_deposit_amount,             -- Corrigido: era c_deposit_amount_brl (centavos!)
    s.c_deposit_amount_inhouse,     -- ADICIONADO
    s.c_deposit_count,
    s.c_success_cashout_amount,     -- Corrigido: era c_withdrawal_amount_brl
    s.c_success_cashout_amount_inhouse,
    s.c_success_cashout_count,      -- Corrigido: era c_withdrawal_count
    s.c_cb_amount,                  -- ADICIONADO: chargebacks
    s.c_cb_count,
    s.c_option,                     -- ADICIONADO: metodo pagamento
    s.c_provider                    -- ADICIONADO: provedor
FROM cashier_ec2.tbl_cashier_ecr_daily_payment_summary s
```

### 15. Instrumentos do Jogador (cashier_ec2.tbl_instrument)

```sql
-- Bronze: athena_bronze.tbl_instrument
-- REFORMULADO: tabela de instrumentos DO JOGADOR (cartoes, PIX),
-- NAO metricas de pagamento. KPIs como taxa de aprovacao
-- calcular na Silver a partir de tbl_cashier_deposit + tbl_cashier_cashout.
SELECT
    ins.c_ecr_id,
    ins.c_instrument,               -- Tipo: PIX, Credit, etc
    ins.c_first_part,
    ins.c_last_part,
    ins.c_status,
    ins.c_use_in_deposit,
    ins.c_use_in_cashout,
    ins.c_last_deposit_date,
    ins.c_deposit_success,
    ins.c_deposit_attempted,
    ins.c_payout_success,
    ins.c_payout_attempted,
    ins.c_chargeback
FROM cashier_ec2.tbl_instrument ins
```

---

## DOMINIO: BONUS E CUSTOS

### 16. Bonus Details (bonus_ec2.tbl_ecr_bonus_details)

```sql
-- Bronze: athena_bronze.tbl_ecr_bonus_details
-- CORRECAO: 6 nomes errados. SELECTs #15/#16 originais unificados.
-- Valores em sub-wallets separadas (CRP/DRP/WRP/RRP) em centavos
SELECT
    bd.c_ecr_id,
    bd.c_bonus_id,
    bd.c_ecr_bonus_id,              -- ADICIONADO
    bd.c_issue_type,                -- Corrigido: era c_bonus_type
    bd.c_criteria_type,             -- ADICIONADO
    bd.c_bonus_status,
    bd.c_is_freebet,                -- ADICIONADO
    bd.c_drp_in_ecr_ccy,            -- DRP = Real
    bd.c_crp_in_ecr_ccy,            -- CRP = Bonus
    bd.c_wrp_in_ecr_ccy,            -- WRP = Bonus
    bd.c_rrp_in_ecr_ccy,            -- RRP = Bonus
    bd.c_wager_amount,              -- Corrigido: era c_rollover_requirement
    bd.c_wager_amount_in_inhouse_ccy,
    bd.c_created_time,              -- Corrigido: era c_issued_date
    bd.c_bonus_expired_date,        -- Corrigido: era c_expiry_date
    bd.c_claimed_date,              -- ADICIONADO
    bd.c_free_spin_used,            -- ADICIONADO
    bd.c_vendor_id                  -- ADICIONADO
FROM bonus_ec2.tbl_ecr_bonus_details bd
```

---

## DOMINIO: RISCO E COMPLIANCE

### 17. Fraud Score (risk_ec2.tbl_ecr_ccf_score)

```sql
-- Bronze: athena_bronze.tbl_ecr_ccf_score
-- Tabela MUITO limitada — 7 colunas total. So tem score numerico.
-- c_risk_level, c_fraud_indicators_json, c_aml_flags_json NAO EXISTEM.
-- Classificacao de risco calcular na Silver.
SELECT
    r.c_ecr_id,
    r.c_ccf_score,
    r.c_bet_factor,                 -- ADICIONADO
    r.c_ccf_timestamp,              -- Corrigido: era c_calculated_date
    r.c_created_time,
    r.c_updated_time
FROM risk_ec2.tbl_ecr_ccf_score r
```

### 18. KYC Level (ecr_ec2.tbl_ecr_kyc_level)

```sql
-- Bronze: athena_bronze.tbl_ecr_kyc_level
-- CORRECAO: 5 de 6 colunas com nome errado
SELECT
    k.c_ecr_id,
    k.c_level,                      -- Corrigido: era c_kyc_level
    k.c_desc,                       -- ADICIONADO
    k.c_grace_action_status,        -- Corrigido: era c_verification_status
    k.c_kyc_limit_nearly_reached,   -- ADICIONADO
    k.c_kyc_reminder_count,         -- ADICIONADO
    k.c_updated_time                -- Corrigido: era c_updated_date
FROM ecr_ec2.tbl_ecr_kyc_level k
```

---

## DOMINIO: CATALOGOS E DIMENSOES

### 19. Game Images (bireports_ec2.tbl_vendor_games_mapping_data)

```sql
-- Bronze: athena_bronze.tbl_vendor_games_mapping_data
-- CORRECAO: c_game_name nao existe, usar c_game_desc
SELECT
    gd.c_game_id,
    gd.c_game_desc,                 -- Corrigido: era c_game_name
    gd.c_vendor_id,
    gd.c_game_category_desc,        -- ADICIONADO
    gd.c_product_id,                -- ADICIONADO
    gd.c_status                     -- ADICIONADO
FROM bireports_ec2.tbl_vendor_games_mapping_data gd
```

### 20. Catalogo de Jogos — dimensao (ps_bi.dim_game)

```sql
-- Bronze: athena_bronze.ps_bi_dim_game
-- JUSTIFICATIVA: dimensao pura (lookup), nao calculo
SELECT
    dg.game_id,
    dg.game_desc,
    dg.vendor_id,
    dg.product_id,
    dg.game_category,
    dg.game_category_desc,
    dg.game_type_id,
    dg.game_type_desc,
    dg.status,
    dg.updated_time
FROM ps_bi.dim_game dg
```

### 21. Dimensao Jogador (ps_bi.dim_user)

```sql
-- Bronze: athena_bronze.ps_bi_dim_user
-- JUSTIFICATIVA: unica fonte de country_code. Dimensao (lookup), nao calculo.
SELECT
    du.ecr_id,
    du.external_id,                 -- = Smartico user_ext_id
    du.registration_date,
    du.country_code,                -- Pais do jogador
    du.last_deposit_date,
    du.last_deposit_amount_inhouse
FROM ps_bi.dim_user du
```

### 22. Signup Info — Device Distribution (ecr_ec2.tbl_ecr_signup_info) — NOVO

```sql
-- Bronze: athena_bronze.tbl_ecr_signup_info
-- ADICIONADO: unica fonte de device/canal de cadastro
SELECT
    si.c_ecr_id,
    si.c_channel,               -- Mobile/Desktop/Tablet
    si.c_sub_channel,           -- Detalhe do canal
    si.c_os_browser_type,       -- SO/Browser
    si.c_device_id,             -- ID do dispositivo
    si.c_hostname,              -- Dominio de origem
    si.c_created_time
FROM ecr_ec2.tbl_ecr_signup_info si
```

### 23. Bonus Summary Details — BTR (bonus_ec2.tbl_bonus_summary_details) — NOVO

```sql
-- Bronze: athena_bronze.tbl_bonus_summary_details
-- ADICIONADO: necessario para BTR (c_actual_issued_amount)
SELECT
    bs.c_ecr_id,
    bs.c_bonus_id,
    bs.c_ecr_bonus_id,
    bs.c_actual_issued_amount,   -- Valor real emitido (para BTR)
    bs.c_issued_drp,
    bs.c_issued_crp,
    bs.c_issued_wrp,
    bs.c_issued_rrp,
    bs.c_offered_crp,
    bs.c_offered_rrp,
    bs.c_offered_drp
FROM bonus_ec2.tbl_bonus_summary_details bs
```

---

## TABELAS PARA VALIDACAO CRUZADA (NAO replicar como Bronze)

As tabelas abaixo sao camadas Gold (pre-calculadas pelo dbt/Pragmatic).
NAO devem entrar na Bronze — usar apenas para **comparar** com os calculos da Silver.

| Tabela | Uso |
|--------|-----|
| `ps_bi.fct_casino_activity_daily` | Validar GGR casino calculado na Silver vs dbt |
| `ps_bi.fct_player_activity_daily` | Validar DAU/deposits vs fund_ec2 |
| `ps_bi.fct_bonus_activity_daily` | Validar custo bonus vs bonus_ec2 |
| `bireports_ec2.tbl_ecr_wise_daily_bi_summary` | Validar metricas diarias consolidadas |
| `bireports_ec2.tbl_ecr` | Validar dados do jogador vs ecr_ec2.tbl_ecr |

> **Regra:** calcular tudo a partir das tabelas _ec2 brutas na Silver.
> Depois comparar com as tabelas Gold acima. Se bater = confianca. Se nao bater = investigar.

---

## RESUMO FINAL

**23 tabelas Bronze — Dados brutos:**

| # | Database | Tabela | Dominio |
|---|----------|--------|---------|
| 1 | ecr_ec2 | tbl_ecr | Aquisicao |
| 2 | cashier_ec2 | tbl_cashier_deposit | FTD / Depositos |
| 3 | ecr_ec2 | tbl_ecr_banner | Marketing |
| 4 | ecr_ec2 | tbl_ecr_flags | Filtros |
| 5 | fund_ec2 | tbl_real_fund_txn | Gaming Core |
| 6 | fund_ec2 | tbl_realcash_sub_fund_txn | Sub-Fund Real |
| 7 | fund_ec2 | tbl_bonus_sub_fund_txn | Sub-Fund Bonus |
| 8 | fund_ec2 | tbl_real_fund_txn_type_mst | Tipos Transacao |
| 9 | vendor_ec2 | tbl_vendor_games_mapping_mst | Catalogo Jogos |
| 10 | vendor_ec2 | tbl_sports_book_bets_info | Sports |
| 11 | vendor_ec2 | tbl_sports_book_bet_details | Sports Legs (NOVO) |
| 12 | bireports_ec2 | tbl_ecr_gaming_sessions | Sessoes |
| 13 | cashier_ec2 | tbl_cashier_cashout | Saques |
| 14 | cashier_ec2 | tbl_cashier_ecr_daily_payment_summary | Financeiro Diario |
| 15 | cashier_ec2 | tbl_instrument | Instrumentos Jogador |
| 16 | bonus_ec2 | tbl_ecr_bonus_details | Bonus |
| 17 | risk_ec2 | tbl_ecr_ccf_score | Risco |
| 18 | ecr_ec2 | tbl_ecr_kyc_level | KYC |
| 19 | bireports_ec2 | tbl_vendor_games_mapping_data | Game Images |
| 20 | ps_bi | dim_game | Catalogo (dimensao) |
| 21 | ps_bi | dim_user | Player (dimensao) |
| 22 | ecr_ec2 | tbl_ecr_signup_info | Device Distribution (NOVO) |
| 23 | bonus_ec2 | tbl_bonus_summary_details | BTR / Bonus Issued (NOVO) |

**TOTAL: 23 tabelas Bronze (21 _ec2 brutos + 2 dimensoes ps_bi)**

**Todas as 23 validadas:** SHOW COLUMNS + SELECT LIMIT 5 no Athena

**NAO incluido:**
- BigQuery/Smartico — fase 2
- Tabelas Gold (ps_bi.fct_*, bireports_ec2.tbl_ecr, tbl_ecr_wise_daily_bi_summary) — validacao cruzada

**Objetivo: Ingestao de dados brutos — Calculos SOMENTE na camada Silver/Views**
