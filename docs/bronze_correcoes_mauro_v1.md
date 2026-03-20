# Correcoes Bronze SELECTs — Validacao Empirica Athena
**De:** Mateus Fabro (Squad Intelligence Engine)
**Para:** Mauro (Analista Senior)
**Data:** 2026-03-20
**Ref:** bronze_selects_kpis_FINAL.pdf

---

## Resumo

Rodamos `SHOW COLUMNS` em cada uma das tabelas listadas no documento Bronze.
Resultado: **69 colunas com nome diferente ou inexistente** em 21 das 27 tabelas testadas.

A maioria sao erros de nomenclatura (nomes "inventados" que nao existem no schema real).
Abaixo o DE-PARA completo, organizado por SELECT.

---

## STATUS POR SELECT

| # | SELECT | Status |
|---|--------|--------|
| 1 | fact_registrations (tbl_ecr) | 1 correcao |
| 2 | fact_ftd_deposits (tbl_cashier_deposit) | OK |
| 3 | dim_marketing_mapping (tbl_ecr_banner) | 4 colunas NAO EXISTEM |
| 4 | fact_gaming_activity (tbl_real_fund_txn) | CRITICO - falta coluna de valor |
| 5 | Sub-Fund Real (tbl_realcash_sub_fund_txn) | OK |
| 6 | Sub-Fund Bonus (tbl_bonus_sub_fund_txn) | OK |
| 7 | Tipos Transacao (tbl_real_fund_txn_type_mst) | 1 correcao |
| 8 | Casino fund_ec2 (tbl_real_fund_txn) | CRITICO - falta coluna de valor |
| 9 | Catalogo Jogos (tbl_vendor_games_mapping_mst) | 6 correcoes |
| 10 | Sports Bets (tbl_sports_book_bets_info) | 1 CRITICO (c_sport_name) |
| 11 | Gaming Sessions (tbl_ecr_gaming_sessions) | 2 correcoes |
| 12 | Cashout (tbl_cashier_cashout) | OK |
| 13 | Daily Payment Summary | 6 correcoes (quase tudo errado) |
| 14 | Instrumentos Pagamento (tbl_instrument) | TABELA DIFERENTE do esperado |
| 15/16 | Bonus Details (tbl_ecr_bonus_details) | 6 correcoes |
| 17 | Fraud/Risk (tbl_ecr_ccf_score) | 4 de 6 colunas NAO EXISTEM |
| 18 | KYC (tbl_ecr_kyc_level) | 5 de 6 colunas NAO EXISTEM |
| 25 | Flags (tbl_ecr_flags) | 2 correcoes |

---

## CORRECOES DETALHADAS POR SELECT

### SELECT 1 — fact_registrations (ecr_ec2.tbl_ecr)

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_ecr_id | OK | - |
| c_external_id | OK | - |
| c_tracker_id | OK | - |
| c_affiliate_id | OK | - |
| c_country_code | NAO | Nao existe. Alternativa: nao ha coluna de pais nesta tabela. Verificar `ps_bi.dim_user.country_code` |
| c_signup_time | OK | - |

**Colunas extras uteis disponiveis:** `c_ecr_status`, `c_email_id`, `c_ip`, `c_jurisdiction`, `c_language`, `c_registration_status`

---

### SELECT 3 — dim_marketing_mapping (ecr_ec2.tbl_ecr_banner)

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_tracker_id | OK | - |
| c_click_id | NAO | **Nao existe.** Nao ha campo de click ID nesta tabela |
| c_utm_source | NAO | **Nao existe.** Nao ha campos UTM |
| c_utm_medium | NAO | **Nao existe.** Nao ha campos UTM |
| c_utm_campaign | NAO | **Nao existe.** Nao ha campos UTM |
| c_affiliate_id | OK | - |
| c_created_time | OK | - |

**Colunas reais da tabela (16):** `c_affiliate_id`, `c_affiliate_name`, `c_banner_id`, `c_created_by`, `c_created_time`, `c_custom1`, `c_custom2`, `c_custom3`, `c_custom4`, `c_ecr_id`, `c_id`, `c_reference_url`, `c_tracker_id`, `c_updated_by`, `c_updated_time`, `cdc_timestamp`

**Nota:** Os click IDs (gclid, fbclid) provavelmente estao nos campos `c_custom1`..`c_custom4` ou em `c_reference_url`. Precisamos investigar.

---

### SELECT 4 — fact_gaming_activity (fund_ec2.tbl_real_fund_txn) [CRITICO]

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_ecr_id | OK | - |
| c_tracker_id | NAO | **Nao existe** em fund_ec2. Tracker esta em `ecr_ec2.tbl_ecr` |
| c_txn_id | OK | - |
| c_txn_type | OK | - |
| c_start_time | OK | - |

**COLUNAS CRITICAS FALTANDO NO SELECT DO MAURO:**

| Coluna | Tipo | Por que precisa |
|---|---|---|
| `c_amount_in_ecr_ccy` | bigint | **VALOR DA TRANSACAO (centavos).** Sem ela nao calcula GGR |
| `c_txn_status` | varchar | Filtrar sucesso vs falha |
| `c_game_id` | varchar | ID do jogo (casino) |
| `c_product_id` | varchar | Separar casino vs sports |
| `c_sub_vendor_id` | varchar | Provedor do jogo (nao existe `c_vendor_id`, so `c_sub_vendor_id`) |
| `c_op_type` | varchar | DB/CR (debito/credito) |
| `c_game_category` | varchar | Categoria do jogo |

**Nota sobre `c_confirmed_amount_in_inhouse_ccy`:** NAO EXISTE. Confirmado — esta coluna que o arquiteto recomendou nao faz parte do schema real.

**Nota sobre `dt` (particao):** NAO EXISTE como coluna visivel. Pode ser particao Iceberg implicita.

**Nota sobre `c_round_id`:** NAO EXISTE nesta tabela. Nao ha campo de round.

---

### SELECT 7 — Tipos Transacao (fund_ec2.tbl_real_fund_txn_type_mst)

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_txn_type | OK | - |
| c_txn_type_name | NAO | Usar `c_internal_description` (descricao interna do tipo) |
| c_op_type | OK | - |
| c_is_gaming_txn | OK | - |
| c_is_cancel_txn | OK | - |
| c_txn_identifier_key | OK | - |

**Colunas extras uteis:** `c_product_id`, `c_is_free_spin_txn`, `c_is_refund_txn_type`, `c_is_settlement_txn_type`

---

### SELECT 9 — Catalogo Jogos (vendor_ec2.tbl_vendor_games_mapping_mst)

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_game_id | OK | - |
| c_game_name | NAO | Usar `c_game_desc` (nome/descricao do jogo) |
| c_vendor_id | OK | - |
| c_sub_vendor_id | OK | - |
| c_product_id | OK | - |
| c_game_category | NAO | Usar `c_game_category_desc` |
| c_game_type_desc | OK | - |
| c_status | OK | - |
| c_has_jackpot | OK | - |
| c_has_free_spins | NAO | Usar `c_free_spin_game` |
| c_technology | NAO | Usar `c_game_technology` |
| c_feature_trigger | NAO | Usar `c_feature_trigger_game` |
| c_updated_dt | NAO | Usar `c_updated_time` |
| c_game_type_id | OK | - |

---

### SELECT 10 — Sports Bets (vendor_ec2.tbl_sports_book_bets_info) [CRITICO]

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_sport_name | **NAO** | **Nao existe nesta tabela.** O nome do esporte esta em `tbl_sports_book_bet_details.c_sport_type_name`. Precisa de JOIN ou trazer a outra tabela tambem. |
| c_sport_type_name | **NAO** | Confirma: nao esta em bets_info, so em bet_details |
| Demais colunas | OK | c_bet_id, c_customer_id, c_total_stake, c_total_return, etc |

**Solucao:** Adicionar `vendor_ec2.tbl_sports_book_bet_details` como tabela Bronze separada (ja validada — todas 12 colunas testadas OK).

---

### SELECT 11 — Gaming Sessions (bireports_ec2.tbl_ecr_gaming_sessions)

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_session_duration_sec | NAO | Usar `c_session_length_in_sec` |
| c_round_count | NAO | Usar `c_game_played_count` |
| Demais | OK | - |

---

### SELECT 13 — Daily Payment Summary (cashier_ec2.tbl_cashier_ecr_daily_payment_summary)

**Quase tudo errado.** Nomes inventados que nao existem:

| Coluna Mauro | Existe? | Coluna Real |
|---|---|---|
| c_date | NAO | `c_created_date` |
| c_deposit_amount_brl | NAO | `c_deposit_amount` (em centavos, nao BRL!) |
| c_deposit_count | OK | - |
| c_withdrawal_amount_brl | NAO | `c_success_cashout_amount` |
| c_withdrawal_count | NAO | `c_success_cashout_count` |
| c_net_deposit_brl | NAO | Nao existe — precisa calcular na view |
| c_avg_deposit_ticket | NAO | Nao existe — precisa calcular na view |

**Colunas extras uteis:** `c_deposit_amount_inhouse` (valor em moeda da casa), `c_cashout_fee_amount`, `c_cb_amount` (chargebacks), `c_option` (metodo pagamento), `c_provider`

---

### SELECT 14 — Instrumentos Pagamento (cashier_ec2.tbl_instrument)

**A tabela existe mas e COMPLETAMENTE diferente do esperado.** `tbl_instrument` e a tabela de **instrumentos financeiros do jogador** (cartoes, PIX, contas), nao uma tabela de metodos de pagamento com metricas.

| Coluna Mauro | Existe? | Realidade |
|---|---|---|
| c_instrument_id | NAO | Nao existe (tem `c_id`) |
| c_instrument_name | NAO | Nao existe (tem `c_instrument` = tipo) |
| c_instrument_type | NAO | Nao existe como tal |
| c_processing_time_avg_minutes | NAO | **Nao existe — metrica calculada** |
| c_approval_rate_pct | NAO | **Nao existe — metrica calculada** |
| c_chargeback_rate_pct | NAO | **Nao existe — metrica calculada** |

**Decisao:** Essa tabela nao serve pra Bronze como o Mauro pensou. Os KPIs de metodos de pagamento (tempo de processamento, taxa de aprovacao, chargeback) precisam ser **calculados** a partir de `tbl_cashier_deposit` + `tbl_cashier_cashout`, nao vem prontos.

---

### SELECT 15/16 — Bonus Details (bonus_ec2.tbl_ecr_bonus_details)

| Coluna Mauro | Existe? | Coluna Real |
|---|---|---|
| c_bonus_amount_brl | NAO | Valores em sub-wallets: `c_crp_in_ecr_ccy`, `c_drp_in_ecr_ccy`, `c_wrp_in_ecr_ccy`, `c_rrp_in_ecr_ccy` (centavos) ou `*_in_inhouse_ccy` |
| c_bonus_type | NAO | Usar `c_issue_type` ou `c_criteria_type` |
| c_rollover_requirement | NAO | Usar `c_wager_amount` ou `c_wager_amount_in_inhouse_ccy` |
| c_rollover_completed | NAO | Nao existe como campo direto — derivar de wager_amount vs atual |
| c_issued_date | NAO | Usar `c_created_time` (timestamp de emissao) |
| c_expiry_date | NAO | Usar `c_bonus_expired_date` |
| c_bonus_id | OK | - |
| c_bonus_status | OK | - |
| c_ecr_id | OK | - |

**Colunas extras uteis:** `c_is_freebet`, `c_free_spin_used`, `c_no_freespin_wins_credited`, `c_win_issue_timestamp`, `c_ecr_bonus_id`

---

### SELECT 17 — Fraud/Risk (risk_ec2.tbl_ecr_ccf_score)

**Tabela muito pequena — apenas 7 colunas no total:**

`c_bet_factor`, `c_ccf_score`, `c_ccf_timestamp`, `c_created_time`, `c_ecr_id`, `c_updated_time`, `cdc_timestamp`

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_ecr_id | OK | - |
| c_ccf_score | OK | - |
| c_risk_level | NAO | **Nao existe** — nao ha classificacao LOW/MEDIUM/HIGH |
| c_calculated_date | NAO | Usar `c_ccf_timestamp` |
| c_fraud_indicators_json | NAO | **Nao existe** — so tem o score numerico |
| c_aml_flags_json | NAO | **Nao existe** |

**Conclusao:** A tabela real so tem o score CCF (numerico) e timestamps. Os campos de risco detalhados (level, fraud indicators, AML flags) nao existem. Precisam ser derivados ou vem de outra fonte.

---

### SELECT 18 — KYC (ecr_ec2.tbl_ecr_kyc_level)

**5 de 6 colunas nao existem:**

| Coluna Mauro | Existe? | Coluna Real |
|---|---|---|
| c_ecr_id | OK | - |
| c_kyc_level | NAO | Usar `c_level` |
| c_verification_status | NAO | Usar `c_grace_action_status` (mais proximo) |
| c_updated_date | NAO | Usar `c_updated_time` |
| c_documents_verified_count | NAO | **Nao existe** |
| c_verification_time_hours | NAO | **Nao existe** |

---

### SELECT 25 — Flags (ecr_ec2.tbl_ecr_flags)

| Coluna Mauro | Existe? | Correcao |
|---|---|---|
| c_ecr_id | OK | - |
| c_test_user | OK | - |
| c_flag_name | NAO | **Nao existe** — tbl_ecr_flags tem flags como colunas individuais |
| c_flag_value | NAO | **Nao existe** — cada flag e uma coluna (c_test_user, c_referral_ban, c_withdrawl_allowed, etc) |

---

## TABELAS FALTANTES (recomendacao de adicionar)

| # | Tabela | Database | Justificativa |
|---|--------|----------|---------------|
| A | tbl_ecr_wise_daily_bi_summary | bireports_ec2 | 109 cols, 1 linha/player/dia — PRINCIPAL tabela de BI, tem dep+bets+wins+GGR |
| B | tbl_ecr | bireports_ec2 | Tabela mestre jogador (48 cols, c_last_login_time, c_test_user) |
| C | dim_user | ps_bi | Dimensao player completa (external_id = Smartico join) |
| D | fct_player_activity_daily | ps_bi | DAU, deposits, bets, wins, GGR — validado pelo dbt |
| E | fct_bonus_activity_daily | ps_bi | Listado no resumo do doc mas sem SELECT |
| F | tbl_sports_book_bet_details | vendor_ec2 | Detalhes das pernas (esporte, evento, liga, odds) — **necessario pro c_sport_type_name** |
| G | tbl_sports_book_info | vendor_ec2 | Transacoes financeiras granulares SB (Lock/Commit/Payout) |

---

## CONTAGEM FINAL

- **Colunas com nome errado:** 69
- **Tabelas com problemas:** 21 de 27
- **Tabelas com dados completamente diferentes do esperado:** 2 (tbl_instrument, tbl_ecr_ccf_score)
- **Tabelas faltantes recomendadas:** 7
- **SELECTs duplicados:** 1 (bonus #15 e #16 sao iguais)

---

## PROXIMOS PASSOS

1. Mauro corrigir os nomes das colunas usando este DE-PARA
2. Remover SELECT #14 (tbl_instrument) — nao serve como Bronze, KPIs de pagamento sao calculados
3. Unificar SELECT #15 e #16 (bonus duplicado)
4. Adicionar as 7 tabelas faltantes
5. Definir nomenclatura do schema Bronze (athena_bronze.* / bq_bronze.* / multibet_raw.*)
6. Rodar SELECT de amostra (LIMIT 10) em cada tabela corrigida para confirmar que retorna dados

**CSV com resultado completo:** `output/validacao_bronze_colunas.csv`
