# CHANGELOG: Bronze SELECTs v1 (Mauro) vs v2 (Corrigido)

**De:** bronze_selects_kpis_FINAL.pdf (Mauro - original)
**Para:** bronze_selects_kpis_CORRIGIDO_v2.pdf (Mateus - validado)
**Data:** 2026-03-20
**Metodo de validacao:** SHOW COLUMNS + SELECT LIMIT 5 em cada tabela no Athena real

---

## RESUMO DAS MUDANCAS

| Metrica | Original (Mauro) | Corrigido (v2) |
|---------|-----------------|----------------|
| Total tabelas | 23 (19 Athena + 4 BigQuery) | 27 (todas Athena/ps_bi) |
| Colunas com nome errado | 69 | 0 (corrigidas) |
| SELECTs duplicados | 1 (#15 = #16) | 0 (unificados) |
| BigQuery | 4 tabelas | Removido (fase 2) |
| Tabelas novas adicionadas | - | 8 |
| Sintaxe PostgreSQL em Athena | Sim (::date) | Corrigido (CAST) |
| Coluna de valor em fund_ec2 | FALTANDO | Adicionada |
| Filtro test_user | `= 0` (integer) | `= false` (boolean — tipo correto no Athena) |
| Colunas BI Summary (#26) | Nomes inventados | Corrigidos via SHOW COLUMNS |
| Colunas dim_user (#27) | first_deposit_date | last_deposit_date (first nao existe) |

---

## MUDANCAS POR SELECT

### SELECT 1 — fact_registrations (ecr_ec2.tbl_ecr)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_country_code | Incluido | REMOVIDO (nao existe). Usar ps_bi.dim_user.country_code |
| c_jurisdiction | - | ADICIONADO (campo mais proximo) |
| c_language | - | ADICIONADO |
| c_ecr_status | - | ADICIONADO |
| Sintaxe date | `c_signup_time::date` | `CAST(c_signup_time AS DATE)` |

### SELECT 2 — fact_ftd_deposits (cashier_ec2.tbl_cashier_deposit)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| Sintaxe date | `c_created_time::date` | `CAST(c_created_time AS DATE)` |
| Status | Mantido `txn_confirmed_success` | Mantido (precisa validar empiricamente) |

### SELECT 3 — dim_marketing_mapping (ecr_ec2.tbl_ecr_banner)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_click_id | Incluido | REMOVIDO (nao existe) |
| c_utm_source | Incluido | REMOVIDO (nao existe) |
| c_utm_medium | Incluido | REMOVIDO (nao existe) |
| c_utm_campaign | Incluido | REMOVIDO (nao existe) |
| c_ecr_id | - | ADICIONADO |
| c_affiliate_name | - | ADICIONADO |
| c_banner_id | - | ADICIONADO |
| c_reference_url | - | ADICIONADO (substitui click_id) |
| c_custom1..4 | - | ADICIONADO (podem conter UTMs/click IDs) |

### SELECT 4 — fact_gaming_activity (fund_ec2.tbl_real_fund_txn) [CRITICO]

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_amount_in_ecr_ccy | FALTANDO! | ADICIONADO (valor transacao - CRITICO) |
| c_txn_status | FALTANDO | ADICIONADO |
| c_game_id | FALTANDO | ADICIONADO |
| c_product_id | FALTANDO | ADICIONADO |
| c_op_type | FALTANDO | ADICIONADO |
| c_game_category | FALTANDO | ADICIONADO |
| c_tracker_id | Incluido | REMOVIDO (nao existe nesta tabela) |
| c_vendor_id | - | Trocado por c_sub_vendor_id |
| Sintaxe date | `::date` | `CAST(... AS DATE)` |

### SELECT 5 — Sub-Fund Real (fund_ec2.tbl_realcash_sub_fund_txn)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_ecr_id | - | ADICIONADO (existe e facilita joins) |

### SELECT 6 — Sub-Fund Bonus (fund_ec2.tbl_bonus_sub_fund_txn)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_ecr_id | - | ADICIONADO |

### SELECT 7 — Tipos Transacao (fund_ec2.tbl_real_fund_txn_type_mst)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_txn_type_name | Incluido | TROCADO por c_internal_description |
| c_is_free_spin_txn | - | ADICIONADO |
| c_is_refund_txn_type | - | ADICIONADO |
| c_is_settlement_txn_type | - | ADICIONADO |
| c_product_id | - | ADICIONADO |

### SELECT 8 — Casino fund_ec2 (tbl_real_fund_txn filtro casino)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_amount_in_ecr_ccy | FALTANDO! | ADICIONADO (CRITICO) |
| c_vendor_id | Incluido | TROCADO por c_sub_vendor_id |
| c_round_id | Incluido | REMOVIDO (nao existe) |
| c_txn_type | FALTANDO | ADICIONADO |
| c_txn_status | FALTANDO | ADICIONADO |
| c_op_type | FALTANDO | ADICIONADO |
| c_game_category | FALTANDO | ADICIONADO |

### SELECT 9 — Catalogo Jogos (vendor_ec2.tbl_vendor_games_mapping_mst)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_game_name | Incluido | TROCADO por c_game_desc |
| c_game_category | Incluido | TROCADO por c_game_category_desc |
| c_has_free_spins | Incluido | TROCADO por c_free_spin_game |
| c_technology | Incluido | TROCADO por c_game_technology |
| c_feature_trigger | Incluido | TROCADO por c_feature_trigger_game |
| c_updated_dt | Incluido | TROCADO por c_updated_time |

### SELECT 10 — Sports Bets (vendor_ec2.tbl_sports_book_bets_info)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_sport_name | Incluido | REMOVIDO (nao existe - nome do esporte esta em bet_details) |
| c_bet_slip_id | - | ADICIONADO (join com bet_details) |
| c_total_odds | - | ADICIONADO |
| c_is_live | - | ADICIONADO |
| c_bet_slip_state | - | ADICIONADO |
| c_transaction_id | - | ADICIONADO |
| JOIN com ecr_ec2 | Incluido | REMOVIDO (fazer na Silver, Bronze = dado bruto) |

### SELECT 10b — Sports Details [TABELA NOVA]

Tabela inteiramente nova. Necessaria para obter c_sport_type_name (nome do esporte).

### SELECT 10c — Sports Transacoes [TABELA NOVA]

Tabela inteiramente nova. Transacoes financeiras granulares do sportsbook.

### SELECT 11 — Gaming Sessions (bireports_ec2.tbl_ecr_gaming_sessions)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_session_duration_sec | Incluido | TROCADO por c_session_length_in_sec |
| c_round_count | Incluido | TROCADO por c_game_played_count |
| c_vendor_id | - | ADICIONADO |
| c_game_category | - | ADICIONADO |

### SELECT 12 — Cashout (cashier_ec2.tbl_cashier_cashout)

Sem mudancas — original estava OK.

### SELECT 13 — Daily Payment Summary

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_date | Incluido | TROCADO por c_created_date |
| c_deposit_amount_brl | Incluido | TROCADO por c_deposit_amount (centavos!) |
| c_withdrawal_amount_brl | Incluido | TROCADO por c_success_cashout_amount |
| c_withdrawal_count | Incluido | TROCADO por c_success_cashout_count |
| c_net_deposit_brl | Incluido | REMOVIDO (nao existe, calcular na view) |
| c_avg_deposit_ticket | Incluido | REMOVIDO (nao existe, calcular na view) |
| c_deposit_amount_inhouse | - | ADICIONADO |
| c_success_cashout_amount_inhouse | - | ADICIONADO |
| c_cb_amount/count | - | ADICIONADO (chargebacks) |
| c_option | - | ADICIONADO (metodo pagamento) |
| c_provider | - | ADICIONADO (provedor) |

### SELECT 14 — Instrumentos Pagamento (cashier_ec2.tbl_instrument)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| REFORMULADO COMPLETO | 6 colunas fictícias | 13 colunas reais da tabela |
| c_instrument_id/name/type | Incluidos | REMOVIDOS (nao existem) |
| c_processing_time_avg_minutes | Incluido | REMOVIDO (metrica calculada, nao existe) |
| c_approval_rate_pct | Incluido | REMOVIDO (metrica calculada) |
| c_chargeback_rate_pct | Incluido | REMOVIDO (metrica calculada) |

### SELECT 15 — Bonus Details (bonus_ec2.tbl_ecr_bonus_details)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_bonus_amount_brl | Incluido | TROCADO por c_drp/crp/wrp/rrp_in_ecr_ccy (sub-wallets) |
| c_bonus_type | Incluido | TROCADO por c_issue_type |
| c_rollover_requirement | Incluido | TROCADO por c_wager_amount |
| c_rollover_completed | Incluido | REMOVIDO (nao existe como campo direto) |
| c_issued_date | Incluido | TROCADO por c_created_time |
| c_expiry_date | Incluido | TROCADO por c_bonus_expired_date |
| c_ecr_bonus_id | - | ADICIONADO |
| c_criteria_type | - | ADICIONADO |
| c_is_freebet | - | ADICIONADO |
| c_claimed_date | - | ADICIONADO |
| c_free_spin_used | - | ADICIONADO |
| c_vendor_id | - | ADICIONADO |
| c_wager_amount_in_inhouse_ccy | - | ADICIONADO |

### SELECT 16 — Bonus (duplicado)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| SELECT #16 inteiro | Duplicata do #15 | REMOVIDO e substituido por ps_bi.fct_bonus_activity_daily |

### SELECT 17 — Fraud/Risk (risk_ec2.tbl_ecr_ccf_score)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_risk_level | Incluido | REMOVIDO (nao existe, tabela so tem 7 colunas) |
| c_calculated_date | Incluido | TROCADO por c_ccf_timestamp |
| c_fraud_indicators_json | Incluido | REMOVIDO (nao existe) |
| c_aml_flags_json | Incluido | REMOVIDO (nao existe) |
| c_bet_factor | - | ADICIONADO |
| c_created_time | - | ADICIONADO |
| c_updated_time | - | ADICIONADO |

### SELECT 18 — KYC (ecr_ec2.tbl_ecr_kyc_level)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_kyc_level | Incluido | TROCADO por c_level |
| c_verification_status | Incluido | TROCADO por c_grace_action_status |
| c_updated_date | Incluido | TROCADO por c_updated_time |
| c_documents_verified_count | Incluido | REMOVIDO (nao existe) |
| c_verification_time_hours | Incluido | REMOVIDO (nao existe) |
| c_desc | - | ADICIONADO |
| c_kyc_limit_nearly_reached | - | ADICIONADO |
| c_kyc_reminder_count | - | ADICIONADO |

### SELECTs 21-24 — BigQuery/Smartico

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| 4 tabelas BigQuery | Incluidas | REMOVIDAS (fase 2 - CRM consulta BigQuery on-demand) |

### SELECT 25 — Flags (ecr_ec2.tbl_ecr_flags)

| Mudanca | Original | Corrigido |
|---------|----------|-----------|
| c_flag_name | Incluido | REMOVIDO (nao existe - flags sao colunas individuais) |
| c_flag_value | Incluido | REMOVIDO (nao existe) |
| c_referral_ban | - | ADICIONADO |
| c_withdrawl_allowed | - | ADICIONADO |
| c_two_factor_auth_enabled | - | ADICIONADO |
| c_hide_username_feed | - | ADICIONADO |

---

## TABELAS NOVAS ADICIONADAS (nao existiam no documento original)

| # | Tabela | Justificativa |
|---|--------|---------------|
| 10b | vendor_ec2.tbl_sports_book_bet_details | Necessaria para c_sport_type_name (nome do esporte). Sem ela, SELECT #10 nao tem esporte. |
| 10c | vendor_ec2.tbl_sports_book_info | Transacoes financeiras granulares do sportsbook (Lock/Commit/Payout/Cancel/Refund) |
| 16 | ps_bi.fct_bonus_activity_daily | Listada no resumo original (tabela #21) mas sem SELECT — agora tem |
| 26 | bireports_ec2.tbl_ecr_wise_daily_bi_summary | Principal tabela de BI — 1 linha/player/dia com depositos, bets, wins, GGR |
| 27 | ps_bi.dim_user | Dimensao player completa — tem country_code que falta no tbl_ecr |
| 28 | bireports_ec2.tbl_ecr | Tabela mestre jogador (48 cols) — mais completa que ecr_ec2.tbl_ecr |
| ps_bi | ps_bi.fct_casino_activity_daily | Ja existia no original — mantida |
| ps_bi | ps_bi.fct_player_activity_daily | Player activity pre-agregada pelo dbt |

---

## CORRECOES DE LOGICA / CONCEITUAIS

| Item | Problema | Correcao |
|------|----------|----------|
| Sintaxe SQL | `::date` (PostgreSQL) em queries Athena | `CAST(x AS DATE)` (Presto) |
| SELECT #4/#8 | SEM coluna de valor (c_amount_in_ecr_ccy) | Adicionada — sem ela nao calcula GGR |
| SELECT #10 | c_sport_name referenciava coluna que nao existe | Nome do esporte vem de tbl_sports_book_bet_details.c_sport_type_name |
| SELECT #10 | JOIN com ecr_ec2 na Bronze | Removido — JOINs complexos ficam na Silver |
| SELECT #13 | Nomes de colunas com sufixo `_brl` | Corrigido — valores estao em centavos, nao BRL |
| SELECT #13 | c_net_deposit_brl e c_avg_deposit_ticket | Removidos — sao campos calculados, nao existem como dado bruto |
| SELECT #14 | Tabela confundida com "metricas de pagamento" | Esclarecido que tbl_instrument e instrumentos DO JOGADOR, nao metricas |
| SELECT #15 | Valor de bonus como campo unico | Corrigido para sub-wallets (DRP/CRP/WRP/RRP) |
| SELECT #15/#16 | Duplicados | Unificados em um SELECT |
| SELECT #17 | 4 campos fictícios | Tabela real so tem 7 colunas (score + timestamps) |
| SELECT #25 | Flags como key-value | Corrigido — flags sao colunas individuais |
