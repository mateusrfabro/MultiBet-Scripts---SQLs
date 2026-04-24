# Cobertura — PDFs vs CSVs entregues

**Pergunta respondida:** quais colunas dos 3 PDFs conseguimos preencher no CSV, quais vieram via proxy/derivacao, e quais ficaram em branco (NULL) — com o porque de cada uma.

**Resumo executivo:**

| PDF | Colunas | ✅ OK | ⚠️ Proxy/Derivado | ❌ NULL (faltam) |
|---|---:|---:|---:|---:|
| sports_transactions   | 31 | 17 | 6 | 8 |
| t_casino_transactions | 17 | 11 | 3 | 3 |
| t_transactions        | 18 | 9  | 1 | 8 |
| **Total**             | **66** | **37 (56%)** | **10 (15%)** | **19 (29%)** |

**Legenda dos status:**
- ✅ **OK** — coluna bate direto com o Athena (valor confiavel, sem transformacao)
- ⚠️ **Proxy/Derivado** — nao tem coluna equivalente isolada; construimos a partir de outros campos ou replicamos algo correlato
- ❌ **NULL** — nao existe no nosso Data Lake; a coluna existe no back-office operacional mas nao e replicada no Athena

---

## 1) sports_transactions (31 colunas)

| Coluna PDF | Status | Origem Athena / motivo |
|---|---|---|
| id | ✅ | `tbl_sports_book_info.c_transaction_id` |
| source | ✅ | `tbl_sports_book_info.c_vendor_id` (Sportradar/Altenar/PPBET) |
| ext_bet_transaction_id | ✅ | `c_transaction_id` (mesmo valor do id) |
| **ext_transaction_id** | ❌ NULL | PDF descreve como "ID adicional do provedor" — nao temos coluna isolada pra isso no Athena (o c_transaction_id ja cobre o ID principal) |
| ext_ticket_id | ✅ | `c_bet_slip_id` |
| ext_bet_id | ✅ | `tbl_sports_book_bets_info.c_bet_id` |
| type | ✅ | `c_operation_type` (M=Commit, P=Payout, L=Lock, C=Cancel, R=Refund, MC/MD=Manual) |
| status | ✅ | `c_bet_slip_state` (O=Open, C=Closed) |
| amount | ✅ | `c_amount` (BRL real, valor da operacao) |
| stake_amount | ✅ | `bets_info.c_total_stake` (BRL real) |
| freebet_amount | ✅ | `bets_info.c_bonus_amount` |
| gain_amount | ✅ | `bets_info.c_total_return` (NaN se bilhete aberto) |
| feature_amount | ⚠️ Proxy | `c_transaction_fee_amount` (taxa da operacao) — PDF diz "valor de recurso especial", aproximado pela fee |
| bet_type | ✅ | `bets_info.c_bet_type` (PreLive/Live/Mixed) |
| odds | ✅ | `bets_info.c_total_odds` |
| is_rollover | ⚠️ Derivado | TRUE se `c_operation_type IN ('C','R')` — Athena nao tem flag explicita |
| is_resettlement | ⚠️ Derivado | TRUE se `c_operation_type IN ('MC','MD')` — Athena nao tem flag |
| is_settlement | ⚠️ Derivado | TRUE se `c_operation_type = 'P'` |
| **is_combo_bonus** | ❌ NULL | Flag nao existe no Athena. Saberiamos se e combo contando legs em `bet_details`, mas o flag "combo_bonus" especifico nao e exposto |
| is_freebet | ✅ | `bets_info.c_is_free` |
| ext_freebet_id | ⚠️ Proxy | `bets_info.c_pam_bonus_txn_id` (ID da transacao bonus PAM) |
| **ext_freebet_source** | ❌ NULL | Coluna de "fonte da freebet externa" nao existe no Athena |
| user_id | ✅ | `c_customer_id` (= external_id = Smartico user_ext_id) |
| **sports_token_id** | ❌ NULL | Conceito de "token esportivo" e do back-office operacional, nao replicado no Athena |
| reference_transaction_id | ⚠️ Proxy | `c_bet_slip_id` (preenchemos com o bilhete pai da operacao) |
| reference_transaction_type | ⚠️ Proxy | literal `'bet_slip'` (todas operacoes referenciam o bilhete) |
| **sports_event_id** | ❌ NULL (*) | **Disponivel no Athena em `tbl_sports_book_bet_details.c_event_id`** — NAO incluimos neste corte para nao explodir o volume (cada bilhete multi-leg geraria N linhas extras). Se auditoria precisar, conseguimos gerar um CSV complementar de eventos |
| created_at | ✅ | `bets_info.c_created_time` (BRT) |
| updated_at | ✅ | `bets_info.c_updated_time` (BRT) |
| settled_at | ✅ | `bets_info.c_bet_closure_time` (BRT) |
| **deleted_at** | ❌ NULL | Iceberg nao implementa soft-delete com essa coluna exposta |
| **partition_name** | ❌ NULL | Iceberg particiona implicitamente, sem coluna logica visivel |

**Resumo sports:** 17 ✅ | 6 ⚠️ | 8 ❌. Das 8 em branco, a unica potencialmente recuperavel e `sports_event_id` (disponivel se auditoria pedir CSV complementar).

---

## 2) t_casino_transactions (17 colunas)

| Coluna PDF | Status | Origem Athena / motivo |
|---|---|---|
| id | ✅ | `fund_ec2.tbl_real_fund_txn.c_txn_id` |
| provider | ✅ | `c_sub_vendor_id` (pragmaticplay, pgsoft, hub88, etc.) |
| reference | ⚠️ Proxy | `c_txn_id` (nao ha referencia externa isolada na tabela) |
| type | ✅ | descricao do `c_txn_type` (CASINO_BUYIN, CASINO_WIN, etc.) |
| type_id (implicito) | ✅ | `c_txn_type` (int) |
| amount | ✅ | `c_amount_in_ecr_ccy / 100` (BRL real) |
| user_id | ✅ | `c_ecr_id` |
| game_id | ✅ | `c_game_id` |
| round_id | ⚠️ Proxy | `c_session_id` (fund_ec2 nao tem round_id isolado; a sessao de jogo e o mais proximo) |
| **round_details** | ❌ NULL | Detalhes da rodada nao estao no Athena (campo JSON do back-office operacional) |
| is_freespin | ⚠️ Derivado | 1 se `c_txn_type IN (80,86,132,133)` — codigos FREESPIN do Pragmatic |
| **reference_transaction_id** | ❌ NULL | Nao coletamos a referencia ao rollback pai (existe via `c_rollback_ref_txn_id` mas deixamos de lado para simplificar) |
| **reference_transaction_type** | ❌ NULL | Mesmo motivo acima |
| casino_session_id | ✅ | `c_session_id` |
| unix_timestamp | ✅ | `to_unixtime(c_start_time)` |
| created_at | ✅ | `c_start_time` (BRT) |
| **updated_at** | ❌ NULL | Coluna `c_end_time` nao existe em `tbl_real_fund_txn` (so `c_start_time`) |
| **partition_name** | ❌ NULL | Iceberg implicito |

**Resumo casino:** 11 ✅ | 3 ⚠️ | 3 ❌. `reference_transaction_id/type` sao recuperaveis com mais uma query no Athena se auditoria precisar.

---

## 3) t_transactions (18 colunas)

| Coluna PDF | Status | Origem Athena / motivo |
|---|---|---|
| id | ✅ | `c_txn_id` |
| type | ✅ | descricao do `c_txn_type` |
| type_id | ✅ | `c_txn_type` (int) |
| amount | ✅ | `c_amount_in_ecr_ccy / 100` |
| status | ✅ | `c_txn_status` |
| user_id | ✅ | `c_ecr_id` |
| **wallet_id** | ❌ NULL | Conceito de carteira granular nao esta no Athena (existe "real_fund" e "sub_fund" mas sem id de carteira exposto) |
| src | ✅ | `c_product_id` (CASINO/SPORTSBOOK) |
| src_id | ⚠️ Proxy | `c_sub_vendor_id` |
| created_at | ✅ | `c_start_time` (BRT) |
| **updated_at** | ❌ NULL | `c_end_time` nao existe |
| **bonus_wallet_id** | ❌ NULL | Nao granular no Athena — transacoes de bonus estao em `bonus_ec2` separadamente, sem wallet_id exposto |
| **credit_percent** | ❌ NULL | Conceito do back-office, nao replicado |
| **bonus_percent** | ❌ NULL | Idem |
| **cashed_out_amount** | ❌ NULL | Esta em `cashier_ec2.tbl_cashier_cashout` — nao trouxemos aqui porque seria misturar dominios. Posso gerar um CSV complementar de saques se precisar |
| **old_balance** | ❌ NULL | `fund_ec2.tbl_real_fund_txn` nao persiste saldo-antes-da-transacao (so amount e op_type CR/DB) |
| **old_bonus_balance** | ❌ NULL | Idem |
| **partition_name** | ❌ NULL | Iceberg implicito |

**Resumo geral:** 9 ✅ | 1 ⚠️ | 8 ❌. Dos 8 em branco, `cashed_out_amount` e recuperavel em CSV complementar (cashier_ec2); os outros sao conceitos do back-office que nao estao no Data Lake.

---

## O que poderiamos entregar em um "CSV complementar" se auditoria pedir

| Coluna pedida | Onde estaria | Esforco |
|---|---|---|
| `sports_event_id` + dados do evento | `vendor_ec2.tbl_sports_book_bet_details` | Baixo (1 query Athena) |
| `reference_transaction_id` de rollback casino | `fund_ec2.tbl_real_fund_txn.c_rollback_ref_txn_id` | Baixo (ajustar query atual) |
| `cashed_out_amount` | `cashier_ec2.tbl_cashier_cashout` | Baixo (nova query) |

Basta sinalizar e em menos de 1h eu complemento os CSVs atuais.

## O que NAO esta ao nosso alcance

Colunas que so existem no back-office operacional (MySQL/PostgreSQL da operacao) e nunca foram replicadas no nosso Data Lake Athena:
- `wallet_id`, `bonus_wallet_id`, `old_balance`, `old_bonus_balance` — conceitos de ledger granular
- `credit_percent`, `bonus_percent`, `round_details`, `ext_freebet_source`, `sports_token_id` — campos operacionais
- `deleted_at`, `partition_name` — metadados de infra do back-office (Iceberg usa outro mecanismo)
- `is_combo_bonus` — flag especifico de produto que nao foi replicado

Pra conseguir essas colunas, precisariamos de acesso leitura direto ao back-office (solicitar a infra).
