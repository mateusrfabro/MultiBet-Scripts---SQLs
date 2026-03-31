# Guia Athena — Pragmatic Solutions (Data Lake Iceberg)
**Versao:** 1.0 | **Data:** 2026-03-31 | **Autor:** Mateus F. (Squad Intelligence Engine)
**Fonte:** Pragmatic Solutions Database Schema Documents v1.0-v1.3, validacoes empiricas, feedbacks do time

---

## 1. Visao Geral

| Aspecto | Detalhe |
|---------|---------|
| **Engine** | Trino/Presto (Apache Iceberg Data Lake) |
| **Regiao** | `sa-east-1` (AWS Sao Paulo) |
| **Conta AWS** | `803633136520` |
| **Tipo de acesso** | **Read-only** (user `mb-prod-db-iceberg-ro`) |
| **S3 Results** | `s3://aws-athena-query-results-803633136520-sa-east-1/` |
| **Databases** | 19 (sufixo `_ec2`, `ps_bi`, `silver`, outros) |
| **Sintaxe SQL** | Presto/Trino |
| **Timestamps** | **UTC** (requer conversao para BRT em toda entrega) |
| **Conexao** | `db/athena.py` → `query_athena(sql, database="fund_ec2")` (boto3 SDK) |
| **Cobranca** | Por volume de dados escaneados (otimizar com particoes e colunas) |

---

## 2. Arquitetura de Camadas

O Data Lake tem 3 camadas com regras diferentes de unidade monetaria:

| Camada | Databases | Valores | Timestamps | Quando usar |
|--------|-----------|---------|------------|-------------|
| **Bruta (`_ec2`)** | fund_ec2, ecr_ec2, bireports_ec2, bonus_ec2, cashier_ec2, casino_ec2, csm_ec2, vendor_ec2, segment_ec2, risk_ec2, fx_ec2, regulatory_ec2, master_ec2, messaging_ec2, mktg_ec2 | **Centavos** (/100) | UTC | Analises granulares, dados transacionais |
| **BI Mart (`ps_bi`)** | ps_bi | **BRL real** | UTC | **Preferir para analises** — pre-agregado via dbt |
| **Silver** | silver | **BRL real** | UTC | Snapshots de players, analises de cohort |

> **EXCECAO:** Sportsbook (`vendor_ec2.tbl_sports_book_*`) ja tem valores em **BRL real**, nao centavos.

---

## 3. Databases e Tabelas por Dominio

### 3.1 `fund_ec2` — Carteira/Ledger do Jogador
**Fonte:** Fund Database Schema Document v1.3 (Pragmatic Solutions)

A tabela mais importante do Data Lake. Cada aposta, ganho, deposito, saque e cancelamento.

#### Tabelas principais (17 listadas; 21 total no schema — 4 de config/referencia omitidas)

| Tabela | Descricao |
|--------|-----------|
| `tbl_real_fund_txn` | **Tabela principal** — todas as transacoes financeiras |
| `tbl_real_fund` | Saldo em tempo real do jogador |
| `tbl_realcash_sub_fund_txn` | Sub-transacoes real cash (conversao moeda) |
| `tbl_bonus_sub_fund_txn` | Sub-transacoes bonus (DRP, WRP, CRP, RRP) |
| `tbl_negative_sub_fund_txn` | Sub-transacoes saldo negativo (fraude, chargeback) |
| `tbl_real_fund_txn_type_mst` | Master de tipos de transacao |
| `tbl_fund_deposit_txn` | Depositos (valor, bonus, pendente) |
| `tbl_fund_pending_deposit_txn` | Depositos pendentes |
| `tbl_real_cash_reserve` | Reservas de real cash |
| `tbl_real_fund_session` | Sessoes de jogo (com vendor details) |
| `tbl_real_pending_fund_session` | Sessoes pendentes |
| `tbl_fund_closed_sessions` | Sessoes fechadas (cron) |
| `tbl_event_level_details` | Eventos sportsbook (ID, status, start/end) |
| `tbl_fund_ended_events_data` | Eventos sportsbook encerrados |
| `tbl_player_level_event_snapshot` | Snapshot: saldo abertura, total bets/wins |
| `tbl_bo_sportsbook_open_bets_settlement` | Settlement apostas abertas SB |
| `tbl_bo_sportsbook_bulk_settlement` | Settlement bulk SB |

#### Colunas-chave de `tbl_real_fund_txn`

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_txn_id` | bigint | PK — ID unico da transacao |
| `c_ecr_id` | bigint | ID interno do jogador (18 digitos) |
| `c_txn_type` | int | Tipo numerico (ver mapeamento completo abaixo) |
| `c_txn_status` | varchar | `INIT`, `SUCCESS`, `FAILURE` |
| `c_amount_in_ecr_ccy` | bigint | **Valor em centavos BRL** (/100) — UNICA coluna de valor. `c_confirmed_amount_in_inhouse_ccy` NAO EXISTE. |
| `c_start_time` | timestamp | Timestamp da transacao (**UTC**) |
| `c_game_id` | varchar | ID do jogo |
| `c_session_id` | varchar | ID da sessao |
| `c_op_type` | varchar | `CR` (credito) ou `DB` (debito) |
| `c_sub_vendor_id` | varchar | Provider (pragmaticplay, hub88, pgsoft, etc.) — **`c_vendor_id` NAO EXISTE nesta tabela** (validado 31/03/2026) |
| `c_event_id` | varchar | ID evento sportsbook |
| `c_channel` | varchar | DESKTOP, MOBILE |
| `c_sub_channel` | varchar | HTML, native, BackOffice |
| `c_product_id` | varchar | **CASINO** ou **SPORTSBOOK** |

#### Mapeamento COMPLETO `c_txn_type`

**Financeiro (depositos, saques, ajustes):**

| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 1 | REAL_CASH_DEPOSIT | CR | Deposito |
| 2 | REAL_CASH_WITHDRAW | DB | Saque |
| 3 | REAL_CASH_ADDITION_BY_CS | CR | Adicao manual CS |
| 4 | REAL_CASH_REMOVAL_BY_CS | DB | Remocao manual CS |
| 6 | REAL_CASH_ADDITION_BY_CAMPAIGN | CR | Credito por campanha |
| 36 | REAL_CASH_CASHOUT_REVERSAL | CR | Estorno de saque |
| 51 | POSITIVE_ADJUSTMENT | CR | Ajuste positivo |
| 52 | NEGATIVE_ADJUSTMENT | DB | Ajuste negativo |
| 54 | CASHOUT_FEE | DB | Taxa de saque |
| 55 | INACTIVE_FEE | DB | Taxa de inatividade |
| 56 | INACTIVE_FEE_REVERSAL | CR | Estorno taxa inatividade |
| 57 | CASHOUT_FEE_REVERSAL | CR | Estorno taxa saque |
| 78 | MIGRATION_TYPE | CR | Migracao de outro sistema |
| 90 | REAL_CASH_DEBIT_FOR_USER_INACTIVITY | DB | Debito por inatividade |
| 95 | IAT_USER_DEBIT | DB | Transferencia interna (saida) |
| 96 | IAT_USER_CREDIT | CR | Transferencia interna (entrada) |
| 126 | REAL_CASH_DEPOSIT_REFUND | DB | Estorno de deposito |
| 129 | WIN_FEES | DB | Taxa sobre ganhos |

**Casino:**

| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 27 | CASINO_BUYIN | DB | Aposta cassino |
| 28 | CASINO_REBUY | DB | Rebuy cassino |
| 29 | CASINO_LEAVE_TABLE | CR | Saida da mesa com saldo |
| 45 | CASINO_WIN | CR | Ganho cassino |
| 65 | JACKPOT_WIN | CR | Jackpot |
| 68 | CASINO_TIP | DB | Gorjeta |
| 72 | CASINO_BUYIN_CANCEL | CR | **Rollback aposta** |
| 73 | CASINO_LEAVE_TABLE_CANCEL | DB | Cancel leave table |
| 76 | CASINO_REBUY_CANCEL | CR | Cancel rebuy |
| 77 | CASINO_WIN_CANCEL | DB | **Rollback ganho** |
| 79 | CASINO_TOURN_WIN | CR | Ganho torneio cassino |
| 80 | CASINO_FREESPIN_WIN | CR | Ganho free spin |
| 86 | CASINO_FREESPIN_WIN_CANCEL | DB | Cancel free spin win |
| 91 | CASINO_REFUND_BET | CR | Reembolso de aposta |
| 114 | JACKPOT_WIN_CANCEL | DB | Cancel jackpot |
| 130 | CASINO_MANUAL_DEBIT | DB | Debito manual cassino |
| 131 | CASINO_MANUAL_CREDIT | CR | Credito manual cassino |
| 132 | CASINO_FREESPIN_BUYIN | DB | Buyin free spin |
| 133 | CASINO_FREESPIN_BUYIN_CANCEL | CR | Cancel buyin free spin |

**Casino Tournaments:**

| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 41 | CASINO_TOURNAMENTS_BUYIN | DB | Buyin torneio |
| 42 | CASINO_TOURNAMENTS_UN_REGISTER | CR | Desregistro torneio |
| 43 | CASINO_TOURNAMENTS_REBUY | DB | Rebuy torneio |
| 44 | CASINO_TOURNAMENTS_PRIZE_AWARD | CR | Premio torneio |

**Sportsbook:**

| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 59 | SB_BUYIN | DB | Aposta esportiva |
| 60 | SB_LEAVE_TABLE | CR | Saida com saldo |
| 61 | SB_BUYIN_CANCEL | CR | Cancel aposta |
| 62 | SB_LEAVE_TABLE_CANCEL | DB | Cancel leave |
| 63 | SB_PLAYER_BET_CANCELLATION | CR | Cancelamento pelo player |
| 64 | SB_SETTLEMENT | CR | Liquidacao (pagamento) |
| 89 | SB_LOWERING_BET | CR | Reducao de aposta |
| 112 | SB_WIN | CR | Ganho esportivo |
| 113 | SB_WIN_CANCEL | DB | Cancel ganho esportivo |
| 122 | SB_LOCK | — | Lock de valor (reserva) |
| 123 | SB_LOCK_CANCEL | — | Cancel lock (confirma bet) |
| 127 | SB_MANUAL_DEBIT | DB | Debito manual SB |
| 128 | SB_MANUAL_CREDIT | CR | Credito manual SB |
| 134 | SB_MANUAL_DEBIT_CANCEL | CR | Cancel debito manual SB |

**Bonus:**

| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 5 | BONUS_BY_CS | CR | Bonus pelo CS |
| 7 | FREE_CHIP_REAL_CONVERSION | CR | Conversao free chip → real |
| 14 | FREE_CHIPS_ADDITION | CR | Adicao de free chips |
| 15 | FREE_CHIPS_REMOVAL | DB | Remocao de free chips |
| 19 | OFFER_BONUS | CR | Oferta de bonus |
| 20 | ISSUE_BONUS | CR | Emissao (wagering batido) |
| 21 | BONUS_TAKE_TO_GAME | DB | Bonus levado ao jogo |
| 22 | BONUS_TAKE_FROM_GAME | CR | Bonus retornado do jogo |
| 30 | BONUS_EXPIRED | DB | Bonus expirado |
| 37 | BONUS_DROPPED | DB | Bonus descartado |
| 39 | BONUS_BLOCKED | DB | Bonus bloqueado |
| 40 | BONUS_WINNINGS_CAPTURED | DB | Ganhos de bonus capturados |
| 53 | BONUS_WIN_CAP_REVERSED | CR | Estorno captura ganhos bonus |
| 87 | BONUS_ISSUE_CAPTURE | DB | Captura excesso de bonus |
| 88 | ISSUE_DROP_AMOUNT_DEBIT | DB | Debito por drop de bonus ativo |
| 92 | INACTIVE_BONUS_CR_REAL_CASH | CR | Credito real cash por bonus inativo |
| 97 | FORFEIT_SYSTEM_OFFER | DB | Forfeit de oferta pelo sistema |
| 124 | CREDIT_CASINOBONUS_INSTANT_BONUS | CR | Bonus instantaneo cassino (CR) |
| 125 | DEBIT_CASINOBONUS_INSTANT_BONUS | DB | Bonus instantaneo cassino (DB) |
| 135 | ISSUE_DROP_AMOUNT_CREDIT | CR | Credito por drop de bonus |
| 140 | PARTIAL_ISSUE_BONUS | CR | Emissao parcial de bonus |
| 141 | RESTRICTED_REALCASH_CONVERSION | CR | Conversao real cash restrito |

**Fraude / Chargeback / Win Reversal:**

| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 31 | FRAUD_CAPTURE_BY_CS | DB | Ajuste por fraude (CS) |
| 32 | CB_CAPTURE_BY_CS | DB | Ajuste por chargeback (CS) |
| 33 | NEGATIVE_BALANCE_FRAUD | CR neg | Saldo negativo por fraude |
| 34 | NEGATIVE_BALANCE_CB | CR neg | Saldo negativo por chargeback |
| 35 | CB_FRAUD_WINREV_CAPTURE | DB | Ajuste compensacao neg balance |
| 38 | REAL_CASH_CB_ADJUSTMENT | DB | Ajuste chargeback real cash |
| 69 | NEGATIVE_BALANCE_WIN_REVERSE | CR neg | Saldo negativo por win reversal |
| 70 | WIN_REVERSE_CAPTURE | DB | Ajuste real cash win reversal |

**Outros (Poker, Bingo, Lotto, Binary Trade, Fantasy):**

| Codigos | Grupo | Descricao |
|---------|-------|-----------|
| 8-13, 16-18, 23-26 | POKER_* | Transacoes de poker |
| 46-50, 58, 66-67, 71, 74-75 | BT_* | Binary Trade |
| 81-85, 93-94 | BINGO_* | Bingo |
| 98-102, 105, 107, 110-111 | LOTTO_* | Lotto |
| 115-121 | FANTASY_* | Fantasy |

---

### 3.2 `ecr_ec2` — Cadastro de Jogadores (Electronic Cash Register)
**Fonte:** ECR Database Schema Document v1.2 (Pragmatic Solutions)

#### `tbl_ecr` — Tabela mestre de registro

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_ecr_id` | bigint | **PK** — ID interno (18 digitos) |
| `c_external_id` | bigint | ID externo (15 digitos) = Smartico `user_ext_id` |
| `c_email_id` | varchar | **Email do jogador** (NAO `c_email`!) |
| `c_registration_status` | varchar | Status do registro |
| `c_ecr_status` | varchar | `play` ou `real` |
| `c_signup_time` | timestamp | Data/hora do cadastro (**UTC**) |
| `c_affiliate_id` | varchar | ID do afiliado |
| `c_tracker_id` | varchar | ID do tracker |

#### `tbl_ecr_profile` — Dados sensiveis (PII)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_ecr_id` | bigint | FK — ID interno |
| `c_fname` | varchar | Nome |
| `c_lname` | varchar | Sobrenome |
| `c_mobile_number` | varchar | Telefone |
| `c_dob` | date | Data de nascimento |
| `c_address1` | varchar | Endereco |

#### `tbl_ecr_kyc_level` — Status KYC

Niveis: `KYC_0`, `KYC_1`, `KYC_2`

#### `tbl_ecr_aml_flags` — Anti-Lavagem (AML)

Verificacoes de terceiros (Onfido, Sphonic). Colunas booleanas: PEP, Sancoes, Adverse Media (`c_field_value` = 1 ou 0).

#### `tbl_ecr_banner` — Afiliados, Trackers e Sinais de Trafego

Contém UTMs, click IDs e dados de atribuicao de marketing.

---

### 3.3 `bireports_ec2` — Resumos Diarios de BI
**Fonte:** BI Reports Database Schema Document v1.2 (Pragmatic Solutions)
> **Valores em centavos** (/100) — e camada `_ec2`, mesma regra de conversao.

| Tabela | Descricao | Atualizacao |
|--------|-----------|-------------|
| `tbl_ecr_wise_daily_bi_summary` | Resumo diario por jogador (depositos, saques, apostas, ganhos, NGR) | Tempo real (Kafka) |
| `tbl_ecr_txn_type_wise_daily_summary` | Resumo diario por tipo de transacao | Tempo real |
| `tbl_cashier_ecr_daily_payment_details` | Resumo diario de pagamentos | Tempo real |
| `tbl_vendor_games_mapping_data` | **Catalogo de jogos consolidado** (game_id, game_desc, vendor_id) | Cron 4:30 AM |
| `tbl_ecr_daily_settled_game_play_summary` | Resumo diario de jogos liquidados | Cron 1:00 AM |
| `tbl_ecr_gaming_sessions` | Sessoes de jogo (duracao, rodadas) | — |

---

### 3.4 `bonus_ec2` — Ciclo de Vida de Bonus
**Fonte:** Bonus Database Schema Document v1.0 (Pragmatic Solutions)

| Tabela | Descricao |
|--------|-----------|
| `tbl_ecr_bonus_details` | Bonus **ATIVOS** (`c_bonus_status = 'BONUS_OFFER'`) |
| `tbl_ecr_bonus_details_inactive` | Historico: EXPIRED, DROPPED, BONUS_ISSUED_OFFER |
| `tbl_bonus_summary_details` | Resumo financeiro — `c_actual_issued_amount` para BTR |
| `tbl_bonus_segment_details` | Vinculo bonus x segmento CRM |

**Como calcular BTR (Bonus Total Redeemed):**
```sql
SELECT SUM(c_actual_issued_amount) / 100.0 AS btr_brl
FROM bonus_ec2.tbl_bonus_summary_details
WHERE ...
```

---

### 3.5 `cashier_ec2` — Depositos e Saques
**Fonte:** Cashier Database Schema Document v1.0 (Pragmatic Solutions)

| Tabela | Descricao |
|--------|-----------|
| `tbl_cashier_deposit` | Logs e status de depositos |
| `tbl_cashier_cashout` | Logs e status de saques |
| `tbl_deposit_withdrawl_flags` | Flags de bloqueio (fraude, limites) |
| `tbl_cashier_cashout_auto_verify_rule_segments` | Regras auto-cashout por segmento |
| `tbl_cashier_ecr_daily_payment_summary` | Resumo diario pagamentos por jogador |
| `tbl_instrument` | Instrumentos de pagamento do jogador |

**Status de depositos (`c_txn_status`):**

| Status | Descricao |
|--------|-----------|
| `txn_in_process` | Em processamento |
| `txn_confirmed_success` | **Sucesso final** (usar este para filtros!) |
| `txn_confirmed_failed` | Falha |
| `txn_return_applied` | Reembolso |
| `cb_applied` | Chargeback |

**Status de saques:**

| Status | Descricao |
|--------|-----------|
| `co_initiated` | Iniciado |
| `co_verified` | Verificado |
| `co_post_to_processor` | Enviado ao gateway |
| `co_success` | **Sucesso final** |
| `co_failed` | Falha |
| `co_reversed` | Estornado |

**Metodos de pagamento:** `c_option` = `PIXP2F`, `PIXPB`, `VISA`, `BANK_TRANSFER`
**Relacao com fund:** `c_wallet_ref_id` (cashier) = `c_txn_id` (fund)

---

### 3.6 `vendor_ec2` — Catalogo de Jogos e Sportsbook
**Fonte:** Vendor Database Schema Document v1.0 (Pragmatic Solutions)

#### `tbl_vendor_games_mapping_mst` — Catalogo master de jogos

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_id` | int | Auto-increment |
| `c_vendor_id` | varchar | pragmaticplay, hub88, etc. |
| `c_sub_vendor_id` | varchar | pgsoft, betsoft, wazdan, etc. |
| `c_game_id` | varchar | ID do jogo (SB = sempre '0') |
| `c_game_desc` | varchar | Nome do jogo |
| `c_game_cat_id` | int | ID categoria |
| `c_game_type_id` | int | ID tipo |
| `c_product_id` | varchar | Casino ou Sportsbook |
| `c_game_technology` | varchar | F=Flash, H5=HTML5, U=Unity |
| `c_client_platform` | varchar | WEB, MOBILE, ANDROID, WINDOWS |
| `c_status` | varchar | active/inactive |

**Vendors conhecidos:** altenar, betago, betsoft, blueprint, booming, booongo, egtmultigame, evolutiongaming, ezugi, greentube, habanero, hub88, hub88_wazdan, infin, isoftbet, netent, onextwo, playngo, playson, pragmaticplay, pushgaming, quickspin, redtiger, relax, spinomenal, sportradar, vanguard, vivo, yggdrasil

**Sub-vendors:** betago, betsoft, egtmultigame, fungaming, nolimitcity, yggdrassil, pgsoft, wazdan

#### `tbl_sports_book_bets_info` — Header do bilhete esportivo

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_customer_id` | bigint | ECR External ID (= Smartico `user_ext_id`) |
| `c_total_stake` | decimal | Valor apostado (**BRL real**, nao centavos) |
| `c_total_return` | decimal | Retorno total (**BRL real**) |
| `c_bet_state` | varchar | Estado do bilhete |

#### `tbl_sports_book_bet_details` — Legs/selecoes

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_event_name` | varchar | Nome do evento |
| `c_market_name` | varchar | Mercado |
| `c_odds` | decimal | Odds |
| `c_leg_status` | varchar | Status da leg |
| `c_sport_name` | varchar | Esporte |
| `c_league_name` | varchar | Liga/campeonato |

#### `tbl_sports_book_info` — Transacoes financeiras SB

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_amount` | decimal(10,2) | Valor **BRL real** (NAO centavos!) |
| `c_customer_id` | int(20) | ECR External ID |
| `c_operation_type` | varchar(5) | L=Lock, M=Commit, P=Payout, C=Cancel, R=Refund, MC=Manual Credit, MD=Manual Debit |
| `c_vendor_id` | varchar(50) | Sportradar, Altenar, PPBET |

---

### 3.7 `csm_ec2` — Customer Service e Risco
**Fonte:** CSM Database Schema Document v1.0 (Pragmatic Solutions)

NAO existe um schema `risk` separado. O risco e distribuido entre `csm`, `ecr` e `cashier`.

| Componente | Tabela | Descricao |
|------------|--------|-----------|
| Alertas fraude | `csm_ec2.tbl_alerts_config`, `tbl_mst_alert` | Regras e filas de alerta |
| AML | `ecr_ec2.tbl_ecr_aml_flags` | Verificacoes Onfido/Sphonic |
| Bloqueios | `cashier_ec2.tbl_deposit_withdrawl_flags` | Flags de bloqueio por fraude |

Tipos de alerta: `fraud_rules`, `risk`, `velocity_alerts_deposit_amounts_daily`
Filas: `aml_alerts`, `extensive_risk`

---

### 3.8 `segment_ec2` — Segmentacao de Jogadores
**DDL completo pendente** — aguardando resposta da Pragmatic Solutions.

| Tabela | Descricao |
|--------|-----------|
| `tbl_segment_rules` | Configuracao de regras (nome, status) |
| `tbl_segment_ecr_particular_details` | Metricas agregadas do jogador |
| `tbl_segment_ecr_payment_details` | Detalhes de pagamento por segmento |

A segmentacao e **dinamica**: o sistema roda a regra e varre o banco buscando `c_ecr_id` que se encaixam.

---

### 3.9 Outros Databases

| Database | Descricao | Status |
|----------|-----------|--------|
| `casino_ec2` | Categorias e tipos de jogos casino | Referencia |
| `risk_ec2` | Risco (distribuido entre csm/ecr/cashier) | Sem tabelas proprias |
| `fx_ec2` | Cambio, taxas de conversao | Referencia |
| `regulatory_ec2` | Compliance, provedores externos | Referencia |
| `master_ec2` | Dados master/config | Referencia |
| `messaging_ec2` | Mensageria | Referencia |
| `mktg_ec2` | Marketing | Referencia |

---

## 4. Camada BI Mart (`ps_bi`) — Pre-agregada via dbt

Valores ja em **BRL real**. **Preferir para analises quando possivel.**

| Tabela | Granularidade | Descricao |
|--------|---------------|-----------|
| `fct_player_activity_daily` | player/dia | Depositos, saques, bets, wins, GGR, NGR, FTD, NRC, login |
| `fct_casino_activity_daily` | player/game/dia | Atividade casino detalhada |
| `fct_deposits_daily` | player/dia | Fluxo de depositos |
| `fct_cashout_daily` | player/dia | Fluxo de saques |
| `fct_bonus_activity_daily` | player/dia | Atividade de bonus |
| `fct_other_transactions_daily` | player/dia | Outras transacoes |
| `fct_player_balance_daily` | player/dia | Saldo diario |
| `fct_player_balance_hourly` | player/hora | Saldo por hora |
| `fct_player_count` | agregado | Contagem de players |
| `dim_user` | player | Dimensao completa do jogador (ambos IDs) |
| `dim_game` | jogo | Dimensao de jogos |
| `dim_bonus` | bonus | Dimensao de bonus |
| `dmu_cooloff` | player | Cool-off/autoexclusao |

### Limitacoes conhecidas do `ps_bi`

> **ALERTA: `dim_game` tem cobertura de apenas ~0.2%.** PG Soft esta ausente.
> Para catalogo de jogos, preferir `bireports_ec2.tbl_vendor_games_mapping_data`.

> **`affiliate_id` no `ps_bi` e VARCHAR**, nao INT. Cuidado com casts em joins.

---

## 5. Camada Silver — Snapshots dbt

Valores em **BRL real**. Timestamps UTC.

| Tabela | Descricao |
|--------|-----------|
| `dmu_dim_user_main` | Snapshot principal do player |
| `dmu_deposits` | Snapshot depositos |
| `dmu_withdrawals` | Snapshot saques |
| `dmu_ecr` | Snapshot cadastro |

---

## 6. Regras SQL Obrigatorias (Trino/Presto)

### 6.1 Fuso Horario (OBRIGATORIO)

O Athena opera em **UTC**. Toda query que filtre ou exiba timestamps DEVE converter para BRT:

```sql
coluna AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
```

> Campos do tipo `DATE` no Athena sao **UTC truncado** — sem hora, mas a data pode divergir
> da data BRT para eventos apos 21h UTC (= 00h BRT). Considere isso em filtros por data.

### 6.2 Filtro Temporal (REGRA DE OURO — otimizacao de custo)

Sempre incluir filtro temporal **antes** de outros predicados para evitar Full Scan no S3 (custo AWS alto).

> **ATENCAO:** A coluna `dt` pode nao existir como coluna visivel em algumas tabelas
> (particao Iceberg implicita). Validar com `SHOW COLUMNS FROM tabela` antes de usar.

**Se `dt` existir como coluna:**
```sql
WHERE f.dt IN ('2026-03-16', '2026-03-17')
  AND f.c_start_time >= TIMESTAMP '2026-03-16'
```

**Se `dt` NAO existir (alternativa):**
```sql
WHERE c_start_time >= TIMESTAMP '2026-03-16'
  AND c_start_time < TIMESTAMP '2026-03-17'
```

### 6.3 Filtro de Produto (OBRIGATORIO em fund_ec2)

```sql
WHERE c_product_id = 'CASINO'  -- ou 'SPORTSBOOK'
```

> Sem este filtro, transacoes de casino e sportsbook se misturam nos resultados.

### 6.4 Status de Transacao

| Contexto | Campo | Valor correto |
|----------|-------|---------------|
| fund_ec2 (transacoes) | `c_txn_status` | `'SUCCESS'` |
| cashier_ec2 (depositos) | `c_txn_status` | `'txn_confirmed_success'` |
| cashier_ec2 (saques) | status | `'co_success'` |

> **ATENCAO:** `'SUCCESS'` e `'txn_confirmed_success'` sao databases DIFERENTES.
> Usar o errado no contexto errado retorna zero linhas SEM erro. Cuidado.

### 6.5 Valores Monetarios

| Camada | Unidade | Conversao |
|--------|---------|-----------|
| `_ec2` (exceto sportsbook) | Centavos | `/ 100.0` |
| `_ec2` sportsbook (vendor_ec2) | **BRL real** | Sem conversao |
| `ps_bi` | BRL real | Sem conversao |
| `silver` | BRL real | Sem conversao |

### 6.6 Sintaxe Presto/Trino

```sql
-- Cast de timestamp
TIMESTAMP '2026-03-16'

-- Truncar data
date_trunc('day', coluna)

-- Funcoes de data
date_add('day', 7, data)
date_diff('day', inicio, fim)

-- Contagem condicional
COUNT_IF(c_txn_type = 72)

-- NAO suportado
CREATE TEMP TABLE  -- usar CTEs: WITH ... AS (...)
```

### 6.7 Filtro de Test Users

```sql
-- No ps_bi
WHERE is_test = false  -- ou is_test = 0

-- No bireports/ecr
WHERE c_test_user = false
```

> Divergencia de ~3% entre fontes com/sem filtro de test users.

### 6.8 Otimizacao de Custo

- **Sempre filtrar por colunas de particao** (`dt`, `date`)
- **Evitar `SELECT *`** — selecionar apenas colunas necessarias
- **Preferir `ps_bi`** (pre-agregado) sobre `_ec2` (bruto) quando possivel
- **Usar `LIMIT`** durante desenvolvimento/testes
- **Dados D-0 sao parciais** — sempre usar D-1 ou anterior para entregas

---

## 7. Relacao de IDs (CRITICO)

| ID | Coluna | Tipo | Onde |
|----|--------|------|------|
| Interno | `c_ecr_id` | bigint (18 dig) | Todas tabelas `_ec2`, `ps_bi.dim_user.ecr_id` |
| Externo | `c_external_id` | bigint (15 dig) | `ecr_ec2.tbl_ecr`, `ps_bi.dim_user.external_id` |
| Smartico | `user_ext_id` | — | BigQuery `j_user`, `tr_*`, `g_*` |
| Sportsbook | `c_customer_id` | — | `vendor_ec2` (= External ID) |

**Regra de join:** Nunca filtrar tabelas `fund_ec2` pela `external_id` diretamente.
Sempre passar pelo `ecr_ec2.tbl_ecr` para converter `c_ecr_id` ↔ `c_external_id`.
Na camada `ps_bi`, o `dim_user` ja tem ambos os IDs.

---

## 8. Receitas SQL — Queries de Referencia

### 8.1 GGR Casino (fund_ec2) — com rollbacks

```sql
-- GGR Casino = Turnover liquido - Wins liquido (descontando rollbacks)
-- OBRIGATORIO: filtro de produto, status e timezone
WITH casino_txns AS (
    SELECT
        date(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_brt,
        -- Apostas (debito do jogador)
        SUM(CASE WHEN c_txn_type IN (27, 28) THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bets,
        -- Rollback de apostas (devolvem ao jogador)
        SUM(CASE WHEN c_txn_type IN (72, 76) THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS rollback_bets,
        -- Ganhos (credito para o jogador)
        SUM(CASE WHEN c_txn_type IN (45, 65, 79, 80) THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS wins,
        -- Rollback de ganhos (devolvem da casa)
        SUM(CASE WHEN c_txn_type IN (77, 86, 114) THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS rollback_wins
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_txn_status = 'SUCCESS'
      AND c_product_id = 'CASINO'
      AND c_start_time >= TIMESTAMP '2026-03-30'
      AND c_start_time < TIMESTAMP '2026-03-31'
    GROUP BY 1
)
SELECT
    data_brt,
    bets - rollback_bets AS turnover_liquido_brl,
    wins - rollback_wins AS wins_liquido_brl,
    (bets - rollback_bets) - (wins - rollback_wins) AS ggr_brl
FROM casino_txns
```

> Sem rollbacks o GGR fica inflado. Esse erro ja aconteceu no time.

### 8.2 Depositos aprovados por metodo (cashier_ec2)

```sql
-- Status CORRETO para cashier: 'txn_confirmed_success' (NAO 'SUCCESS')
SELECT
    c_option AS metodo_pagamento,
    COUNT(*) AS qtd_depositos,
    SUM(c_amount / 100.0) AS total_brl,
    AVG(c_amount / 100.0) AS ticket_medio_brl
FROM cashier_ec2.tbl_cashier_deposit
WHERE c_txn_status = 'txn_confirmed_success'
  AND date(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
      BETWEEN DATE '2026-03-24' AND DATE '2026-03-30'
GROUP BY 1
ORDER BY total_brl DESC
```

### 8.3 Registros intraday (ecr_ec2)

```sql
-- ecr_ec2 = melhor fonte intraday para registros (99% match com BigQuery)
SELECT
    date(c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS data_brt,
    COUNT(*) AS total_registros,
    COUNT_IF(c_ecr_status = 'real') AS registros_real
FROM ecr_ec2.tbl_ecr
WHERE c_signup_time >= TIMESTAMP '2026-03-30'
  AND c_signup_time < TIMESTAMP '2026-03-31'
GROUP BY 1
```

### 8.4 Join fund_ec2 com ecr_ec2 (converter IDs + filtrar test users)

```sql
-- fund_ec2 NAO tem coluna de test user. Fazer JOIN com ecr para filtrar.
SELECT
    e.c_external_id,
    f.c_txn_type,
    f.c_amount_in_ecr_ccy / 100.0 AS valor_brl
FROM fund_ec2.tbl_real_fund_txn f
JOIN ecr_ec2.tbl_ecr e ON f.c_ecr_id = e.c_ecr_id
WHERE f.c_txn_status = 'SUCCESS'
  AND f.c_product_id = 'CASINO'
  AND f.c_start_time >= TIMESTAMP '2026-03-30'
  AND f.c_start_time < TIMESTAMP '2026-03-31'
  -- Filtro de test users via ecr (fund nao tem is_test)
  AND e.c_ecr_status = 'real'
```

### 8.5 Join cross-bank Athena + BigQuery (via Python)

```python
# Chave: external_id (Athena) = user_ext_id (BigQuery)
import pandas as pd
from db.athena import query_athena
from db.bigquery import query_bigquery

df_athena = query_athena("""
    SELECT external_id, ecr_id
    FROM ps_bi.dim_user
    WHERE is_test = false
""", database="ps_bi")

df_bq = query_bigquery("""
    SELECT user_ext_id, core_affiliate_id, has_ftd
    FROM `smartico-bq6.dwh_ext_24105.j_user`
""")

df_merged = df_athena.merge(df_bq, left_on='external_id', right_on='user_ext_id', how='left')
```

---

## 9. Gaps e Pendencias

| Item | Status | Acao necessaria |
|------|--------|-----------------|
| DDL completo `segment_ec2` | Pendente | Solicitar a Pragmatic Solutions |
| Tabelas master de afiliados | Nao replicadas | Solicitar replicacao ao infra |
| Schema `risk_ec2` detalhado | Distribuido | Documentar mapeamento completo |
| `dim_game` cobertura 0.2% | Conhecido | Usar `bireports_ec2.tbl_vendor_games_mapping_data` |
| PG Soft ausente no `ps_bi` | Conhecido | Usar `bireports_ec2` para analises de jogos |

---

*Documento consolidado em 2026-03-31.*
*Fontes: Pragmatic Solutions Database Schema Documents v1.0-v1.3, validacoes empiricas, feedbacks acumulados do time de dados MultiBet.*
