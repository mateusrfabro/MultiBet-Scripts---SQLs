# Mapeamento de Bancos de Dados — MultiBet
**Data:** 2026-03-18 | **Autor:** Mateus F. (Squad Intelligence Engine)
**Objetivo:** Documentar a estrutura completa dos dois bancos para alinhamento com o arquiteto.

---

## 1. Visao Geral

| Aspecto | Athena (Pragmatic Solutions) | BigQuery (Smartico CRM) |
|---------|------------------------------|-------------------------|
| **Engine** | Trino/Presto (Iceberg Data Lake) | Google BigQuery |
| **Regiao** | sa-east-1 (AWS) | GCP |
| **Tipo de acesso** | Read-only (user `mb-prod-db-iceberg-ro`) | Read-only |
| **Projeto/Dataset** | 19 databases (sufixo `_ec2`, `ps_bi`, `silver`) | `smartico-bq6.dwh_ext_24105` |
| **Sintaxe SQL** | Presto/Trino | Standard SQL (BigQuery) |
| **Timestamps** | UTC (requer conversao para BRT) | UTC (Smartico padrao) |
| **Unidade monetaria** | Centavos (/100) nos `_ec2`; BRL real no `ps_bi` e `silver` | BRL real |
| **Conexao** | `db/athena.py` (boto3) | `db/bigquery.py` (google-cloud) |

---

## 2. Athena — Databases e Camadas

### 2.1 Camada Bruta (`_ec2`) — Dados Transacionais
Replica direta do Pragmatic Solutions. Valores em **centavos** (/100). Timestamps em **UTC**.

| Database | Descricao | Tabelas-chave |
|----------|-----------|---------------|
| `fund_ec2` | Carteira/ledger do jogador | `tbl_real_fund_txn`, `tbl_real_fund`, `tbl_realcash_sub_fund_txn`, `tbl_bonus_sub_fund_txn` |
| `ecr_ec2` | Cadastro, KYC, IDs, AML | `tbl_ecr`, `tbl_ecr_profile`, `tbl_ecr_kyc_level`, `tbl_ecr_aml_flags`, `tbl_ecr_banner` |
| `bireports_ec2` | Resumos diarios BI, catalogo jogos | `tbl_ecr_wise_daily_bi_summary`, `tbl_vendor_games_mapping_data` |
| `bonus_ec2` | Ciclo de vida de bonus, wagering | `tbl_ecr_bonus_details`, `tbl_ecr_bonus_details_inactive`, `tbl_bonus_summary_details`, `tbl_bonus_segment_details` |
| `cashier_ec2` | Depositos, saques, gateways | `tbl_cashier_deposit`, `tbl_cashier_cashout`, `tbl_deposit_withdrawl_flags` |
| `casino_ec2` | Categorias e tipos de jogos | `tbl_casino_game_category_mst`, `tbl_casino_game_type_mst` |
| `csm_ec2` | Customer Service, alertas fraude | `tbl_alerts_config`, `tbl_mst_alert` (27 objetos) |
| `vendor_ec2` | Catalogo jogos, sportsbook, vendors | `tbl_vendor_games_mapping_mst`, `tbl_sports_book_bets_info`, `tbl_sports_book_bet_details`, `tbl_sports_book_info` |
| `segment_ec2` | Segmentacao de jogadores | `tbl_segment_rules`, `tbl_segment_ecr_particular_details`, `tbl_segment_ecr_payment_details` |
| `risk_ec2` | Risco (distribuido entre csm/ecr/cashier) | — |
| `fx_ec2` | Cambio, taxas de conversao | — |
| `regulatory_ec2` | Compliance, provedores externos | — |
| `master_ec2` | Dados master/config | — |
| `messaging_ec2` | Mensageria | — |
| `mktg_ec2` | Marketing | — |

### 2.2 Camada BI Mart (`ps_bi`) — Pre-agregada via dbt
Valores ja em **BRL real** (nao centavos). Timestamps UTC. **Preferir para analises.**

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
| `dim_user` | player | Dimensao completa do jogador |
| `dim_game` | jogo | Dimensao de jogos |
| `dim_bonus` | bonus | Dimensao de bonus |
| `dmu_cooloff` | player | Cool-off/autoexclusao |

### 2.3 Camada Silver — Snapshots dbt
Valores em **BRL real**. Timestamps UTC.

| Tabela | Descricao |
|--------|-----------|
| `dmu_dim_user_main` | Snapshot principal do player |
| `dmu_deposits` | Snapshot depositos |
| `dmu_withdrawals` | Snapshot saques |
| `dmu_ecr` | Snapshot cadastro |

---

## 3. Athena — Detalhamento de Tabelas Criticas

### 3.1 `fund_ec2.tbl_real_fund_txn` — Tabela principal de transacoes

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_txn_id` | bigint | PK — ID unico da transacao |
| `c_ecr_id` | bigint | ID interno do jogador (18 digitos) |
| `c_txn_type` | int | Tipo numerico (ver mapeamento completo abaixo) |
| `c_txn_status` | varchar | `INIT`, `SUCCESS`, `FAILURE` |
| `c_amount_in_ecr_ccy` | bigint | Valor em centavos BRL (/100) |
| `c_start_time` | timestamp | Timestamp da transacao (UTC) |
| `c_game_id` | varchar | ID do jogo |
| `c_session_id` | varchar | ID da sessao |
| `c_op_type` | varchar | `CR` (credito) ou `DB` (debito) |
| `c_vendor_id` | varchar | Provider (pragmaticplay, hub88, etc.) |
| `c_event_id` | varchar | ID evento sportsbook |
| `c_channel` | varchar | DESKTOP, MOBILE |
| `c_sub_channel` | varchar | HTML, native, BackOffice |

#### Mapeamento `c_txn_type` (135+ tipos)

**Financeiro:**
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
| 78 | MIGRATION_TYPE | CR | Migracao |
| 126 | REAL_CASH_DEPOSIT_REFUND | DB | Estorno de deposito |

**Casino:**
| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 27 | CASINO_BUYIN | DB | Aposta cassino |
| 28 | CASINO_REBUY | DB | Rebuy cassino |
| 45 | CASINO_WIN | CR | Ganho cassino |
| 65 | JACKPOT_WIN | CR | Jackpot |
| 72 | CASINO_BUYIN_CANCEL | CR | Rollback aposta |
| 77 | CASINO_WIN_CANCEL | DB | Rollback ganho |
| 80 | CASINO_FREESPIN_WIN | CR | Ganho free spin |

**Sportsbook:**
| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 59 | SB_BUYIN | DB | Aposta esportiva |
| 61 | SB_BUYIN_CANCEL | CR | Cancel aposta |
| 64 | SB_SETTLEMENT | CR | Liquidacao |
| 112 | SB_WIN | CR | Ganho esportivo |
| 113 | SB_WIN_CANCEL | DB | Cancel ganho |

**Bonus:**
| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 5 | BONUS_BY_CS | CR | Bonus pelo CS |
| 14 | FREE_CHIPS_ADDITION | CR | Adicao free chips |
| 19 | OFFER_BONUS | CR | Oferta de bonus |
| 20 | ISSUE_BONUS | CR | Emissao (wagering batido) |
| 30 | BONUS_EXPIRED | DB | Bonus expirado |
| 37 | BONUS_DROPPED | DB | Bonus descartado |

**Fraude/Chargeback:**
| Cod | Constante | Op | Descricao |
|-----|-----------|-----|-----------|
| 31 | FRAUD_CAPTURE_BY_CS | DB | Ajuste por fraude |
| 32 | CB_CAPTURE_BY_CS | DB | Ajuste por chargeback |
| 33 | NEG_BAL_FRAUD | CR neg | Saldo negativo fraude |
| 34 | NEG_BAL_CB | CR neg | Saldo negativo chargeback |

### 3.2 `ecr_ec2.tbl_ecr` — Cadastro de jogadores

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_ecr_id` | bigint | PK — ID interno (18 digitos) |
| `c_external_id` | bigint | ID externo (15 digitos) = Smartico `user_ext_id` |
| `c_registration_status` | varchar | Status do registro |
| `c_ecr_status` | varchar | `play` ou `real` |
| `c_signup_time` | timestamp | Data/hora do cadastro (UTC) |
| `c_affiliate_id` | varchar | ID do afiliado |

### 3.3 `vendor_ec2` — Sportsbook (3 tabelas)

#### `tbl_sports_book_bets_info` — Header do bilhete
| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `c_customer_id` | bigint | ECR External ID (= Smartico `user_ext_id`) |
| `c_total_stake` | decimal | Valor apostado (BRL real, nao centavos) |
| `c_total_return` | decimal | Retorno total (BRL real) |
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
| `c_amount` | decimal(10,2) | Valor BRL real (nao centavos!) |
| `c_customer_id` | int(20) | ECR External ID |
| `c_operation_type` | varchar(5) | L=Lock, M=Commit, P=Payout, C=Cancel, R=Refund |
| `c_vendor_id` | varchar(50) | Sportradar, Altenar, PPBET |

### 3.4 `cashier_ec2` — Depositos e Saques

#### Status de depositos (`c_txn_status`)
| Status | Descricao |
|--------|-----------|
| `txn_in_process` | Em processamento |
| `txn_confirmed_success` | Sucesso final |
| `txn_confirmed_failed` | Falha |
| `txn_return_applied` | Reembolso |
| `cb_applied` | Chargeback |

#### Status de saques
| Status | Descricao |
|--------|-----------|
| `co_initiated` | Iniciado |
| `co_verified` | Verificado |
| `co_success` | Sucesso final |
| `co_failed` | Falha |
| `co_reversed` | Estornado |

### 3.5 `bonus_ec2` — Ciclo de vida de bonus

| Tabela | Descricao |
|--------|-----------|
| `tbl_ecr_bonus_details` | Bonus ATIVOS (`c_bonus_status = 'BONUS_OFFER'`) |
| `tbl_ecr_bonus_details_inactive` | Historico (EXPIRED, DROPPED, BONUS_ISSUED_OFFER) |
| `tbl_bonus_summary_details` | Resumo financeiro — `c_actual_issued_amount` para BTR |
| `tbl_bonus_segment_details` | Vinculo bonus x segmento CRM |

### 3.6 `ps_bi.dim_user` — Dimensao de jogador (camada BI)

| Coluna | Descricao |
|--------|-----------|
| `external_id` | = Smartico `user_ext_id` (bridge direto) |
| `ecr_id` | ID interno 18 digitos |
| + metricas agregadas | registros, FTDs, conversao, etc. |

---

## 4. BigQuery (Smartico CRM) — Dataset Completo

### 4.1 Conexao
- **Projeto billing:** `smr-dwh`
- **Dataset:** `smartico-bq6.dwh_ext_24105`
- **Sintaxe:** `` SELECT * FROM `smartico-bq6.dwh_ext_24105.nome_view` ``

### 4.2 Prefixos e Organizacao (90 views)

| Prefixo | Tipo | Qtd | Descricao |
|---------|------|-----|-----------|
| `dm_` | Dimensao | 37 | Lookup/cadastro (achievements, bonus, segmento, produto...) |
| `g_` | Gamification | 11 | Torneios, minigames, shop, UX |
| `j_` | Journey | 9 | Jornada do usuario, comunicacoes, bonus, webhooks |
| `tr_` | Transactions | 26 | Eventos transacionais (deposito, saque, bet, login...) |
| `jp_` | Jackpot | 1 | Apostas de jackpot |
| `raf_` | Raffle | 2 | Rifas |
| `ml_` | Machine Learning | 1 | Preferencias de players |

### 4.3 Views Detalhadas

#### Dimensoes (`dm_`) — 37 views
| View | Descricao |
|------|-----------|
| `dm_ach` | Achievements (conquistas) |
| `dm_ach_activity` | Atividades de achievement |
| `dm_ach_custom_sections` | Secoes customizadas |
| `dm_ach_level` | Niveis de achievement |
| `dm_ach_points_change_source` | Fontes de mudanca de pontos |
| `dm_ach_task` | Tasks de achievement |
| `dm_activity_type` | Tipos de atividade |
| `dm_audience` | Audiencias |
| `dm_automation_rule` | Regras de automacao CRM |
| `dm_bonus_template` | Templates de bonus |
| `dm_brand` | Marcas |
| `dm_casino_game_name` | Nomes de jogos casino |
| `dm_casino_game_type` | Tipos de jogos casino |
| `dm_casino_provider_name` | Nomes de provedores casino |
| `dm_churn_rank` | Ranking de churn |
| `dm_com_fail_reason` | Motivos de falha de comunicacao |
| `dm_deal` | Deals/ofertas |
| `dm_engagement_fail_reason` | Motivos de falha de engajamento |
| `dm_event_type` | Tipos de evento |
| `dm_funnel_marker` | Marcadores de funil |
| `dm_j_formula` | Formulas de jornada |
| `dm_jp_template` | Templates de jackpot |
| `dm_product` | Produtos |
| `dm_providers_mail` | Provedores de email |
| `dm_providers_sms` | Provedores de SMS |
| `dm_raffle` | Rifas |
| `dm_resource` | Recursos |
| `dm_rfm_category` | Categorias RFM |
| `dm_saw_prize` | Premios Spin-a-Wheel |
| `dm_saw_template` | Templates Spin-a-Wheel |
| `dm_segment` | Segmentos CRM |
| `dm_shop_item` | Itens da loja |
| `dm_sport_league` | Ligas esportivas |
| `dm_sport_type` | Tipos de esporte |
| `dm_tag` | Tags |
| `dm_tag_entity` | Entidades de tag |
| `dm_tournament` | Torneios |
| `dm_tournament_instance` | Instancias de torneio |

#### Gamificacao (`g_`) — 11 views
| View | Descricao |
|------|-----------|
| `g_ach_claimed` | Achievements reclamados |
| `g_ach_completed` | Achievements completados |
| `g_ach_levels_changed` | Mudancas de nivel |
| `g_ach_optins` | Opt-ins de achievement |
| `g_ach_points_change_log` | Log de mudanca de pontos |
| `g_gems_diamonds_change_log` | Log de gems/diamonds |
| `g_minigames` | Minigames (ex: Spin-a-Wheel) |
| `g_shop_transactions` | Transacoes da loja |
| `g_tournament_analytics` | Analytics de torneios |
| `g_tournament_winners` | Vencedores de torneios |
| `g_ux` | Eventos de UX |

#### Jornada (`j_`) — 9 views
| View | Descricao |
|------|-----------|
| `j_automation_rule_progress` | Progresso de regras de automacao |
| `j_av` | Activity verification |
| `j_bonuses` | Bonus emitidos pelo CRM |
| `j_communication` | Comunicacoes enviadas (push, email, SMS) |
| `j_engagements` | Engajamentos (popups, in-app messages) |
| `j_events_stats_daily` | Estatisticas de eventos (diario) |
| `j_events_stats_hourly` | Estatisticas de eventos (por hora) |
| `j_user` | Dados do usuario no CRM |
| `j_user_no_enums` | Dados do usuario sem enums |
| `j_webhooks_facts` | Fatos de webhooks |

#### Transacoes (`tr_`) — 26 views
| View | Descricao |
|------|-----------|
| `tr_acc_deposit_approved` | Deposito aprovado |
| `tr_acc_deposit_failed` | Deposito falhou |
| `tr_acc_withdrawal_approved` | Saque aprovado |
| `tr_ach_achievement_completed` | Achievement completado |
| `tr_ach_level_changed` | Nivel de achievement mudou |
| `tr_ach_points_added` | Pontos adicionados |
| `tr_ach_points_deducted` | Pontos deduzidos |
| `tr_casino_bet` | Aposta casino |
| `tr_casino_win` | Ganho casino |
| `tr_client_action` | Acao do cliente |
| `tr_core_bonus_failed` | Bonus falhou |
| `tr_core_bonus_given` | Bonus concedido |
| `tr_core_dynamic_bonus_calculated` | Bonus dinamico calculado |
| `tr_core_dynamic_bonus_issued` | Bonus dinamico emitido |
| `tr_core_fin_stats_update` | Update de stats financeiras |
| `tr_login` | Login |
| `tr_minigame_attempt` | Tentativa minigame |
| `tr_minigame_spins_issued` | Spins emitidos |
| `tr_minigame_win` | Ganho minigame |
| `tr_shop_item_purchase_successed` | Compra na loja |
| `tr_sport_bet_open` | Aposta esportiva aberta |
| `tr_sport_bet_selection_open` | Selecao esportiva aberta |
| `tr_sport_bet_selection_settled` | Selecao esportiva liquidada |
| `tr_sport_bet_settled` | Aposta esportiva liquidada |
| `tr_tournament_lose` | Derrota em torneio |
| `tr_tournament_user_registered` | Registro em torneio |
| `tr_tournament_win` | Vitoria em torneio |

#### Outros
| View | Descricao |
|------|-----------|
| `jp_bet` | Apostas de jackpot |
| `ml_player_preferences` | Preferencias ML do player |
| `raf_tickets` | Tickets de rifa |
| `raf_won_prizes` | Premios ganhos em rifas |

---

## 5. Bridge entre os Bancos (Chave de Integracao)

### 5.1 Join principal: Athena <-> BigQuery

```
Athena ecr_ec2.tbl_ecr.c_external_id  =  BigQuery j_user.user_ext_id (Smartico)
Athena ps_bi.dim_user.external_id      =  BigQuery j_user.user_ext_id (Smartico)
```

| Campo Athena | Onde | Campo BigQuery | Onde | Descricao |
|-------------|------|----------------|------|-----------|
| `c_external_id` | `ecr_ec2.tbl_ecr` | `user_ext_id` | `j_user`, `tr_*`, `g_*`, etc. | ID externo do player |
| `external_id` | `ps_bi.dim_user` | `user_ext_id` | `j_user` | Mesmo ID (camada BI) |
| `c_ecr_id` | todas tabelas `_ec2` | — | — | ID interno (nao existe no BigQuery) |
| `ecr_id` | `ps_bi.dim_user` | — | — | ID interno na camada BI |

### 5.2 Fluxo de Join

```
                    ATHENA                                    BIGQUERY
                    ------                                    --------
fund_ec2.tbl_real_fund_txn.c_ecr_id
        |
        v
ecr_ec2.tbl_ecr.c_ecr_id  -->  c_external_id  ====  user_ext_id  -->  j_user, tr_*, g_*
        |
        v
ps_bi.dim_user.ecr_id  -->  external_id  ===========  user_ext_id
```

### 5.3 Regra de join CRITICA
- **Nunca filtrar tabelas do `fund_ec2` pela external_id diretamente** — sempre passar pelo `ecr_ec2.tbl_ecr` para fazer a conversao `c_ecr_id` <-> `c_external_id`.
- Na camada `ps_bi`, o `dim_user` ja tem ambos os IDs, facilitando o join.
- No sportsbook (`vendor_ec2`), o `c_customer_id` ja e o External ID (= Smartico `user_ext_id`).

---

## 6. Mapeamento Conceitual: Onde encontrar cada tipo de dado

| Conceito de negocio | Athena (tabela/camada) | BigQuery (view) |
|---------------------|------------------------|-----------------|
| **Cadastro do player** | `ecr_ec2.tbl_ecr` / `ps_bi.dim_user` | `j_user` / `j_user_no_enums` |
| **Depositos** | `fund_ec2.tbl_real_fund_txn` (type=1) / `cashier_ec2.tbl_cashier_deposit` / `ps_bi.fct_deposits_daily` | `tr_acc_deposit_approved` |
| **Saques** | `fund_ec2.tbl_real_fund_txn` (type=2) / `cashier_ec2.tbl_cashier_cashout` / `ps_bi.fct_cashout_daily` | `tr_acc_withdrawal_approved` |
| **Apostas casino** | `fund_ec2.tbl_real_fund_txn` (type=27) / `ps_bi.fct_casino_activity_daily` | `tr_casino_bet` |
| **Ganhos casino** | `fund_ec2.tbl_real_fund_txn` (type=45) / `ps_bi.fct_casino_activity_daily` | `tr_casino_win` |
| **Apostas esportivas** | `fund_ec2.tbl_real_fund_txn` (type=59) / `vendor_ec2.tbl_sports_book_bets_info` | `tr_sport_bet_open` |
| **Resultados esportivos** | `fund_ec2.tbl_real_fund_txn` (type=64,112) / `vendor_ec2.tbl_sports_book_bet_details` | `tr_sport_bet_settled` |
| **Bonus concedidos** | `bonus_ec2.tbl_ecr_bonus_details` / `ps_bi.fct_bonus_activity_daily` | `tr_core_bonus_given` / `j_bonuses` |
| **Bonus expirados/drops** | `bonus_ec2.tbl_ecr_bonus_details_inactive` | — |
| **BTR (custo bonus)** | `bonus_ec2.tbl_bonus_summary_details` | — |
| **Login** | — | `tr_login` |
| **Comunicacoes CRM** | — | `j_communication` |
| **Engajamentos CRM** | — | `j_engagements` |
| **Segmentos** | `segment_ec2.tbl_segment_rules` | `dm_segment` |
| **Gamificacao** | — | `g_ach_*`, `g_tournament_*`, `g_minigames` |
| **Catálogo de jogos** | `bireports_ec2.tbl_vendor_games_mapping_data` / `ps_bi.dim_game` | `dm_casino_game_name` |
| **KYC** | `ecr_ec2.tbl_ecr_kyc_level` | — |
| **AML/Fraude** | `ecr_ec2.tbl_ecr_aml_flags` / `csm_ec2.tbl_alerts_config` | — |
| **Saldo do player** | `fund_ec2.tbl_real_fund` / `ps_bi.fct_player_balance_daily` | — |
| **Resumo diario BI** | `bireports_ec2.tbl_ecr_wise_daily_bi_summary` / `ps_bi.fct_player_activity_daily` | `j_events_stats_daily` |
| **Afiliados/Trackers** | `ecr_ec2.tbl_ecr_banner` (UTMs, click IDs) | — |
| **RFM** | — | `dm_rfm_category` |
| **Churn** | — | `dm_churn_rank` |

---

## 7. Regras Tecnicas Importantes

### 7.1 Athena (Trino/Presto)
- **Fuso:** `coluna AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'`
- **Valores _ec2:** centavos → `/100.0`
- **Valores ps_bi/silver:** ja em BRL real
- **Status fund:** `c_txn_status = 'SUCCESS'`
- **Status cashier deposito:** `c_txn_status = 'txn_confirmed_success'`
- **Status cashier saque:** `co_success`
- **Sem CREATE TEMP TABLE** — usar CTEs (`WITH ... AS`)
- **Custo por scan** — sempre filtrar por colunas de particao (`dt`, `date`)
- **Read-only** — sem INSERT/UPDATE/DELETE

### 7.2 BigQuery (Smartico)
- **Sintaxe:** `` `smartico-bq6.dwh_ext_24105.view_name` ``
- **Valores:** ja em BRL real
- **Projeto billing:** `smr-dwh`

### 7.3 Conversao de IDs
```
Athena c_ecr_id (interno 18 dig) --> ecr_ec2.tbl_ecr --> c_external_id (15 dig) = BigQuery user_ext_id
Athena ps_bi.dim_user.external_id = BigQuery user_ext_id (atalho direto)
Athena vendor_ec2 c_customer_id = BigQuery user_ext_id (sportsbook)
```

---

## 8. Pendencias e Gaps Conhecidos

| Item | Status | Acao necessaria |
|------|--------|-----------------|
| DDL completo `segment_ec2` | Pendente | Solicitar a Pragmatic Solutions |
| Tabelas master de afiliados (`tbl_affiliate_mst_config`, `tbl_affiliate_tracker_mapping`) | Nao replicadas no Data Lake | Solicitar replicacao ao time de infra |
| Schema `risk_ec2` detalhado | Distribuido entre csm/ecr/cashier | Documentar mapeamento completo |
| `game_image_url` no pipeline grandes_ganhos | NULL | Time de dev preenche via catalogo |

---

## 9. Diagrama de Relacionamento (Simplificado)

```
+---------------------------+          +---------------------------+
|     ATHENA (Pragmatic)    |          |   BIGQUERY (Smartico CRM) |
+---------------------------+          +---------------------------+
|                           |          |                           |
| ecr_ec2.tbl_ecr           |  bridge  | j_user                    |
|   c_ecr_id (PK interno)   |          |   user_ext_id             |
|   c_external_id ===================> |   (= c_external_id)       |
|                           |          |                           |
| fund_ec2.tbl_real_fund_txn|          | tr_casino_bet             |
|   c_ecr_id (FK)           |          | tr_casino_win             |
|   c_txn_type, c_amount... |          | tr_acc_deposit_approved   |
|                           |          | tr_acc_withdrawal_approved|
| cashier_ec2               |          |                           |
|   tbl_cashier_deposit     |          | j_bonuses                 |
|   tbl_cashier_cashout     |          | j_communication           |
|                           |          | j_engagements             |
| bonus_ec2                 |          |                           |
|   tbl_ecr_bonus_details   |          | dm_segment                |
|   tbl_bonus_summary       |          | dm_bonus_template         |
|                           |          | dm_rfm_category           |
| vendor_ec2 (sportsbook)   |          | dm_churn_rank             |
|   c_customer_id =====================|   (= user_ext_id)         |
|                           |          |                           |
| ps_bi (BI Mart - dbt)     |          | g_* (gamificacao)         |
|   dim_user.external_id =============|   (= user_ext_id)         |
|   fct_player_activity_daily|         |                           |
|   fct_casino_activity_daily|         | ml_player_preferences     |
+---------------------------+          +---------------------------+
```

---

*Documento gerado em 2026-03-18 para revisao do arquiteto.*
*Fonte: documentacao Pragmatic Solutions (PDFs v1.0-v1.3), Smartico CRM, validacoes empiricas no ambiente de producao.*
