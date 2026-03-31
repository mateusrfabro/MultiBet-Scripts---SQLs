# Guia BigQuery — Smartico CRM
**Versao:** 1.0 | **Data:** 2026-03-31 | **Autor:** Mateus F. (Squad Intelligence Engine)
**Fonte:** Smartico CRM (BigQuery DWH), validacoes empiricas, feedbacks do time

---

## 1. Visao Geral

| Aspecto | Detalhe |
|---------|---------|
| **Engine** | Google BigQuery (Standard SQL) |
| **Projeto billing** | `smr-dwh` |
| **Dataset** | `smartico-bq6.dwh_ext_24105` |
| **Conta de servico** | `dwh-ext-24105@smr-dwh.iam.gserviceaccount.com` |
| **Tipo de acesso** | **Read-only** |
| **Timestamps** | UTC (padrao Smartico) |
| **Valores monetarios** | **BRL real** (sem conversao necessaria) |
| **Credenciais** | `bigquery_credentials.json` (raiz do projeto) |
| **Conexao** | `db/bigquery.py` → `query_bigquery(sql)` |
| **Python** | `C:/Users/NITRO/AppData/Local/Programs/Python/Python312/python.exe` |

---

## 2. Sintaxe de Query

```sql
SELECT col1, col2, col3  -- evitar SELECT * em producao
FROM `smartico-bq6.dwh_ext_24105.nome_da_view`
WHERE ...
```

> Sempre usar backticks em volta do caminho completo `projeto.dataset.view`.

---

## 3. Organizacao das Views (90 total)

O Smartico organiza seus dados em prefixos por dominio:

| Prefixo | Tipo | Qtd | Descricao |
|---------|------|-----|-----------|
| `dm_` | Dimensao | 42 | Lookup/cadastro (achievements, bonus, segmento, produto, avatars, banners...) |
| `g_` | Gamification | 12 | Torneios, minigames, shop, UX, avatars |
| `j_` | Journey | 10 | Jornada do usuario, comunicacoes, bonus, webhooks |
| `tr_` | Transactions | 29 | Eventos transacionais (deposito, saque, bet, login, logout...) |
| `jp_` | Jackpot | 1 | Apostas de jackpot |
| `raf_` | Raffle | 2 | Rifas |
| `ml_` | Machine Learning | 1 | Preferencias de players |
| `bnr_` | Banner | 2 | Clicks e views de banners |
| `churn_*` | Churn/LTV | 1 | Churn prediction e LTV |

> **NOTA:** Total validado em 31/03/2026: **102 views** (era 90 na documentacao anterior).
> 12 views novas: `bnr_click`, `bnr_view`, `churn_and_ltv`, `dm_avatar`, `dm_avatars_prompt`,
> `dm_bnr_banner`, `dm_bnr_placement`, `dm_bo_users`, `dm_saw_skin`, `g_avatar_customized`,
> `tr_gf_avatar_changed`, `tr_user_logged_out`.

---

## 4. Catalogo Completo de Views (102 total — validado 31/03/2026)

### 4.1 Dimensoes (`dm_`) — 42 views

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
| `dm_avatar` | Avatars disponiveis **(NOVA)** |
| `dm_avatars_prompt` | Prompts de avatar **(NOVA)** |
| `dm_bnr_banner` | Banners cadastrados **(NOVA)** |
| `dm_bnr_placement` | Posicionamentos de banner **(NOVA)** |
| `dm_bo_users` | Usuarios do BackOffice **(NOVA)** |
| `dm_saw_skin` | Skins de Spin-a-Wheel **(NOVA)** |

### 4.2 Gamificacao (`g_`) — 12 views

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
| `g_avatar_customized` | Customizacoes de avatar **(NOVA)** |

### 4.3 Jornada (`j_`) — 10 views

| View | Descricao | Uso tipico |
|------|-----------|------------|
| `j_user` | **Dados do usuario no CRM** | Cadastro, join com Athena |
| `j_user_no_enums` | Dados do usuario sem enums | Consultas mais rapidas |
| `j_bonuses` | Bonus emitidos pelo CRM | Analise de campanhas bonus |
| `j_communication` | Comunicacoes enviadas (push, email, SMS) | Performance de campanhas |
| `j_engagements` | Engajamentos (popups, in-app messages) | Performance de engajamento |
| `j_events_stats_daily` | Estatisticas de eventos (diario) | Resumo diario |
| `j_events_stats_hourly` | Estatisticas de eventos (por hora) | Analise horaria |
| `j_automation_rule_progress` | Progresso de regras de automacao | Monitoramento CRM |
| `j_av` | Activity verification | Verificacao de atividade |
| `j_webhooks_facts` | Fatos de webhooks | Integracao externa |

> **CORRECAO:** A tabela de usuarios e `j_user` (NAO `dm_user_main`).
> O campo de afiliado e `core_affiliate_id` (validado empiricamente).

#### Colunas-chave de `j_user` (view mais importante)

| Coluna | Descricao |
|--------|-----------|
| `user_ext_id` | **ID externo** — chave de join com Athena (`c_external_id`, `external_id`) |
| `core_affiliate_id` | ID do afiliado (NAO `affiliate_id`) |
| `has_ftd` | **INTEGER** (0/1) — tem First Time Deposit? NAO e booleano |
| `ftd_date` | Data do primeiro deposito |
| `registration_date` | Data de registro |
| `churn_rank` | Ranking de risco de churn |
| `country` | Pais |
| `currency` | Moeda |

### 4.4 Transacoes (`tr_`) — 29 views

| View | Descricao | Uso tipico |
|------|-----------|------------|
| `tr_acc_deposit_approved` | Deposito aprovado | Analise de depositos |
| `tr_acc_deposit_failed` | Deposito falhou | Diagnostico de falhas |
| `tr_acc_withdrawal_approved` | Saque aprovado | Analise de saques |
| `tr_casino_bet` | Aposta casino | Volume de apostas |
| `tr_casino_win` | Ganho casino | Volume de ganhos |
| `tr_login` | Login | Analise de retencao/atividade |
| `tr_core_bonus_given` | Bonus concedido | Custo de bonus |
| `tr_core_bonus_failed` | Bonus falhou | Diagnostico |
| `tr_core_dynamic_bonus_calculated` | Bonus dinamico calculado | CRM dinamico |
| `tr_core_dynamic_bonus_issued` | Bonus dinamico emitido | CRM dinamico |
| `tr_core_fin_stats_update` | Update de stats financeiras | Reconciliacao |
| `tr_sport_bet_open` | Aposta esportiva aberta | Volume SB |
| `tr_sport_bet_selection_open` | Selecao esportiva aberta | Detalhes SB |
| `tr_sport_bet_selection_settled` | Selecao esportiva liquidada | Resultado SB |
| `tr_sport_bet_settled` | Aposta esportiva liquidada | Resultado SB |
| `tr_client_action` | Acao do cliente | Tracking de comportamento |
| `tr_ach_achievement_completed` | Achievement completado | Gamificacao |
| `tr_ach_level_changed` | Nivel de achievement mudou | Gamificacao |
| `tr_ach_points_added` | Pontos adicionados | Gamificacao |
| `tr_ach_points_deducted` | Pontos deduzidos | Gamificacao |
| `tr_minigame_attempt` | Tentativa minigame | Gamificacao |
| `tr_minigame_spins_issued` | Spins emitidos | Gamificacao |
| `tr_minigame_win` | Ganho minigame | Gamificacao |
| `tr_shop_item_purchase_successed` | Compra na loja | Gamificacao |
| `tr_tournament_lose` | Derrota em torneio | Gamificacao |
| `tr_tournament_user_registered` | Registro em torneio | Gamificacao |
| `tr_tournament_win` | Vitoria em torneio |
| `tr_gf_avatar_changed` | Avatar alterado **(NOVA)** |
| `tr_user_logged_out` | Logout do usuario **(NOVA)** | Gamificacao |

### 4.5 Outros

| View | Descricao |
|------|-----------|
| `jp_bet` | Apostas de jackpot |
| `ml_player_preferences` | Preferencias ML do player |
| `raf_tickets` | Tickets de rifa |
| `raf_won_prizes` | Premios ganhos em rifas |
| `bnr_click` | Clicks em banners **(NOVA)** |
| `bnr_view` | Views de banners **(NOVA)** |
| `churn_and_ltv` | Churn prediction e LTV **(NOVA)** |

---

## 5. Regras e Gotchas Importantes

### 5.1 Funil CRM (`fact_type_id`)

O Smartico usa `fact_type_id` para marcar etapas do funil:

| fact_type_id | Etapa |
|--------------|-------|
| 1 | Enviado |
| 2 | Entregue |
| 3 | Aberto |
| 4 | Clicado |
| 5 | Convertido |

### 5.2 Bonus — Duplo Filtro Obrigatorio

Ao analisar campanhas de bonus, sempre usar **duplo filtro**:
```sql
WHERE entity_id = <bonus_id>
  AND template_id = <template_id>
```
> Sem duplo filtro, bonus de diferentes campanhas podem se misturar.

### 5.3 UTMs — SEMPRE NULL

> **UTMs nao sao exportados pelo Smartico para o BigQuery.**
> Campos como `utm_source`, `utm_medium`, `utm_campaign` sao **SEMPRE NULL** no BQ.
> Para dados de atribuicao/UTM, usar `ecr_ec2.tbl_ecr_banner` no Athena.

### 5.4 Game IDs Conhecidos (Smartico)

| Jogo | game_id Smartico |
|------|-----------------|
| Fortune Tiger | 45838245 |
| Ratinho | 45881668 |

### 5.5 Dados Intraday

Para dados do **dia corrente** (D-0):
- **FTD intraday:** BigQuery e a melhor fonte (mais atualizado que Athena)
- **FTD D-1 ou anterior:** `bireports_ec2` + `dim_user` no Athena

> `has_ftd` no BigQuery e **INTEGER** (0/1), nao booleano.

### 5.6 Correcoes de Schema Validadas

| O que dizem os docs | Realidade validada |
|---------------------|-------------------|
| Tabela de usuarios: `dm_user_main` | **Correto:** `j_user` |
| Campo de afiliado: `affiliate_id` | **Correto:** `core_affiliate_id` |

---

## 6. Bridge com Athena (Chave de Integracao)

### 6.1 Join principal

```
Athena ecr_ec2.tbl_ecr.c_external_id  =  BigQuery j_user.user_ext_id
Athena ps_bi.dim_user.external_id      =  BigQuery j_user.user_ext_id
Athena vendor_ec2.c_customer_id        =  BigQuery j_user.user_ext_id (sportsbook)
```

### 6.2 Fluxo de Join

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

### 6.3 Regra de Join

- **Nunca filtrar tabelas `fund_ec2` pela `external_id` diretamente** — sempre passar pelo `ecr_ec2.tbl_ecr`
- Na camada `ps_bi`, o `dim_user` ja tem ambos os IDs (atalho direto)
- No sportsbook (`vendor_ec2`), `c_customer_id` = External ID = `user_ext_id`

---

## 7. Mapeamento Conceitual — Onde Encontrar Cada Dado

| Conceito | BigQuery (Smartico) | Equivalente Athena |
|----------|--------------------|--------------------|
| Cadastro player | `j_user` / `j_user_no_enums` | `ecr_ec2.tbl_ecr` / `ps_bi.dim_user` |
| Depositos | `tr_acc_deposit_approved` | `cashier_ec2.tbl_cashier_deposit` / `ps_bi.fct_deposits_daily` |
| Saques | `tr_acc_withdrawal_approved` | `cashier_ec2.tbl_cashier_cashout` / `ps_bi.fct_cashout_daily` |
| Apostas casino | `tr_casino_bet` | `fund_ec2.tbl_real_fund_txn` (type=27) |
| Ganhos casino | `tr_casino_win` | `fund_ec2.tbl_real_fund_txn` (type=45) |
| Apostas esportivas | `tr_sport_bet_open` | `vendor_ec2.tbl_sports_book_bets_info` |
| Resultados esportivos | `tr_sport_bet_settled` | `vendor_ec2.tbl_sports_book_bet_details` |
| Bonus concedidos | `tr_core_bonus_given` / `j_bonuses` | `bonus_ec2.tbl_ecr_bonus_details` |
| Login | `tr_login` | — (nao disponivel no Athena) |
| Comunicacoes CRM | `j_communication` | — |
| Engajamentos CRM | `j_engagements` | — |
| Segmentos | `dm_segment` | `segment_ec2.tbl_segment_rules` |
| Gamificacao | `g_ach_*`, `g_tournament_*`, `g_minigames` | — |
| Catalogo jogos | `dm_casino_game_name` | `bireports_ec2.tbl_vendor_games_mapping_data` |
| RFM | `dm_rfm_category` | — |
| Churn | `dm_churn_rank` | — |
| KYC | — | `ecr_ec2.tbl_ecr_kyc_level` |
| AML/Fraude | — | `ecr_ec2.tbl_ecr_aml_flags` / `csm_ec2` |
| Saldo player | — | `fund_ec2.tbl_real_fund` / `ps_bi.fct_player_balance_daily` |
| Afiliados/Trackers | — (UTMs NULL!) | `ecr_ec2.tbl_ecr_banner` |
| BTR (custo bonus) | — | `bonus_ec2.tbl_bonus_summary_details` |

---

## 8. Dados Exclusivos do BigQuery (nao existem no Athena)

Estes dados **so existem** no Smartico/BigQuery:

| Dominio | Views | Descricao |
|---------|-------|-----------|
| **Login** | `tr_login` | Historico de logins |
| **CRM Comunicacoes** | `j_communication` | Push, email, SMS enviados |
| **CRM Engajamentos** | `j_engagements` | Popups, in-app messages |
| **CRM Automacao** | `j_automation_rule_progress`, `dm_automation_rule` | Regras e progresso |
| **Gamificacao** | `g_*` (11 views) | Achievements, torneios, minigames, shop |
| **RFM** | `dm_rfm_category` | Segmentacao Recency-Frequency-Monetary |
| **Churn** | `dm_churn_rank` | Ranking de risco de churn |
| **ML Preferences** | `ml_player_preferences` | Preferencias inferidas |
| **Rifas** | `raf_tickets`, `raf_won_prizes` | Rifas do CRM |

---

## 9. Dados Exclusivos do Athena (nao existem no BigQuery)

Para referencia inversa — o que so o Athena tem:

| Dominio | Tabelas Athena |
|---------|----------------|
| Saldo em tempo real | `fund_ec2.tbl_real_fund` |
| Transacoes granulares | `fund_ec2.tbl_real_fund_txn` |
| KYC/AML | `ecr_ec2.tbl_ecr_kyc_level`, `tbl_ecr_aml_flags` |
| Bonus detalhado | `bonus_ec2.*` |
| Afiliados/UTMs | `ecr_ec2.tbl_ecr_banner` |
| Metodos de pagamento | `cashier_ec2.tbl_instrument` |
| Catalogo jogos completo | `vendor_ec2.tbl_vendor_games_mapping_mst` |
| Sessoes de jogo | `bireports_ec2.tbl_ecr_gaming_sessions` |
| Sportsbook detalhado | `vendor_ec2.tbl_sports_book_*` |

---

---

## 10. Conversao Timezone no BigQuery

O BigQuery usa sintaxe diferente do Athena para converter UTC para BRT:

```sql
-- BigQuery Standard SQL
DATETIME(timestamp_col, 'America/Sao_Paulo')

-- Exemplo: filtrar depositos de hoje em BRT
SELECT *
FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved`
WHERE DATE(DATETIME(event_time, 'America/Sao_Paulo')) = CURRENT_DATE()
```

---

## 11. Receitas SQL — Queries de Referencia

### 11.1 FTDs intraday (j_user)

```sql
-- BigQuery e a melhor fonte para FTD do dia corrente (D-0)
-- has_ftd e INTEGER (0/1), NAO booleano
SELECT
    user_ext_id,
    core_affiliate_id,
    ftd_date
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE has_ftd = 1
  AND DATE(ftd_date) = CURRENT_DATE()
```

### 11.2 Funil CRM completo (j_communication)

```sql
-- fact_type_id: 1=Enviado, 2=Entregue, 3=Aberto, 4=Clicado, 5=Convertido
-- O campo fact_type_id esta presente em j_communication e j_engagements
SELECT
    fact_type_id,
    CASE fact_type_id
        WHEN 1 THEN 'Enviado'
        WHEN 2 THEN 'Entregue'
        WHEN 3 THEN 'Aberto'
        WHEN 4 THEN 'Clicado'
        WHEN 5 THEN 'Convertido'
    END AS etapa,
    COUNT(*) AS total
FROM `smartico-bq6.dwh_ext_24105.j_communication`
WHERE DATE(DATETIME(event_time, 'America/Sao_Paulo'))
      BETWEEN '2026-03-24' AND '2026-03-30'
GROUP BY 1, 2
ORDER BY 1
```

---

*Documento consolidado em 2026-03-31. Revisado por agentes Auditor, Best-Practices e Extractor.*
*Fontes: Smartico CRM (BigQuery DWH), validacoes empiricas, feedbacks acumulados do time de dados MultiBet.*
