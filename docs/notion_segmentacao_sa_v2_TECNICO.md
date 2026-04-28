# Segmentação SA — Roadmap v2 (Castrin: 57 colunas)

> **Status (27/04/2026 23:55):** v2.0 entregue com 25 col. Castrin pediu 57.
> Plano em 3 entregas (v2.1 → v2.2 → v2.3).

---

## 1. Contexto

**Demanda Castrin (27/04 noite):** após aprovar o e-mail, pediu "validar as
colunas — tem aquelas colunas do CSV que te mandei?". Referência:
`Downloads/players_segmento_SA (1).csv` com **57 colunas**.

**Status hoje:** pipeline `pipelines/segmentacao_sa_diaria.py` em produção
gera **25 colunas**. Faltam **32 colunas** (LIFECYCLE_STATUS, métricas 30d,
top jogos, BTR, KYC etc.).

**Insumos do Castrin (encontrados em Downloads):**
- `GUIA_REPRODUCAO_PCR.md` — manual completo do pipeline dele (8 scripts, 90d)
- `SQL - Matriz de Risco/` — 22 SQLs das tags + `export_smartico_risk.py`
- `players_segmento_SA (1).csv` — CSV referência com 57 col (snapshot antigo)

---

## 2. Decisões metodológicas

### 2.1 Janela 30d
**Decisão:** janela rolling 30d terminando em **D-1** (exclui dia parcial).

**Por quê:** alinhada com pipeline PCR upstream (que usa 90d rolling até D-1).
Padrão auditável e replicável diariamente.

**Não tentamos** bater valor-a-valor com o CSV do Castrin — confirmamos
empiricamente (com 5 players sample) que a janela dele NÃO é 30d puro,
60d, abril calendário ou 90d. Provavelmente é snapshot mais antigo dele.

### 2.2 Fonte única para 30d
**Tabela:** `ps_bi.fct_player_activity_daily` (view dbt do time de dados,
em BRL pré-agregado por dia/jogador).

**Por quê:** tem TODAS as colunas que precisamos (GGR, NGR, depósito,
saque, casino_realbet, sb_realbet, bonus). 1 query única para 12 cols.

### 2.3 Risk_tags (Super Nova DB)
**Tabela:** `multibet.risk_tags` (populada diariamente pelo Mauro/Gusta).
- 3.8M linhas | 1.38M players únicos | snapshot diário (último: 2026-04-27 05h)
- **23 tags de risco + score_bruto + score_norm + tier**

**`BONUS_ABUSE_FLAG`** = `(potencial_abuser != 0 OR promo_chainer != 0)`.
Não precisa rodar as 23 SQLs do Castrin — Mauro já populou.

### 2.4 KYC
**Fonte:** `ecr_ec2.tbl_ecr_kyc_level` (Athena, partição em c_updated_time).
**Lookup:** snapshot mais recente por `c_ecr_id` (ROW_NUMBER OVER).

### 2.5 LIFECYCLE_STATUS (regra de negócio)
- `NEW`: tenure < 30d **AND** num_deposits < 3 (alinha com Castrin)
- `ACTIVE`: recency ≤ 7 dias
- `AT_RISK`: 8 ≤ recency ≤ 30 dias
- `CHURNED`: 31 ≤ recency ≤ 90 dias
- `DORMANT`: recency > 90 dias

---

## 3. Plano de entrega em 3 fases

### v2.1 — Hoje noite / amanhã 04h (cron)

**+11 colunas (Blocos 4, 5, 6):** ✅ **JÁ CODADAS E TESTADAS**

| Bloco | Colunas | Fonte |
|---|---|---|
| 4 — Derivadas | LIFECYCLE_STATUS, RG_STATUS, ACCOUNT_RESTRICTED_FLAG, SELF_EXCLUDED_FLAG, PRIMARY_VERTICAL | Pandas (sem query) |
| 5 — Risk tags | BONUS_ABUSE_FLAG | `multibet.risk_tags` |
| 6 — KYC | KYC_STATUS, kyc_level, self_exclusion_status, cool_off_status, restricted_product | `ecr_ec2.tbl_ecr_kyc_level` |

**Status:** funcionando em teste com 200 players A+S.
- LIFECYCLE: 178 AT_RISK + 22 CHURNED
- BONUS_ABUSE_FLAG=1 em 30 (15%)
- KYC: 100% cobertura

**Total v2.1: 25 + 11 = 36 colunas.**

### v2.2 — D+1 (30/04)

**+12 colunas (Blocos 1+2):** 🟡 **CÓDIGO PRONTO, FALTA TESTAR**

| Bloco | Colunas | Fonte |
|---|---|---|
| 1 — Financeiras 30d | GGR_30D, NGR_30D, DEPOSIT_AMOUNT_30D, DEPOSIT_COUNT_30D, WITHDRAWAL_AMOUNT_30D, WITHDRAWAL_COUNT_30D, AVG_DEPOSIT_TICKET_30D, AVG_DEPOSIT_TICKET_LIFETIME | `ps_bi.fct_player_activity_daily` (1 query) |
| 2 — Aposta 30d | BET_AMOUNT_30D, BET_COUNT_30D, AVG_BET_TICKET_30D, AVG_BET_TICKET_TIER, AVG_DEPOSIT_TICKET_TIER | Mesma query + groupby pandas |

**Total v2.2: 36 + 12 = 48 colunas.**

### v2.3 — D+2 (01/05)

**+9 colunas (Blocos 3 + BTR):** 🔴 **A CODAR**

| Bloco | Colunas | Fonte |
|---|---|---|
| 3 — Top jogos por TIER | TOP_PROVIDER_1/2, TOP_GAME_1/2, TOP_GAME_1/2/3_TIER_TURNOVER, TOP_GAME_1/2/3_TIER_ROUNDS, DOMINANT_WEEKDAY, DOMINANT_TIMEBUCKET, LAST_PRODUCT_PLAYED | `ps_bi.fct_casino_activity_daily/hourly` (SQL pronto no Castrin) |
| 5 — BTR | BONUS_ISSUED_30D, BTR_30D, BTR_CASINO_30D, BTR_SPORT_30D, LAST_BONUS_DATE, LAST_BONUS_TYPE, BONUS_DEPENDENCY_RATIO_LIFETIME, NGR_PER_BONUS_REAL_30D | `bonus_ec2.*` (BTR já existe em `clustering-btr-utm-campaign/`) |

**Total v2.3: 48 + 9 = 57 colunas. ✅ Match completo.**

---

## 4. Arquitetura

```
┌─────────────────────────────────────────────────────────┐
│ pipelines/segmentacao_sa_diaria.py (orquestrador main)  │
│                                                         │
│  carrega_pcr_atual() ──────────► multibet.pcr_atual    │
│  carrega_matriz_risco() ──────► multibet.matriz_risco  │
│  filtrar_a_e_s() ──────────────► A + S (~12k players)  │
│  calcular_tendencia()                                   │
│  juntar_matriz()                                        │
│                                                         │
│  ┌─ NOVO: chamar segmentacao_sa_enriquecimento.py ─┐  │
│  │  bloco_4_derivaveis()    +5 col (sem query)      │  │
│  │  bloco_5_risk_tags()     +1 col (SuperNova DB)   │  │
│  │  bloco_6_kyc()           +5 col (Athena)         │  │
│  │  bloco_1_2_metricas_30d() +12 col (Athena)       │  │
│  │  [v2.3]                                          │  │
│  │  bloco_3_top_jogos_tier() +8 col (Athena)        │  │
│  │  bloco_btr()              +7 col (Athena)        │  │
│  └──────────────────────────────────────────────────┘  │
│                                                         │
│  gravar_segmentacao() ─────► multibet.segmentacao_sa_   │
│  gerar_csv() + legenda                                  │
│  enviar_email() (Castrin + 6 destinatários)             │
└─────────────────────────────────────────────────────────┘
```

**Cron EC2:** `0 7 * * *` (07:00 UTC = 04:00 BRT, 30min após PCR upstream).

---

## 5. Pontos de otimização (não-urgente)

> Itens para revisar quando 57 col estiverem em produção e estabilizadas.

### 5.1 Performance Athena
- **TIER averages** (AVG_DEPOSIT_TICKET_TIER, AVG_BET_TICKET_TIER) hoje
  são calculadas em pandas pós-merge. Pode ir pro SQL via window function.
- **Bloco 6 (KYC)** tem `ROW_NUMBER OVER (PARTITION BY c_ecr_id)` —
  varre tabela toda. Adicionar filtro de `c_updated_time >= D-365`?

### 5.2 Custo Athena
- Validar que filtro `player_id IN (...)` está usando push-down em
  `ps_bi.fct_player_activity_daily` (snapshot dbt já é particionado).
- Avaliar se vale criar tabela materializada `s3_staging.segmentacao_sa_30d`.

### 5.3 Idempotência
- DELETE WHERE snapshot_date + INSERT é seguro mas custoso. Avaliar UPSERT.

### 5.4 Monitoramento
- Adicionar `celula_monitor_diario` para os novos campos:
  - alertar se `KYC_STATUS` cobertura < 95%
  - alertar se distribuição `LIFECYCLE_STATUS` mudar > 20% vs dia anterior

---

## 6. Riscos identificados

| Risco | Probab. | Impacto | Mitigação |
|---|---|---|---|
| Castrin esperava match valor-a-valor com CSV antigo dele | Alta | Médio | Comunicar janela 30d nossa = D-30 a D-1. CSV dele é snapshot antigo. |
| `ecr_ec2.tbl_ecr_kyc_level` está com dados desatualizados | Baixa | Alto | Validar empiricamente coverage > 95%. Cross-check com Mauro. |
| Bloco 3 (top jogos) muito custoso no Athena | Média | Baixo | Calcular **por TIER** (1x), não por player. ~40 células = barato. |
| `dim_game` incompleto (PG Soft) — TOP_GAME virá com lacunas | Alta | Médio | Documentar gap. Fallback para `vendor_ec2.tbl_vendor_games_mapping_mst`. |
| BigQuery desativado quebra pipeline do Castrin se rodarmos as 23 SQLs | N/A | N/A | NÃO rodamos. Consumimos `multibet.risk_tags` direto. |

---

## 7. Decisões pendentes

- [ ] **Castrin**: aprova plano v2.1/v2.2/v2.3 (3 dias)? Ou prefere segurar deploy 2 dias e ir direto pro 57?
- [ ] **Validação CSV sample**: enviar amanhã CSV de teste com 48 col (v2.2 simulado) pra ele validar formato/distribuições antes de subir cron quinta.
- [ ] **`AVG_*_TICKET_TIER` definição**: confirmar com Castrin se é "média do (rating × matriz)" ou outra coisa. Hoje implementado como média.
- [ ] **`BONUS_ABUSE_FLAG`**: confirmar se `potencial_abuser != 0 OR promo_chainer != 0` é o critério dele, ou só uma das duas.

---

## 8. Histórico

| Data | Versão | O que mudou |
|---|---|---|
| 27/04 noite | v2.0 | Pipeline atual (25 col) — em produção. |
| 27/04 noite | v2.1-dev | +11 col (Blocos 4, 5, 6) codadas e testadas. Aguarda integração no pipeline. |
| 27/04 noite | v2.2-dev | +12 col (Bloco 1+2) codadas. **Falta testar.** |
| — | v2.3 | +9 col (Bloco 3 + BTR) — a codar. |

---

## 9. Próximos passos imediatos

1. ✅ **Documentação** (este arquivo)
2. ⏳ **Testar Blocos 1+2 / 3 / 5b** com 200 players (script `solicitacoes_pontuais/segmentacao_SA_v2/test_enriquecimento.py`)
3. ⏳ **Gerar CSV sample** com 57 cols pra Castrin validar via DM
4. ⏳ **Castrin valida** sample
5. ⏳ **Integrar** `segmentacao_sa_enriquecimento.py` no pipeline principal
6. ⏳ **Atualizar DDL/INSERT/CSV/legenda**
7. ⏳ **Git commit + push** (CLAUDE.md regra de ouro: git first)
8. ⏳ **Passar link GitHub para Gusta** — ele substitui no orquestrador EC2.
   **NÃO** rodar `deploy_*.sh` direto (regra `feedback_handoff_pipelines_orquestrador_gusta.md`).
   Mensagem padrão: "Gusta, novo pipeline segmentacao_sa v2 (57 col) pronto. Link: <github-url>.
   Substituir no orquestrador no lugar do segmentacao_sa atual. Roda 04h BRT."
9. ⏳ **Acompanhar cron** do dia seguinte — confirmar saída + e-mail enviado

---

## 10. Arquivos do projeto (referência)

- **Pipeline principal**: `pipelines/segmentacao_sa_diaria.py`
- **Módulo enriquecimento NOVO**: `pipelines/segmentacao_sa_enriquecimento.py`
- **Test script**: `solicitacoes_pontuais/segmentacao_SA_v2/test_enriquecimento.py`
- **Validação Bloco 1**: `solicitacoes_pontuais/segmentacao_SA_v2/bloco1_validacao_30d.py`
- **Drill-down debug**: `solicitacoes_pontuais/segmentacao_SA_v2/bloco1_drilldown_player.py`
- **Pipeline Castrin (referência)**: `solicitacoes_pontuais/segmentacao_SA_v2/pipeline_castrin_referencia/`
- **22 SQLs Matriz Risco**: `solicitacoes_pontuais/segmentacao_SA_v2/sql_matriz_risco/`

---

*Última atualização: 2026-04-27 23:55 BRT*
