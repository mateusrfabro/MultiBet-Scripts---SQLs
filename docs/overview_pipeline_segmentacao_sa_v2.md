# Overview executivo — Pipeline Segmentação SA v2

> Status: 100% codado, em validação técnica final (28/04/2026 ~10:50 BRT).
> Auditoria completa abaixo.

---

## 1. O que o pipeline faz (passo a passo)

```
1. CONEXÃO Super Nova DB (SSH tunnel via bastion)
2. CARREGA multibet.pcr_atual         (~136k jogadores, snapshot D-1 do PCR)
3. CARREGA multibet.matriz_risco       (~155k jogadores, tier comportamental)
4. CARREGA pcr_ratings de 7 dias atrás (lookback para tendência temporal)
5. ATUALIZA multibet.celula_monitor_diario
   (cruzamento Rating × Matriz, flag 3 rodadas negativas consecutivas)
6. FILTRA Rating A ou S (~10.9k jogadores)
7. CALCULA tendência (Estável/Subindo/Caindo) — híbrido threshold + temporal
8. JUNTA matriz_risco (external_id ↔ user_ext_id)
9. ENRIQUECE com 32 colunas em 6 blocos:
   - Bloco 4: lifecycle/RG/flags (5 col, sem query)
   - Bloco 5: bonus_abuse_flag via risk_tags (1 col, Super Nova DB)
   - Bloco 6: KYC + restrições (5 col, Athena ecr_ec2)
   - Bloco 1+2: métricas 30d financeiras + aposta (12 col, Athena ps_bi)
   - Bloco 3: top jogos/providers/horário por TIER (14 col, Athena ps_bi + game_image_mapping)
   - Bloco 5b: BTR + bonus extras (8 col, Athena ps_bi)
10. PERSISTE em multibet.segmentacao_sa_diaria (idempotente: DELETE+INSERT do dia)
11. GERA CSV BR (sep=";", decimal=",") + legenda explicativa em output/
12. ENVIA E-MAIL com CSV + legenda anexos
13. (OPCIONAL) PUBLICA tags SEG_* no Smartico via API
14. ATUALIZA view multibet.segmentacao_sa_atual (sempre aponta MAX(snapshot_date))
```

**Tempo total esperado em produção (10.9k A+S):** ~25-35 min
- Carregamento PCR + Matriz: ~10s
- Filtro + tendência + matriz join: ~2s
- Enriquecimento Bloco 4 (sem query): instantâneo
- Bloco 5 (risk_tags): ~5s
- Bloco 6 (KYC, 3 batches Athena): ~6 min
- Bloco 1+2 (métricas 30d, 3 batches): ~5 min
- Bloco 3 (top jogos, 3 queries × 3 batches): ~10 min
- Bloco 5b (BTR + bonus, 3 batches × 3 CTEs): ~5 min
- Persistência banco + CSV + email: ~30s
- Smartico (se habilitado, 10.9k events): ~3 min

---

## 2. Atualizações no banco

| Objeto | Tipo | O que |
|---|---|---|
| `multibet.segmentacao_sa_diaria` | tabela | Histórico diário (incremental, nunca TRUNCATE). 79 colunas (47 base + 32 v2). |
| `multibet.celula_monitor_diario` | tabela | Cruzamento Rating × Matriz, flag investigar quando célula tem 3 rodadas negativas. |
| `multibet.segmentacao_sa_atual` | view | Sempre o snapshot mais recente. |

**Idempotência diária:** rodar 2x no mesmo dia gera mesmo resultado (DELETE WHERE snapshot_date + INSERT). Histórico de outros dias é preservado.

---

## 3. CSV via e-mail

**Path:** `output/players_segmento_SA_<data>_FINAL.csv` + `_legenda.txt`
**Formato:** BR — `sep=";"`, `decimal=","`, `encoding=utf-8-sig` (Excel-friendly)
**Colunas:** 57 colunas em 5 blocos (identificação, valor 90/30d, comportamento, bônus, regulatório)

**Destinatários** (configurado em `pipelines/segmentacao_sa_diaria.py` linha 74):
```
EMAIL_DESTINATARIOS = ["caio.ferreira@multi.bet.br"]   # MODO TESTE
```
Lista completa preservada comentada (descomentar após OK do Caio):
- victor.campello@multi.bet.br
- liliane.carvalho@multi.bet.br
- raphael.braga@multi.bet.br
- ext.andreza.ribeiro@multi.bet.br
- felipe.lio@multi.bet.br
- gabriel.tameirao@multi.bet.br

**Assunto:** `Segmentacao A+S diaria — <data>`
**Corpo:** HTML enxuto com resumo (total, S, A, GGR 90d, distribuição tendência)

---

## 4. Smartico API (publicação de tags)

**Bucket:** `core_external_markers` (alinhado com Raphael, 28/04/2026)
**Tags publicadas (4 tags por jogador):**
- `SEG_TREND_<SUBINDO|CAINDO|ESTAVEL>`
- `SEG_LIFECYCLE_<NEW|ACTIVE|AT_RISK|CHURNED|DORMANT>`
- `SEG_RG_<NORMAL|CLOSED|COOL_OFF>`
- `SEG_BONUS_ABUSER` (somente se BONUS_ABUSE_FLAG = 1)

**Operação atômica por jogador (preserva tags de outros pipelines no mesmo bucket):**
```json
{
  "^core_external_markers": ["SEG_TREND_*", "SEG_LIFECYCLE_*", "SEG_RG_*", "SEG_BONUS_*"],
  "+core_external_markers": ["SEG_TREND_SUBINDO", "SEG_LIFECYCLE_AT_RISK", "SEG_RG_NORMAL"],
  "skip_cjm": true
}
```

**Diff vs snapshot anterior** (cobre furo "player que sumiu de A+S"):
- Carrega snapshot de ontem em `multibet.segmentacao_sa_diaria`
- Players que estavam ontem mas SUMIRAM hoje: envia REMOVE puro (sem ADD)
- Evita tags fantasma no perfil Smartico

**Fail-safe:**
- `skip_cjm=True` SEMPRE (popula estado, não dispara Automation/Missions — recomendado em prod inicial)
- `dry_run=True` por default — só envia com `--smartico-confirm`
- `--smartico-canary` envia para 1 jogador só (rating A estável, real_user, sem abuso, PVS no IQR)

**Auditoria detalhada no log:**
```
AUDITORIA — Players de entrada: 10,941
  - Excluídos por external_id inválido: 0
  - Excluídos por nenhuma tag válida: 0
  - Válidos para ADD: 10,941
  Eventos ADD: 10,941 | REMOVE: 152 (saíram de A+S desde ontem)
```

---

## 5. Cron / Horário

| Pipeline | Cron (UTC) | Horário (BRT) | Status |
|---|---|---|---|
| PCR upstream | `0 6 * * *` | 03:00 | Em prod |
| Matriz Risco upstream | `30 5 * * *` | 02:30 | Em prod |
| **Segmentação A+S (este)** | `0 7 * * *` | **04:00** | **A subir via Gusta** |

Atual fluxo: cron 04:00 → carrega PCR (já rodou 03:00) + Matriz (já rodou 02:30) → enriquece → grava banco → CSV → e-mail → Smartico.

---

## 6. O que falta (antes de subir produção)

| Item | Status | Dono |
|---|---|---|
| Pipeline 100% codado | ✅ | — |
| Teste local com 200 players | ✅ | — |
| Teste local com 10.9k players (full) | 🟡 em curso | Mateus |
| CSV de 57 col validado por Castrin | ⏳ | Castrin |
| Smartico DRY-RUN (10.9k events) validado | ⏳ | — |
| Smartico CANARY (1 jogador real) com Raphael | ⏳ | Mateus + Raphael |
| Git commit + push | ⏳ | Mateus |
| Notion público atualizado | ✅ | — |
| Notion técnico atualizado | ✅ | — |
| Mensagem para Gusta substituir no orquestrador | ⏳ | Mateus |
| Acompanhar cron 04h do dia seguinte | ⏳ | Mateus |

---

## 7. Comandos úteis (cheatsheet)

```bash
# Teste local sem banco/email (validação rápida)
python pipelines/segmentacao_sa_diaria.py --no-db --no-email

# Produção (banco + e-mail, sem Smartico)
python pipelines/segmentacao_sa_diaria.py

# + Smartico DRY-RUN (gera JSON em reports/, NÃO envia)
python pipelines/segmentacao_sa_diaria.py --push-smartico --smartico-dry-run

# + Smartico CANARY (1 jogador real)
python pipelines/segmentacao_sa_diaria.py --push-smartico --smartico-canary --smartico-confirm

# + Smartico FULL (todos os 10.9k jogadores) — após validação canary
python pipelines/segmentacao_sa_diaria.py --push-smartico --smartico-confirm
```

---

## 8. Auditoria automática (logs)

Pipeline gera logs em `pipelines/logs/` (na EC2 quando deployado) e console em local.

**Métricas auditáveis em cada run:**
- Total de jogadores carregados do PCR
- Total de jogadores na Matriz
- Filtro A+S (deve ser ~7-8% da base PCR)
- Distribuição de tendência (Estável/Subindo/Caindo)
- Cobertura matriz (deve ser ≥85% identificados)
- Distribuição LIFECYCLE_STATUS
- Cobertura KYC (deve ser ~100%)
- Cobertura métricas 30d (% com atividade)
- Cobertura nome de jogo (~85-90% — gap conhecido em IDs antigos)
- Smartico: ADD events, REMOVE events, sent, failed
- Tempo total

**Anomalias monitoradas:**
- Célula com 3+ rodadas NGR negativas consecutivas → flag_investigar=true
- KYC cobertura < 95% → log WARNING
- Player sem tags válidas → log WARNING + amostra dos suspeitos

---

## 9. Fluxo completo de exemplo (28/04/2026)

```
04:00 BRT — Cron dispara
04:00:01 — Conecta Super Nova DB via bastion
04:00:15 — Carrega multibet.pcr_atual: 136.751 jogadores
04:00:18 — Carrega multibet.matriz_risco: 155.302 jogadores
04:00:20 — Atualiza celula_monitor_diario: 36 células (2 com flag investigar)
04:00:21 — Filtra A+S: 10.941 jogadores (1.806 S + 9.135 A)
04:00:25 — Calcula tendência: 9.879 Estável + 542 Subindo + 520 Caindo
04:00:26 — Junta matriz: cobertura 96.3%
04:00:27 — Bloco 4 deriváveis: LIFECYCLE/RG/FLAGS aplicados
04:00:30 — Bloco 5 risk_tags: 1.806 BONUS_ABUSE_FLAG=1
04:06:30 — Bloco 6 KYC: 100% cobertura (Athena 3 batches)
04:11:30 — Bloco 1+2 30d: 67% atividade no mês
04:21:30 — Bloco 3 top jogos por TIER: 88% cobertura nome
04:26:30 — Bloco 5b BTR: 4.290 com BTR_30D não-nulo
04:27:00 — Persiste em multibet.segmentacao_sa_diaria (10.941 linhas)
04:27:15 — Gera CSV (output/players_segmento_SA_2026-04-28_FINAL.csv) + legenda
04:27:20 — Envia e-mail para caio.ferreira@multi.bet.br
04:27:25 — Smartico push (skip_cjm=true): ADD=10.941, REMOVE=152
04:30:30 — Smartico API: sent=11.093, failed=0
04:30:31 — Pipeline concluído em 30min32s
```

---

## 10. Riscos identificados e mitigações

| Risco | Mitigação |
|---|---|
| Athena query estoura 256KB com 10k+ IDs | ✅ Batching 4k IDs por query |
| dim_game incompleto (PG Soft falta) | ✅ Trocado por `multibet.game_image_mapping` (88% cobertura) |
| Player sem tags Smartico válidas | ✅ Auditoria explícita no log + amostra dos suspeitos |
| Player que saiu de A+S mantém tags antigas no Smartico | ✅ Diff vs snapshot anterior + REMOVE puro |
| Smartico publicação dispara automation por engano | ✅ `skip_cjm=True` SEMPRE |
| Envio Smartico em prod sem revisão Raphael | ✅ `dry_run=True` default + `--smartico-confirm` obrigatório |
| Pipeline rodar antes do PCR upstream terminar | ✅ Cron 04:00 = 30min após PCR (03:00) e 1h30 após Matriz (02:30) |

---

*Gerado em 28/04/2026 ~10:50 BRT — última atualização após pipeline completo terminar.*
