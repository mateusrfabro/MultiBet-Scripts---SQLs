# README — Auditoria SQL + Fixes aplicados (20/04/2026)

Entrega consolidada do trabalho de revisao de SQL do dia 20/04/2026.
Gatilho: feedback do Gusta (analista senior) nas views gold Casino/Sportsbook
apontando 4 tipos de B.O. recorrentes. Decisao: auditar proativamente os demais
sistemas em producao antes que o problema apareca no impacto de negocio.

---

## 1. Contexto e gatilho

Em 20/04/2026 o Gusta mandou no WhatsApp que tinha "muito B.O." nos SQLs
das views verticais (Casino/Sportsbook), citando 4 categorias:

1. **Queries dropando linha** (INNER JOIN silencioso)
2. **Scan muito grande no Athena** (janela fixa + TRUNCATE+INSERT diario)
3. **Divergencia no JOIN** (cast implicito, tie fraco, MAX de string)
4. **Join quebrado** (CTEs com filtro temporal diferente)

Ele esta corrigindo, mas queria aprender o padrao pra nao repetir. A partir
dai formalizamos um checklist de 6 perguntas e estendemos a revisao pros
outros sistemas criticos em producao: **PCR** (Player Credit Rating) e
**Matriz de Risco v2**.

Detalhes do checklist: [memory/feedback_sql_review_checklist_gusta.md](../memory/feedback_sql_review_checklist_gusta.md)

---

## 2. Auditorias realizadas

### 2.1 PCR (Player Credit Rating)

**Relatorio completo:** [auditoria_sql_pcr_20260420.md](auditoria_sql_pcr_20260420.md)

**Achados:**
- **3 criticos:**
  1. `c_category` nao filtrado antes do ranking PVS -> 11.6% da base (fraud, closed, rg_closed, play_user) contamina percentis e recebe tag PCR no Smartico (compliance issue com rg_closed).
  2. `ROW_NUMBER() OVER (... ORDER BY c_category)` e ordem **alfabetica**, nao temporal — jogador com historico misto sempre retorna `closed` (C < F < R).
  3. Sem amostra minima no HAVING -> FTD recente com 1 deposito vai automaticamente pra rating E (proposta separada abaixo).
- **2 medios + 3 baixos** (detalhes no relatorio).

**Veredicto:** BLOQUEADO ate correcao dos criticos.

### 2.2 Matriz de Risco v2

**Relatorio completo:** [auditoria_sql_matriz_risco_20260420.md](auditoria_sql_matriz_risco_20260420.md)

**Achados:**
- **1 critico:** `RG_ALERT_PLAYER.sql` usa `AVG(sessions_count)` em distribuicao skewed -> jogador esporadico com 1 dia de spike entra no bucket de Jogo Responsavel (compliance regulatorio).
- **8 medios:** CASHOUT_AND_RUN com DATE UTC truncada, POTENCIAL_ABUSER com full scan sem filtro + proxy ruim, ZERO_RISK/ENGAGED/BEHAV_RISK com AVG em cauda longa, ROLLBACK_PLAYER sem `c_txn_status`, push CRM sem score_norm no diff, MULTI_GAME usando hora UTC.
- **3 padroes sistemicos:** janela 90d fixa em 21/21 SQLs, cascata users+brand INNER JOIN redundante, pipeline duplicado em `scripts/` vs `ec2_deploy/`.

**Veredicto:** push 02:30 BRT de hoje absorve via diff; fixar ate amanha 02:00 BRT.

---

## 3. Fixes aplicados (11 arquivos)

| # | Arquivo | Fix |
|---|---|---|
| 1 | [pipelines/pcr_pipeline.py](../pipelines/pcr_pipeline.py) | `ORDER BY c_ecr_id` (deterministico) + `AND c_category = 'real_user'` |
| 2 | [ec2_deploy/sql/risk_matrix/RG_ALERT_PLAYER.sql](../ec2_deploy/sql/risk_matrix/RG_ALERT_PLAYER.sql) | AVG -> `APPROX_PERCENTILE(0.5)` + `active_days >= 5` |
| 3 | [scripts/sql/risk_matrix/RG_ALERT_PLAYER.sql](../scripts/sql/risk_matrix/RG_ALERT_PLAYER.sql) | idem (copia local) |
| 4 | [ec2_deploy/sql/risk_matrix/CASHOUT_AND_RUN.sql](../ec2_deploy/sql/risk_matrix/CASHOUT_AND_RUN.sql) | DATE -> TIMESTAMP com janela `+24h`/`+48h` |
| 5 | [scripts/sql/risk_matrix/CASHOUT_AND_RUN.sql](../scripts/sql/risk_matrix/CASHOUT_AND_RUN.sql) | idem |
| 6 | [ec2_deploy/sql/risk_matrix/POTENCIAL_ABUSER.sql](../ec2_deploy/sql/risk_matrix/POTENCIAL_ABUSER.sql) | Proxy first_deposit -> `ecr_ec2.tbl_ecr.c_created_time` |
| 7 | [scripts/sql/risk_matrix/POTENCIAL_ABUSER.sql](../scripts/sql/risk_matrix/POTENCIAL_ABUSER.sql) | idem |
| 8 | [ec2_deploy/sql/risk_matrix/ROLLBACK_PLAYER.sql](../ec2_deploy/sql/risk_matrix/ROLLBACK_PLAYER.sql) | `+ AND c_txn_status = 'SUCCESS'` |
| 9 | [scripts/sql/risk_matrix/ROLLBACK_PLAYER.sql](../scripts/sql/risk_matrix/ROLLBACK_PLAYER.sql) | idem |
| 10 | [ec2_deploy/sql/risk_matrix/MULTI_GAME_PLAYER.sql](../ec2_deploy/sql/risk_matrix/MULTI_GAME_PLAYER.sql) | UTC -> BRT (`AT TIME ZONE`) |
| 11 | [scripts/sql/risk_matrix/MULTI_GAME_PLAYER.sql](../scripts/sql/risk_matrix/MULTI_GAME_PLAYER.sql) | idem |

**Cada fix tem comentario inline** apontando: data da auditoria, numero do achado, motivo da mudanca.

---

## 4. Pendentes e decisoes

### 4.1 EM SHADOW MODE — PCR_RATING_NEW para novatos

**Status:** codigo implementado em shadow mode, aguardando aprovacao CRM pra ativar push.

**Documento:** [proposta_pcr_rating_new_20260420.md](proposta_pcr_rating_new_20260420.md)

**O que foi implementado (20/04/2026 pos-decisao "podemos criar"):**
- `pipelines/pcr_pipeline.py` (v1.3):
  - Constantes `NOVATO_DAYS_THRESHOLD = 14` e `NOVATO_DEPOSITS_THRESHOLD = 3`
  - `atribuir_rating()` separa novatos ANTES de calcular percentis do PVS
    (maduros nao ficam distorcidos pela cauda estatistica dos novos)
  - Rating NEW e atribuido a jogadores com `days_active < 14 OU num_deposits < 3`
  - DDL alterado de `VARCHAR(2)` pra `VARCHAR(10)` com ALTER idempotente
  - View `pcr_resumo` inclui NEW no `ORDER BY` (posicao 7, apos E)
- `scripts/push_pcr_to_smartico.py`:
  - Mapping `"NEW": "PCR_RATING_NEW"` adicionado
  - Flag `PUSH_NEW_TAG_ENABLED = False` (shadow mode)
  - `_query_snapshot` filtra NEW do push enquanto flag estiver off
- Tabela recebe rating NEW normalmente (disponivel pra analise local),
  mas o push Smartico **NAO envia** NEW enquanto a flag nao for ativada

**Para ativar push (apos aprovacao):**
1. Confirmar com Raphael que tag `PCR_RATING_NEW` existe no tenant Smartico
   (provisionamento via ticket JIRA se nao existir — ver 4.2 abaixo)
2. Confirmar jornada de boas-vindas configurada + testada
3. Trocar `PUSH_NEW_TAG_ENABLED = False` para `True` em push_pcr_to_smartico.py
4. Rollout 3 fases: canary 1 user -> amostra 10 -> full
   (seguir `memory/feedback_smartico_push_rollout_playbook.md`)

### 4.2 EM VALIDACAO — score_norm no Smartico (pesquisa tecnica concluida)

**Status:** pesquisa na documentacao Smartico concluida em 20/04/2026; precisa validacao operacional com Raphael antes de implementar.

**Resposta tecnica confirmada na doc oficial Smartico:**
- **SIM**, Smartico consome fields numericos custom (separados das tags)
- Tags usam mecanismo `core_external_markers` / `markers2`
- **Custom Properties** sao tipados (numerico, data, etc) e pushados via
  endpoint S2S `update_profile` (`https://imgX.smr.vc/s2s-api`)
- Custom Properties **podem ser usadas em segmentacao** no Query Builder
  (ex: `score_norm >= 70` e criterio valido)
- **Pre-requisito:** custom fields precisam ser **provisionados pelo
  suporte Smartico via JIRA** — nao se cria via API

**Fontes:** smartico.ai docs (user-profile-properties, custom-fields-attributes,
query-builder, data-integration), smarticoai/public-api (GitHub).

**3 perguntas pro Raphael (destravar decisao):**
1. Ja temos custom properties numericas provisionadas no nosso tenant
   Smartico, ou precisamos abrir ticket JIRA no suporte deles pra criar um
   campo `score_norm` (INTEGER 0-100)?
2. Qual endpoint/evento S2S voce usa hoje pra `update_profile` — pra eu
   alinhar o payload de `push_risk_matrix_to_smartico.py`?
3. Voce enxerga valor em ter o score numerico cru no painel pra montar
   automations com faixas dinamicas (ex: "score caiu X pontos em 7 dias"),
   ou as tags discretas ja atendem 100% dos casos de uso de CRM planejados?

**Recomendacao tecnica:** enviar `score_norm` como property numerica
(granularidade pra automations) + manter as tags `RISK_TIER_*`
(legibilidade em dashboard e uso imediato em flows ja configurados). Nao
substitui, complementa.

### 4.3 BLOQUEADO ATE CONCLUIR 4.1 + 4.2 — AVG -> mediana em ENGAGED/ZERO_RISK/BEHAV_RISK

**Status:** bloqueado ate decisao de 4.1 e 4.2 concluir.

**Motivo do bloqueio:** trocar AVG por mediana muda quem cai em cada
bucket do CRM. Se essa mudanca acontecer antes de 4.1 (tag NEW) e 4.2
(score_norm), o Raphael vai ter que validar 3 mudancas de contrato CRM
ao mesmo tempo — excesso de risco operacional. Melhor sequenciar:
1. Primeiro: ativar NEW (4.1) e configurar score_norm se aprovado (4.2)
2. Depois: trocar AVG -> mediana nas 3 tags comportamentais, com shadow
   mode de 1 semana + rollout gradual

**Decisao:** **nao trocar agora.**

**Por que levantei:** AVG em distribuicao skewed (cauda longa) engana.
Exemplo concreto em ENGAGED_PLAYER:

> Grupo de 10 jogadores onde 9 tem 5 sessoes/dia e 1 tem 100 sessoes/dia:
> - **AVG** = (9\*5 + 100) / 10 = **14.5** -> classificado como "ENGAGED" (faixa 3-10... nesse caso ultrapassa, fica fora)
> - **Mediana** = **5** -> classificado como casual (dentro da faixa 3-10)
>
> AVG puxado por 1 outlier distorce a categorizacao. Mediana reflete o
> comportamento tipico do grupo.

**Por que nao e critico como o RG_ALERT (que ja corrigi):** RG_ALERT e
tag regulatoria de Jogo Responsavel — false-positive tem impacto de
compliance. ENGAGED/ZERO_RISK/BEHAV_RISK sao tags de segmentacao de
marketing; false-positive gera campanha mal direcionada mas nao
regulatorio.

**Por que e perigoso mexer sem aviso:** trocar AVG por mediana **muda
quem cai em cada bucket**. Jogador que hoje e ENGAGED pode sair; outro
pode entrar. Base do CRM oscila, campanhas em curso perdem/ganham
destinatarios, relatorios historicos ficam incomparaveis com os novos.

**Acao:** proximo ciclo de sprint com:
1. Avisar Raphael/Castrin 3 dias antes
2. Rodar em shadow mode (AVG atual + mediana nova) por 1 semana
3. Documentar o shift no changelog do CRM

### 4.4 Mantido como esta — unificar `scripts/sql/` vs `ec2_deploy/sql/`

**Decisao:** nao unificar agora.

**Por que:** o projeto esta saindo da arquitetura EC2/cron automatizada.
Vamos manter tudo no git local e decidir pra onde levar quando a
infraestrutura estabilizar. Nao vale a pena mexer na duplicacao agora
porque a estrutura de destino ainda nao esta definida.

**Acao:** ficar de olho quando for decidir a nova arquitetura (provavel
orquestrador pelo Gusta — ver `project_ec2_migracao_orquestrador.md`).

---

## 5. Processos/feedbacks registrados na memoria

Trabalho de hoje gerou 2 novos feedbacks criticos na memoria:

- [memory/feedback_sql_review_checklist_gusta.md](../memory/feedback_sql_review_checklist_gusta.md) — **6 perguntas antes de push SQL** (INNER JOIN silencioso, full scan fixo, CTEs com filtro temporal diferente, MAX de string, cast implicito, COUNT DISTINCT em CASE)
- [memory/feedback_gatekeeper_deploy_automatizado.md](../memory/feedback_gatekeeper_deploy_automatizado.md) — **auditor OBRIGATORIO** antes de deploy EC2, front ou push CRM; **extractor OBRIGATORIO** em primeira escrita de pipeline Athena; bloquear deploy se auditoria nao rodou

---

## 6. Status do git (ATENCAO)

**Nenhum arquivo PCR esta no git.** O pipeline PCR inteiro e untracked:

| Arquivo | Status git |
|---|---|
| `pipelines/pcr_pipeline.py` | ??? untracked (NUNCA commitado) |
| `scripts/push_pcr_to_smartico.py` | ??? untracked |
| `scripts/pcr_scoring.py` | ??? untracked |
| `docs/pcr_player_credit_rating.md` | ??? untracked |
| `PCR_Player_Credit_Rating_v1.html` | ??? untracked |
| `PCR_Player_Credit_Rating_v1.1.html` | ??? untracked |
| `PCR_Player_Credit_Rating_v1.2.html` | ??? untracked |
| `ec2_deploy/deploy_pcr_pipeline.sh` | ??? untracked |
| `ec2_deploy/run_pcr_pipeline.sh` | ??? untracked |
| `reports/pcr_*.csv` | ??? untracked |
| `docs/auditoria_sql_pcr_20260420.md` | ??? untracked (criado hoje) |
| `docs/auditoria_sql_matriz_risco_20260420.md` | ??? untracked (criado hoje) |
| `docs/proposta_pcr_rating_new_20260420.md` | ??? untracked (criado hoje) |
| `docs/README_auditoria_sql_20260420.md` | ??? untracked (este arquivo) |

**Matriz de Risco:** SQLs ja estao no git, so precisam ser commitados
(status `M` modified — os 10 fixes aplicados hoje).

### Plano de commit sugerido

Organizar em 4 commits semanticos (cada um um tema):

```bash
# 1. Onboarding do PCR no git (estava fora)
git add pipelines/pcr_pipeline.py scripts/push_pcr_to_smartico.py \
        scripts/pcr_scoring.py docs/pcr_player_credit_rating.md \
        PCR_Player_Credit_Rating_v1*.html \
        ec2_deploy/deploy_pcr_pipeline.sh ec2_deploy/run_pcr_pipeline.sh \
        reports/pcr_*.csv reports/pcr_*_legenda.txt \
        reports/smartico_pcr_*
git commit -m "feat: onboarding PCR (Player Credit Rating) v1.2 completo"

# 2. Fixes de auditoria (PCR + Matriz)
git add pipelines/pcr_pipeline.py \
        ec2_deploy/sql/risk_matrix/*.sql \
        scripts/sql/risk_matrix/*.sql
git commit -m "fix: auditoria SQL 20/04 — PCR c_category + ROW_NUMBER + 5 SQLs matriz (RG_ALERT, CASHOUT_AND_RUN, POTENCIAL_ABUSER, ROLLBACK, MULTI_GAME)"

# 3. Documentacao de auditoria
git add docs/auditoria_sql_pcr_20260420.md \
        docs/auditoria_sql_matriz_risco_20260420.md \
        docs/proposta_pcr_rating_new_20260420.md \
        docs/README_auditoria_sql_20260420.md
git commit -m "docs: auditorias SQL (PCR + Matriz Risco) + proposta PCR_RATING_NEW"

# 4. Push
git push origin main
```

Opcao alternativa (mais rapido, menos organizado):

```bash
git add pipelines/pcr_pipeline.py scripts/push_pcr_to_smartico.py \
        ec2_deploy/ scripts/ docs/ PCR_*.html reports/pcr_* reports/smartico_pcr_*
git commit -m "feat: PCR + auditoria SQL (PCR + Matriz Risco) com 11 fixes aplicados"
git push origin main
```

---

## 7. Proximos passos (em ordem)

1. **[FEITO]** Commitar tudo no git (a8686e3 + 217d4ef + 6e50b0c + da927e8 + 20a32e6)

1b. **[FEITO — 20/04/2026 18:20 BRT]** Schema v1.3 aplicado em producao via
   `scripts/apply_pcr_schema_v13.py` — rating agora e VARCHAR(10), views
   recriadas com NEW no ORDER BY. Proximo ciclo do pipeline PCR grava
   snapshot com rating NEW naturalmente.

2. **[EM ANDAMENTO — Raphael + Castrin]** Decidir PCR_RATING_NEW
   - Codigo ja em shadow mode (tabela grava NEW, push nao envia)
   - Levar proposta tecnica na reuniao: [proposta_pcr_rating_new_20260420.md](proposta_pcr_rating_new_20260420.md)
   - Decisao necessaria: (a) aprovar thresholds 14d/3dep? (b) criar tag no Smartico + jornada boas-vindas?
   - Apos aprovacao: trocar `PUSH_NEW_TAG_ENABLED = False` -> `True` + rollout 3 fases

3. **[EM ANDAMENTO — Raphael]** Confirmar viabilidade de `score_norm` como Smartico Custom Property
   - Pesquisa tecnica concluida: Smartico SUPORTA (via S2S `update_profile`)
   - 3 perguntas especificas pro Raphael listadas na secao 4.2
   - Resposta define se vale o trabalho de provisionar via JIRA

4. **[BLOQUEADO ATE 2 E 3 CONCLUIREM]** AVG -> mediana em ENGAGED/ZERO_RISK/BEHAV_RISK
   - NAO fazer ate 4.1 (PCR_RATING_NEW) e 4.2 (score_norm) fecharem
   - Motivo: evitar 3 mudancas de contrato CRM simultaneas
   - Plano: shadow mode 1 semana + rollout gradual apos validacao

5. **[AGUARDANDO GUSTA]** Unificar `scripts/sql/` vs `ec2_deploy/sql/`
   - Esperar decisao da nova arquitetura pos-migracao EC2
   - Ver `project_ec2_migracao_orquestrador.md`

---

## 8. Aprendizado meta

O checklist funcionou. Aplicado em 2 sistemas em producao, encontrou:
- **4 criticos** (1 de compliance regulatorio no RG_ALERT)
- **10 medios** (custo + imprecisao metrica)
- **6 padroes sistemicos** (que se corrigidos arrumam 10+ SQLs de uma vez)

Tempo total: ~30 minutos (2 auditores em paralelo + review + fixes).
Tempo que seria gasto corrigindo dano em producao: incalculavel.

**Custo/beneficio: passar pelo auditor antes de deploy e essencial** (ver
`feedback_gatekeeper_deploy_automatizado.md` — regra formalizada).
