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

### 4.1 Pendente — PCR_RATING_NEW para novatos

**Status:** proposta tecnica pronta, aguardando reuniao 15 min com Raphael (CRM) + Castrin (Head).

**Documento:** [proposta_pcr_rating_new_20260420.md](proposta_pcr_rating_new_20260420.md)

**Resumo:** FTD recente (com < 14 dias de atividade OU < 3 depositos) cai
automaticamente em rating E por instabilidade estatistica da formula PVS.
Solucao: criar bucket separado `PCR_RATING_NEW` com jornada de boas-vindas
no Smartico. Nao mexi no codigo sem aprovacao porque muda o contrato com
o CRM.

### 4.2 Mantido como esta — score_norm no diff do push Smartico

**Decisao:** deixar como esta ate confirmar com Raphael.

**Por que levantei:** hoje o push pra Smartico envia update quando o
conjunto de tags muda (`set(tags) != set(prev_tags)`). Se `score_norm`
vai de 47 pra 49 (ambos dentro da mesma tag `RISK_TIER_MEDIANO`), o push
NAO e enviado. Isso esta **correto** se o Smartico so consome as tags.
Se o Smartico consome o score numerico tambem (pra segmentacao fina), o
valor fica stale do lado dele.

**Por que nao e critico:** a auditoria nao confirmou que Smartico usa
score_norm — so levantou a possibilidade. Se confirmado que usa so as
tags, o comportamento atual e o correto e nao precisa fix.

**Acao:** 1 pergunta pro Raphael. Resposta "so tags" -> fecha o ticket.
Resposta "usa score" -> ajuste de 1 linha no criterio de diff.

### 4.3 Mantido como esta — AVG -> mediana em ENGAGED/ZERO_RISK/BEHAV_RISK

**Decisao:** nao trocar agora.

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

## 7. Proximos passos

1. **Commitar tudo** (estrutura acima ou compactada)
2. **Agendar reuniao 15 min** com Raphael + Castrin pra decidir PCR_RATING_NEW (doc: `proposta_pcr_rating_new_20260420.md`)
3. **1 pergunta pro Raphael** sobre score_norm no Smartico (4.2)
4. **Proximo sprint:** avaliar AVG -> mediana em ENGAGED/ZERO_RISK/BEHAV_RISK com rollout em shadow mode (4.3)
5. **Aguardar Gusta** definir nova arquitetura pra entao decidir unificacao `scripts/sql/` vs `ec2_deploy/sql/` (4.4)

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
