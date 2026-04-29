# Teste de Validação — Smartico Tags `PCR_RATING_*`

**Data:** 29/04/2026
**Fonte do snapshot:** `multibet.pcr_atual` (snapshot_date = 2026-04-29)
**Pipeline:** `segmentacao_sa_diaria.py` (cron 04:00 BRT, EC2 Prefect)
**Amostra CSV:** [`smartico_amostra_validacao_2026-04-29.csv`](smartico_amostra_validacao_2026-04-29.csv)
**Validador:** Mateus Fabro

---

## Contexto

O pipeline `segmentacao_sa_diaria.py` rodou no orquestrador EC2 às 04:00 BRT do dia 29/04/2026 (duração: 11min57s) e fez push de tags `PCR_RATING_*` para 135.545 jogadores no Smartico via `core_external_markers`.

Este documento valida visualmente 10 casos representativos no Backoffice da Smartico, confirmando que cada jogador tem **exatamente 1** tag `PCR_RATING_*` aplicada e que ela corresponde ao rating calculado pelo PCR.

## Metodologia

- **Distribuição:** 1 S + 2 A + 2 B + 2 C + 1 D + 1 E + 1 NEW = 10 jogadores
- **Critério de seleção:** top N por PVS dentro de cada rating (jogadores mais representativos do tier)
- **Validação manual:** consultar cada `user_ext_id` no Smartico e conferir aba External Markers

## Critérios de aprovação

| Critério | Esperado |
|---|---|
| Tag presente | ✅ Tem 1 tag `PCR_RATING_*` |
| Tag correta | ✅ Bate com o rating calculado |
| Sem duplicata | ✅ NÃO existem 2+ tags `PCR_RATING_*` simultâneas |

## Resultados

| # | user_ext_id | Nome | Rating | Tag esperada | Tag observada | Resultado | Print |
|---|---|---|---|---|---|---|---|
| 1 | `870561766836487` | TIAGO GOMES | S | `PCR_RATING_S` | `PCR_RATING_S` | ✅ OK | `01_S_870561766836487.png` |
| 2 | `490481772387022` | LUCAS GOMES | A | `PCR_RATING_A` | `PCR_RATING_A` | ✅ OK | `02_A_490481772387022.png` |
| 3 | `30037747` | Kaique Marques | A | `PCR_RATING_A` | `PCR_RATING_A` | ✅ OK | `03_A_30037747.png` |
| 4 | `965021774033706` | IRLAN SANTOS | B | `PCR_RATING_B` | `PCR_RATING_B` | ✅ OK | `04_B_965021774033706.png` |
| 5 | `558801772363375` | DENIS ALBUQUERQUE | B | `PCR_RATING_B` | `PCR_RATING_B` | ✅ OK | `05_B_558801772363375.png` |
| 6 | `677901771269439` | ALLAN OLIVEIRA | C | `PCR_RATING_C` | `PCR_RATING_C` | ✅ OK | `06_C_677901771269439.png` |
| 7 | `899561772620226` | CESAR PEZZINI | C | `PCR_RATING_C` | `PCR_RATING_C` | ✅ OK | `07_C_899561772620226.png` |
| 8 | `30266394` | Rodrigo Maia | D | `PCR_RATING_D` | `PCR_RATING_D` | ✅ OK | `08_D_30266394.png` |
| 9 | `29710145` | Pedro Gomes | E | `PCR_RATING_E` | `PCR_RATING_E` | ✅ OK | `09_E_29710145.png` |
| 10 | `118301775148991` | RAFAELLA COSTA | NEW | `PCR_RATING_NEW` | `PCR_RATING_NEW` | ✅ OK | `10_NEW_118301775148991.png` |

## Conclusão

✅ **APROVADO — 10/10 OK**

Todos os 10 jogadores da amostra têm a tag `PCR_RATING_*` correta no `core_external_markers`, sem duplicatas, batendo exatamente com o rating calculado pelo PCR no snapshot 2026-04-29.

O pipeline `segmentacao_sa_diaria.py` está funcionando como esperado em produção, e o push Smartico de 135.545 jogadores feito às 04:00 BRT de 29/04 foi aplicado corretamente.

## Observações relevantes (não impactam aprovação)

1. **PCR vs Matriz de Risco são métricas independentes.** Alguns jogadores apresentam combinações que parecem contraditórias mas são válidas:
   - **IRLAN SANTOS** (#4) — `PCR_RATING_B` + `RISK_TIER_RUIM` (PCR mede valor monetário/PVS, matriz mede comportamento — um jogador pode valer muito e ter comportamento ruim).
   - **ALLAN OLIVEIRA** (#6) — `PCR_RATING_C` + `RISK_TIER_MUITO_RUIM` (mesmo padrão).

2. **Pedro Gomes (#9)** está com `Account status = BLOCKED` mas mesmo assim teve a tag `PCR_RATING_E` aplicada. **Esperado:** o pipeline aplica tag a todos os jogadores no snapshot, independente de status de conta — a régua de campanha do CRM é quem deve filtrar contas ativas.

3. **RAFAELLA COSTA (#10 — NEW)** tem apenas 1 external marker (`PCR_RATING_NEW`) — não tem `RISK_*` ainda. **Esperado:** jogadora muito nova (registrada 02/04/2026) ainda não tem histórico suficiente pra cálculo de matriz de risco comportamental.

---

## Anexos

Prints individuais (10) em [`prints_smartico_29_04_2026/`](prints_smartico_29_04_2026/):

- `01_S_870561766836487.png` — TIAGO GOMES
- `02_A_490481772387022.png` — LUCAS GOMES
- `03_A_30037747.png` — Kaique Marques
- `04_B_965021774033706.png` — IRLAN SANTOS
- `05_B_558801772363375.png` — DENIS ALBUQUERQUE
- `06_C_677901771269439.png` — ALLAN OLIVEIRA
- `07_C_899561772620226.png` — CESAR PEZZINI
- `08_D_30266394.png` — Rodrigo Maia
- `09_E_29710145.png` — Pedro Gomes
- `10_NEW_118301775148991.png` — RAFAELLA COSTA
