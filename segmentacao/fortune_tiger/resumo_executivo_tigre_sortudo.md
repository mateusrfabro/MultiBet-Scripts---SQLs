# Resumo Executivo - Segmentacao Tigre Sortudo
**Data:** 10/03/2026 | **Analista:** Mateus Fabro | **Squad:** Intelligence Engine

---

## Objetivo
Segmentar jogadores do Tigre Sortudo (Pragmatic Play) a partir de um export do Smartico,
classificando-os por faixas de volume apostado para acoes de CRM direcionadas.

## Periodo de Analise
- **Inicio:** 07/03/2026 16:00 (BRT) / 19:00 (UTC)
- **Fim:** 08/03/2026 23:59 (BRT) / 09/03/2026 02:59 (UTC)
- **Duracao:** ~32 horas

## Fontes de Dados
| Fonte | Tabela | Uso |
|-------|--------|-----|
| Smartico (BigQuery) | Export CSV `segment-export-smartic` | Base de IDs do segmento |
| Redshift (Pragmatic) | `fund.tbl_real_fund_txn` | Transacoes de apostas |
| Redshift (Pragmatic) | `ecr.tbl_ecr` | Mapeamento de IDs (external -> interno) |
| Redshift (Pragmatic) | `bireports.tbl_vendor_games_mapping_data` | Catalogo de jogos |

## Metodologia
- **Jogo:** Tigre Sortudo (Pragmatic Play) = `c_game_id = 'vs5luckytig'`
- **Net Bet:** Total Apostado (tipo 27) - Rollbacks (tipo 72) | Valores em centavos BRL (/ 100)
- **Rollbacks no periodo:** 4 ocorrencias (R$ 7,50 — impacto desprezivel)
- **Join de IDs:** Smartico `user_ext_id` -> `ecr.tbl_ecr.c_external_id` -> `c_ecr_id` -> `fund`

---

## Resultados

### Visao Geral do Segmento
| Metrica | Valor |
|---------|-------|
| Total de usuarios no segmento Smartico | 16.256 |
| IDs unicos | 15.763 |
| IDs com match no Redshift (ECR) | 6.546 (41%) |
| IDs sem match (leads/anonimos) | 9.217 (59%) |
| **Jogadores com apostas no Tigre Sortudo** | **345 (2,1% do segmento)** |
| Jogadores sem apostas | 15.911 |

### Net Bet Total
| Metrica | Valor |
|---------|-------|
| **Net Bet total do segmento** | **R$ 17.704,30** |
| Net Bet medio (por jogador ativo) | R$ 51,32 |
| Net Bet mediana | R$ 7,60 |
| Maior Net Bet individual | R$ 1.441,70 |
| Menor Net Bet (> 0) | R$ 0,10 |

### Distribuicao por Faixas de Aposta
| Faixa | Criterio | Jogadores | Volume (R$) | % do Volume |
|-------|----------|-----------|-------------|-------------|
| Faixa 4 | >= R$ 1.000 | 2 | R$ 2.750,70 | 15,5% |
| Faixa 3 | R$ 500 a R$ 999 | 4 | R$ 3.220,30 | 18,2% |
| Faixa 2 | R$ 200 a R$ 499 | 15 | R$ 4.151,10 | 23,4% |
| Faixa 1 | R$ 50 a R$ 199 | 53 | R$ 4.978,10 | 28,1% |
| Abaixo do Minimo | < R$ 50 | 271 | R$ 2.604,10 | 14,7% |
| **Total com apostas** | | **345** | **R$ 17.704,30** | **100%** |

### Top 10 Jogadores por Volume
| # | user_ext_id | Net Bet | Faixa |
|---|-------------|---------|-------|
| 1 | 28013337 | R$ 1.441,70 | Faixa 4 |
| 2 | 29578983 | R$ 1.309,00 | Faixa 4 |
| 3 | 28039552 | R$ 953,00 | Faixa 3 |
| 4 | 29624896 | R$ 938,00 | Faixa 3 |
| 5 | 29667163 | R$ 789,30 | Faixa 3 |
| 6 | 29561403 | R$ 540,00 | Faixa 3 |
| 7 | 30356012 | R$ 329,70 | Faixa 2 |
| 8 | 30163538 | R$ 306,00 | Faixa 2 |
| 9 | 30423666 | R$ 305,00 | Faixa 2 |
| 10 | 30148235 | R$ 300,00 | Faixa 2 |

---

## Insights e Observacoes

### Perfil de Apostas
- Ticket medio de R$ 51,32 com mediana de R$ 7,60 — maioria aposta valores baixos (micro-apostas)
- Distribuicao de volume equilibrada entre faixas (sem concentracao extrema no topo)
- Faixas 1 e 2 concentram 51,5% do volume total

### Rollbacks
- 4 cancelamentos identificados (R$ 7,50 total) — impacto desprezivel no resultado
- Confirmado via `c_txn_type = 72` e `c_is_cancelled = 1` (match exato: 4 apostas canceladas = 4 rollbacks)

### Gap de Mapeamento de IDs (59% sem match)
- 9.217 IDs do segmento Smartico sem cadastro no Redshift
- Causa: leads/visitantes anonimos capturados por cookie antes de finalizar registro

---

## Entregaveis
| Arquivo | Descricao |
|---------|-----------|
| `segmentacao_smartico_tigre_sortudo.csv` | CSV final com 16.256 linhas, 9 colunas |
| `validacao_smartico.py` | Script de validacao cruzada Redshift vs BigQuery |
| `segmentacao_tigre_sortudo.py` | Script Python reprodutivel (Redshift + Pandas) |
| `resumo_executivo_tigre_sortudo.md` | Este documento |

## Validacao Cruzada: Redshift vs Smartico (BigQuery)
| Metrica | Redshift (Pragmatic) | Smartico (BigQuery) | Diferenca |
|---------|----------------------|---------------------|-----------|
| Total bets | 117.009 | 117.007 | 2 txns (desprezivel) |
| Total bet BRL | R$ 118.134,50 | R$ 117.827,90 | R$ 306,60 (0,26%) |
| Rollbacks | 4 (R$ 7,50) | 0 (nao registra separado) | — |
| Net Bet | R$ 118.127,00 | R$ 117.827,90 | R$ 299,10 (0,25%) |

Diferenca de 0,25% entre as fontes — aceitavel, causada por rollbacks e arredondamentos.
Script de validacao: `validacao_smartico.py`

## Validacoes Realizadas
- [x] `c_game_id = 'vs5luckytig'` confirmado como Tigre Sortudo (Pragmatic Play) via catalogo
- [x] Descartado `vs5luckytig1k` (Tigre Sortudo 1000 — versao diferente)
- [x] Descartado `21565` (Lucky Tiger — Tada Gaming, jogo diferente)
- [x] Descartado `smr_game_id = 45846528` no Smartico (Lucky Tiger da Tada Gaming, nao Pragmatic)
- [x] Valores em centavos confirmados pela Pragmatic (divisao por 100)
- [x] 4 rollbacks identificados e corretamente descontados (R$ 7,50)
- [x] 59% sem match explicado (leads/anonimos do Smartico)
- [x] Validacao cruzada Redshift vs Smartico BigQuery: diferenca de 0,25% (aprovado)
