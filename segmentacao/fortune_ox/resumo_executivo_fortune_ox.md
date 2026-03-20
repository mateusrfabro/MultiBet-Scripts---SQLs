# Resumo Executivo - Segmentacao Fortune Ox
**Data:** 13/03/2026 | **Analista:** Mateus Fabro | **Squad:** Intelligence Engine

---

## Objetivo
Segmentar jogadores do Fortune Ox (PG Soft) a partir do segmento opt-in da promocao RETEM_PROMO_RELAMPAGO_120326,
classificando-os por faixas de volume apostado para acionamento de CRM (mark user).

## Periodo de Analise
- **Inicio:** 12/03/2026 18:00 (BRT) / 21:00 (UTC)
- **Fim:** 12/03/2026 22:00 (BRT) / 13/03/2026 01:00 (UTC)
- **Duracao:** 4 horas (janela promocional relampago)

## Fontes de Dados
| Fonte | Tabela / View | Uso |
|-------|---------------|-----|
| Smartico (BigQuery) | `j_user.core_tags` | Base de IDs com opt-in (tag RETEM_PROMO_RELAMPAGO_120326) |
| Redshift (Pragmatic) | `fund.tbl_real_fund_txn` | Transacoes de apostas (c_txn_type 27 e 72) |
| Redshift (Pragmatic) | `ecr.tbl_ecr` | Mapeamento de IDs (c_external_id -> c_ecr_id) |
| Redshift (Pragmatic) | `bireports.tbl_vendor_games_mapping_data` | Catalogo de jogos (game_id = '2603') |

## Metodologia
- **Jogo:** Fortune Ox (PG Soft) = `c_game_id = '2603'` (confirmado via catalogo Pragmatic)
- **Segmento Smartico:** https://drive-6.smartico.ai/24105#/j_segment/28929
- **Net Bet:** Total Apostado (txn_type=27) - Rollbacks (txn_type=72) | Valores em centavos BRL (/100)
- **Rollbacks no periodo:** 0 ocorrencias — nenhum jogador desclassificado
- **Join de IDs:** Smartico `user_ext_id` -> `ecr.tbl_ecr.c_external_id` -> `c_ecr_id` -> `fund`

---

## Resultados

### Visao Geral do Segmento
| Metrica | Valor |
|---------|-------|
| Total de usuarios marcados (opt-in) | 3.263 |
| **Jogadores com apostas no Fortune Ox no periodo** | **218 (6,7% do segmento)** |
| Jogadores sem apostas no periodo | 3.045 (93,3%) |
| Desclassificados (rollback) | 0 |
| **Jogadores elegiveis (alguma faixa)** | **117 (53,7% dos que jogaram)** |
| Abaixo do minimo (< R$ 30) | 101 |

### Net Bet Total
| Metrica | Valor |
|---------|-------|
| **Net Bet total do segmento** | **R$ 30.326,00** |
| Net Bet medio (por jogador ativo) | R$ 139,11 |
| Net Bet mediana | R$ 35,75 |
| Maior Net Bet individual | R$ 8.189,00 |
| Menor Net Bet (> 0) | R$ 0,50 |

### Distribuicao por Faixas de Aposta
| Faixa | Criterio | Jogadores | Volume (R$) | % do Volume |
|-------|----------|-----------|-------------|-------------|
| Faixa 4 | >= R$ 600 | 5 | R$ 14.432,00 | 47,6% |
| Faixa 3 | R$ 300 a R$ 599,99 | 10 | R$ 3.976,00 | 13,1% |
| Faixa 2 | R$ 100 a R$ 299,99 | 46 | R$ 7.696,50 | 25,4% |
| Faixa 1 | R$ 30 a R$ 99,99 | 56 | R$ 3.164,00 | 10,4% |
| Abaixo do Minimo | < R$ 30 | 101 | R$ 1.057,50 | 3,5% |
| Desclassificados (rollback) | — | 0 | R$ 0,00 | — |
| **Total elegiveis (faixas 1-4)** | | **117** | **R$ 29.268,50** | **96,5%** |

### Top 10 Jogadores por Volume
| # | user_ext_id | Net Bet | Faixa |
|---|-------------|---------|-------|
| 1 | 713871766520158 | R$ 8.189,00 | Faixa 4 |
| 2 | 712832 | R$ 3.390,00 | Faixa 4 |
| 3 | 28006366 | R$ 1.455,00 | Faixa 4 |
| 4 | 29580968 | R$ 788,00 | Faixa 4 |
| 5 | 29833015 | R$ 610,00 | Faixa 4 |
| 6 | 322961771979052 | R$ 530,00 | Faixa 3 |
| 7 | 29423171 | R$ 444,00 | Faixa 3 |
| 8 | 509831761765004 | R$ 413,50 | Faixa 3 |
| 9 | 194001765482118 | R$ 391,50 | Faixa 3 |
| 10 | 425221773276473 | R$ 389,00 | Faixa 3 |

---

## Insights e Observacoes

### Perfil de Apostas
- Ticket medio de R$ 139,11 com mediana de R$ 35,75 — distribuicao assimetrica pelo jogador #1 (R$ 8.189,00)
- Faixa 1 (R$ 30-99) concentra o maior numero de jogadores elegiveis (56), mas Faixa 4 domina 47,6% do volume
- Janela curta de 4h resultou em 93% dos marcados sem atividade no jogo — esperado para relampago

### Concentracao de Volume
- Top 2 jogadores representam 38,4% do volume total (R$ 11.579,00)
- Faixas 3 e 4 somam apenas 15 jogadores mas concentram 60,7% do volume (R$ 18.408,00)

### Rollbacks
- Zero rollbacks no periodo — todos os 218 jogadores mantidos como elegiveis para classificacao por faixa
- Confirmado via `c_txn_type = 72` (CASINO_BUYIN_CANCEL)

---

## Entregaveis
| Arquivo | Descricao |
|---------|-----------|
| `segmentacao_fortune_ox_relampago_120326.csv` | CSV final com 3.263 linhas, 12 colunas |
| `validacao_smartico.py` | Script de validacao cruzada Redshift vs BigQuery |
| `segmentacao_fortune_ox.py` | Script Python reprodutivel (BigQuery + Redshift + Pandas) |
| `resumo_executivo_fortune_ox.md` | Este documento |

## Validacao Cruzada: Redshift vs Smartico (BigQuery)
| Metrica | Redshift (Pragmatic) | Smartico (BigQuery) | Diferenca |
|---------|----------------------|---------------------|-----------|
| Jogadores unicos | 218 | 218 | 0 (exato) |
| Total bet BRL | R$ 30.326,00 | R$ 30.267,20 | R$ 58,80 (0,19%) |
| Rollbacks | 0 | 0 | — |
| Net Bet | R$ 30.326,00 | R$ 30.267,20 | R$ 58,80 (0,19%) |

Diferenca de 0,19% entre as fontes — aceitavel, causada por latencia de ingestao entre plataformas.
Script de validacao: `validacao_smartico.py`

## Validacoes Realizadas
- [x] `c_game_id = '2603'` confirmado como Fortune Ox (PG Soft) no catalogo `bireports.tbl_vendor_games_mapping_data`
- [x] `casino_last_bet_game_name = 45846458` confirmado no Smartico via `dm_casino_game_name` (smr_game_id)
- [x] Usuarios extraidos via tag `RETEM_PROMO_RELAMPAGO_120326` em `j_user.core_tags` — 3.263 com opt-in confirmado
- [x] Valores em centavos confirmados pela documentacao Pragmatic v1.3 — divisao por 100 aplicada
- [x] Zero rollbacks (txn_type=72) no periodo — nenhum jogador desclassificado
- [x] Mapeamento de IDs validado: Smartico `user_ext_id` = `ecr.tbl_ecr.c_external_id`
- [x] Cada jogador aparece em apenas uma faixa (a mais alta atingida) — sem duplicidade de pagamento
- [x] Periodo em UTC: 2026-03-12 21:00:00 -> 2026-03-13 01:00:00 (= 12/03 18h00-22h00 BRT)
- [x] Validacao cruzada Redshift vs BigQuery (Smartico): 218 jogadores em ambas as fontes, diferenca de R$ 58,80 (0,19%) — dados consistentes
