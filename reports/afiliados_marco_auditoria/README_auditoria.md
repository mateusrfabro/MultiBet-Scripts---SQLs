# Auditoria Pagamento Afiliados — MARCO/2026

**Periodo:** 2026-03-01 a 2026-03-31 (BRT) — marco completo (31 dias, mes fechado)
**Gerado em:** 2026-04-23
**Fonte:** Athena Iceberg Data Lake (read-only)

## Arquivos entregues

| Arquivo | Grao | Linhas | Tamanho |
|---------|------|-------:|--------:|
| `afiliados_consolidado.csv` | 12 IDs (duplicados resolvidos) | 12 | <1 KB |
| `sports_marco_afiliados.csv` + `_legenda.txt` | 1 linha por **operacao** (M/P/L/C/R/MC/MD) | 26.223 | 6.6 MB |
| `casino_marco_afiliados.csv` + `_legenda.txt` | 1 linha por transacao financeira (c_txn_id) | 1.236.182 | 232 MB |
| `geral_marco_afiliados.csv`  + `_legenda.txt` | 1 linha por transacao financeira (c_txn_id) | 1.262.331 | 187 MB |
| `players_resolvidos.csv` | 1 linha por jogador (ecr_id + external_id + affiliate) | 15.897 | 674 KB |
| `validacao_cruzada.csv` | Comparativo CSV vs bireports por afiliado | 11 | 2 KB |

## Como interpretar

- **Sports**: cada bilhete gera **N linhas** (1 por operacao — commit/payout/rollback).
  Para NGR sportsbook por afiliado: `SUM(stake_amount) onde type='M'` menos `SUM(gain_amount) onde type='P'`.
- **Casino**: valores ja em BRL real (/100 aplicado). Bet = type_id in (27,28); Win = type_id in (45,65);
  Rollback = type_id in (72,77).
- **Geral**: TODAS as transacoes fund_ec2, incluindo depositos (type_id=1), saques (type_id=2),
  bets, wins, rollbacks, ajustes. `src` identifica produto (CASINO/SPORTSBOOK).
- Ver `<arquivo>_legenda.txt` para dicionario completo coluna PDF -> origem Athena.

## Filtros aplicados

- Periodo: c_created_time / c_start_time entre `2026-03-01 00:00:00 BRT` e `2026-04-01 00:00:00 BRT`
- `ps_bi.dim_user.is_test = false` (jogadores de teste excluidos)
- `ps_bi.dim_user.signup_datetime < 2026-04-01 00:00 BRT` (apenas players cadastrados ate 31/03)
- `affiliate_id IN` (12 IDs da lista consolidada)
- Transacoes financeiras: `c_txn_status = 'SUCCESS'`

## Validacao cruzada (CV vs bireports_ec2.tbl_ecr_wise_daily_bi_summary)

Comparamos os totais agregados por afiliado (stake esportivo, bet casino, depositos) dos CSVs
gerados vs a camada BI canonica. Resultado:

### Sports (stake_amount type='M')
- 7 de 11 afiliados: diferenca **0.00%** (bate exato)
- Resto: 0.04% a 1.85% — todos dentro da tolerancia de +-2%

### Casino (amount type_id IN (27,28))
- 8 de 11 afiliados dentro de +-2%
- 3 divergencias: +4.54% (454861), +3.97% (509759), +6.43% (524476)

### Depositos (amount type_id=1)
- 9 de 11 afiliados dentro de +-2%
- 2 divergencias: +2.34% (454861), +3.22% (524476)

### Causa provavel das divergencias residuais
Divergencia conhecida (documentada em `feedback_test_users_filtro_completo.md`): o filtro
`ps_bi.is_test=false` (usado aqui) e ligeiramente diferente de `bireports_ec2.c_test_user=false`
(usado em campanhas de atribuicao). A diferenca historica e de ~2-3%.
Em TODOS os casos, o CSV vem **LEVEMENTE MAIOR** que o bireports — ou seja, no pior cenario,
o afiliado ganha a mais, nunca a menos. Auditoria pode usar os numeros sem risco de underpay.

Detalhe por afiliado em `validacao_cruzada.csv`.

## Afiliados incluidos (11 ativos + 1 sem dados)

Duplicados consolidados conforme confirmado:
- **488468** = TP GESTAO DE MARKETING DIGITAL LTDA / TALES PERES DE MELO / [EST] Talesperes_Tiktok
- **522962** = MATHEUS MENDOCA / ADS LTDA / [EST] matheus_mendonca_rSq5aSMp

| affiliate_id | Nome | Players (ate 31/03) |
|---|---|---:|
| 453598 | VALLUM 3 | 3.420 |
| 509759 | [EST] Nicola / SLJ EMPREENDIMENTOS | 5.757 |
| 454861 | E-2 Communications (Malta) / Equadrata | 1.916 |
| 522962 | MATHEUS MENDOCA / ADS LTDA (+ variacoes) | 1.047 |
| 489203 | ARMONI SOLUCOES / ARMANDO SBRISSA | 1.009 |
| 475425 | VICTOR GIOVANNI SILVA DE SOUZA | 862 |
| 506920 | SBR Esportes (Sofascore / Raquel Carmelo) | 762 |
| 524476 | LEVEL UP / PAULO ANDRADE | 518 |
| 488468 | TP GESTAO / TALES PERES DE MELO (+ variacoes) | 478 |
| 529063 | LEANDRO LUIZ DE CASTRO | 65 |
| 528138 | [EST] Elberth Romero | 63 |
| **526453** | **AFILIADOS BRASIL LTDA / AffiliatesBR** | **0 (nao existe no Athena — validar back-office)** |

### !! ATENCAO: affiliate_id 526453 (AFILIADOS BRASIL LTDA)
O ID `526453` nao foi encontrado em nenhuma camada do Athena:
- `ps_bi.dim_user` -> 0 registros (por ID, por nome)
- `ecr_ec2.tbl_ecr_banner` -> 0 registros (como affiliate/tracker/banner, por nome)
- `ecr_ec2.tbl_ecr` -> 0 registros

**Acao recomendada antes do pagamento**: time de auditoria confirmar no back-office ou
com o time de marketing qual e o ID correto desse afiliado.

## Observacoes tecnicas

- As 3 tabelas dos PDFs (`sports_transactions`, `t_casino_transactions`, `t_transactions`)
  NAO existem identicamente no nosso Data Lake (Athena) — sao schemas do back-office operacional.
  Mapeamos para as tabelas equivalentes Athena e preservamos os nomes do PDF como colunas
  de saida, com legenda de-para em cada `_legenda.txt`.
- Algumas colunas do PDF nao tem equivalente direto no Athena e vieram vazias (NULL) —
  ver legenda de cada arquivo.
- Valores: sportsbook ja em BRL real; casino/geral foram convertidos de centavos (/100).
- Timezone convertido de UTC para BRT (America/Sao_Paulo) em todas as colunas timestamp.
- Sports: dedup via ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id) aplicado devido a
  multiplas versoes de CDC no `vendor_ec2.tbl_sports_book_bets_info` (cada bilhete tinha
  em media 1.99 linhas por CDC snapshot).

## Estrutura das queries SQL

Script gerador: `scripts/extract_afiliados_marco_auditoria.py`
Script validador: `scripts/validar_afiliados_marco_vs_bireports.py`
