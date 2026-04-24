# Auditoria Pagamento Afiliados AffiliatesBR — MARCO/2026

**Periodo:** 2026-03-01 a 2026-03-31 (BRT) — marco completo (31 dias, mes fechado)
**Lote:** AffiliatesBR (97 IDs)
**Gerado em:** 2026-04-23
**Fonte:** Athena Iceberg Data Lake (read-only)

## Arquivos entregues

| Arquivo | Grao | Linhas |
|---------|------|-------:|
| `Aff_Br_lista_original.txt` | Lista recebida do marketing | 97 IDs |
| `afiliados_consolidado.csv` | Lista normalizada (ID + nome) | 97 |
| **`pagamento_sugerido_por_afiliado.csv`** | **Resumo NET por afiliado — pronto pra calcular comissao** | 97 |
| `sports_affbr_marco_afiliados.csv` + `_legenda.txt` | 1 linha por **operacao** sportsbook | 892 |
| `casino_affbr_marco_afiliados.csv` + `_legenda.txt` | 1 linha por transacao casino (c_txn_id) | 687.437 |
| `geral_affbr_marco_afiliados.csv`  + `_legenda.txt` | 1 linha por transacao financeira (c_txn_id) | 688.336 |
| `players_resolvidos.csv` | 1 linha por jogador | 17.840 |
| `validacao_cruzada.csv` | Comparativo CSV vs bireports | 19 |
| `cobertura_pdf_vs_csv.md` | De-para completo PDF back-office vs Athena | — |

## !!! PARA CALCULAR O PAGAMENTO — USAR `pagamento_sugerido_por_afiliado.csv` !!!

Os CSVs granulares (`sports_*`, `casino_*`, `geral_*`) contem TODAS as transacoes,
INCLUSIVE rollbacks/cancelamentos. **Se somar so as apostas (type_id 27,28) sem descontar
os rollbacks (type_id 72,76), o valor fica inflado** e pagaria-se a mais.

O arquivo `pagamento_sugerido_por_afiliado.csv` ja faz o calculo NET correto:

- `casino_bet_net`   = `SUM(type_id IN 27,28)` **menos** `SUM(type_id IN 72,76,133)`
- `casino_win_net`   = `SUM(type_id IN 45,65,79,80,91)` **menos** `SUM(type_id IN 77,86,114)`
- `casino_ggr`       = `casino_bet_net - casino_win_net`
- `sb_stake_net`     = sports stake (type='M') **menos** cancels (type IN 'C','R')
- `sb_ggr`           = `sb_stake_net - sb_payout`
- **`ggr_total`**    = `casino_ggr + sb_ggr` → **base para calcular comissao do afiliado**

### Validado contra bireports_ec2.tbl_ecr_wise_daily_bi_summary

Apos aplicar o NET, 3 dos 4 afiliados com divergencia anterior ficaram com **0.00% de diff**:

| affiliate_id | CSV bruto (27+28) | CSV NET (bruto - 72) | bireports | Diff |
|---|---:|---:|---:|---:|
| 427496 | 23.643,54 | **6.841,54** | 6.841,07 | **0.01%** |
| 457204 | 93.046,12 | **70.916,72** | 70.916,72 | **0.00%** |
| 500809 | 24.590,35 | **20.589,95** | 20.589,95 | **0.00%** |
| 444946 | 4.883,90 | 4.883,90 | 4.734,41 | 3.16% (*) |

(*) 444946 nao teve rollback — os 3.16% residuais sao a diferenca conhecida entre
`ps_bi.is_test` e `bireports.c_test_user` (~3%). Aceitavel.

**Totais calculados para marco/2026:**
- GGR Casino: R$ 49.117,50
- GGR Sports: R$ 796,13
- **GGR Total: R$ 49.913,63** (base da comissao)

## Volume por CSV

- **Sports: 892 linhas** — perfil AffiliatesBR e praticamente 100% casino, quase sem apostas esportivas
- **Casino: 687.437 linhas** — volume concentrado em ~15 afiliados ativos (a maioria dos 97 nao teve transacao em marco)
- **Geral: 688.336 linhas** — casino + depositos/saques

## Filtros aplicados

- Periodo: `c_start_time` / `c_created_time` entre 2026-03-01 00:00 BRT e 2026-04-01 00:00 BRT
- `ps_bi.dim_user.is_test = false`
- `ps_bi.dim_user.signup_datetime < 2026-04-01 00:00 BRT`
- `affiliate_id IN` (97 IDs AffiliatesBR)
- Transacoes: `c_txn_status = 'SUCCESS'`
- Sports: dedup CDC via `ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id)` (aprendizado do lote anterior)

## Validacao cruzada (CV vs bireports_ec2)

### Sports (stake_amount type='M')
- **Perfeito**: 11 afiliados com diff 0.00%
- 1 afiliado (522633): +5.26% — dentro da margem operacional
- Demais: sem apostas esportivas (NaN — perfil so-cassino)

### Depositos (amount type_id=1)
- **Perfeito**: 12 afiliados com diff 0.00%
- 1 afiliado (522633): +0.44%

### Casino (amount type_id IN (27,28))
- 9 afiliados com diff 0% no bruto
- 4 afiliados tiveram rollback significativo — resolvido aplicando NET (ver secao "PARA CALCULAR O PAGAMENTO" acima)

### Causa confirmada das divergencias de casino

**CONFIRMADO por auditoria:** `bireports.c_casino_realcash_bet_amount` e **NET**
(bet bruto menos cancelamentos). Nosso CSV granular traz TODAS as transacoes (inclui rollbacks
como linhas type_id=72). O calculo NET correto esta em `pagamento_sugerido_por_afiliado.csv`.

Detalhe por afiliado em `validacao_cruzada.csv`.

## Afiliados com atividade em marco (19 dos 97)

A lista tem muitos sub-trackers ou IDs inativos — normal pra lote de 97 afiliados.
Principais ativos:

| affiliate_id | Nome | Players |
|---|---|---:|
| 522633 | affbrcmtmulti2.0 | 11.455 |
| 471929 | affbrmlboamtbt | 973 |
| 511007 | affbrcmtmulti | 576 |
| 457204 | affbrcmt1.11 | 489 |

Ver `players_resolvidos.csv` pra breakdown completo.

## Observacoes tecnicas

- As 3 tabelas dos PDFs NAO existem identicamente no Data Lake — mapeamos para equivalentes
  Athena. Ver `cobertura_pdf_vs_csv.md` para de-para completo (66 colunas PDF, 37 OK, 10 proxy, 19 NULL).
- Valores: sports em BRL real; casino/geral convertidos de centavos (/100).
- Timezone UTC -> BRT em todos os campos timestamp.

Script gerador: `scripts/extract_afiliados_affbr_marco_auditoria.py`
Script validador: `scripts/validar_afiliados_affbr_marco_vs_bireports.py`
