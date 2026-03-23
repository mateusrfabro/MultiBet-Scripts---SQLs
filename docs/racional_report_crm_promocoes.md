# Racional Tecnico — Report Promocoes CRM (Mar/2026)

**Pipeline:** `pipelines/report_crm_promocoes.py`
**Entrega:** `output/report_crm_promocoes_mar2026.xlsx`
**Data de execucao:** 2026-03-20
**Autor:** Mateus F. (analytics)
**Para validacao:** Arquiteto de Dados

---

## 1. Demanda

CRM solicitou, para 6 promocoes de jogos (07-15/mar):
- Quantidade de usuarios por faixa de turnover (independente do opt-in)
- Turnover total do jogo em 3 periodos: mes anterior, durante, dia(s) seguinte(s)
- UAP (Unique Active Players) nos mesmos 3 periodos

## 2. Fonte de Dados

### 2.1 Fonte principal: `fund_ec2.tbl_real_fund_txn`

**Justificativa da escolha:**
- As promocoes tem janelas com hora especifica (ex: 18h as 22h), exigindo precisao de timestamp.
- `ps_bi.fct_casino_activity_daily` agrega por dia — perderia precisao em promos curtas (ex: Fortune Ox, 4h).
- `fund_ec2` tem granularidade transacional com `c_start_time` (timestamp UTC).

**Filtros aplicados:**
| Campo | Valor | Justificativa |
|-------|-------|---------------|
| `c_txn_type` | `= 27` | Somente apostas (bet). 45=win, 72=rollback excluidos. |
| `c_txn_status` | `= 'SUCCESS'` | Status confirmado no schema fund_ec2. |
| `c_amount_in_ecr_ccy` | `/ 100.0` | Valores em centavos no fund_ec2 → dividir por 100 para BRL. |
| `c_test_user` | `= false` | Excluir test users via JOIN com `bireports_ec2.tbl_ecr`. |

### 2.2 Validacao cruzada: BigQuery Smartico (`tr_casino_bet`)

Comparacao de UAP e turnover total no periodo DURING para cada promo.

## 3. Otimizacao de Custo Athena

### 3.1 Problema: fund_ec2 NAO tem particao `dt`

A tabela `tbl_real_fund_txn` nao possui coluna de particao. O Athena cobra por volume de dados escaneados no S3. Sem filtro de timestamp, escanearia petabytes.

### 3.2 Solucao: filtro de timestamp PRIMEIRO no WHERE

Conforme instrucao do arquiteto, o filtro de `c_start_time` vem como **primeira condicao** do WHERE:

```sql
WHERE f.c_start_time >= TIMESTAMP '2026-03-07 17:00:00'   -- PRIMEIRO
  AND f.c_start_time <  TIMESTAMP '2026-03-19 03:00:00'   -- PRIMEIRO
  AND f.c_game_id IN (...)                                  -- depois
  AND f.c_txn_type = 27                                     -- depois
  AND f.c_txn_status = 'SUCCESS'                            -- depois
```

### 3.3 Estrategia de queries: apenas 2 scans

Para minimizar custo, consolidamos 6 promos x 3 periodos em apenas **2 queries**:

| Query | Periodo | Retorno | Range UTC | Motivo |
|-------|---------|---------|-----------|--------|
| **Q1** | DURING + AFTER | Per-user (c_ecr_id, promo_period, turnover_brl) | Mar 7→19 | Precisa dados por user para classificar em faixas |
| **Q2** | BEFORE | Agregado (promo_period, uap, turnover_brl) | Fev 7→16 | So precisa totais, sem user-level |

**Tecnica:** CTE com CASE WHEN que rotula cada transacao pelo promo+periodo correspondente. Transacoes fora de qualquer janela recebem NULL e sao filtradas no `WHERE promo_period IS NOT NULL`.

Clausulas DURING vem antes de AFTER no CASE WHEN para garantir prioridade em caso de sobreposicao temporal no mesmo game_id.

## 4. Conversao de Fuso Horario

| Horario BRT na demanda | Conversao UTC (Athena) | Regra |
|------------------------|----------------------|-------|
| `14h BRT` | `17:00:00 UTC` | BRT + 3h |
| `23h59 BRT` | `< 03:00:00 UTC dia seguinte` | Boundary exclusivo |
| `22h BRT` | `< 01:00:00 UTC dia seguinte` | Boundary exclusivo |
| `18h BRT` | `21:00:00 UTC` | BRT + 3h |

Usar `<` (exclusivo) no end timestamp evita incluir transacoes do segundo seguinte.

## 5. Regra dos Periodos Comparativos

Confirmado com CRM:

- **Mes anterior:** Mesma janela exata de horas/dias, deslocada 1 mes para tras.
  Ex: Promo 14h 07/03 → 23h59 08/03 ⟹ Before = 14h 07/02 → 23h59 08/02

- **Dia seguinte:** Mesma janela exata de horas/dias, deslocada para o(s) dia(s) imediatamente apos.
  Ex: Promo 14h 07/03 → 23h59 08/03 (2 dias) ⟹ After = 14h 09/03 → 23h59 10/03

## 6. Game IDs Utilizados

| Jogo | game_id (Athena/Redshift) | smr_game_id (Smartico BQ) | Vendor | Validacao |
|------|--------------------------|--------------------------|--------|-----------|
| Tigre Sortudo | `4776` | `45838245` | PG Soft | fund_ec2: 2.1M bets mar |
| Fortune Rabbit | `8842` | `45708862` | PG Soft | fund_ec2: 3.3M bets mar |
| Gates of Olympus | `vs20olympgate` | `45805477` | Pragmatic Play | fund_ec2: 209K bets mar |
| Sweet Bonanza | `vs20fruitsw` | `45883879` | Pragmatic Play | fund_ec2: 209K bets mar |
| Fortune Ox | `2603` | `45846458` | PG Soft | fund_ec2: 1.5M bets mar |
| Ratinho Sortudo | `vs10forwild` | `45881668` | Pragmatic Play | fund_ec2: 172K bets mar |
| Macaco Sortudo | `vs5luckym` | `45872323` | Pragmatic Play | fund_ec2: 183K bets mar |

**Fonte dos IDs:**
- game_id Athena: `bireports_ec2.tbl_vendor_games_mapping_data`
- smr_game_id Smartico: `smartico-bq6.dwh_ext_24105.dm_casino_game_name`
- Todos validados com transacoes reais no fund_ec2 (Mar 1-15, 2026)

## 7. Classificacao em Faixas

- Turnover = soma de bets (`c_txn_type = 27`) por user durante a promo
- Cada user e classificado na faixa onde seu turnover total se encaixa
- Faixas verificadas da maior para menor (match na primeira compativel)
- Users abaixo da faixa minima sao marcados como "Abaixo de R$X"
- Promo Combo (P6): turnover e a SOMA dos 3 jogos por user (GROUP BY ecr_id, sem split por game)

## 8. Resultados da Validacao Cruzada (Athena vs BigQuery)

| Promo | Athena UAP | BQ UAP | Diff | Athena Turnover | BQ Turnover | Diff | Status |
|-------|-----------|--------|------|----------------|-------------|------|--------|
| Tigre Sortudo | 1.740 | 1.744 | 0.2% | R$ 358.757 | R$ 360.780 | 0.6% | OK |
| Fortune Rabbit | 841 | 842 | 0.1% | R$ 312.373 | R$ 312.849 | 0.2% | OK |
| Gates of Olympus | 618 | 619 | 0.2% | R$ 2.322.918 | R$ 2.323.418 | 0.02% | OK |
| Sweet Bonanza | 383 | 385 | 0.5% | R$ 33.057 | R$ 33.169 | 0.3% | OK |
| Fortune Ox | 454 | 454 | 0.0% | R$ 85.377 | R$ 85.313 | 0.1% | OK |
| Combo FDS | 3.309 | 3.312 | 0.1% | R$ 1.216.976 | R$ 1.216.659 | 0.03% | OK |

**Threshold de aceitacao:** < 5% de divergencia.
**Resultado:** Todas as 6 promos ficaram abaixo de 1%. Divergencia residual explicada por:
- Diferenca de processamento de bets em transito (pipeline delay)
- Possivel diferenca na definicao de test users entre Athena e Smartico

## 9. Pontos que Preciso Validar com Voce

1. **Filtro de test users:** Estou usando `bireports_ec2.tbl_ecr.c_test_user = false`. Essa e a tabela correta para esse filtro, ou existe alternativa mais eficiente que evite o JOIN adicional?

2. **Custo do scan:** A Q1 (DURING+AFTER) cobre 12 dias de fund_ec2 (Mar 7-19). A Q2 (BEFORE) cobre 9 dias (Fev 7-16). Sem particao `dt`, o filtro de timestamp e suficiente para o Athena/Iceberg fazer file pruning? Ou ha uma forma melhor de limitar o scan?

3. **Rollbacks:** A demanda fala em "Gire R$X" (turnover de apostas). Estou usando SOMENTE `c_txn_type = 27` (bets), excluindo rollbacks (72) e wins (45). O correto para "turnover" e contar apenas bets, certo? Ou deveria subtrair rollbacks?

4. **Combo promo (3 jogos):** Estou somando turnover dos 3 jogos por user para classificar nas faixas. Se um user apostou R$300 no Ratinho + R$300 no Tigre, ele vai para faixa R$400-R$799 (total R$600). Confirma que essa e a leitura correta da demanda?

5. **Gates of Olympus — concentracao:** R$ 2.3M de turnover, com 98.6% concentrado em 40 users (faixa R$500+). Isso e um risco operacional que devo sinalizar para o CRM, ou e esperado dado o perfil do jogo?

---

**Proximos passos apos validacao:**
- Ajustar conforme feedback do arquiteto
- Entregar Excel final ao CRM
- Avaliar se vale produtizar como pipeline recorrente (proximo ciclo de promos)
