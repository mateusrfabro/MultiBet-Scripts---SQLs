# Catalogo de Metricas — MultiBet

Referencia canonica: **metrica -> tabela -> campo -> exemplo validado**.

> Documento vivo. Quando uma metrica precisar de fonte alternativa (ex: gap dbt
> em 24/04/2026), atualizar com a nova fonte canonica + data da decisao.

## Como ler

Cada metrica tem:
- **Definicao de negocio**
- **Fonte canonica** (atualizada)
- **Exemplo SQL validado** (com data de validacao)
- **Cuidados/pegadinhas** conhecidos
- **Quem valida** (dono no time)

---

## REG (New Registered Customer)

**Definicao:** novos cadastros no periodo (signup_datetime).

**Fonte canonica:** `ps_bi.dim_user` (filtro is_test) ou `ecr_ec2.tbl_ecr` (raw, intraday).

**Exemplo SQL — D-1 com afiliado:**
```sql
SELECT COUNT(*) AS reg
FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) IN ('464673')
  AND (is_test = false OR is_test IS NULL)
  AND CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-04-23'
```

**Cuidados:**
- `affiliate_id` e VARCHAR no ps_bi (CAST obrigatorio)
- `signup_datetime` em UTC -> converter BRT
- Cross-check com `ecr_ec2.tbl_ecr.c_signup_time` pode divergir ate ~15% (raw nao filtra test users)

**Validado em:** 25/04/2026 (CLI `affiliate-daily`)
**Dono:** Mateus / Mauro

---

## FTD (First Time Deposit)

**Definicao:** primeiro deposito confirmado de um player.

**Fonte canonica:** `ps_bi.dim_user.has_ftd` / `ftd_date` / `ftd_amount_inhouse`

**Exemplo SQL — FTDs no dia, por afiliado:**
```sql
SELECT COUNT_IF(CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-04-23') AS ftd,
       SUM(CASE WHEN CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-04-23'
                THEN ftd_amount_inhouse ELSE 0 END) AS ftd_deposit_brl
FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) IN ('464673')
  AND (is_test = false OR is_test IS NULL)
```

**Cuidados:**
- `ftd_amount_inhouse` ja em BRL (nao centavos)
- Para cohort "sem FTD" — anti-join `cashier_ec2` (delay dbt vaza FTDs recentes)
- Fonte intraday alternativa: `cashier_ec2.tbl_cashier_deposit` com status `txn_confirmed_success`

**Validado em:** 25/04/2026
**Dono:** Mateus

---

## GGR (Gross Gaming Revenue)

**Definicao:** Receita bruta da casa = apostas - ganhos do jogador (realcash).

**Fonte canonica:** `bireports_ec2.tbl_ecr_wise_daily_bi_summary` (gap-resistant) ou `ps_bi.fct_player_activity_daily` (BRL ja consolidado **mas com gap em 24/04**).

**Exemplo SQL — GGR Cassino + Sport por afiliado, dia:**
```sql
WITH base_players AS (
    SELECT DISTINCT ecr_id FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ('464673')
      AND (is_test = false OR is_test IS NULL)
)
SELECT
    SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount) / 100.0 AS ggr_casino_brl,
    SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount) / 100.0          AS ggr_sport_brl
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
INNER JOIN base_players p ON s.c_ecr_id = p.ecr_id
WHERE s.c_created_date = DATE '2026-04-23'
```

**Cuidados:**
- bireports_ec2 e em CENTAVOS — sempre `/100.0`
- Use `realcash` (sub-fund isolation), nao `bonus`
- ps_bi.fct_player_activity_daily esta com **gap desde 06/04/2026** — use bireports

**Validado em:** 25/04/2026
**Dono:** Mateus / Mauro

---

## NGR (Net Gaming Revenue)

**Definicao:** GGR - BTR (Bonus Turned Real) - RCA (Real Cancelado). Formula canonica.

**Proxy operacional (mais comum):** `NGR = GGR - bonus_issued`

**Fonte canonica:** `ps_bi.fct_player_activity_daily.ngr_base` (em BRL) — **mas com gap em 24/04**.

**Alternativa enquanto gap:** calcular manualmente via `bireports_ec2`:
```sql
SELECT
    SUM((c_casino_realcash_bet_amount - c_casino_realcash_win_amount) +
        (c_sb_realcash_bet_amount - c_sb_realcash_win_amount) -
        c_bonus_issued_amount) / 100.0 AS ngr_proxy_brl
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
WHERE c_created_date = DATE '2026-04-23'
  AND c_ecr_id IN (...)  -- base_players
```

**Cuidados:**
- BTR (Bonus Turned Real) **nao e** bonus_issued. BTR = bonus que virou cash real (sub-fund txn type 20)
- Para NGR canonica: precisa ler `tbl_realcash_sub_fund_txn` (feedback_btr_valor_na_subfund.md)

**Validado em:** 25/04/2026 (proxy)
**Dono:** Mauro

---

## BTR (Bonus Turned Real)

**Definicao:** Bonus que foi convertido em saldo real do jogador (wagering completo).

**Fonte canonica:** `fund_ec2.tbl_realcash_sub_fund_txn` (txn type 20).

**Cuidados criticos:**
- `tbl_real_fund_txn` (sem sub_) — campo `c_amount_in_ecr_ccy` SEMPRE 0 para type 20
- Valor real esta em `tbl_realcash_sub_fund_txn` — feedback_btr_valor_na_subfund.md (validado pelo Mauro 19/04)
- Sub-fund tem CDC duplicate: dedup `ROW_NUMBER OVER (PARTITION BY c_txn_id ORDER BY cdc_timestamp DESC)`

**Tem implementacao oficial em:** `GL-Analytics-M-L/sync_all_aquisicao/btr.sql`

**Validado em:** 19/04/2026
**Dono:** Mauro

---

## Bases de cohort comuns

| Base | Fonte canonica | Filtro core |
|---|---|---|
| Players de um afiliado | `ps_bi.dim_user` | `CAST(affiliate_id AS VARCHAR) = '...'` + is_test |
| Players de um tracker | `ps_bi.dim_user` | `tracker_id = '...'` + is_test |
| Players com FTD em janela | `ps_bi.dim_user` | `ftd_date BETWEEN ... AND ...` |
| Players SEM FTD | `ps_bi.dim_user` | `has_ftd = false` + anti-join cashier (validar delay) |
| Atividade granular jogo | `ps_bi.fct_casino_activity_daily` | `activity_date = ...` |
| Atividade SB granular | `vendor_ec2.tbl_sports_book_bet_details` | `c_ts_realstart BETWEEN ...` (**ja em BRT, NAO converter**) |

---

## Pegadinhas conhecidas (consultar antes de usar)

1. **`ts_realend`/`ts_realstart` (sportsbook) JA esta em BRT.** NAO aplicar conversao UTC->BRT (validado com ESPN 14/04/2026)
2. **`affiliate_id` em ps_bi e VARCHAR.** Sempre CAST, comparar com string
3. **`fct_player_activity_daily` esta em GAP** (parou em 06/04/2026). Usar bireports_ec2 enquanto nao resolver
4. **DIM dbt (dim_user) pode estar OK enquanto FATO dbt (fct_*) esta parada.** Validar separadamente
5. **`vendor_ec2.tbl_sports_book_bet_details` tem CDC 2x.** Dedup por `c_bet_slip_id` + ROW_NUMBER

---

## Roadmap deste catalogo

- [x] REG, FTD, GGR, NGR, BTR (versao 0)
- [ ] Hold Rate (Casino, Sportsbook)
- [ ] LTV / ARPU
- [ ] NRC (New Registered Customer ativos)
- [ ] Net Deposit / P&L
- [ ] Sportsbook: Win/Loss por faixa odds, Cashout vs Settlement
- [ ] Casino: top jogos, RTP observado vs contratual

**Para adicionar metrica:** escrever no formato acima (definicao -> fonte -> SQL -> cuidados -> dono),
e validar empiricamente antes de marcar como canonica.
