#!/usr/bin/env python3
"""
Análise Comparativa Segundas-Feiras — PARTE 2
Fallback bireports_ec2 para 23/03 + Horário + Casino + Sportsbook
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.athena import query_athena
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 250)
pd.set_option('display.float_format', '{:,.2f}'.format)
pd.set_option('display.max_rows', 100)

REPORT = []

def log(msg=""):
    print(msg)
    REPORT.append(str(msg))

def qry(sql, db="ps_bi", label=""):
    try:
        df = query_athena(sql, database=db)
        return df
    except Exception as e:
        log(f"  [ERRO {label}]: {e}")
        return None

DATAS_PS_BI = "DATE '2026-03-23', DATE '2026-03-16', DATE '2026-03-09'"
DATAS_DEP = "'2026-03-23', '2026-03-16', '2026-03-09'"

log("=" * 95)
log("PARTE 2: FALLBACK + HORARIO + CASINO + SPORTSBOOK")
log(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')} BRT")
log("=" * 95)

# ====================================================================
# A. FALLBACK: GGR de 23/03 via bireports_ec2
# (ps_bi nao tem gaming data de hoje)
# ====================================================================
log("\n" + "=" * 95)
log("SECAO A: GAMING DATA 23/03 via bireports_ec2.tbl_ecr_wise_daily_bi_summary")
log("(ps_bi nao processou casino/sports de hoje ainda)")
log("=" * 95)

# bireports_ec2 valores em centavos, dividir por 100
df_bi23 = qry("""
SELECT
    CAST(c_date AS VARCHAR) AS dt,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    SUM(c_casino_realcash_bet) / 100.0 AS casino_real_bets,
    SUM(c_casino_realcash_win) / 100.0 AS casino_real_wins,
    (SUM(c_casino_realcash_bet) - SUM(c_casino_realcash_win)) / 100.0 AS casino_ggr_real,
    SUM(c_casino_bonus_bet) / 100.0 AS casino_bonus_bets,
    SUM(c_casino_bonus_win) / 100.0 AS casino_bonus_wins,
    SUM(c_sb_realcash_bet) / 100.0 AS sb_real_bets,
    SUM(c_sb_realcash_win) / 100.0 AS sb_real_wins,
    (SUM(c_sb_realcash_bet) - SUM(c_sb_realcash_win)) / 100.0 AS sb_ggr_real,
    SUM(c_sb_bonus_bet) / 100.0 AS sb_bonus_bets,
    SUM(c_sb_bonus_win) / 100.0 AS sb_bonus_wins,
    -- GGR total = (casino_real + casino_bonus + sb_real + sb_bonus) bets - wins
    (SUM(c_casino_realcash_bet) + SUM(c_casino_bonus_bet)
     + SUM(c_sb_realcash_bet) + SUM(c_sb_bonus_bet)
     - SUM(c_casino_realcash_win) - SUM(c_casino_bonus_win)
     - SUM(c_sb_realcash_win) - SUM(c_sb_bonus_win)) / 100.0 AS ggr_total,
    -- Depositos e saques
    SUM(c_total_deposit_amount) / 100.0 AS depositos,
    SUM(c_total_deposit_count) AS dep_count,
    SUM(c_total_withdrawal_amount) / 100.0 AS saques,
    SUM(c_total_withdrawal_count) AS saq_count
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary b
JOIN bireports_ec2.tbl_ecr e ON b.c_ecr_id = e.c_ecr_id
WHERE CAST(c_date AS VARCHAR) IN ('2026-03-23', '2026-03-16', '2026-03-09')
  AND e.c_test_user = false
GROUP BY 1
ORDER BY 1 DESC
""", db="bireports_ec2", label="bireports fallback")

if df_bi23 is not None and len(df_bi23) > 0:
    for _, r in df_bi23.iterrows():
        dt = r['dt']
        casino_total_bets = r['casino_real_bets'] + r['casino_bonus_bets']
        casino_total_wins = r['casino_real_wins'] + r['casino_bonus_wins']
        casino_total_ggr = casino_total_bets - casino_total_wins
        casino_margin = (casino_total_ggr / casino_total_bets * 100) if casino_total_bets > 0 else 0

        sb_total_bets = r['sb_real_bets'] + r['sb_bonus_bets']
        sb_total_wins = r['sb_real_wins'] + r['sb_bonus_wins']
        sb_total_ggr = sb_total_bets - sb_total_wins
        sb_margin = (sb_total_ggr / sb_total_bets * 100) if sb_total_bets > 0 else 0

        net_dep = r['depositos'] - r['saques']

        parcial = "(PARCIAL)" if "03-23" in str(dt) else "(COMPLETO)"
        log(f"\n  {dt} {parcial}")
        log(f"  Fonte: bireports_ec2 (centavos/100, filtro test_user=false)")
        log(f"  ---")
        log(f"  Depositos:       R$ {r['depositos']:>14,.2f} | Qty: {int(r['dep_count']):,}")
        log(f"  Saques:          R$ {r['saques']:>14,.2f} | Qty: {int(r['saq_count']):,}")
        log(f"  Net Deposit:     R$ {net_dep:>14,.2f}")
        log(f"  ---")
        log(f"  Casino Bets (T): R$ {casino_total_bets:>14,.2f} | Wins: R$ {casino_total_wins:>14,.2f}")
        log(f"  Casino GGR (T):  R$ {casino_total_ggr:>14,.2f} ({casino_margin:.2f}%)")
        log(f"  Casino Real:     Bets R$ {r['casino_real_bets']:>12,.2f} | Wins R$ {r['casino_real_wins']:>12,.2f} | GGR R$ {r['casino_ggr_real']:>12,.2f}")
        log(f"  Casino Bonus:    Bets R$ {r['casino_bonus_bets']:>12,.2f} | Wins R$ {r['casino_bonus_wins']:>12,.2f}")
        log(f"  ---")
        log(f"  SB Bets (T):     R$ {sb_total_bets:>14,.2f} | Wins: R$ {sb_total_wins:>14,.2f}")
        log(f"  SB GGR (T):      R$ {sb_total_ggr:>14,.2f} ({sb_margin:.2f}%)")
        log(f"  SB Real:         Bets R$ {r['sb_real_bets']:>12,.2f} | Wins R$ {r['sb_real_wins']:>12,.2f} | GGR R$ {r['sb_ggr_real']:>12,.2f}")
        log(f"  ---")
        log(f"  GGR Total:       R$ {r['ggr_total']:>14,.2f}")

    # Tabela comparativa
    log(f"\n  COMPARATIVO BIREPORTS (com test_user filter):")
    log(f"  {'KPI':<25} | {'23/03 PARC':>16} | {'16/03':>16} | {'09/03':>16}")
    log(f"  {'-'*25}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}")
    rows_data = []
    for _, r in df_bi23.iterrows():
        rows_data.append(r)

    kpis = [
        ('depositos', 'Depositos R$'),
        ('dep_count', 'Depositos Qty'),
        ('saques', 'Saques R$'),
        ('saq_count', 'Saques Qty'),
        ('casino_ggr_real', 'Casino GGR Real'),
        ('sb_ggr_real', 'SB GGR Real'),
        ('ggr_total', 'GGR Total'),
    ]
    for col, name in kpis:
        vals = []
        for r in rows_data:
            v = r[col]
            if col.endswith('_count'):
                vals.append(f"{int(v):>16,}")
            else:
                vals.append(f"R$ {v:>13,.2f}")
        line = f"  {name:<25}"
        for v in vals:
            line += f" | {v}"
        log(line)

else:
    log("  [ERRO] Sem dados em bireports_ec2")

# ====================================================================
# B. DEPOSITOS POR HORA (fct_deposits_hourly)
# ====================================================================
log("\n" + "=" * 95)
log("SECAO B: DEPOSITOS POR HORA DO DIA (BRT)")
log("=" * 95)

df_hourly = qry(f"""
SELECT
    CAST(created_date AS VARCHAR) AS dt,
    created_hour AS hora_utc,
    CASE WHEN created_hour >= 3 THEN created_hour - 3 ELSE created_hour + 21 END AS hora_brt,
    SUM(success_amount_base) AS dep_brl,
    SUM(success_count) AS dep_count
FROM ps_bi.fct_deposits_hourly
WHERE CAST(created_date AS VARCHAR) IN ({DATAS_DEP})
GROUP BY 1, 2
ORDER BY 1 DESC, 2
""", label="Hourly Deposits")

if df_hourly is not None and len(df_hourly) > 0:
    pivot_amt = df_hourly.pivot_table(
        index='hora_brt', columns='dt',
        values='dep_brl', aggfunc='sum', fill_value=0
    ).sort_index()

    pivot_qty = df_hourly.pivot_table(
        index='hora_brt', columns='dt',
        values='dep_count', aggfunc='sum', fill_value=0
    ).sort_index()

    # Depositos por hora (R$)
    log(f"\n  Depositos por Hora BRT (R$):")
    header = f"  {'Hora':>6}"
    for col in sorted(pivot_amt.columns, reverse=True):
        label = "23/03P" if "03-23" in str(col) else str(col)[-5:]
        header += f" | {label:>12}"
    log(header)
    log(f"  {'-'*6}" + ("-+-" + "-"*12) * len(pivot_amt.columns))

    for hora in range(24):
        line = f"  {hora:>4}h "
        for col in sorted(pivot_amt.columns, reverse=True):
            v = pivot_amt.loc[hora, col] if hora in pivot_amt.index else 0
            line += f" | R${v:>9,.0f}"
        log(line)

    # Totais
    log(f"  {'-'*6}" + ("-+-" + "-"*12) * len(pivot_amt.columns))
    line = f"  TOTAL"
    for col in sorted(pivot_amt.columns, reverse=True):
        line += f" | R${pivot_amt[col].sum():>9,.0f}"
    log(line)

    # Quantidade
    log(f"\n  Quantidade Depositos por Hora BRT:")
    header = f"  {'Hora':>6}"
    for col in sorted(pivot_qty.columns, reverse=True):
        label = "23/03P" if "03-23" in str(col) else str(col)[-5:]
        header += f" | {label:>10}"
    log(header)
    log(f"  {'-'*6}" + ("-+-" + "-"*10) * len(pivot_qty.columns))

    for hora in range(24):
        line = f"  {hora:>4}h "
        for col in sorted(pivot_qty.columns, reverse=True):
            v = int(pivot_qty.loc[hora, col]) if hora in pivot_qty.index else 0
            line += f" | {v:>10,}"
        log(line)

else:
    log("  Sem dados horarios em ps_bi, tentando bireports_ec2...")

# ====================================================================
# C. CASINO POR CATEGORIA (16/03 e 09/03 via ps_bi)
# ====================================================================
log("\n" + "=" * 95)
log("SECAO C: GGR CASINO POR CATEGORIA (Slots/Live/etc)")
log("=" * 95)

df_cat = qry(f"""
SELECT
    c.activity_date,
    COALESCE(g.game_category_desc, 'SemCategoria') AS categoria,
    SUM(c.bet_amount_base) AS bets_total,
    SUM(c.win_amount_base) AS wins_total,
    SUM(c.bet_amount_base) - SUM(c.win_amount_base) AS ggr,
    SUM(c.real_bet_amount_base) - SUM(c.real_win_amount_base) AS ggr_real,
    SUM(c.bet_count) AS rodadas,
    COUNT(DISTINCT c.player_id) AS jogadores
FROM ps_bi.fct_casino_activity_daily c
LEFT JOIN ps_bi.dim_game g ON c.game_id = g.game_id
WHERE c.activity_date IN ({DATAS_PS_BI})
  AND c.product_id = 'casino'
GROUP BY c.activity_date, COALESCE(g.game_category_desc, 'SemCategoria')
ORDER BY c.activity_date DESC, ggr DESC
""", label="Casino Categories")

if df_cat is not None and len(df_cat) > 0:
    for dt in sorted(df_cat['activity_date'].unique(), reverse=True):
        subset = df_cat[df_cat['activity_date'] == dt].copy()
        total_ggr = subset['ggr'].sum()
        dt_str = str(dt)
        parcial = "(PARCIAL - pode estar vazio)" if "03-23" in dt_str else ""
        log(f"\n  {dt_str} {parcial}")
        log(f"  {'Categoria':<20} | {'GGR Total':>14} | {'GGR Real':>14} | {'Rodadas':>10} | {'Players':>8} | {'%GGR':>6}")
        log(f"  {'-'*20}-+-{'-'*14}-+-{'-'*14}-+-{'-'*10}-+-{'-'*8}-+-{'-'*6}")
        for _, r in subset.iterrows():
            pct_v = (r['ggr'] / total_ggr * 100) if total_ggr != 0 else 0
            log(f"  {r['categoria']:<20} | R$ {r['ggr']:>11,.2f} | R$ {r['ggr_real']:>11,.2f} | {int(r['rodadas']):>10,} | {int(r['jogadores']):>8,} | {pct_v:>5.1f}%")
        log(f"  {'TOTAL':<20} | R$ {total_ggr:>11,.2f} | R$ {subset['ggr_real'].sum():>11,.2f} | {int(subset['rodadas'].sum()):>10,} | {int(subset['jogadores'].sum()):>8,} |")

# ====================================================================
# D. TOP 15 JOGOS POR GGR (16/03 e 09/03 + 23/03 se tiver)
# ====================================================================
log("\n" + "=" * 95)
log("SECAO D: TOP 15 JOGOS POR GGR")
log("=" * 95)

df_games = qry(f"""
SELECT
    c.activity_date,
    c.game_id,
    g.game_desc,
    g.vendor_id,
    g.game_category_desc AS categoria,
    SUM(c.bet_amount_base) AS bets,
    SUM(c.win_amount_base) AS wins,
    SUM(c.bet_amount_base) - SUM(c.win_amount_base) AS ggr,
    SUM(c.bet_count) AS rodadas,
    COUNT(DISTINCT c.player_id) AS jogadores,
    CASE WHEN SUM(c.bet_amount_base) > 0
         THEN (SUM(c.bet_amount_base) - SUM(c.win_amount_base)) / SUM(c.bet_amount_base) * 100
         ELSE 0 END AS hold_pct
FROM ps_bi.fct_casino_activity_daily c
LEFT JOIN ps_bi.dim_game g ON c.game_id = g.game_id
WHERE c.activity_date IN ({DATAS_PS_BI})
  AND c.product_id = 'casino'
GROUP BY c.activity_date, c.game_id, g.game_desc, g.vendor_id, g.game_category_desc
HAVING SUM(c.bet_amount_base) > 0
ORDER BY c.activity_date DESC, ggr DESC
""", label="Top Games")

if df_games is not None and len(df_games) > 0:
    for dt in sorted(df_games['activity_date'].unique(), reverse=True):
        subset = df_games[df_games['activity_date'] == dt].head(15)
        dt_str = str(dt)
        parcial = "(PARCIAL)" if "03-23" in dt_str else ""
        log(f"\n  {dt_str} {parcial}")
        log(f"  {'#':>3} | {'Jogo':<32} | {'Vendor':<14} | {'GGR':>12} | {'Bets':>12} | {'Hold%':>6} | {'Plrs':>6} | {'Rounds':>9}")
        log(f"  {'-'*3}-+-{'-'*32}-+-{'-'*14}-+-{'-'*12}-+-{'-'*12}-+-{'-'*6}-+-{'-'*6}-+-{'-'*9}")
        for i, (_, r) in enumerate(subset.iterrows(), 1):
            name = str(r['game_desc'])[:30] if r['game_desc'] else 'N/A'
            vendor = str(r['vendor_id'])[:12] if r['vendor_id'] else 'N/A'
            log(f"  {i:>3} | {name:<32} | {vendor:<14} | R${r['ggr']:>9,.0f} | R${r['bets']:>9,.0f} | {r['hold_pct']:>5.1f}% | {int(r['jogadores']):>6,} | {int(r['rodadas']):>9,}")

    # Anomalias de ranking
    dates = sorted(df_games['activity_date'].unique(), reverse=True)
    # Apenas para datas com dados (excluir 23/03 se vazio)
    dates_with_data = [d for d in dates if len(df_games[df_games['activity_date'] == d]) > 0]

    if len(dates_with_data) >= 2:
        log(f"\n  ANOMALIAS DE RANKING:")
        top_sets = {}
        for d in dates_with_data:
            top_sets[str(d)] = set(df_games[df_games['activity_date'] == d].head(15)['game_id'].tolist())

        for i, d1 in enumerate(dates_with_data):
            for d2 in dates_with_data[i+1:]:
                only_d1 = top_sets[str(d1)] - top_sets[str(d2)]
                only_d2 = top_sets[str(d2)] - top_sets[str(d1)]
                if only_d1:
                    log(f"\n  No Top15 de {d1} mas NAO em {d2}:")
                    for gid in only_d1:
                        info = df_games[(df_games['game_id'] == gid) & (df_games['activity_date'] == d1)].iloc[0]
                        log(f"    - {info['game_desc']} (GGR: R$ {info['ggr']:,.0f}, Hold: {info['hold_pct']:.1f}%)")

# ====================================================================
# E. SPORTSBOOK POR ESPORTE (vendor_ec2)
# ====================================================================
log("\n" + "=" * 95)
log("SECAO E: SPORTSBOOK POR ESPORTE (vendor_ec2)")
log("=" * 95)

utc_ranges = [
    ('2026-03-23', "TIMESTAMP '2026-03-23 03:00:00'", "TIMESTAMP '2026-03-24 03:00:00'"),
    ('2026-03-16', "TIMESTAMP '2026-03-16 03:00:00'", "TIMESTAMP '2026-03-17 03:00:00'"),
    ('2026-03-09', "TIMESTAMP '2026-03-09 03:00:00'", "TIMESTAMP '2026-03-10 03:00:00'"),
]

sb_parts = []
for dt, utc_start, utc_end in utc_ranges:
    sb_parts.append(f"""
    SELECT
        '{dt}' AS segunda,
        d.c_sport_type_name AS esporte,
        b.c_bet_type AS tipo_aposta,
        CASE WHEN b.c_is_live = true THEN 'Live' ELSE 'PreLive' END AS live_flag,
        COUNT(DISTINCT b.c_bet_slip_id) AS bilhetes,
        COUNT(DISTINCT b.c_customer_id) AS apostadores,
        SUM(b.c_total_stake) AS stake_total,
        SUM(CASE WHEN b.c_bet_state = 'C' THEN COALESCE(b.c_total_return, 0) ELSE 0 END) AS payout_closed,
        SUM(CASE WHEN b.c_bet_state = 'C' THEN b.c_total_stake ELSE 0 END) AS stake_closed,
        SUM(CASE WHEN b.c_bet_state = 'O' THEN b.c_total_stake ELSE 0 END) AS stake_open,
        COUNT(DISTINCT CASE WHEN b.c_bet_state = 'O' THEN b.c_bet_slip_id END) AS bilhetes_abertos
    FROM vendor_ec2.tbl_sports_book_bets_info b
    LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
        ON b.c_bet_slip_id = d.c_bet_slip_id
        AND b.c_transaction_id = d.c_transaction_id
    WHERE b.c_created_time >= {utc_start}
      AND b.c_created_time < {utc_end}
      AND b.c_transaction_type = 'M'
    GROUP BY 1, 2, 3, 4
    """)

df_sb = qry(" UNION ALL ".join(sb_parts), db="vendor_ec2", label="SB by Sport")

if df_sb is not None and len(df_sb) > 0:
    # Por esporte agregado
    for dt in sorted(df_sb['segunda'].unique(), reverse=True):
        subset = df_sb[df_sb['segunda'] == dt]
        agg = subset.groupby('esporte').agg({
            'bilhetes': 'sum', 'apostadores': 'sum',
            'stake_total': 'sum', 'payout_closed': 'sum',
            'stake_closed': 'sum', 'stake_open': 'sum',
            'bilhetes_abertos': 'sum'
        }).sort_values('stake_total', ascending=False)

        total_stake = agg['stake_total'].sum()
        total_ggr = (agg['stake_closed'] - agg['payout_closed']).sum()
        parcial = "(PARCIAL)" if "03-23" in dt else ""

        log(f"\n  {dt} {parcial}")
        log(f"  {'Esporte':<22} | {'Stake':>11} | {'GGR Settl':>11} | {'Hold%':>6} | {'Bilh':>7} | {'Plrs':>6} | {'Open':>11}")
        log(f"  {'-'*22}-+-{'-'*11}-+-{'-'*11}-+-{'-'*6}-+-{'-'*7}-+-{'-'*6}-+-{'-'*11}")
        for esporte, r in agg.head(12).iterrows():
            ggr = r['stake_closed'] - r['payout_closed']
            hold = (ggr / r['stake_closed'] * 100) if r['stake_closed'] > 0 else 0
            esp_str = str(esporte)[:22] if esporte else 'N/A'
            log(f"  {esp_str:<22} | R${r['stake_total']:>8,.0f} | R${ggr:>8,.0f} | {hold:>5.1f}% | {int(r['bilhetes']):>7,} | {int(r['apostadores']):>6,} | R${r['stake_open']:>8,.0f}")

        overall_hold = (total_ggr / agg['stake_closed'].sum() * 100) if agg['stake_closed'].sum() > 0 else 0
        log(f"  {'TOTAL':<22} | R${total_stake:>8,.0f} | R${total_ggr:>8,.0f} | {overall_hold:>5.1f}% |")

    # Live vs PreLive
    log(f"\n  LIVE vs PRE-LIVE:")
    agg_live = df_sb.groupby(['segunda', 'live_flag']).agg({
        'stake_total': 'sum', 'stake_closed': 'sum', 'payout_closed': 'sum', 'bilhetes': 'sum'
    }).reset_index()
    agg_live['ggr'] = agg_live['stake_closed'] - agg_live['payout_closed']
    agg_live['hold'] = agg_live.apply(lambda r: (r['ggr'] / r['stake_closed'] * 100) if r['stake_closed'] > 0 else 0, axis=1)
    agg_live = agg_live.sort_values(['segunda', 'live_flag'], ascending=[False, True])

    log(f"  {'Data':<12} | {'Tipo':<8} | {'Stake':>12} | {'GGR':>12} | {'Hold%':>6} | {'Bilhetes':>9}")
    log(f"  {'-'*12}-+-{'-'*8}-+-{'-'*12}-+-{'-'*12}-+-{'-'*6}-+-{'-'*9}")
    for _, r in agg_live.iterrows():
        log(f"  {r['segunda']:<12} | {r['live_flag']:<8} | R${r['stake_total']:>9,.0f} | R${r['ggr']:>9,.0f} | {r['hold']:>5.1f}% | {int(r['bilhetes']):>9,}")

    # Esportes que mudaram significativamente
    log(f"\n  VARIACAO POR ESPORTE (23/03 vs media 16+09):")
    dates_all = sorted(df_sb['segunda'].unique(), reverse=True)
    if len(dates_all) >= 2:
        curr_dt = dates_all[0]
        prev_dts = dates_all[1:]

        curr_agg = df_sb[df_sb['segunda'] == curr_dt].groupby('esporte').agg({
            'stake_total': 'sum', 'stake_closed': 'sum', 'payout_closed': 'sum'
        })
        curr_agg['ggr'] = curr_agg['stake_closed'] - curr_agg['payout_closed']

        prev_agg = df_sb[df_sb['segunda'].isin(prev_dts)].groupby('esporte').agg({
            'stake_total': 'sum', 'stake_closed': 'sum', 'payout_closed': 'sum'
        })
        prev_agg['ggr'] = prev_agg['stake_closed'] - prev_agg['payout_closed']
        prev_agg = prev_agg / len(prev_dts)  # media por segunda

        merged = curr_agg.join(prev_agg, lsuffix='_now', rsuffix='_avg')
        merged['stake_var'] = merged.apply(
            lambda r: ((r['stake_total_now'] / r['stake_total_avg']) - 1) * 100
            if r['stake_total_avg'] > 100 else 0, axis=1
        )
        merged['ggr_diff'] = merged['ggr_now'] - merged['ggr_avg']
        merged = merged.dropna().sort_values('ggr_diff')

        log(f"  {'Esporte':<22} | {'Stake 23/03':>11} | {'Stake Media':>11} | {'Var%':>7} | {'GGR 23/03':>11} | {'GGR Media':>11} | {'Diff':>11}")
        log(f"  {'-'*22}-+-{'-'*11}-+-{'-'*11}-+-{'-'*7}-+-{'-'*11}-+-{'-'*11}-+-{'-'*11}")
        for esporte, r in merged.iterrows():
            if abs(r['stake_total_avg']) > 100:
                esp_str = str(esporte)[:22]
                log(f"  {esp_str:<22} | R${r['stake_total_now']:>8,.0f} | R${r['stake_total_avg']:>8,.0f} | {r['stake_var']:>+6.0f}% | R${r['ggr_now']:>8,.0f} | R${r['ggr_avg']:>8,.0f} | R${r['ggr_diff']:>8,.0f}")

else:
    log("  [ERRO] Sem dados sportsbook")

# ====================================================================
# SALVAR
# ====================================================================
report_path = os.path.join(os.path.dirname(__file__), '..', 'reports', 'analise_segundas_parte2.txt')
os.makedirs(os.path.dirname(report_path), exist_ok=True)
with open(report_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(REPORT))
log(f"\nReport salvo em: {report_path}")
