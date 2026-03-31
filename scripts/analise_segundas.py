#!/usr/bin/env python3
"""
Análise Comparativa Segundas-Feiras — MultiBet
23/03/2026 (PARCIAL) vs 16/03/2026 vs 09/03/2026

Squad: extractor + product-analyst + auditor
Fonte: ps_bi (dbt BI mart, valores em BRL) + vendor_ec2 (sportsbook, valores em BRL)
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
DATAS_PS_BI = "DATE '2026-03-23', DATE '2026-03-16', DATE '2026-03-09'"
DATAS_DEP = "'2026-03-23', '2026-03-16', '2026-03-09'"

def log(msg=""):
    print(msg)
    REPORT.append(str(msg))

def qry(sql, db="ps_bi", label=""):
    try:
        df = query_athena(sql, database=db)
        return df
    except Exception as e:
        log(f"  ⛔ ERRO [{label}]: {e}")
        return None

def fmt(v, prefix="R$ "):
    if pd.isna(v): return "N/A"
    return f"{prefix}{v:,.2f}" if isinstance(v, (int, float)) else str(v)

def pct(part, total):
    if total == 0 or pd.isna(total): return "N/A"
    return f"{(part / total * 100):,.2f}%"

# ====================================================================
log("=" * 95)
log("📊 ANÁLISE COMPARATIVA SEGUNDAS-FEIRAS — MultiBet")
log(f"📅 Datas: 23/03 (PARCIAL ~50%), 16/03 (completo), 09/03 (completo)")
log(f"🕐 Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')} BRT")
log(f"📌 Fonte: ps_bi (dbt BI mart) | vendor_ec2 (sportsbook)")
log(f"⚠️  23/03 é o dia corrente — dados PARCIAIS. Comparar PROPORÇÕES, não absolutos.")
log("=" * 95)

# ====================================================================
# 1. KPIs OVERVIEW (fct_player_activity_daily — tudo em um)
# ====================================================================
log("\n" + "=" * 95)
log("📊 SEÇÃO 1: KPIs OVERVIEW POR SEGUNDA-FEIRA")
log("=" * 95)

df_kpis = qry(f"""
SELECT
    activity_date,
    COUNT(DISTINCT player_id) AS jogadores_ativos,
    SUM(login_count) AS logins,
    SUM(nrc_count) AS registros_nrc,
    SUM(ftd_count) AS ftds,
    -- Depósitos
    SUM(deposit_success_count) AS dep_count,
    SUM(deposit_success_base) AS dep_brl,
    -- Saques
    SUM(cashout_success_count) AS saq_count,
    SUM(cashout_success_base) AS saq_brl,
    -- Net Deposit
    SUM(deposit_success_base) - SUM(cashout_success_base) AS net_deposit,
    -- Casino Total (real + bonus)
    SUM(casino_bet_amount_base) AS casino_bets_total,
    SUM(casino_win_amount_base) AS casino_wins_total,
    -- Casino Realcash
    SUM(casino_realbet_base) AS casino_real_bets,
    SUM(casino_real_win_base) AS casino_real_wins,
    -- Casino Bonus
    SUM(casino_bonusbet_base) AS casino_bonus_bets,
    SUM(casino_bonus_win_base) AS casino_bonus_wins,
    -- Sportsbook Total
    SUM(sb_bet_amount_base) AS sb_bets_total,
    SUM(sb_win_amount_base) AS sb_wins_total,
    -- Sportsbook Real
    SUM(sb_realbet_base) AS sb_real_bets,
    SUM(sb_real_win_base) AS sb_real_wins,
    -- Sportsbook Bonus
    SUM(sb_bonusbet_base) AS sb_bonus_bets,
    SUM(sb_bonus_win_base) AS sb_bonus_wins,
    -- GGR / NGR
    SUM(ggr_base) AS ggr_total,
    SUM(ngr_base) AS ngr_total,
    -- Jackpots
    SUM(jackpot_win_amount_base) AS jackpot_wins,
    SUM(jackpot_contribution_base) AS jackpot_contrib,
    -- Bonus
    SUM(bonus_issued_base) AS bonus_issued,
    SUM(bonus_granted_base) AS bonus_granted,
    SUM(bonus_turnedreal_base) AS bonus_turned_real
FROM ps_bi.fct_player_activity_daily
WHERE activity_date IN ({DATAS_PS_BI})
GROUP BY activity_date
ORDER BY activity_date DESC
""", label="KPIs Overview")

if df_kpis is not None and len(df_kpis) > 0:
    for _, r in df_kpis.iterrows():
        dt = str(r['activity_date'])
        casino_ggr = r['casino_bets_total'] - r['casino_wins_total']
        sb_ggr = r['sb_bets_total'] - r['sb_wins_total']
        casino_margin = (casino_ggr / r['casino_bets_total'] * 100) if r['casino_bets_total'] > 0 else 0
        sb_margin = (sb_ggr / r['sb_bets_total'] * 100) if r['sb_bets_total'] > 0 else 0

        log(f"\n  {'='*70}")
        log(f"  📅 {dt} {'(⚠️ PARCIAL)' if '03-23' in dt else '(COMPLETO)'}")
        log(f"  {'='*70}")
        log(f"  👥 Jogadores Ativos: {int(r['jogadores_ativos']):,}")
        log(f"  🔑 Logins:          {int(r['logins']):,}")
        log(f"  📝 Registros (NRC): {int(r['registros_nrc']):,}")
        log(f"  🆕 FTDs:            {int(r['ftds']):,}")
        log(f"  ")
        log(f"  💰 Depósitos:       {fmt(r['dep_brl'])} | Qty: {int(r['dep_count']):,}")
        log(f"  💸 Saques:          {fmt(r['saq_brl'])} | Qty: {int(r['saq_count']):,}")
        log(f"  📈 Net Deposit:     {fmt(r['net_deposit'])}")
        log(f"  ")
        log(f"  🎰 Casino Bets (Total): {fmt(r['casino_bets_total'])} | Wins: {fmt(r['casino_wins_total'])}")
        log(f"     Casino GGR (Total):  {fmt(casino_ggr)} ({casino_margin:.2f}%)")
        log(f"     Casino Real Bets:    {fmt(r['casino_real_bets'])} | Real Wins: {fmt(r['casino_real_wins'])}")
        log(f"     Casino Bonus Bets:   {fmt(r['casino_bonus_bets'])} | Bonus Wins: {fmt(r['casino_bonus_wins'])}")
        log(f"  ")
        log(f"  ⚽ SB Bets (Total):     {fmt(r['sb_bets_total'])} | Wins: {fmt(r['sb_wins_total'])}")
        log(f"     SB GGR (Total):      {fmt(sb_ggr)} ({sb_margin:.2f}%)")
        log(f"     SB Real Bets:        {fmt(r['sb_real_bets'])} | Real Wins: {fmt(r['sb_real_wins'])}")
        log(f"     SB Bonus Bets:       {fmt(r['sb_bonus_bets'])} | Bonus Wins: {fmt(r['sb_bonus_wins'])}")
        log(f"  ")
        log(f"  📊 GGR Total (ps_bi):   {fmt(r['ggr_total'])}")
        log(f"  📊 NGR Total:           {fmt(r['ngr_total'])}")
        log(f"  🎯 Jackpot Wins:        {fmt(r['jackpot_wins'])}")
        log(f"  🎁 Bonus Issued:        {fmt(r['bonus_issued'])}")

    # Tabela comparativa resumida
    log(f"\n  {'─'*95}")
    log(f"  📋 RESUMO COMPARATIVO:")
    log(f"  {'─'*95}")
    header = f"  {'KPI':<25} | {'23/03 (PARCIAL)':>20} | {'16/03':>20} | {'09/03':>20}"
    log(header)
    log(f"  {'-'*25}-+-{'-'*20}-+-{'-'*20}-+-{'-'*20}")

    kpis_names = [
        ('dep_brl', 'Depósitos (R$)'),
        ('dep_count', 'Depósitos (Qty)'),
        ('ftds', 'FTDs'),
        ('saq_brl', 'Saques (R$)'),
        ('net_deposit', 'Net Deposit (R$)'),
        ('ggr_total', 'GGR Total (R$)'),
        ('ngr_total', 'NGR Total (R$)'),
        ('logins', 'Logins'),
        ('registros_nrc', 'Registros (NRC)'),
        ('jogadores_ativos', 'Jogadores Ativos'),
    ]

    for col, name in kpis_names:
        vals = []
        for _, r in df_kpis.iterrows():
            v = r[col]
            if isinstance(v, float) and col in ['dep_brl', 'saq_brl', 'net_deposit', 'ggr_total', 'ngr_total']:
                vals.append(f"R$ {v:>14,.2f}")
            else:
                vals.append(f"{int(v):>20,}")
        log(f"  {name:<25} | {vals[0]:>20} | {vals[1]:>20} | {vals[2]:>20}")

else:
    log("  ⛔ Sem dados em fct_player_activity_daily")

# ====================================================================
# 2. DEPÓSITOS POR HORA (fct_deposits_hourly)
# ====================================================================
log("\n" + "=" * 95)
log("⏰ SEÇÃO 2: DEPÓSITOS POR HORA DO DIA")
log("   Nota: created_hour é UTC. BRT = UTC - 3h")
log("=" * 95)

df_hourly = qry(f"""
SELECT
    CAST(created_date AS VARCHAR) AS dt,
    created_hour AS hora_utc,
    -- Ajuste BRT: hora_utc - 3 (com wrap)
    CASE WHEN created_hour >= 3 THEN created_hour - 3 ELSE created_hour + 21 END AS hora_brt,
    SUM(success_amount_base) AS dep_brl,
    SUM(success_count) AS dep_count
FROM ps_bi.fct_deposits_hourly
WHERE CAST(created_date AS VARCHAR) IN ({DATAS_DEP})
GROUP BY 1, 2
ORDER BY 1 DESC, 2
""", label="Hourly Deposits")

if df_hourly is not None and len(df_hourly) > 0:
    # Pivot: hora BRT vs data
    pivot_amt = df_hourly.pivot_table(
        index='hora_brt', columns='dt',
        values='dep_brl', aggfunc='sum', fill_value=0
    ).sort_index()
    pivot_qty = df_hourly.pivot_table(
        index='hora_brt', columns='dt',
        values='dep_count', aggfunc='sum', fill_value=0
    ).sort_index()

    log(f"\n  💰 Depósitos por Hora BRT (R$):")
    log(f"  {'Hora BRT':>8} | ", end="")
    for col in pivot_amt.columns:
        log(f"{'23/03 PARC' if '03-23' in str(col) else str(col)[-5:]:>14} | ", end="")
    log()
    log(f"  {'-'*8}-+-{('-'*14 + '-+-') * len(pivot_amt.columns)}")

    for hora in range(24):
        if hora in pivot_amt.index:
            log(f"  {hora:>6}h  | ", end="")
            for col in pivot_amt.columns:
                v = pivot_amt.loc[hora, col] if hora in pivot_amt.index else 0
                bar = "█" * min(int(v / 3000), 20)
                log(f"{v:>12,.0f} | ", end="")
            log()
        else:
            log(f"  {hora:>6}h  | ", end="")
            for col in pivot_amt.columns:
                log(f"{'—':>12} | ", end="")
            log()

    # Totais
    log(f"  {'─'*8}-+-{('─'*14 + '-+-') * len(pivot_amt.columns)}")
    log(f"  {'TOTAL':>8} | ", end="")
    for col in pivot_amt.columns:
        log(f"{pivot_amt[col].sum():>12,.0f} | ", end="")
    log()

    # Contagem
    log(f"\n  📊 Quantidade de Depósitos por Hora BRT:")
    log(f"  {'Hora BRT':>8} | ", end="")
    for col in pivot_qty.columns:
        log(f"{'23/03 PARC' if '03-23' in str(col) else str(col)[-5:]:>14} | ", end="")
    log()
    for hora in range(24):
        if hora in pivot_qty.index:
            log(f"  {hora:>6}h  | ", end="")
            for col in pivot_qty.columns:
                v = pivot_qty.loc[hora, col] if hora in pivot_qty.index else 0
                log(f"{int(v):>12,} | ", end="")
            log()
else:
    log("  ⛔ Sem dados horários")

# ====================================================================
# 3. CASINO POR CATEGORIA DE JOGO
# ====================================================================
log("\n" + "=" * 95)
log("🎰 SEÇÃO 3: GGR CASINO POR CATEGORIA DE JOGO")
log("=" * 95)

df_cat = qry(f"""
SELECT
    c.activity_date,
    COALESCE(g.game_category_desc, 'Sem Categoria') AS categoria,
    SUM(c.bet_amount_base) AS bets_total,
    SUM(c.win_amount_base) AS wins_total,
    SUM(c.bet_amount_base) - SUM(c.win_amount_base) AS ggr,
    SUM(c.real_bet_amount_base) AS real_bets,
    SUM(c.real_win_amount_base) AS real_wins,
    SUM(c.real_bet_amount_base) - SUM(c.real_win_amount_base) AS ggr_real,
    SUM(c.bet_count) AS rodadas,
    COUNT(DISTINCT c.player_id) AS jogadores
FROM ps_bi.fct_casino_activity_daily c
LEFT JOIN ps_bi.dim_game g ON c.game_id = g.game_id
WHERE c.activity_date IN ({DATAS_PS_BI})
  AND c.product_id = 'casino'
GROUP BY c.activity_date, COALESCE(g.game_category_desc, 'Sem Categoria')
ORDER BY c.activity_date DESC, ggr DESC
""", label="Casino Categories")

if df_cat is not None and len(df_cat) > 0:
    for dt in df_cat['activity_date'].unique():
        subset = df_cat[df_cat['activity_date'] == dt].copy()
        total_ggr = subset['ggr'].sum()
        dt_str = str(dt)
        log(f"\n  📅 {dt_str} {'(PARCIAL)' if '03-23' in dt_str else ''}")
        log(f"  {'Categoria':<20} | {'GGR Total':>14} | {'GGR Real':>14} | {'Bets':>14} | {'Rodadas':>10} | {'Players':>8} | {'% GGR':>6}")
        log(f"  {'-'*20}-+-{'-'*14}-+-{'-'*14}-+-{'-'*14}-+-{'-'*10}-+-{'-'*8}-+-{'-'*6}")
        for _, r in subset.iterrows():
            pct_v = (r['ggr'] / total_ggr * 100) if total_ggr != 0 else 0
            log(f"  {r['categoria']:<20} | R$ {r['ggr']:>11,.2f} | R$ {r['ggr_real']:>11,.2f} | R$ {r['bets_total']:>11,.2f} | {int(r['rodadas']):>10,} | {int(r['jogadores']):>8,} | {pct_v:>5.1f}%")
        log(f"  {'TOTAL':<20} | R$ {total_ggr:>11,.2f} | R$ {subset['ggr_real'].sum():>11,.2f} | R$ {subset['bets_total'].sum():>11,.2f} | {int(subset['rodadas'].sum()):>10,} | {int(subset['jogadores'].sum()):>8,} |")

# ====================================================================
# 4. TOP 15 GAMES POR GGR
# ====================================================================
log("\n" + "=" * 95)
log("🏆 SEÇÃO 4: TOP 15 JOGOS POR GGR (CADA SEGUNDA)")
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
ORDER BY c.activity_date DESC, ggr DESC
""", label="Top Games")

if df_games is not None and len(df_games) > 0:
    for dt in df_games['activity_date'].unique():
        subset = df_games[df_games['activity_date'] == dt].head(15)
        dt_str = str(dt)
        log(f"\n  📅 {dt_str} {'(PARCIAL)' if '03-23' in dt_str else ''}")
        log(f"  {'#':>3} | {'Jogo':<35} | {'Vendor':<15} | {'GGR':>14} | {'Bets':>14} | {'Hold%':>6} | {'Players':>8} | {'Rodadas':>10}")
        log(f"  {'-'*3}-+-{'-'*35}-+-{'-'*15}-+-{'-'*14}-+-{'-'*14}-+-{'-'*6}-+-{'-'*8}-+-{'-'*10}")
        for i, (_, r) in enumerate(subset.iterrows(), 1):
            name = str(r['game_desc'])[:33] if r['game_desc'] else 'N/A'
            vendor = str(r['vendor_id'])[:13] if r['vendor_id'] else 'N/A'
            log(f"  {i:>3} | {name:<35} | {vendor:<15} | R$ {r['ggr']:>11,.2f} | R$ {r['bets']:>11,.2f} | {r['hold_pct']:>5.1f}% | {int(r['jogadores']):>8,} | {int(r['rodadas']):>10,}")

    # Jogos que estão no top de uma segunda mas NÃO de outra (anomalias)
    log(f"\n  🔍 ANOMALIAS: Jogos com mudança significativa de ranking")
    log(f"  {'─'*90}")
    dates = sorted(df_games['activity_date'].unique(), reverse=True)
    if len(dates) >= 2:
        top_23 = set(df_games[df_games['activity_date'] == dates[0]].head(15)['game_id'].tolist()) if len(dates) > 0 else set()
        top_16 = set(df_games[df_games['activity_date'] == dates[1]].head(15)['game_id'].tolist()) if len(dates) > 1 else set()
        top_09 = set(df_games[df_games['activity_date'] == dates[2]].head(15)['game_id'].tolist()) if len(dates) > 2 else set()

        only_23 = top_23 - top_16 - top_09
        only_prev = (top_16 | top_09) - top_23

        if only_23:
            log(f"  🆕 Jogos no Top 15 de 23/03 que NÃO estavam antes:")
            for gid in only_23:
                info = df_games[(df_games['game_id'] == gid) & (df_games['activity_date'] == dates[0])].iloc[0]
                log(f"     - {info['game_desc']} (GGR: R$ {info['ggr']:,.2f})")

        if only_prev:
            log(f"  ❌ Jogos que SAÍRAM do Top 15 em 23/03:")
            for gid in only_prev:
                rows = df_games[(df_games['game_id'] == gid)]
                for _, r in rows.iterrows():
                    log(f"     - {r['game_desc']} em {r['activity_date']}: GGR R$ {r['ggr']:,.2f}")

# ====================================================================
# 5. SPORTSBOOK POR ESPORTE
# ====================================================================
log("\n" + "=" * 95)
log("⚽ SEÇÃO 5: SPORTSBOOK POR ESPORTE")
log("=" * 95)

# Usar vendor_ec2 para detalhes por esporte
# Ranges UTC para cada segunda BRT
utc_ranges = [
    ('2026-03-23', "TIMESTAMP '2026-03-23 03:00:00'", "TIMESTAMP '2026-03-24 03:00:00'"),
    ('2026-03-16', "TIMESTAMP '2026-03-16 03:00:00'", "TIMESTAMP '2026-03-17 03:00:00'"),
    ('2026-03-09', "TIMESTAMP '2026-03-09 03:00:00'", "TIMESTAMP '2026-03-10 03:00:00'"),
]

# Build UNION ALL query for all 3 dates
sb_union_parts = []
for dt, utc_start, utc_end in utc_ranges:
    sb_union_parts.append(f"""
    SELECT
        '{dt}' AS segunda,
        d.c_sport_type_name AS esporte,
        b.c_bet_type AS tipo,
        COUNT(DISTINCT b.c_bet_slip_id) AS bilhetes,
        COUNT(DISTINCT b.c_customer_id) AS apostadores,
        SUM(b.c_total_stake) AS stake_total,
        SUM(CASE WHEN b.c_bet_state = 'C' THEN b.c_total_return ELSE 0 END) AS payout_total,
        SUM(CASE WHEN b.c_bet_state = 'C' THEN b.c_total_stake - COALESCE(b.c_total_return, 0) ELSE 0 END) AS ggr_settled,
        SUM(CASE WHEN b.c_bet_state = 'O' THEN b.c_total_stake ELSE 0 END) AS stake_open
    FROM vendor_ec2.tbl_sports_book_bets_info b
    LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
        ON b.c_bet_slip_id = d.c_bet_slip_id
        AND b.c_transaction_id = d.c_transaction_id
    WHERE b.c_created_time >= {utc_start}
      AND b.c_created_time < {utc_end}
      AND b.c_transaction_type = 'M'
    GROUP BY 1, 2, 3
    """)

df_sb = qry(" UNION ALL ".join(sb_union_parts), db="vendor_ec2", label="Sportsbook by Sport")

if df_sb is not None and len(df_sb) > 0:
    # Agregado por esporte e segunda
    for dt in sorted(df_sb['segunda'].unique(), reverse=True):
        subset = df_sb[df_sb['segunda'] == dt].copy()
        agg = subset.groupby('esporte').agg({
            'bilhetes': 'sum',
            'apostadores': 'sum',
            'stake_total': 'sum',
            'payout_total': 'sum',
            'ggr_settled': 'sum',
            'stake_open': 'sum'
        }).sort_values('stake_total', ascending=False)

        total_stake = agg['stake_total'].sum()
        total_ggr = agg['ggr_settled'].sum()

        log(f"\n  📅 {dt} {'(PARCIAL)' if '03-23' in dt else ''}")
        log(f"  {'Esporte':<25} | {'Stake':>12} | {'GGR Settled':>12} | {'Hold%':>6} | {'Bilhetes':>9} | {'Players':>8} | {'Open':>12}")
        log(f"  {'-'*25}-+-{'-'*12}-+-{'-'*12}-+-{'-'*6}-+-{'-'*9}-+-{'-'*8}-+-{'-'*12}")
        for esporte, r in agg.head(15).iterrows():
            hold = (r['ggr_settled'] / r['stake_total'] * 100) if r['stake_total'] > 0 else 0
            log(f"  {str(esporte)[:25]:<25} | R$ {r['stake_total']:>9,.0f} | R$ {r['ggr_settled']:>9,.0f} | {hold:>5.1f}% | {int(r['bilhetes']):>9,} | {int(r['apostadores']):>8,} | R$ {r['stake_open']:>9,.0f}")
        log(f"  {'TOTAL':<25} | R$ {total_stake:>9,.0f} | R$ {total_ggr:>9,.0f} | {(total_ggr/total_stake*100) if total_stake > 0 else 0:>5.1f}% |")

    # Live vs PreLive comparison
    log(f"\n  🔄 LIVE vs PRE-LIVE por segunda:")
    agg_tipo = df_sb.groupby(['segunda', 'tipo']).agg({
        'stake_total': 'sum', 'ggr_settled': 'sum', 'bilhetes': 'sum'
    }).reset_index().sort_values(['segunda', 'tipo'], ascending=[False, True])
    for _, r in agg_tipo.iterrows():
        hold = (r['ggr_settled'] / r['stake_total'] * 100) if r['stake_total'] > 0 else 0
        log(f"  {r['segunda']} | {r['tipo']:<10} | Stake: R$ {r['stake_total']:>10,.0f} | GGR: R$ {r['ggr_settled']:>10,.0f} | Hold: {hold:.1f}% | Bilhetes: {int(r['bilhetes']):,}")

# ====================================================================
# 6. FTD ANÁLISE (ticket médio, por hora)
# ====================================================================
log("\n" + "=" * 95)
log("🆕 SEÇÃO 6: ANÁLISE DE FTDs")
log("=" * 95)

# FTD count já está no KPIs. Vamos pegar ticket médio via deposits
df_ftd = qry(f"""
SELECT
    activity_date,
    SUM(ftd_count) AS total_ftds,
    -- Ticket médio de depósito geral (proxy)
    SUM(deposit_success_base) / NULLIF(SUM(deposit_success_count), 0) AS ticket_medio_dep,
    -- FTD como % dos registros
    CAST(SUM(ftd_count) AS DOUBLE) / NULLIF(CAST(SUM(nrc_count) AS DOUBLE), 0) * 100 AS conversao_nrc_ftd
FROM ps_bi.fct_player_activity_daily
WHERE activity_date IN ({DATAS_PS_BI})
GROUP BY activity_date
ORDER BY activity_date DESC
""", label="FTD Analysis")

if df_ftd is not None and len(df_ftd) > 0:
    log(f"\n  {'Data':<15} | {'FTDs':>8} | {'Ticket Médio Dep':>18} | {'Conversão NRC→FTD':>18}")
    log(f"  {'-'*15}-+-{'-'*8}-+-{'-'*18}-+-{'-'*18}")
    for _, r in df_ftd.iterrows():
        log(f"  {str(r['activity_date']):<15} | {int(r['total_ftds']):>8,} | R$ {r['ticket_medio_dep']:>15,.2f} | {r['conversao_nrc_ftd']:>15,.1f}%")

# ====================================================================
# 7. JOGOS DISTORCIDOS — Hold Rate anormal
# ====================================================================
log("\n" + "=" * 95)
log("⚠️ SEÇÃO 7: JOGOS COM HOLD RATE ANORMAL (23/03 vs média)")
log("=" * 95)

if df_games is not None and len(df_games) > 0:
    dates = sorted(df_games['activity_date'].unique(), reverse=True)
    if len(dates) >= 2:
        # Média hold por jogo nas semanas anteriores
        prev = df_games[df_games['activity_date'].isin(dates[1:])].copy()
        avg_hold = prev.groupby('game_id').agg({
            'ggr': 'mean',
            'bets': 'mean',
            'hold_pct': 'mean',
            'game_desc': 'first',
            'vendor_id': 'first'
        }).rename(columns={'ggr': 'ggr_avg', 'bets': 'bets_avg', 'hold_pct': 'hold_avg'})

        # Dados de 23/03
        curr = df_games[df_games['activity_date'] == dates[0]].copy()
        curr = curr[curr['bets'] > 500]  # Só jogos com volume mínimo

        merged = curr.merge(avg_hold, on='game_id', suffixes=('_23', '_prev'))
        merged['hold_diff'] = merged['hold_pct'] - merged['hold_avg']
        merged = merged.sort_values('hold_diff')

        # Jogos com hold muito abaixo (jogador ganhando muito)
        low_hold = merged[merged['hold_diff'] < -5].head(10)
        if len(low_hold) > 0:
            log(f"\n  🔴 Jogos com HOLD MUITO ABAIXO da média (jogadores ganhando mais):")
            log(f"  {'Jogo':<35} | {'Hold 23/03':>10} | {'Hold Média':>10} | {'Diff':>8} | {'GGR 23/03':>12}")
            log(f"  {'-'*35}-+-{'-'*10}-+-{'-'*10}-+-{'-'*8}-+-{'-'*12}")
            for _, r in low_hold.iterrows():
                name = str(r['game_desc_23'])[:33] if r['game_desc_23'] else 'N/A'
                log(f"  {name:<35} | {r['hold_pct']:>9.1f}% | {r['hold_avg']:>9.1f}% | {r['hold_diff']:>+7.1f}% | R$ {r['ggr']:>9,.0f}")

        # Jogos com hold muito acima
        high_hold = merged[merged['hold_diff'] > 5].sort_values('hold_diff', ascending=False).head(10)
        if len(high_hold) > 0:
            log(f"\n  🟢 Jogos com HOLD MUITO ACIMA da média (casa ganhando mais):")
            log(f"  {'Jogo':<35} | {'Hold 23/03':>10} | {'Hold Média':>10} | {'Diff':>8} | {'GGR 23/03':>12}")
            log(f"  {'-'*35}-+-{'-'*10}-+-{'-'*10}-+-{'-'*8}-+-{'-'*12}")
            for _, r in high_hold.iterrows():
                name = str(r['game_desc_23'])[:33] if r['game_desc_23'] else 'N/A'
                log(f"  {name:<35} | {r['hold_pct']:>9.1f}% | {r['hold_avg']:>9.1f}% | {r['hold_diff']:>+7.1f}% | R$ {r['ggr']:>9,.0f}")

# ====================================================================
# 8. RESUMO EXECUTIVO
# ====================================================================
log("\n" + "=" * 95)
log("📋 SEÇÃO 8: RESUMO EXECUTIVO — INSIGHTS & ALERTAS")
log("=" * 95)

if df_kpis is not None and len(df_kpis) >= 2:
    d23 = df_kpis[df_kpis['activity_date'].astype(str).str.contains('03-23')]
    d16 = df_kpis[df_kpis['activity_date'].astype(str).str.contains('03-16')]
    d09 = df_kpis[df_kpis['activity_date'].astype(str).str.contains('03-09')]

    if len(d23) > 0 and len(d16) > 0:
        r23 = d23.iloc[0]
        r16 = d16.iloc[0]
        r09 = d09.iloc[0] if len(d09) > 0 else None

        log(f"\n  ⚠️  ATENÇÃO: 23/03 tem ~50% dos dados ({int(r23['jogadores_ativos']):,} jogadores vs {int(r16['jogadores_ativos']):,} em 16/03)")
        log(f"  Para comparação justa, os indicadores PROPORCIONAIS são mais relevantes:")

        # Métricas proporcionais
        dep_per_player_23 = r23['dep_brl'] / r23['jogadores_ativos'] if r23['jogadores_ativos'] > 0 else 0
        dep_per_player_16 = r16['dep_brl'] / r16['jogadores_ativos'] if r16['jogadores_ativos'] > 0 else 0

        ggr_per_player_23 = r23['ggr_total'] / r23['jogadores_ativos'] if r23['jogadores_ativos'] > 0 else 0
        ggr_per_player_16 = r16['ggr_total'] / r16['jogadores_ativos'] if r16['jogadores_ativos'] > 0 else 0

        casino_margin_23 = (r23['casino_bets_total'] - r23['casino_wins_total']) / r23['casino_bets_total'] * 100 if r23['casino_bets_total'] > 0 else 0
        casino_margin_16 = (r16['casino_bets_total'] - r16['casino_wins_total']) / r16['casino_bets_total'] * 100 if r16['casino_bets_total'] > 0 else 0

        sb_margin_23 = (r23['sb_bets_total'] - r23['sb_wins_total']) / r23['sb_bets_total'] * 100 if r23['sb_bets_total'] > 0 else 0
        sb_margin_16 = (r16['sb_bets_total'] - r16['sb_wins_total']) / r16['sb_bets_total'] * 100 if r16['sb_bets_total'] > 0 else 0

        ftd_conv_23 = r23['ftds'] / r23['registros_nrc'] * 100 if r23['registros_nrc'] > 0 else 0
        ftd_conv_16 = r16['ftds'] / r16['registros_nrc'] * 100 if r16['registros_nrc'] > 0 else 0

        log(f"\n  {'Métrica Proporcional':<30} | {'23/03':>15} | {'16/03':>15} | {'Variação':>10}")
        log(f"  {'-'*30}-+-{'-'*15}-+-{'-'*15}-+-{'-'*10}")
        log(f"  {'Dep/Jogador':.<30} | R$ {dep_per_player_23:>12,.2f} | R$ {dep_per_player_16:>12,.2f} | {((dep_per_player_23/dep_per_player_16)-1)*100 if dep_per_player_16 > 0 else 0:>+8.1f}%")
        log(f"  {'GGR/Jogador':.<30} | R$ {ggr_per_player_23:>12,.2f} | R$ {ggr_per_player_16:>12,.2f} | {((ggr_per_player_23/ggr_per_player_16)-1)*100 if ggr_per_player_16 > 0 else 0:>+8.1f}%")
        log(f"  {'Casino Margin':.<30} | {casino_margin_23:>14.2f}% | {casino_margin_16:>14.2f}% | {casino_margin_23 - casino_margin_16:>+8.1f}pp")
        log(f"  {'SB Margin':.<30} | {sb_margin_23:>14.2f}% | {sb_margin_16:>14.2f}% | {sb_margin_23 - sb_margin_16:>+8.1f}pp")
        log(f"  {'FTD/NRC Conversão':.<30} | {ftd_conv_23:>14.1f}% | {ftd_conv_16:>14.1f}% | {ftd_conv_23 - ftd_conv_16:>+8.1f}pp")

log("\n" + "=" * 95)
log("FIM DA ANÁLISE")
log("=" * 95)

# ====================================================================
# SALVAR REPORT
# ====================================================================
report_path = os.path.join(os.path.dirname(__file__), '..', 'reports', 'analise_segundas_23_16_09.txt')
os.makedirs(os.path.dirname(report_path), exist_ok=True)
with open(report_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(REPORT))
print(f"\n📁 Report salvo em: {report_path}")
