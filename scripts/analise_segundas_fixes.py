#!/usr/bin/env python3
"""
Análise Segundas - CORREÇÕES
A. bireports_ec2 com colunas corretas (c_created_date)
B. Top games com nomes via tbl_vendor_games_mapping_data
C. Sportsbook separado por data (evitar esgotamento de recursos)
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.athena import query_athena
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 250)
pd.set_option('display.float_format', '{:,.2f}'.format)

REPORT = []

def log(msg=""):
    print(msg)
    REPORT.append(str(msg))

def qry(sql, db="ps_bi", label=""):
    try:
        return query_athena(sql, database=db)
    except Exception as e:
        log(f"  [ERRO {label}]: {e}")
        return None

DATAS_PS_BI = "DATE '2026-03-23', DATE '2026-03-16', DATE '2026-03-09'"

# ====================================================================
# A. BIREPORTS_EC2 FALLBACK — colunas corretas
# ====================================================================
log("=" * 95)
log("SECAO A: GAMING DATA via bireports_ec2 (colunas corrigidas)")
log("Fonte: tbl_ecr_wise_daily_bi_summary + tbl_ecr (test_user filter)")
log("Valores em centavos / 100 = BRL")
log("=" * 95)

df_bi = qry("""
SELECT
    CAST(b.c_created_date AS VARCHAR) AS dt,
    COUNT(DISTINCT b.c_ecr_id) AS jogadores,
    SUM(b.c_login_count) AS logins,
    SUM(b.c_sign_up_count) AS registros,
    SUM(b.c_conversion_count) AS ftds,
    -- Depositos
    SUM(b.c_deposit_success_amount) / 100.0 AS depositos,
    SUM(b.c_deposit_success_count) AS dep_count,
    -- Saques
    SUM(b.c_co_success_amount) / 100.0 AS saques,
    SUM(b.c_co_success_count) AS saq_count,
    -- Casino
    SUM(b.c_casino_bet_amount) / 100.0 AS casino_bets_total,
    SUM(b.c_casino_win_amount) / 100.0 AS casino_wins_total,
    SUM(b.c_casino_realcash_bet_amount) / 100.0 AS casino_real_bets,
    SUM(b.c_casino_realcash_win_amount) / 100.0 AS casino_real_wins,
    (SUM(b.c_casino_bet_amount) - SUM(b.c_casino_win_amount)) / 100.0 AS casino_ggr_total,
    (SUM(b.c_casino_realcash_bet_amount) - SUM(b.c_casino_realcash_win_amount)) / 100.0 AS casino_ggr_real,
    -- Sportsbook
    SUM(b.c_sb_bet_amount) / 100.0 AS sb_bets_total,
    SUM(b.c_sb_win_amount) / 100.0 AS sb_wins_total,
    SUM(b.c_sb_realcash_bet_amount) / 100.0 AS sb_real_bets,
    SUM(b.c_sb_realcash_win_amount) / 100.0 AS sb_real_wins,
    (SUM(b.c_sb_bet_amount) - SUM(b.c_sb_win_amount)) / 100.0 AS sb_ggr_total,
    (SUM(b.c_sb_realcash_bet_amount) - SUM(b.c_sb_realcash_win_amount)) / 100.0 AS sb_ggr_real,
    -- GGR geral
    (SUM(b.c_casino_bet_amount) + SUM(b.c_sb_bet_amount)
     - SUM(b.c_casino_win_amount) - SUM(b.c_sb_win_amount)) / 100.0 AS ggr_geral,
    -- Bonus
    SUM(b.c_bonus_issued_amount) / 100.0 AS bonus_issued
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary b
JOIN bireports_ec2.tbl_ecr e ON b.c_ecr_id = e.c_ecr_id
WHERE CAST(b.c_created_date AS VARCHAR) IN ('2026-03-23', '2026-03-16', '2026-03-09')
  AND e.c_test_user = false
GROUP BY 1
ORDER BY 1 DESC
""", db="bireports_ec2", label="bireports GGR")

if df_bi is not None and len(df_bi) > 0:
    for _, r in df_bi.iterrows():
        dt = r['dt']
        casino_margin = (r['casino_ggr_total'] / r['casino_bets_total'] * 100) if r['casino_bets_total'] > 0 else 0
        sb_margin = (r['sb_ggr_total'] / r['sb_bets_total'] * 100) if r['sb_bets_total'] > 0 else 0
        net_dep = r['depositos'] - r['saques']
        parcial = "(PARCIAL)" if "03-23" in str(dt) else "(COMPLETO)"

        log(f"\n  {dt} {parcial}")
        log(f"  Jogadores: {int(r['jogadores']):,} | Logins: {int(r['logins']):,} | Registros: {int(r['registros']):,} | FTDs: {int(r['ftds']):,}")
        log(f"  Depositos:  R$ {r['depositos']:>14,.2f} ({int(r['dep_count']):,})")
        log(f"  Saques:     R$ {r['saques']:>14,.2f} ({int(r['saq_count']):,})")
        log(f"  Net Dep:    R$ {net_dep:>14,.2f}")
        log(f"  Casino Bets Total: R$ {r['casino_bets_total']:>14,.2f} | Wins: R$ {r['casino_wins_total']:>14,.2f}")
        log(f"  Casino GGR Total:  R$ {r['casino_ggr_total']:>14,.2f} ({casino_margin:.2f}%)")
        log(f"  Casino GGR Real:   R$ {r['casino_ggr_real']:>14,.2f}")
        log(f"  SB Bets Total:     R$ {r['sb_bets_total']:>14,.2f} | Wins: R$ {r['sb_wins_total']:>14,.2f}")
        log(f"  SB GGR Total:      R$ {r['sb_ggr_total']:>14,.2f} ({sb_margin:.2f}%)")
        log(f"  GGR Geral:         R$ {r['ggr_geral']:>14,.2f}")

    # Tabela comparativa
    log(f"\n  COMPARATIVO BIREPORTS_EC2:")
    log(f"  {'KPI':<25} | {'23/03 PARC':>16} | {'16/03':>16} | {'09/03':>16}")
    log(f"  {'-'*25}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}")
    kpis = [
        ('depositos', 'Depositos R$'), ('dep_count', 'Dep Qty'), ('saques', 'Saques R$'),
        ('saq_count', 'Saq Qty'), ('casino_ggr_total', 'Casino GGR Total'),
        ('casino_ggr_real', 'Casino GGR Real'), ('sb_ggr_total', 'SB GGR Total'),
        ('sb_ggr_real', 'SB GGR Real'), ('ggr_geral', 'GGR Geral'),
        ('logins', 'Logins'), ('registros', 'Registros'), ('ftds', 'FTDs'),
    ]
    rows = [r for _, r in df_bi.iterrows()]
    for col, name in kpis:
        line = f"  {name:<25}"
        for r in rows:
            v = r[col]
            if col.endswith('_count') or col in ['logins', 'registros', 'ftds', 'dep_count', 'saq_count']:
                line += f" | {int(v):>16,}"
            else:
                line += f" | R$ {v:>13,.2f}"
        log(line)

# ====================================================================
# B. TOP 15 JOGOS COM NOMES (via tbl_vendor_games_mapping_data)
# ====================================================================
log("\n" + "=" * 95)
log("SECAO B: TOP 15 JOGOS POR GGR (nomes via bireports_ec2)")
log("=" * 95)

df_games = qry(f"""
SELECT
    c.activity_date,
    c.game_id,
    COALESCE(m.c_game_desc, CONCAT('game_', c.game_id)) AS game_name,
    COALESCE(m.c_vendor_id, 'unknown') AS vendor,
    SUM(c.bet_amount_base) AS bets,
    SUM(c.win_amount_base) AS wins,
    SUM(c.bet_amount_base) - SUM(c.win_amount_base) AS ggr,
    SUM(c.real_bet_amount_base) - SUM(c.real_win_amount_base) AS ggr_real,
    SUM(c.bet_count) AS rodadas,
    COUNT(DISTINCT c.player_id) AS jogadores,
    CASE WHEN SUM(c.bet_amount_base) > 0
         THEN (SUM(c.bet_amount_base) - SUM(c.win_amount_base)) / SUM(c.bet_amount_base) * 100
         ELSE 0 END AS hold_pct
FROM ps_bi.fct_casino_activity_daily c
LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data m
    ON c.game_id = m.c_game_id
WHERE c.activity_date IN ({DATAS_PS_BI})
  AND c.product_id = 'casino'
GROUP BY c.activity_date, c.game_id, m.c_game_desc, m.c_vendor_id
HAVING SUM(c.bet_amount_base) > 0
ORDER BY c.activity_date DESC, ggr DESC
""", label="Top Games v2")

if df_games is not None and len(df_games) > 0:
    for dt in sorted(df_games['activity_date'].unique(), reverse=True):
        subset = df_games[df_games['activity_date'] == dt].head(15)
        if len(subset) == 0:
            continue
        dt_str = str(dt)
        parcial = "(PARCIAL)" if "03-23" in dt_str else ""
        log(f"\n  {dt_str} {parcial}")
        log(f"  {'#':>3} | {'Jogo':<32} | {'Vendor':<16} | {'GGR':>12} | {'Bets':>12} | {'Hold%':>6} | {'Plrs':>6} | {'Rounds':>9}")
        log(f"  {'-'*3}-+-{'-'*32}-+-{'-'*16}-+-{'-'*12}-+-{'-'*12}-+-{'-'*6}-+-{'-'*6}-+-{'-'*9}")
        for i, (_, r) in enumerate(subset.iterrows(), 1):
            name = str(r['game_name'])[:30]
            vendor = str(r['vendor'])[:14]
            log(f"  {i:>3} | {name:<32} | {vendor:<16} | R${r['ggr']:>9,.0f} | R${r['bets']:>9,.0f} | {r['hold_pct']:>5.1f}% | {int(r['jogadores']):>6,} | {int(r['rodadas']):>9,}")

    # Anomalias entre datas
    dates = sorted(df_games['activity_date'].unique(), reverse=True)
    dates_with_data = [d for d in dates if len(df_games[(df_games['activity_date'] == d) & (df_games['bets'] > 0)]) > 5]

    if len(dates_with_data) >= 2:
        log(f"\n  MUDANCAS DE RANKING:")
        top_sets = {}
        for d in dates_with_data:
            top_ids = df_games[df_games['activity_date'] == d].head(15)['game_id'].tolist()
            top_sets[str(d)] = set(top_ids)

        d1, d2 = str(dates_with_data[0]), str(dates_with_data[1])
        only_new = top_sets[d1] - top_sets[d2]
        only_old = top_sets[d2] - top_sets[d1]

        if only_new:
            log(f"\n  Entraram no Top 15 em {d1} (novos):")
            for gid in only_new:
                info = df_games[(df_games['game_id'] == gid) & (df_games['activity_date'] == dates_with_data[0])]
                if len(info) > 0:
                    r = info.iloc[0]
                    log(f"    + {r['game_name']} (GGR: R$ {r['ggr']:,.0f}, Hold: {r['hold_pct']:.1f}%)")
        if only_old:
            log(f"\n  Sairam do Top 15 em {d1} (antes estavam):")
            for gid in only_old:
                info = df_games[(df_games['game_id'] == gid) & (df_games['activity_date'] == dates_with_data[1])]
                if len(info) > 0:
                    r = info.iloc[0]
                    log(f"    - {r['game_name']} era #{df_games[df_games['activity_date'] == dates_with_data[1]].head(15).reset_index().index[df_games[df_games['activity_date'] == dates_with_data[1]].head(15)['game_id'] == gid].tolist()[0] + 1 if gid in df_games[df_games['activity_date'] == dates_with_data[1]].head(15)['game_id'].values else '?'} (GGR: R$ {r['ggr']:,.0f})")

    # Jogos com hold rate distorcido
    if len(dates_with_data) >= 2:
        log(f"\n  JOGOS COM HOLD RATE DISTORCIDO:")
        d_curr = dates_with_data[0]
        d_prev = dates_with_data[1:]

        curr = df_games[df_games['activity_date'] == d_curr].copy()
        curr = curr[curr['bets'] > 500]

        prev = df_games[df_games['activity_date'].isin(d_prev)].groupby('game_id').agg({
            'ggr': 'mean', 'bets': 'mean', 'hold_pct': 'mean',
            'game_name': 'first', 'vendor': 'first'
        }).rename(columns={'hold_pct': 'hold_avg', 'ggr': 'ggr_avg'})

        merged = curr.merge(prev, on='game_id', suffixes=('', '_prev'))
        merged['hold_diff'] = merged['hold_pct'] - merged['hold_avg']

        # Muito abaixo
        low = merged[merged['hold_diff'] < -5].sort_values('hold_diff').head(8)
        if len(low) > 0:
            log(f"\n  HOLD MUITO ABAIXO da media (casa perdendo mais):")
            log(f"  {'Jogo':<32} | {'Hold Agora':>10} | {'Hold Med':>10} | {'Diff':>8} | {'GGR':>12}")
            for _, r in low.iterrows():
                log(f"  {str(r['game_name'])[:30]:<32} | {r['hold_pct']:>9.1f}% | {r['hold_avg']:>9.1f}% | {r['hold_diff']:>+7.1f}% | R${r['ggr']:>9,.0f}")

        high = merged[merged['hold_diff'] > 5].sort_values('hold_diff', ascending=False).head(8)
        if len(high) > 0:
            log(f"\n  HOLD MUITO ACIMA da media (casa ganhando mais):")
            log(f"  {'Jogo':<32} | {'Hold Agora':>10} | {'Hold Med':>10} | {'Diff':>8} | {'GGR':>12}")
            for _, r in high.iterrows():
                log(f"  {str(r['game_name'])[:30]:<32} | {r['hold_pct']:>9.1f}% | {r['hold_avg']:>9.1f}% | {r['hold_diff']:>+7.1f}% | R${r['ggr']:>9,.0f}")

# ====================================================================
# C. SPORTSBOOK POR DATA (queries separadas)
# ====================================================================
log("\n" + "=" * 95)
log("SECAO C: SPORTSBOOK POR ESPORTE (queries individuais)")
log("=" * 95)

utc_ranges = [
    ('2026-03-23', "TIMESTAMP '2026-03-23 03:00:00'", "TIMESTAMP '2026-03-24 03:00:00'"),
    ('2026-03-16', "TIMESTAMP '2026-03-16 03:00:00'", "TIMESTAMP '2026-03-17 03:00:00'"),
    ('2026-03-09', "TIMESTAMP '2026-03-09 03:00:00'", "TIMESTAMP '2026-03-10 03:00:00'"),
]

all_sb = []
for dt, utc_start, utc_end in utc_ranges:
    log(f"\n  Consultando sportsbook {dt}...")
    df_sb = qry(f"""
    SELECT
        d.c_sport_type_name AS esporte,
        CASE WHEN b.c_is_live = true THEN 'Live' ELSE 'PreLive' END AS live_flag,
        COUNT(DISTINCT b.c_bet_slip_id) AS bilhetes,
        COUNT(DISTINCT b.c_customer_id) AS apostadores,
        SUM(b.c_total_stake) AS stake_total,
        SUM(CASE WHEN b.c_bet_state = 'C' THEN COALESCE(b.c_total_return, 0) ELSE 0 END) AS payout_closed,
        SUM(CASE WHEN b.c_bet_state = 'C' THEN b.c_total_stake ELSE 0 END) AS stake_closed,
        SUM(CASE WHEN b.c_bet_state = 'O' THEN b.c_total_stake ELSE 0 END) AS stake_open
    FROM vendor_ec2.tbl_sports_book_bets_info b
    LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
        ON b.c_bet_slip_id = d.c_bet_slip_id
        AND b.c_transaction_id = d.c_transaction_id
    WHERE b.c_created_time >= {utc_start}
      AND b.c_created_time < {utc_end}
      AND b.c_transaction_type = 'M'
    GROUP BY 1, 2
    """, db="vendor_ec2", label=f"SB {dt}")

    if df_sb is not None and len(df_sb) > 0:
        df_sb['segunda'] = dt
        all_sb.append(df_sb)

        # Por esporte
        agg = df_sb.groupby('esporte').agg({
            'bilhetes': 'sum', 'apostadores': 'sum',
            'stake_total': 'sum', 'payout_closed': 'sum',
            'stake_closed': 'sum', 'stake_open': 'sum'
        }).sort_values('stake_total', ascending=False)

        total_stake = agg['stake_total'].sum()
        total_ggr = (agg['stake_closed'] - agg['payout_closed']).sum()
        overall_hold = (total_ggr / agg['stake_closed'].sum() * 100) if agg['stake_closed'].sum() > 0 else 0
        parcial = "(PARCIAL)" if "03-23" in dt else ""

        log(f"\n  {dt} {parcial} | Stake Total: R$ {total_stake:,.0f} | GGR Settled: R$ {total_ggr:,.0f} | Hold: {overall_hold:.1f}%")
        log(f"  {'Esporte':<22} | {'Stake':>11} | {'GGR':>11} | {'Hold%':>6} | {'Bilh':>7} | {'Plrs':>6} | {'Open':>11}")
        log(f"  {'-'*22}-+-{'-'*11}-+-{'-'*11}-+-{'-'*6}-+-{'-'*7}-+-{'-'*6}-+-{'-'*11}")
        for esporte, r in agg.head(12).iterrows():
            ggr = r['stake_closed'] - r['payout_closed']
            hold = (ggr / r['stake_closed'] * 100) if r['stake_closed'] > 0 else 0
            log(f"  {str(esporte)[:22]:<22} | R${r['stake_total']:>8,.0f} | R${ggr:>8,.0f} | {hold:>5.1f}% | {int(r['bilhetes']):>7,} | {int(r['apostadores']):>6,} | R${r['stake_open']:>8,.0f}")

if len(all_sb) >= 2:
    df_all = pd.concat(all_sb)

    # Live vs PreLive
    log(f"\n  LIVE vs PRE-LIVE:")
    agg_live = df_all.groupby(['segunda', 'live_flag']).agg({
        'stake_total': 'sum', 'stake_closed': 'sum', 'payout_closed': 'sum', 'bilhetes': 'sum'
    }).reset_index()
    agg_live['ggr'] = agg_live['stake_closed'] - agg_live['payout_closed']
    agg_live['hold'] = agg_live.apply(lambda r: (r['ggr']/r['stake_closed']*100) if r['stake_closed'] > 0 else 0, axis=1)
    agg_live = agg_live.sort_values(['segunda', 'live_flag'], ascending=[False, True])

    log(f"  {'Data':<12} | {'Tipo':<8} | {'Stake':>12} | {'GGR':>12} | {'Hold%':>6} | {'Bilhetes':>9}")
    log(f"  {'-'*12}-+-{'-'*8}-+-{'-'*12}-+-{'-'*12}-+-{'-'*6}-+-{'-'*9}")
    for _, r in agg_live.iterrows():
        log(f"  {r['segunda']:<12} | {r['live_flag']:<8} | R${r['stake_total']:>9,.0f} | R${r['ggr']:>9,.0f} | {r['hold']:>5.1f}% | {int(r['bilhetes']):>9,}")

    # Variacao por esporte
    log(f"\n  VARIACAO POR ESPORTE:")
    curr_dt = sorted(df_all['segunda'].unique(), reverse=True)[0]
    prev_dts = sorted(df_all['segunda'].unique(), reverse=True)[1:]

    curr = df_all[df_all['segunda'] == curr_dt].groupby('esporte').agg({'stake_total': 'sum', 'stake_closed': 'sum', 'payout_closed': 'sum'})
    curr['ggr'] = curr['stake_closed'] - curr['payout_closed']

    prev = df_all[df_all['segunda'].isin(prev_dts)].groupby('esporte').agg({'stake_total': 'sum', 'stake_closed': 'sum', 'payout_closed': 'sum'})
    prev['ggr'] = prev['stake_closed'] - prev['payout_closed']
    prev = prev / len(prev_dts)

    merged = curr.join(prev, lsuffix='_now', rsuffix='_avg').dropna()
    merged['stake_var'] = merged.apply(
        lambda r: ((r['stake_total_now']/r['stake_total_avg'])-1)*100 if r['stake_total_avg'] > 50 else 0, axis=1)
    merged['ggr_diff'] = merged['ggr_now'] - merged['ggr_avg']
    merged = merged.sort_values('ggr_diff')

    log(f"  {'Esporte':<22} | {'Stake Now':>11} | {'Stake Med':>11} | {'Var%':>7} | {'GGR Now':>11} | {'GGR Med':>11} | {'Diff GGR':>11}")
    log(f"  {'-'*22}-+-{'-'*11}-+-{'-'*11}-+-{'-'*7}-+-{'-'*11}-+-{'-'*11}-+-{'-'*11}")
    for esporte, r in merged.iterrows():
        if abs(r['stake_total_avg']) > 50:
            log(f"  {str(esporte)[:22]:<22} | R${r['stake_total_now']:>8,.0f} | R${r['stake_total_avg']:>8,.0f} | {r['stake_var']:>+6.0f}% | R${r['ggr_now']:>8,.0f} | R${r['ggr_avg']:>8,.0f} | R${r['ggr_diff']:>8,.0f}")

# ====================================================================
# SALVAR
# ====================================================================
report_path = os.path.join(os.path.dirname(__file__), '..', 'reports', 'analise_segundas_fixes.txt')
os.makedirs(os.path.dirname(report_path), exist_ok=True)
with open(report_path, 'w', encoding='utf-8') as f:
    f.write('\n'.join(REPORT))
log(f"\nReport salvo em: {report_path}")
