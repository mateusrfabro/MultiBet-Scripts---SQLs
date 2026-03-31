#!/usr/bin/env python3
"""
Reextracao completa ~16h BRT + Validacao BigQuery Smartico
23/03, 16/03, 09/03/2026
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.athena import query_athena
from db.bigquery import query_bigquery
import pandas as pd
import json
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.width', 250)
pd.set_option('display.float_format', '{:,.2f}'.format)

RESULTS = {}

def qry_athena(sql, db="ps_bi", label=""):
    try:
        return query_athena(sql, database=db)
    except Exception as e:
        print(f"  [ERRO ATHENA {label}]: {e}")
        return None

def qry_bq(sql, label=""):
    try:
        return query_bigquery(sql)
    except Exception as e:
        print(f"  [ERRO BQ {label}]: {e}")
        return None

BQ_DS = "smartico-bq6.dwh_ext_24105"

print("=" * 95)
print(f"REEXTRACAO COMPLETA ~{datetime.now().strftime('%H:%M')} BRT")
print("=" * 95)

# ============================================================
# 1. ATHENA: bireports_ec2 atualizado
# ============================================================
print("\n--- 1. ATHENA bireports_ec2 (atualizado) ---")
df_bi = qry_athena("""
SELECT
    CAST(b.c_created_date AS VARCHAR) AS dt,
    COUNT(DISTINCT b.c_ecr_id) AS jogadores,
    SUM(b.c_login_count) AS logins,
    SUM(b.c_sign_up_count) AS registros,
    SUM(b.c_conversion_count) AS ftds,
    SUM(b.c_deposit_success_amount) / 100.0 AS depositos,
    SUM(b.c_deposit_success_count) AS dep_count,
    SUM(b.c_co_success_amount) / 100.0 AS saques,
    SUM(b.c_co_success_count) AS saq_count,
    SUM(b.c_casino_bet_amount) / 100.0 AS casino_bets,
    SUM(b.c_casino_win_amount) / 100.0 AS casino_wins,
    (SUM(b.c_casino_bet_amount) - SUM(b.c_casino_win_amount)) / 100.0 AS casino_ggr,
    SUM(b.c_casino_realcash_bet_amount) / 100.0 AS casino_real_bets,
    SUM(b.c_casino_realcash_win_amount) / 100.0 AS casino_real_wins,
    (SUM(b.c_casino_realcash_bet_amount) - SUM(b.c_casino_realcash_win_amount)) / 100.0 AS casino_ggr_real,
    SUM(b.c_sb_bet_amount) / 100.0 AS sb_bets,
    SUM(b.c_sb_win_amount) / 100.0 AS sb_wins,
    (SUM(b.c_sb_bet_amount) - SUM(b.c_sb_win_amount)) / 100.0 AS sb_ggr,
    SUM(b.c_sb_realcash_bet_amount) / 100.0 AS sb_real_bets,
    SUM(b.c_sb_realcash_win_amount) / 100.0 AS sb_real_wins,
    (SUM(b.c_sb_realcash_bet_amount) - SUM(b.c_sb_realcash_win_amount)) / 100.0 AS sb_ggr_real,
    (SUM(b.c_casino_bet_amount) + SUM(b.c_sb_bet_amount)
     - SUM(b.c_casino_win_amount) - SUM(b.c_sb_win_amount)) / 100.0 AS ggr_geral,
    SUM(b.c_bonus_issued_amount) / 100.0 AS bonus_issued
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary b
JOIN bireports_ec2.tbl_ecr e ON b.c_ecr_id = e.c_ecr_id
WHERE CAST(b.c_created_date AS VARCHAR) IN ('2026-03-23', '2026-03-16', '2026-03-09')
  AND e.c_test_user = false
GROUP BY 1
ORDER BY 1 DESC
""", db="bireports_ec2", label="bireports atualizado")

if df_bi is not None and len(df_bi) > 0:
    print("\nATHENA bireports_ec2:")
    for _, r in df_bi.iterrows():
        dt = r['dt']
        net = r['depositos'] - r['saques']
        cm = (r['casino_ggr'] / r['casino_bets'] * 100) if r['casino_bets'] > 0 else 0
        sm = (r['sb_ggr'] / r['sb_bets'] * 100) if r['sb_bets'] > 0 else 0
        print(f"\n  {dt}:")
        print(f"    Jogadores: {int(r['jogadores']):,} | Logins: {int(r['logins']):,}")
        print(f"    Registros: {int(r['registros']):,} | FTDs: {int(r['ftds']):,}")
        print(f"    Depositos: R$ {r['depositos']:,.2f} ({int(r['dep_count']):,})")
        print(f"    Saques:    R$ {r['saques']:,.2f} ({int(r['saq_count']):,})")
        print(f"    Net Dep:   R$ {net:,.2f}")
        print(f"    Casino:    Bets R$ {r['casino_bets']:,.2f} | Wins R$ {r['casino_wins']:,.2f} | GGR R$ {r['casino_ggr']:,.2f} ({cm:.2f}%)")
        print(f"    Casino Real: GGR R$ {r['casino_ggr_real']:,.2f}")
        print(f"    SB:        Bets R$ {r['sb_bets']:,.2f} | Wins R$ {r['sb_wins']:,.2f} | GGR R$ {r['sb_ggr']:,.2f} ({sm:.2f}%)")
        print(f"    GGR Geral: R$ {r['ggr_geral']:,.2f}")

        RESULTS[f"athena_{dt}"] = {
            'jogadores': int(r['jogadores']), 'logins': int(r['logins']),
            'registros': int(r['registros']), 'ftds': int(r['ftds']),
            'depositos': float(r['depositos']), 'dep_count': int(r['dep_count']),
            'saques': float(r['saques']), 'saq_count': int(r['saq_count']),
            'net_deposit': float(net),
            'casino_bets': float(r['casino_bets']), 'casino_wins': float(r['casino_wins']),
            'casino_ggr': float(r['casino_ggr']), 'casino_margin': float(cm),
            'casino_ggr_real': float(r['casino_ggr_real']),
            'sb_bets': float(r['sb_bets']), 'sb_wins': float(r['sb_wins']),
            'sb_ggr': float(r['sb_ggr']), 'sb_margin': float(sm),
            'ggr_geral': float(r['ggr_geral']),
        }

# ============================================================
# 2. BIGQUERY SMARTICO: FTDs, Registros, Depositos, Logins
# ============================================================
print("\n\n--- 2. BIGQUERY SMARTICO ---")

# 2a. FTDs
print("\n  FTDs (j_user.core_ftd_date):")
df_ftd = qry_bq(f"""
    SELECT
        DATE(core_ftd_date) AS dt,
        COUNT(*) AS ftds
    FROM `{BQ_DS}.j_user`
    WHERE DATE(core_ftd_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
    GROUP BY 1 ORDER BY 1 DESC
""", label="FTDs")
if df_ftd is not None:
    print(df_ftd.to_string(index=False))
    for _, r in df_ftd.iterrows():
        RESULTS.setdefault(f"bq_{r['dt']}", {})['ftds'] = int(r['ftds'])

# 2b. Registros
print("\n  Registros (j_user.core_registration_date):")
df_reg = qry_bq(f"""
    SELECT
        DATE(core_registration_date) AS dt,
        COUNT(*) AS registros
    FROM `{BQ_DS}.j_user`
    WHERE DATE(core_registration_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
    GROUP BY 1 ORDER BY 1 DESC
""", label="Registros")
if df_reg is not None:
    print(df_reg.to_string(index=False))
    for _, r in df_reg.iterrows():
        RESULTS.setdefault(f"bq_{r['dt']}", {})['registros'] = int(r['registros'])

# 2c. Depositos
print("\n  Depositos (tr_acc_deposit_approved):")
df_dep = qry_bq(f"""
    SELECT
        DATE(event_date) AS dt,
        COUNT(*) AS dep_count,
        SUM(amount) AS dep_amount,
        COUNT(DISTINCT user_ext_id) AS depositantes
    FROM `{BQ_DS}.tr_acc_deposit_approved`
    WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
    GROUP BY 1 ORDER BY 1 DESC
""", label="Depositos")
if df_dep is not None:
    print(df_dep.to_string(index=False))
    for _, r in df_dep.iterrows():
        RESULTS.setdefault(f"bq_{r['dt']}", {})['dep_count'] = int(r['dep_count'])
        RESULTS.setdefault(f"bq_{r['dt']}", {})['dep_amount'] = float(r['dep_amount'])

# 2d. Logins
print("\n  Logins (tr_login):")
df_login = qry_bq(f"""
    SELECT
        DATE(event_date) AS dt,
        COUNT(*) AS login_events,
        COUNT(DISTINCT user_ext_id) AS unique_logins
    FROM `{BQ_DS}.tr_login`
    WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
    GROUP BY 1 ORDER BY 1 DESC
""", label="Logins")
if df_login is not None:
    print(df_login.to_string(index=False))
    for _, r in df_login.iterrows():
        RESULTS.setdefault(f"bq_{r['dt']}", {})['login_events'] = int(r['login_events'])
        RESULTS.setdefault(f"bq_{r['dt']}", {})['unique_logins'] = int(r['unique_logins'])

# 2e. Casino bets + wins
print("\n  Casino Bets (tr_casino_bet):")
df_cbets = qry_bq(f"""
    SELECT DATE(event_date) AS dt, SUM(amount) AS bets, COUNT(*) AS bet_count
    FROM `{BQ_DS}.tr_casino_bet`
    WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
    GROUP BY 1 ORDER BY 1 DESC
""", label="Casino Bets")
if df_cbets is not None:
    print(df_cbets.to_string(index=False))

print("\n  Casino Wins (tr_casino_win):")
df_cwins = qry_bq(f"""
    SELECT DATE(event_date) AS dt, SUM(amount) AS wins, COUNT(*) AS win_count
    FROM `{BQ_DS}.tr_casino_win`
    WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
    GROUP BY 1 ORDER BY 1 DESC
""", label="Casino Wins")
if df_cwins is not None:
    print(df_cwins.to_string(index=False))

if df_cbets is not None and df_cwins is not None:
    merged = df_cbets.merge(df_cwins, on='dt', how='outer').fillna(0)
    merged['ggr'] = merged['bets'] - merged['wins']
    print("\n  Casino GGR (BQ):")
    for _, r in merged.iterrows():
        print(f"    {r['dt']}: Bets R$ {r['bets']:,.2f} | Wins R$ {r['wins']:,.2f} | GGR R$ {r['ggr']:,.2f}")
        RESULTS.setdefault(f"bq_{r['dt']}", {})['casino_bets'] = float(r['bets'])
        RESULTS.setdefault(f"bq_{r['dt']}", {})['casino_wins'] = float(r['wins'])
        RESULTS.setdefault(f"bq_{r['dt']}", {})['casino_ggr'] = float(r['ggr'])

# ============================================================
# 3. TABELA COMPARATIVA FINAL
# ============================================================
print("\n\n" + "=" * 120)
print("COMPARATIVO FINAL: Athena bireports_ec2 vs BigQuery Smartico")
print("=" * 120)

DATAS = ['2026-03-23', '2026-03-16', '2026-03-09']
metrics = [
    ('ftds', 'FTDs'),
    ('registros', 'Registros'),
    ('dep_count', 'Dep Qty'),
    ('dep_amount', 'Dep R$'),
    ('unique_logins', 'Logins (unique)'),
    ('casino_ggr', 'Casino GGR'),
]

print(f"\n{'Data':<12} | {'Metrica':<16} | {'Athena':>16} | {'BigQuery':>16} | {'Diff':>8} | {'Status':>10}")
print(f"{'-'*12}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}-+-{'-'*8}-+-{'-'*10}")

for dt in DATAS:
    a = RESULTS.get(f"athena_{dt}", {})
    b = RESULTS.get(f"bq_{dt}", {})

    for key, label in metrics:
        a_key = 'depositos' if key == 'dep_amount' else key
        av = a.get(a_key)
        bv = b.get(key)

        a_str = f"{av:,.0f}" if av is not None else "N/A"
        b_str = f"{bv:,.0f}" if bv is not None else "N/A"

        if av and bv and av != 0:
            diff = ((bv - av) / abs(av)) * 100
            diff_str = f"{diff:+.1f}%"
            if abs(diff) <= 2:
                status = "OK"
            elif abs(diff) <= 10:
                status = "ALERTA"
            else:
                status = "DIVERGE"
        else:
            diff_str = "N/A"
            status = "N/A"

        print(f"{dt:<12} | {label:<16} | {a_str:>16} | {b_str:>16} | {diff_str:>8} | {status:>10}")
    print(f"{'-'*12}-+-{'-'*16}-+-{'-'*16}-+-{'-'*16}-+-{'-'*8}-+-{'-'*10}")

# Save results as JSON for the HTML update
results_path = os.path.join(os.path.dirname(__file__), '..', 'reports', 'validacao_data.json')
with open(results_path, 'w') as f:
    json.dump(RESULTS, f, indent=2, default=str)
print(f"\nDados salvos em: {results_path}")
