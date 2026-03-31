#!/usr/bin/env python3
"""
Validacao cruzada BigQuery Smartico vs Athena
Segundas: 23/03, 16/03, 09/03/2026
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.bigquery import query_bigquery
from db.athena import query_athena
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.width', 200)
pd.set_option('display.float_format', '{:,.2f}'.format)

def log(msg=""):
    print(msg)

DATAS = ['2026-03-23', '2026-03-16', '2026-03-09']
BQ_DS = "smartico-bq6.dwh_ext_24105"

log("=" * 95)
log("VALIDACAO CRUZADA: BigQuery Smartico vs Athena")
log("Datas: 23/03, 16/03, 09/03/2026")
log("=" * 95)

# ============================================================
# 1. Discover j_user columns (FTD, registration)
# ============================================================
log("\n--- Descobrindo colunas j_user ---")
try:
    df_cols = query_bigquery(f"""
        SELECT column_name, data_type
        FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'j_user'
        ORDER BY ordinal_position
    """)
    # Filter for date/time and key columns
    date_cols = df_cols[df_cols['column_name'].str.contains('date|time|ftd|regist|created|first', case=False)]
    log("Colunas com data/FTD/registro:")
    for _, r in date_cols.iterrows():
        log(f"  {r['column_name']}: {r['data_type']}")
except Exception as e:
    log(f"  Erro INFORMATION_SCHEMA: {e}")
    log("  Tentando SELECT * LIMIT 0...")
    try:
        df_sample = query_bigquery(f"SELECT * FROM `{BQ_DS}.j_user` LIMIT 1")
        for c in df_sample.columns:
            log(f"  {c}")
    except Exception as e2:
        log(f"  Erro: {e2}")

# ============================================================
# 2. FTDs por dia (BigQuery)
# ============================================================
log("\n--- FTDs por dia (BigQuery j_user) ---")
try:
    df_ftd_bq = query_bigquery(f"""
        SELECT
            DATE(ftd_date) AS dt,
            COUNT(*) AS ftds_bq
        FROM `{BQ_DS}.j_user`
        WHERE DATE(ftd_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    log(df_ftd_bq.to_string(index=False))
except Exception as e:
    log(f"  Erro ftd_date: {e}")
    # Try alternative column names
    log("  Tentando core_ftd_date...")
    try:
        df_ftd_bq = query_bigquery(f"""
            SELECT
                DATE(core_ftd_date) AS dt,
                COUNT(*) AS ftds_bq
            FROM `{BQ_DS}.j_user`
            WHERE DATE(core_ftd_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
            GROUP BY 1
            ORDER BY 1 DESC
        """)
        log(df_ftd_bq.to_string(index=False))
    except Exception as e2:
        log(f"  Erro core_ftd_date: {e2}")
        df_ftd_bq = None

# ============================================================
# 3. Registros por dia (BigQuery)
# ============================================================
log("\n--- Registros por dia (BigQuery j_user) ---")
try:
    df_reg_bq = query_bigquery(f"""
        SELECT
            DATE(registration_date) AS dt,
            COUNT(*) AS registros_bq
        FROM `{BQ_DS}.j_user`
        WHERE DATE(registration_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    log(df_reg_bq.to_string(index=False))
except Exception as e:
    log(f"  Erro registration_date: {e}")
    log("  Tentando core_registration_date...")
    try:
        df_reg_bq = query_bigquery(f"""
            SELECT
                DATE(core_registration_date) AS dt,
                COUNT(*) AS registros_bq
            FROM `{BQ_DS}.j_user`
            WHERE DATE(core_registration_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
            GROUP BY 1
            ORDER BY 1 DESC
        """)
        log(df_reg_bq.to_string(index=False))
    except Exception as e2:
        log(f"  Erro: {e2}")
        df_reg_bq = None

# ============================================================
# 4. Depositos por dia (BigQuery tr_acc_deposit_approved)
# ============================================================
log("\n--- Depositos por dia (BigQuery tr_acc_deposit_approved) ---")
try:
    # First discover columns
    df_dep_cols = query_bigquery(f"""
        SELECT * FROM `{BQ_DS}.tr_acc_deposit_approved` LIMIT 1
    """)
    log(f"  Colunas: {list(df_dep_cols.columns)}")

    df_dep_bq = query_bigquery(f"""
        SELECT
            DATE(event_date) AS dt,
            COUNT(*) AS dep_count_bq,
            SUM(amount) AS dep_amount_bq
        FROM `{BQ_DS}.tr_acc_deposit_approved`
        WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    log(df_dep_bq.to_string(index=False))
except Exception as e:
    log(f"  Erro: {e}")
    df_dep_bq = None

# ============================================================
# 5. Logins por dia (BigQuery tr_login)
# ============================================================
log("\n--- Logins por dia (BigQuery tr_login) ---")
try:
    df_login_bq = query_bigquery(f"""
        SELECT
            DATE(event_date) AS dt,
            COUNT(*) AS login_events_bq,
            COUNT(DISTINCT user_ext_id) AS unique_logins_bq
        FROM `{BQ_DS}.tr_login`
        WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    log(df_login_bq.to_string(index=False))
except Exception as e:
    log(f"  Erro: {e}")
    df_login_bq = None

# ============================================================
# 6. Casino bets/wins (BigQuery tr_casino_bet + tr_casino_win)
# ============================================================
log("\n--- Casino Bets por dia (BigQuery tr_casino_bet) ---")
try:
    df_casino_bq = query_bigquery(f"""
        SELECT
            DATE(event_date) AS dt,
            COUNT(*) AS bet_count_bq,
            SUM(amount) AS bet_amount_bq
        FROM `{BQ_DS}.tr_casino_bet`
        WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    log(df_casino_bq.to_string(index=False))
except Exception as e:
    log(f"  Erro: {e}")
    df_casino_bq = None

log("\n--- Casino Wins por dia (BigQuery tr_casino_win) ---")
try:
    df_casino_win_bq = query_bigquery(f"""
        SELECT
            DATE(event_date) AS dt,
            COUNT(*) AS win_count_bq,
            SUM(amount) AS win_amount_bq
        FROM `{BQ_DS}.tr_casino_win`
        WHERE DATE(event_date) IN ('2026-03-23', '2026-03-16', '2026-03-09')
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    log(df_casino_win_bq.to_string(index=False))
except Exception as e:
    log(f"  Erro: {e}")
    df_casino_win_bq = None

# ============================================================
# 7. COMPARATIVO FINAL
# ============================================================
log("\n" + "=" * 95)
log("COMPARATIVO: BigQuery Smartico vs Athena bireports_ec2 vs Athena ps_bi")
log("=" * 95)

# Athena values (from our earlier queries)
athena_data = {
    '2026-03-23': {'ftd_psbi': 274, 'nrc_psbi': 923, 'dep_psbi': 352365, 'ftd_bi': 294, 'nrc_bi': 980, 'dep_bi': 458570},
    '2026-03-16': {'ftd_psbi': 958, 'nrc_psbi': 2206, 'dep_psbi': 1248223, 'ftd_bi': 955, 'nrc_bi': 2204, 'dep_bi': 1010422},
    '2026-03-09': {'ftd_psbi': 827, 'nrc_psbi': 1984, 'dep_psbi': 1217570, 'ftd_bi': 827, 'nrc_bi': 1984, 'dep_bi': 1227577},
}

log(f"\n  {'Data':<12} | {'Metrica':<15} | {'BigQuery':>14} | {'Athena ps_bi':>14} | {'Athena bireports':>16} | {'BQ vs ps_bi':>12} | {'BQ vs bireports':>15}")
log(f"  {'-'*12}-+-{'-'*15}-+-{'-'*14}-+-{'-'*14}-+-{'-'*16}-+-{'-'*12}-+-{'-'*15}")

for dt in DATAS:
    a = athena_data.get(dt, {})

    # FTDs
    ftd_bq = None
    if df_ftd_bq is not None and len(df_ftd_bq) > 0:
        match = df_ftd_bq[df_ftd_bq['dt'].astype(str) == dt]
        if len(match) > 0:
            ftd_bq = int(match.iloc[0]['ftds_bq'])

    ftd_psbi = a.get('ftd_psbi', '')
    ftd_bi = a.get('ftd_bi', '')

    bq_str = str(ftd_bq) if ftd_bq is not None else 'N/A'
    diff_psbi = f"{((ftd_bq - ftd_psbi) / ftd_psbi * 100):+.1f}%" if ftd_bq and ftd_psbi else 'N/A'
    diff_bi = f"{((ftd_bq - ftd_bi) / ftd_bi * 100):+.1f}%" if ftd_bq and ftd_bi else 'N/A'
    log(f"  {dt:<12} | {'FTDs':<15} | {bq_str:>14} | {str(ftd_psbi):>14} | {str(ftd_bi):>16} | {diff_psbi:>12} | {diff_bi:>15}")

    # Registros
    reg_bq = None
    if df_reg_bq is not None and len(df_reg_bq) > 0:
        match = df_reg_bq[df_reg_bq['dt'].astype(str) == dt]
        if len(match) > 0:
            reg_bq = int(match.iloc[0]['registros_bq'])

    nrc_psbi = a.get('nrc_psbi', '')
    nrc_bi = a.get('nrc_bi', '')
    bq_str = str(reg_bq) if reg_bq is not None else 'N/A'
    diff_psbi = f"{((reg_bq - nrc_psbi) / nrc_psbi * 100):+.1f}%" if reg_bq and nrc_psbi else 'N/A'
    diff_bi = f"{((reg_bq - nrc_bi) / nrc_bi * 100):+.1f}%" if reg_bq and nrc_bi else 'N/A'
    log(f"  {dt:<12} | {'Registros':<15} | {bq_str:>14} | {str(nrc_psbi):>14} | {str(nrc_bi):>16} | {diff_psbi:>12} | {diff_bi:>15}")

    # Depositos
    dep_bq = None
    if df_dep_bq is not None and len(df_dep_bq) > 0:
        match = df_dep_bq[df_dep_bq['dt'].astype(str) == dt]
        if len(match) > 0:
            dep_bq = float(match.iloc[0]['dep_amount_bq'])

    dep_psbi = a.get('dep_psbi', '')
    dep_bi = a.get('dep_bi', '')
    bq_str = f"R$ {dep_bq:,.0f}" if dep_bq is not None else 'N/A'
    diff_psbi = f"{((dep_bq - dep_psbi) / dep_psbi * 100):+.1f}%" if dep_bq and dep_psbi else 'N/A'
    diff_bi = f"{((dep_bq - dep_bi) / dep_bi * 100):+.1f}%" if dep_bq and dep_bi else 'N/A'
    log(f"  {dt:<12} | {'Depositos R$':<15} | {bq_str:>14} | {'R$ '+str(f'{dep_psbi:,}'):>14} | {'R$ '+str(f'{dep_bi:,}'):>16} | {diff_psbi:>12} | {diff_bi:>15}")

    # Logins
    login_bq = None
    login_unique_bq = None
    if df_login_bq is not None and len(df_login_bq) > 0:
        match = df_login_bq[df_login_bq['dt'].astype(str) == dt]
        if len(match) > 0:
            login_bq = int(match.iloc[0]['login_events_bq'])
            login_unique_bq = int(match.iloc[0]['unique_logins_bq'])

    bq_str = f"{login_unique_bq:,}" if login_unique_bq is not None else 'N/A'
    log(f"  {dt:<12} | {'Logins (unique)':<15} | {bq_str:>14} | {'':>14} | {'':>16} | {'':>12} | {'':>15}")

    log(f"  {'-'*12}-+-{'-'*15}-+-{'-'*14}-+-{'-'*14}-+-{'-'*16}-+-{'-'*12}-+-{'-'*15}")

# Casino GGR comparison
if df_casino_bq is not None and df_casino_win_bq is not None:
    log(f"\n  CASINO GGR (BigQuery):")
    merged = df_casino_bq.merge(df_casino_win_bq, on='dt', how='outer').fillna(0)
    merged['ggr_bq'] = merged['bet_amount_bq'] - merged['win_amount_bq']
    for _, r in merged.iterrows():
        dt = str(r['dt'])
        log(f"  {dt}: Bets R$ {r['bet_amount_bq']:,.2f} | Wins R$ {r['win_amount_bq']:,.2f} | GGR R$ {r['ggr_bq']:,.2f}")

log("\n" + "=" * 95)
log("FIM DA VALIDACAO")
log("=" * 95)
