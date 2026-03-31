#!/usr/bin/env python3
"""Descobre colunas exatas das tabelas ps_bi para a análise de segundas."""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db.athena import query_athena

tables = [
    "fct_player_activity_daily",
    "fct_deposits_daily",
    "fct_deposits_hourly",
    "fct_cashout_daily",
    "fct_casino_activity_daily",
    "fct_player_count",
    "dim_game",
]

for t in tables:
    print(f"\n{'='*60}")
    print(f"TABLE: ps_bi.{t}")
    print(f"{'='*60}")
    try:
        df = query_athena(f"SELECT * FROM {t} LIMIT 0", database="ps_bi")
        for col in df.columns:
            print(f"  {col}")
    except Exception as e:
        print(f"  ERROR: {e}")

# Also check if 23/03 data exists in ps_bi
print(f"\n{'='*60}")
print("DATA AVAILABILITY CHECK")
print(f"{'='*60}")
try:
    df = query_athena("""
        SELECT activity_date, COUNT(*) as rows
        FROM fct_player_activity_daily
        WHERE activity_date IN (DATE '2026-03-23', DATE '2026-03-16', DATE '2026-03-09')
        GROUP BY activity_date
        ORDER BY activity_date DESC
    """, database="ps_bi")
    print(df.to_string(index=False))
except Exception as e:
    print(f"ERROR: {e}")
