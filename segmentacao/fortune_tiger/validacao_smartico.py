"""Validação cruzada Tigre Sortudo: Smartico (BigQuery) vs Redshift"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db.bigquery import query_bigquery

# Tigre Sortudo no Smartico = smr_game_id 45870229 (PRAGMATICPLAY)
sql = """
    SELECT
        COUNT(DISTINCT b.user_id) AS total_jogadores_smartico,
        COUNT(*) AS total_bets,
        SUM(CAST(b.casino_last_bet_amount_real AS FLOAT64)) AS total_bet_brl,
        SUM(CASE WHEN b.casino_is_rollback = true THEN 1 ELSE 0 END) AS qtd_rollbacks,
        SUM(CASE WHEN b.casino_is_rollback = true THEN CAST(b.casino_last_bet_amount_real AS FLOAT64) ELSE 0 END) AS total_rollback_brl
    FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
    WHERE b.casino_last_bet_game_name = 45870229
      AND b.event_time BETWEEN '2026-03-07 19:00:00' AND '2026-03-09 02:59:59'
"""

df = query_bigquery(sql)
print("=== SMARTICO (BigQuery) - Tigre Sortudo - Periodo completo ===")
print(df.to_string())

total_bet = df.iloc[0]["total_bet_brl"]
total_rb = df.iloc[0]["total_rollback_brl"]
print(f"\nNet Bet Smartico: R$ {total_bet - total_rb:.2f}")

print("\n=== COMPARATIVO (Redshift) ===")
print("Redshift: 117.009 bets | R$ 118.134,50 total bet | 4 rollbacks (R$ 7,50)")
print(f"Redshift Net Bet: R$ {118134.50 - 7.50:.2f}")
