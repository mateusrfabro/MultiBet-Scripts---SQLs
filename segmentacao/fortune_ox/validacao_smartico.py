"""
Validação cruzada Fortune Ox: Smartico (BigQuery) vs Redshift
Promoção: RETEM_PROMO_RELAMPAGO_120326
Período: 12/03/2026 18h–22h BRT (21h–01h UTC)

Padrão idêntico ao validacao_smartico.py do Fortune Tiger/Tigre Sortudo.
Fortune Ox no Smartico = casino_last_bet_game_name = smr_game_id = 45846458
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from db.bigquery import query_bigquery
import pandas as pd

MARK_TAG       = "RETEM_PROMO_RELAMPAGO_120326"
FORTUNE_OX_SMR = 45846458   # smr_game_id = casino_last_bet_game_name no tr_casino_bet

# ── 1. BigQuery: apenas usuários marcados, jogo Fortune Ox, no período ───────
sql_bq = f"""
SELECT
    COUNT(DISTINCT b.user_id)                                              AS total_jogadores_bq,
    COUNT(*)                                                               AS total_bets,
    SUM(CAST(b.casino_last_bet_amount_real AS FLOAT64))                    AS total_bet_brl,
    SUM(CASE WHEN b.casino_is_rollback = true THEN 1     ELSE 0 END)      AS qtd_rollbacks,
    SUM(CASE WHEN b.casino_is_rollback = true
             THEN CAST(b.casino_last_bet_amount_real AS FLOAT64) ELSE 0 END)
                                                                           AS total_rollback_brl
FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
INNER JOIN `smartico-bq6.dwh_ext_24105.j_user` u
    ON u.user_id = b.user_id
WHERE b.casino_last_bet_game_name = {FORTUNE_OX_SMR}
  AND b.event_time BETWEEN '2026-03-12 21:00:00' AND '2026-03-13 01:00:00'
  AND '{MARK_TAG}' IN UNNEST(u.core_tags)
"""

df_bq = query_bigquery(sql_bq)
print("=== SMARTICO (BigQuery) — Fortune Ox — Marcados RETEM_PROMO_RELAMPAGO_120326 ===")
print(df_bq.to_string())

total_bet_bq = float(df_bq.iloc[0]["total_bet_brl"] or 0)
total_rb_bq  = float(df_bq.iloc[0]["total_rollback_brl"] or 0)
n_jogadores_bq = int(df_bq.iloc[0]["total_jogadores_bq"] or 0)
net_bet_bq   = total_bet_bq - total_rb_bq
print(f"\nNet Bet Smartico:   R$ {net_bet_bq:,.2f}")

# ── 2. Comparativo com Redshift ───────────────────────────────────────────────
# Valores vindos do CSV gerado pelo segmentacao_fortune_ox.py
RS_JOGADORES  = 218
RS_TOTAL_BET  = 30326.00
RS_ROLLBACKS  = 0
RS_NET_BET    = 30326.00

print("\n=== REDSHIFT (Pragmatic Solutions) — Fortune Ox — Marcados ===")
print(f"Jogadores:      {RS_JOGADORES:,}")
print(f"Total apostado: R$ {RS_TOTAL_BET:,.2f}")
print(f"Rollbacks:      {RS_ROLLBACKS}")
print(f"Net Bet:        R$ {RS_NET_BET:,.2f}")

# ── 3. Delta ──────────────────────────────────────────────────────────────────
diff_jogadores = abs(n_jogadores_bq - RS_JOGADORES)
diff_valor     = abs(net_bet_bq - RS_NET_BET)
pct_diff       = diff_valor / RS_NET_BET * 100 if RS_NET_BET > 0 else 0

print("\n=== COMPARATIVO FINAL ===")
print(f"{'Fonte':<25} {'Jogadores':>12} {'Net Bet':>14}")
print("-" * 55)
print(f"{'BigQuery (Smartico)':<25} {n_jogadores_bq:>12,} {'R$ ' + f'{net_bet_bq:,.2f}':>14}")
print(f"{'Redshift (Pragmatic)':<25} {RS_JOGADORES:>12,} {'R$ ' + f'{RS_NET_BET:,.2f}':>14}")
print("-" * 55)
print(f"{'Diferença':<25} {diff_jogadores:>12,} {'R$ ' + f'{diff_valor:,.2f}':>14}  ({pct_diff:.2f}%)")

if pct_diff <= 1.0:
    print("\n→ DADOS CONSISTENTES (diferença dentro de 1%)")
else:
    print(f"\n→ ATENÇÃO: diferença de {pct_diff:.2f}% — investigar antes de entregar.")
