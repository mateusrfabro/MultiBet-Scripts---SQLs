"""
Análise completa do Sportsbook — 17/03/2026
Agora com detalhe de odds, eventos, esportes e mercados.
"""

import sys
sys.path.insert(0, ".")
from db.athena import query_athena
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_colwidth", 50)
pd.set_option("display.max_rows", 60)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# ──────────────────────────────────────────────────────────
# ETAPA 1: Top 30 maiores PAYOUTS com detalhe completo
# Join: tbl_sports_book_info (pagamentos) + tbl_sports_book_bet_details (odds/evento)
# + tbl_sports_book_bets_info (stake/return/tipo de aposta)
# ──────────────────────────────────────────────────────────
print("=" * 120)
print("ETAPA 1 — Top 30 maiores pagamentos COM detalhe de evento, odds, mercado")
print("=" * 120)

sql_top_detail = """
SELECT
    p.c_customer_id,
    p.c_amount AS payout_valor,
    b.c_total_stake AS stake,
    b.c_total_return AS retorno_total,
    b.c_total_odds AS odds_total,
    b.c_bet_type,
    b.c_is_live AS live_bet,
    d.c_sport_type_name AS esporte,
    d.c_tournament_name AS torneio,
    d.c_event_name AS evento,
    d.c_vs_participant_home AS time_casa,
    d.c_vs_participant_away AS time_fora,
    d.c_market_name AS mercado,
    d.c_selection_name AS selecao,
    d.c_odds AS odd_leg,
    d.c_leg_status AS status_leg,
    p.c_time_stamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS hora_payout_brt,
    p.c_bet_slip_id
FROM vendor_ec2.tbl_sports_book_info p
LEFT JOIN vendor_ec2.tbl_sports_book_bets_info b
    ON p.c_bet_slip_id = b.c_bet_slip_id
    AND p.c_customer_id = b.c_customer_id
LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
    ON p.c_bet_slip_id = d.c_bet_slip_id
    AND p.c_customer_id = d.c_customer_id
WHERE p.c_time_stamp >= TIMESTAMP '2026-03-17 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_time_stamp <  TIMESTAMP '2026-03-18 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_operation_type = 'P'
  AND p.c_amount > 0
ORDER BY p.c_amount DESC
LIMIT 30
"""
df_top = query_athena(sql_top_detail, database="vendor_ec2")
print(df_top.to_string(index=False))

# ──────────────────────────────────────────────────────────
# ETAPA 2: Top esportes por volume de payouts
# ──────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("ETAPA 2 — Payouts por esporte")
print("=" * 120)

sql_esportes = """
SELECT
    COALESCE(d.c_sport_type_name, 'N/A') AS esporte,
    COUNT(DISTINCT p.c_bet_slip_id) AS qtd_bilhetes,
    COUNT(DISTINCT p.c_customer_id) AS clientes,
    SUM(p.c_amount) AS total_payout,
    AVG(p.c_amount) AS ticket_medio_payout
FROM vendor_ec2.tbl_sports_book_info p
LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
    ON p.c_bet_slip_id = d.c_bet_slip_id
    AND p.c_customer_id = d.c_customer_id
WHERE p.c_time_stamp >= TIMESTAMP '2026-03-17 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_time_stamp <  TIMESTAMP '2026-03-18 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_operation_type = 'P'
  AND p.c_amount > 0
GROUP BY 1
ORDER BY total_payout DESC
"""
df_esportes = query_athena(sql_esportes, database="vendor_ec2")
print(df_esportes.to_string(index=False))

# ──────────────────────────────────────────────────────────
# ETAPA 3: Top torneios/ligas por payout
# ──────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("ETAPA 3 — Top 20 torneios por total de payouts")
print("=" * 120)

sql_torneios = """
SELECT
    COALESCE(d.c_sport_type_name, 'N/A') AS esporte,
    COALESCE(d.c_tournament_name, 'N/A') AS torneio,
    COUNT(DISTINCT p.c_bet_slip_id) AS qtd_bilhetes,
    COUNT(DISTINCT p.c_customer_id) AS clientes,
    SUM(p.c_amount) AS total_payout,
    MAX(p.c_amount) AS maior_payout
FROM vendor_ec2.tbl_sports_book_info p
LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
    ON p.c_bet_slip_id = d.c_bet_slip_id
    AND p.c_customer_id = d.c_customer_id
WHERE p.c_time_stamp >= TIMESTAMP '2026-03-17 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_time_stamp <  TIMESTAMP '2026-03-18 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_operation_type = 'P'
  AND p.c_amount > 0
GROUP BY 1, 2
ORDER BY total_payout DESC
LIMIT 20
"""
df_torneios = query_athena(sql_torneios, database="vendor_ec2")
print(df_torneios.to_string(index=False))

# ──────────────────────────────────────────────────────────
# ETAPA 4: Top 20 jogadores — lucro líquido COM detalhe
# ──────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("ETAPA 4 — Top 20 jogadores por LUCRO com detalhe do maior payout")
print("=" * 120)

sql_lucro = """
WITH player_pnl AS (
    SELECT
        c_customer_id,
        SUM(CASE WHEN c_operation_type = 'M' THEN c_amount ELSE 0 END) AS total_apostado,
        SUM(CASE WHEN c_operation_type = 'P' THEN c_amount ELSE 0 END) AS total_ganho,
        COUNT(CASE WHEN c_operation_type = 'M' THEN 1 END) AS qtd_apostas,
        COUNT(CASE WHEN c_operation_type = 'P' THEN 1 END) AS qtd_payouts
    FROM vendor_ec2.tbl_sports_book_info
    WHERE c_time_stamp >= TIMESTAMP '2026-03-17 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
      AND c_time_stamp <  TIMESTAMP '2026-03-18 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
    GROUP BY c_customer_id
)
SELECT
    pp.c_customer_id,
    pp.total_apostado,
    pp.total_ganho,
    (pp.total_ganho - pp.total_apostado) AS lucro,
    pp.qtd_apostas,
    pp.qtd_payouts,
    CASE WHEN pp.total_apostado > 0
         THEN ROUND(pp.total_ganho / pp.total_apostado * 100, 1)
         ELSE 0 END AS pct_retorno
FROM player_pnl pp
WHERE pp.total_ganho > 0
ORDER BY lucro DESC
LIMIT 20
"""
df_lucro = query_athena(sql_lucro, database="vendor_ec2")
print(df_lucro.to_string(index=False))

# ──────────────────────────────────────────────────────────
# ETAPA 5: Detalhe dos bilhetes dos top 5 jogadores
# ──────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("ETAPA 5 — Bilhetes detalhados dos top 5 jogadores com maior lucro")
print("=" * 120)

top5_ids = df_lucro["c_customer_id"].head(5).tolist()
top5_str = ",".join(str(x) for x in top5_ids)

sql_bilhetes_top5 = f"""
SELECT
    b.c_customer_id,
    b.c_bet_slip_id,
    b.c_bet_type,
    b.c_total_stake AS stake,
    b.c_total_return AS retorno,
    b.c_total_odds AS odds_total,
    b.c_is_live AS ao_vivo,
    d.c_sport_type_name AS esporte,
    d.c_tournament_name AS torneio,
    d.c_event_name AS evento,
    d.c_market_name AS mercado,
    d.c_selection_name AS selecao,
    d.c_odds AS odd_leg,
    d.c_leg_status AS status,
    b.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS hora_aposta_brt
FROM vendor_ec2.tbl_sports_book_bets_info b
LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
    ON b.c_bet_slip_id = d.c_bet_slip_id
    AND b.c_customer_id = d.c_customer_id
WHERE b.c_customer_id IN ({top5_str})
  AND b.c_created_time >= TIMESTAMP '2026-03-16 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND b.c_created_time <  TIMESTAMP '2026-03-18 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
ORDER BY b.c_customer_id, b.c_created_time
"""
df_bilhetes = query_athena(sql_bilhetes_top5, database="vendor_ec2")
print(df_bilhetes.to_string(index=False))

# ──────────────────────────────────────────────────────────
# ETAPA 6: Live vs Pre-match (proporção de payouts)
# ──────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("ETAPA 6 — Payouts: Live vs Pre-match")
print("=" * 120)

sql_live = """
SELECT
    CASE WHEN d.c_is_live = true THEN 'AO VIVO' ELSE 'PRE-MATCH' END AS tipo,
    COUNT(DISTINCT p.c_bet_slip_id) AS qtd_bilhetes,
    COUNT(DISTINCT p.c_customer_id) AS clientes,
    SUM(p.c_amount) AS total_payout,
    AVG(p.c_amount) AS ticket_medio
FROM vendor_ec2.tbl_sports_book_info p
LEFT JOIN vendor_ec2.tbl_sports_book_bet_details d
    ON p.c_bet_slip_id = d.c_bet_slip_id
    AND p.c_customer_id = d.c_customer_id
WHERE p.c_time_stamp >= TIMESTAMP '2026-03-17 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_time_stamp <  TIMESTAMP '2026-03-18 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_operation_type = 'P'
  AND p.c_amount > 0
GROUP BY 1
ORDER BY total_payout DESC
"""
df_live = query_athena(sql_live, database="vendor_ec2")
print(df_live.to_string(index=False))

# ──────────────────────────────────────────────────────────
# ETAPA 7: Distribuição por faixa de odds
# ──────────────────────────────────────────────────────────
print("\n" + "=" * 120)
print("ETAPA 7 — Payouts por faixa de odds")
print("=" * 120)

sql_faixa_odds = """
SELECT
    CASE
        WHEN CAST(b.c_total_odds AS DOUBLE) < 1.5 THEN '< 1.50'
        WHEN CAST(b.c_total_odds AS DOUBLE) < 2.0 THEN '1.50 - 1.99'
        WHEN CAST(b.c_total_odds AS DOUBLE) < 3.0 THEN '2.00 - 2.99'
        WHEN CAST(b.c_total_odds AS DOUBLE) < 5.0 THEN '3.00 - 4.99'
        WHEN CAST(b.c_total_odds AS DOUBLE) < 10.0 THEN '5.00 - 9.99'
        WHEN CAST(b.c_total_odds AS DOUBLE) < 50.0 THEN '10.00 - 49.99'
        ELSE '50+'
    END AS faixa_odds,
    COUNT(DISTINCT p.c_bet_slip_id) AS qtd_bilhetes,
    SUM(p.c_amount) AS total_payout,
    AVG(p.c_amount) AS ticket_medio
FROM vendor_ec2.tbl_sports_book_info p
JOIN vendor_ec2.tbl_sports_book_bets_info b
    ON p.c_bet_slip_id = b.c_bet_slip_id
    AND p.c_customer_id = b.c_customer_id
WHERE p.c_time_stamp >= TIMESTAMP '2026-03-17 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_time_stamp <  TIMESTAMP '2026-03-18 00:00:00' AT TIME ZONE 'America/Sao_Paulo'
  AND p.c_operation_type = 'P'
  AND p.c_amount > 0
  AND b.c_total_odds IS NOT NULL
GROUP BY 1
ORDER BY 1
"""
df_odds = query_athena(sql_faixa_odds, database="vendor_ec2")
print(df_odds.to_string(index=False))

print("\n" + "=" * 120)
print("ANALISE COMPLETA FINALIZADA")
print("=" * 120)