"""
Aprofundamento: jogadores com ZERO wins + jogos específicos deles
+ análise de queda do GGR de fevereiro para março
"""
import sys
sys.path.insert(0, '.')
from db.redshift import query_redshift
import pandas as pd

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 300)
pd.set_option('display.float_format', '{:,.2f}'.format)

# ============================================================
# 1) DETALHE DOS JOGADORES COM ZERO WINS E VOLUME > R$10K
#    (potencial lavagem de dinheiro ou bug de dados)
# ============================================================
sql_zero_wins = """
SELECT
    e.c_external_id,
    e.c_signup_time::date AS registro,
    DATEDIFF('day', e.c_signup_time, CURRENT_DATE) AS dias_desde_registro,
    COUNT(CASE WHEN f.c_txn_type = 27 THEN 1 END) AS qtd_apostas,
    COUNT(CASE WHEN f.c_txn_type = 45 THEN 1 END) AS qtd_wins,
    COUNT(CASE WHEN f.c_txn_type = 72 THEN 1 END) AS qtd_rollbacks,
    SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) / 100.0 AS apostado_brl,
    COALESCE(SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END), 0) / 100.0 AS ganho_brl,
    COUNT(DISTINCT f.c_game_id) AS jogos_distintos,
    COUNT(DISTINCT f.c_start_time::date) AS dias_ativos,
    e.c_ip AS ip_registro
FROM fund.tbl_real_fund_txn f
JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (27, 45, 72)
  AND f.c_start_time >= DATEADD('day', -30, CURRENT_DATE)
GROUP BY 1, 2, 3, 11
HAVING COUNT(CASE WHEN f.c_txn_type = 45 THEN 1 END) = 0
   AND SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) > 1000000
ORDER BY apostado_brl DESC
LIMIT 20
"""

print('=== JOGADORES COM ZERO WINS E APOSTAS > R$10K (últimos 30 dias) ===')
print('ALERTA: Pode indicar lavagem de dinheiro ou bug nos dados\n')
df1 = query_redshift(sql_zero_wins)
print(df1.to_string(index=False))

# ============================================================
# 2) Em quais jogos esses jogadores apostam?
# ============================================================
sql_jogos_zero_win = """
WITH zero_win_players AS (
    SELECT f.c_ecr_id
    FROM fund.tbl_real_fund_txn f
    WHERE f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN (27, 45)
      AND f.c_start_time >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY 1
    HAVING COUNT(CASE WHEN f.c_txn_type = 45 THEN 1 END) = 0
       AND SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) > 1000000
)
SELECT
    g.c_game_desc AS jogo,
    g.c_vendor_id AS provider,
    COUNT(*) AS qtd_apostas,
    COUNT(DISTINCT f.c_ecr_id) AS jogadores,
    SUM(f.c_amount_in_ecr_ccy) / 100.0 AS total_apostado_brl
FROM fund.tbl_real_fund_txn f
JOIN zero_win_players zw ON zw.c_ecr_id = f.c_ecr_id
LEFT JOIN bireports.tbl_vendor_games_mapping_data g ON g.c_game_id = f.c_game_id
WHERE f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type = 27
  AND f.c_start_time >= DATEADD('day', -30, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY total_apostado_brl DESC
LIMIT 15
"""

print('\n\n=== JOGOS ONDE JOGADORES "ZERO WIN" APOSTAM ===\n')
df2 = query_redshift(sql_jogos_zero_win)
print(df2.to_string(index=False))

# ============================================================
# 3) Comparativo semanal GGR (tendência de queda Fev -> Mar)
# ============================================================
sql_semanal = """
SELECT
    DATE_TRUNC('week', f.c_start_time)::date AS semana,
    COUNT(CASE WHEN f.c_txn_type = 27 THEN 1 END) AS qtd_apostas,
    COUNT(DISTINCT f.c_ecr_id) AS jogadores_unicos,
    SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) / 100.0 AS apostado_brl,
    SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END) / 100.0 AS ganho_brl,
    (SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END)
     - COALESCE(SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END), 0)) / 100.0 AS ggr_brl,
    ROUND(
      (SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END)
       - COALESCE(SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END), 0))::FLOAT
      / NULLIF(SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END), 0) * 100, 2
    ) AS hold_rate_pct
FROM fund.tbl_real_fund_txn f
WHERE f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (27, 45)
  AND f.c_start_time >= DATEADD('day', -60, CURRENT_DATE)
GROUP BY 1
ORDER BY 1 DESC
"""

print('\n\n=== GGR SEMANAL (últimas 8 semanas) ===\n')
df3 = query_redshift(sql_semanal)
print(df3.to_string(index=False))

# ============================================================
# 4) Player 30413583 — win rate 1853% — detalhes
# ============================================================
sql_suspect = """
SELECT
    f.c_start_time::date AS dia,
    g.c_game_desc AS jogo,
    g.c_vendor_id AS provider,
    f.c_txn_type,
    COUNT(*) AS qtd_txns,
    SUM(f.c_amount_in_ecr_ccy) / 100.0 AS valor_brl
FROM fund.tbl_real_fund_txn f
JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
LEFT JOIN bireports.tbl_vendor_games_mapping_data g ON g.c_game_id = f.c_game_id
WHERE e.c_external_id = '30413583'
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (27, 45)
  AND f.c_start_time >= DATEADD('day', -30, CURRENT_DATE)
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 4
"""

print('\n\n=== DETALHE PLAYER 30413583 (WIN RATE 1853%) ===\n')
df4 = query_redshift(sql_suspect)
print(df4.to_string(index=False))