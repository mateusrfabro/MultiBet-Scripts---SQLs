"""
Análise exploratória de insights e detecção de anomalias — MultiBet
"""
import sys
sys.path.insert(0, '.')
from db.redshift import query_redshift
import pandas as pd

pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 300)
pd.set_option('display.float_format', '{:,.2f}'.format)

# ============================================================
# 1) TOP 30 JOGADORES COM MAIOR LUCRO (possíveis fraudadores)
# ============================================================
sql_players = """
SELECT
    e.c_external_id,
    LEFT(e.c_email_id, 3) || '***' AS email_mask,
    e.c_signup_time::date AS registro,
    COUNT(CASE WHEN f.c_txn_type = 27 THEN 1 END) AS qtd_apostas,
    COUNT(CASE WHEN f.c_txn_type = 45 THEN 1 END) AS qtd_wins,
    SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) / 100.0 AS apostado_brl,
    SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END) / 100.0 AS ganho_brl,
    (SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END)
     - SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END)) / 100.0 AS lucro_jogador_brl,
    ROUND(SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END)::FLOAT
          / NULLIF(SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END), 0) * 100, 2) AS win_rate_pct
FROM fund.tbl_real_fund_txn f
JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (27, 45)
  AND f.c_start_time >= DATEADD('day', -30, CURRENT_DATE)
GROUP BY 1, 2, 3
HAVING SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) > 100000
ORDER BY lucro_jogador_brl DESC
LIMIT 30
"""

print('=== TOP 30 JOGADORES COM MAIOR LUCRO (últimos 30 dias) ===')
print('(Win rate > 100% = jogador lucrando mais do que aposta)\n')
df1 = query_redshift(sql_players)
print(df1.to_string(index=False))

# ============================================================
# 2) JOGADORES COM WIN RATE MUITO ALTO E VOLUME SIGNIFICATIVO
#    (red flag de fraude — abuso de bug, conluio, etc.)
# ============================================================
sql_winrate = """
SELECT
    e.c_external_id,
    e.c_signup_time::date AS registro,
    COUNT(CASE WHEN f.c_txn_type = 27 THEN 1 END) AS qtd_apostas,
    SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) / 100.0 AS apostado_brl,
    SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END) / 100.0 AS ganho_brl,
    ROUND(SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END)::FLOAT
          / NULLIF(SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END), 0) * 100, 2) AS win_rate_pct,
    -- Dias ativos
    COUNT(DISTINCT f.c_start_time::date) AS dias_ativos
FROM fund.tbl_real_fund_txn f
JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
WHERE f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (27, 45)
  AND f.c_start_time >= DATEADD('day', -30, CURRENT_DATE)
GROUP BY 1, 2
HAVING SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) > 50000
   AND (SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END)::FLOAT
        / NULLIF(SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END), 0)) > 1.5
ORDER BY win_rate_pct DESC
LIMIT 20
"""

print('\n\n=== JOGADORES COM WIN RATE > 150% e R$500+ apostados (RED FLAG) ===\n')
df2 = query_redshift(sql_winrate)
print(df2.to_string(index=False))

# ============================================================
# 3) VARIAÇÃO BRUSCA DO GGR DIA A DIA (outliers)
# ============================================================
sql_var = """
WITH daily AS (
    SELECT
        f.c_start_time::date AS dia,
        (SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END)
         - COALESCE(SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END), 0)) / 100.0 AS ggr_brl
    FROM fund.tbl_real_fund_txn f
    WHERE f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN (27, 45)
      AND f.c_start_time >= DATEADD('day', -60, CURRENT_DATE)
    GROUP BY 1
)
SELECT
    dia,
    ggr_brl,
    LAG(ggr_brl) OVER (ORDER BY dia) AS ggr_anterior,
    ROUND((ggr_brl - LAG(ggr_brl) OVER (ORDER BY dia))
          / NULLIF(ABS(LAG(ggr_brl) OVER (ORDER BY dia)), 0) * 100, 2) AS variacao_pct
FROM daily
ORDER BY dia DESC
LIMIT 30
"""

print('\n\n=== VARIAÇÃO DIÁRIA DO GGR (últimos 30 dias) ===\n')
df3 = query_redshift(sql_var)
print(df3.to_string(index=False))

# ============================================================
# 4) CONCENTRAÇÃO: jogadores que representam % desproporcional do GGR negativo
# ============================================================
sql_concentracao = """
WITH player_ggr AS (
    SELECT
        f.c_ecr_id,
        e.c_external_id,
        (SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END)
         - COALESCE(SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy END), 0)) / 100.0 AS ggr_jogador
    FROM fund.tbl_real_fund_txn f
    JOIN ecr.tbl_ecr e ON e.c_ecr_id = f.c_ecr_id
    WHERE f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN (27, 45)
      AND f.c_start_time >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY 1, 2
    HAVING SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy END) > 0
)
SELECT
    c_external_id,
    ggr_jogador,
    ROUND(ggr_jogador / (SELECT SUM(ggr_jogador) FROM player_ggr) * 100, 4) AS pct_do_ggr_total
FROM player_ggr
WHERE ggr_jogador < 0
ORDER BY ggr_jogador ASC
LIMIT 20
"""

print('\n\n=== TOP 20 JOGADORES COM MAIOR GGR NEGATIVO (casa perdendo para eles) ===\n')
df4 = query_redshift(sql_concentracao)
print(df4.to_string(index=False))