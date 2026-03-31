"""
Investigacao: Validar dados do alerta FTD (rollbacks suspeitos)
================================================================
Verifica se os jogadores reportados pelo alerta-ftd realmente
possuem os rollbacks indicados e analisa padroes completos.

Data do alerta: 2026-03-28
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from db.athena import query_athena

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)

# IDs do alerta (ecr_ids)
PLAYERS = [
    122650591789884041,
    545570261762335630,
    300472041791201457,
    551411774002817207,
    557272641791363992,
    818241774693292851,
]
IDS_STR = ", ".join(str(p) for p in PLAYERS)

# ── 1. Validar contagem de rollbacks hoje (BRT) ──
print("=" * 80)
print("1) CONTAGEM DE ROLLBACKS HOJE (BRT) — por jogador + jogo")
print("=" * 80)

sql_rollbacks = f"""
SELECT
    f.c_ecr_id,
    f.c_game_id,
    COALESCE(g.c_game_desc, CAST(f.c_game_id AS VARCHAR)) AS game_name,
    COUNT(*) AS rollback_count,
    SUM(f.c_amount_in_ecr_ccy) / 100.0 AS rollback_valor_brl,
    MIN(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_rb,
    MAX(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_rb
FROM fund_ec2.tbl_real_fund_txn f
LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data g
    ON CAST(f.c_game_id AS VARCHAR) = CAST(g.c_game_id AS VARCHAR)
WHERE f.c_ecr_id IN ({IDS_STR})
  AND f.c_txn_type = 72
  AND f.c_txn_status = 'SUCCESS'
  AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
GROUP BY f.c_ecr_id, f.c_game_id, g.c_game_desc
ORDER BY rollback_count DESC
"""
df1 = query_athena(sql_rollbacks, database="fund_ec2")
print(df1.to_string(index=False))
print()

# ── 2. Visao completa de transacoes HOJE por tipo (bets, wins, rollbacks) ──
print("=" * 80)
print("2) TRANSACOES COMPLETAS HOJE — por jogador e tipo")
print("   (27=Bet, 45=Win, 72=Rollback)")
print("=" * 80)

sql_full = f"""
SELECT
    f.c_ecr_id,
    f.c_txn_type,
    CASE f.c_txn_type
        WHEN 27 THEN 'BET'
        WHEN 45 THEN 'WIN'
        WHEN 72 THEN 'ROLLBACK'
        ELSE CAST(f.c_txn_type AS VARCHAR)
    END AS tipo,
    COUNT(*) AS qtd,
    SUM(f.c_amount_in_ecr_ccy) / 100.0 AS valor_brl,
    COUNT(DISTINCT f.c_game_id) AS jogos_distintos
FROM fund_ec2.tbl_real_fund_txn f
WHERE f.c_ecr_id IN ({IDS_STR})
  AND f.c_txn_status = 'SUCCESS'
  AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
GROUP BY f.c_ecr_id, f.c_txn_type
ORDER BY f.c_ecr_id, f.c_txn_type
"""
df2 = query_athena(sql_full, database="fund_ec2")
print(df2.to_string(index=False))
print()

# ── 3. Ratio rollback/bet — indicador de anomalia ──
print("=" * 80)
print("3) RATIO ROLLBACK/BET + GGR REAL por jogador (hoje)")
print("=" * 80)

sql_ratio = f"""
WITH txns AS (
    SELECT
        f.c_ecr_id,
        COUNT_IF(f.c_txn_type = 27) AS bets,
        COUNT_IF(f.c_txn_type = 45) AS wins,
        COUNT_IF(f.c_txn_type = 72) AS rollbacks,
        SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS bet_brl,
        SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS win_brl,
        SUM(CASE WHEN f.c_txn_type = 72 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS rb_brl
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_ecr_id IN ({IDS_STR})
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_product_id = 'CASINO'
      AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
    GROUP BY f.c_ecr_id
)
SELECT
    t.c_ecr_id,
    t.bets,
    t.wins,
    t.rollbacks,
    ROUND(CAST(t.rollbacks AS DOUBLE) / NULLIF(t.bets, 0) * 100, 1) AS pct_rb_sobre_bets,
    ROUND(t.bet_brl, 2) AS bet_brl,
    ROUND(t.win_brl, 2) AS win_brl,
    ROUND(t.rb_brl, 2) AS rb_brl,
    ROUND(t.bet_brl - t.rb_brl - t.win_brl, 2) AS ggr_real_brl
FROM txns t
ORDER BY t.rollbacks DESC
"""
df3 = query_athena(sql_ratio, database="fund_ec2")
print(df3.to_string(index=False))
print()

# ── 4. Historico dos ultimos 7 dias — esses players sao recorrentes? ──
print("=" * 80)
print("4) HISTORICO 7 DIAS — rollbacks por dia (esses players sao recorrentes?)")
print("=" * 80)

sql_hist = f"""
SELECT
    f.c_ecr_id,
    CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dia_brt,
    COUNT_IF(f.c_txn_type = 27) AS bets,
    COUNT_IF(f.c_txn_type = 72) AS rollbacks,
    ROUND(CAST(COUNT_IF(f.c_txn_type = 72) AS DOUBLE) / NULLIF(COUNT_IF(f.c_txn_type = 27), 0) * 100, 1) AS pct_rb
FROM fund_ec2.tbl_real_fund_txn f
WHERE f.c_ecr_id IN ({IDS_STR})
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_product_id = 'CASINO'
  AND f.c_start_time >= TIMESTAMP '2026-03-21 03:00:00'
GROUP BY f.c_ecr_id, CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
HAVING COUNT_IF(f.c_txn_type = 72) > 0
ORDER BY f.c_ecr_id, dia_brt DESC
"""
df4 = query_athena(sql_hist, database="fund_ec2")
print(df4.to_string(index=False))
print()

# ── 5. Info do jogador (cadastro, ultimo login, teste?) ──
print("=" * 80)
print("5) PERFIL DOS JOGADORES (cadastro, signup, test user?)")
print("=" * 80)

sql_profile = f"""
SELECT
    e.c_ecr_id,
    e.c_external_id,
    e.c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS signup_brt,
    e.c_last_login_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS last_login_brt,
    e.c_test_user
FROM bireports_ec2.tbl_ecr e
WHERE e.c_ecr_id IN ({IDS_STR})
"""
df5 = query_athena(sql_profile, database="bireports_ec2")
print(df5.to_string(index=False))
print()

# ── 6. Verificar se rollbacks tem padrao temporal (burst?) ──
print("=" * 80)
print("6) TIMELINE DE ROLLBACKS HOJE — janelas de 30min (burst detection)")
print("=" * 80)

sql_timeline = f"""
SELECT
    f.c_ecr_id,
    date_trunc('hour', f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
        + INTERVAL '30' MINUTE * CAST(
            FLOOR(MINUTE(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') / 30.0) AS INTEGER
          ) AS bloco_30min,
    COUNT(*) AS rollbacks,
    SUM(f.c_amount_in_ecr_ccy) / 100.0 AS valor_brl
FROM fund_ec2.tbl_real_fund_txn f
WHERE f.c_ecr_id IN ({IDS_STR})
  AND f.c_txn_type = 72
  AND f.c_txn_status = 'SUCCESS'
  AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
GROUP BY f.c_ecr_id, 2
ORDER BY f.c_ecr_id, bloco_30min
"""
df6 = query_athena(sql_timeline, database="fund_ec2")
print(df6.to_string(index=False))
print()

print("=" * 80)
print("INVESTIGACAO CONCLUIDA")
print("=" * 80)