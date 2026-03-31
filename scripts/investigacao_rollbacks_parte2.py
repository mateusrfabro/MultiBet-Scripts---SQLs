"""
Investigacao parte 2: perfil dos jogadores + timeline de rollbacks
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from db.athena import query_athena

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)

IDS_STR = "122650591789884041, 545570261762335630, 300472041791201457, 551411774002817207, 557272641791363992, 818241774693292851"

# ── 5. Info do jogador (bireports_ec2.tbl_ecr — colunas reais) ──
print("=" * 80)
print("5) PERFIL DOS JOGADORES (cadastro, ultimo login, test user?)")
print("=" * 80)

sql_profile = f"""
SELECT
    e.c_ecr_id,
    e.c_external_id,
    e.c_created_date,
    e.c_last_login_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS last_login_brt,
    e.c_test_user,
    e.c_ecr_status
FROM bireports_ec2.tbl_ecr e
WHERE e.c_ecr_id IN ({IDS_STR})
"""
df5 = query_athena(sql_profile, database="bireports_ec2")
print(df5.to_string(index=False))
print()

# ── 6. Timeline de rollbacks hoje — janelas de 30min (burst detection) ──
print("=" * 80)
print("6) TIMELINE DE ROLLBACKS HOJE — janelas de 1 hora")
print("=" * 80)

sql_timeline = f"""
SELECT
    f.c_ecr_id,
    HOUR(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora_brt,
    COUNT_IF(f.c_txn_type = 27) AS bets,
    COUNT_IF(f.c_txn_type = 45) AS wins,
    COUNT_IF(f.c_txn_type = 72) AS rollbacks,
    SUM(CASE WHEN f.c_txn_type = 72 THEN f.c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS rb_brl
FROM fund_ec2.tbl_real_fund_txn f
WHERE f.c_ecr_id IN ({IDS_STR})
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_product_id = 'CASINO'
  AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
GROUP BY f.c_ecr_id, HOUR(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
ORDER BY f.c_ecr_id, hora_brt
"""
df6 = query_athena(sql_timeline, database="fund_ec2")
print(df6.to_string(index=False))
print()

# ── 7. Amostra de transacoes brutas — top player (545570261762335630, o mais anomalo) ──
print("=" * 80)
print("7) AMOSTRA TRANSACOES BRUTAS — player 545570261762335630 (183% rb/bet)")
print("   20 primeiras transacoes do dia para entender sequencia")
print("=" * 80)

sql_raw = """
SELECT
    f.c_ecr_id,
    f.c_txn_type,
    CASE f.c_txn_type
        WHEN 27 THEN 'BET'
        WHEN 45 THEN 'WIN'
        WHEN 72 THEN 'ROLLBACK'
        ELSE CAST(f.c_txn_type AS VARCHAR)
    END AS tipo,
    f.c_game_id,
    f.c_amount_in_ecr_ccy / 100.0 AS valor_brl,
    f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS hora_brt,
    f.c_txn_id,
    f.c_ref_txn_id
FROM fund_ec2.tbl_real_fund_txn f
WHERE f.c_ecr_id = 545570261762335630
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_product_id = 'CASINO'
  AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
ORDER BY f.c_start_time
LIMIT 30
"""
df7 = query_athena(sql_raw, database="fund_ec2")
print(df7.to_string(index=False))
print()

# ── 8. Verificar se rollbacks referenciam bets reais (c_ref_txn_id) ──
print("=" * 80)
print("8) ROLLBACKS COM REFERENCIA A BET? — player 545570261762335630")
print("   c_ref_txn_id != 0 indica que o rollback referencia uma aposta")
print("=" * 80)

sql_ref = """
SELECT
    f.c_txn_type,
    CASE f.c_txn_type
        WHEN 27 THEN 'BET'
        WHEN 45 THEN 'WIN'
        WHEN 72 THEN 'ROLLBACK'
        ELSE CAST(f.c_txn_type AS VARCHAR)
    END AS tipo,
    COUNT(*) AS qtd,
    COUNT_IF(f.c_ref_txn_id != 0) AS com_ref_txn,
    COUNT_IF(f.c_ref_txn_id = 0 OR f.c_ref_txn_id IS NULL) AS sem_ref_txn
FROM fund_ec2.tbl_real_fund_txn f
WHERE f.c_ecr_id = 545570261762335630
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_product_id = 'CASINO'
  AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
GROUP BY f.c_txn_type
ORDER BY f.c_txn_type
"""
df8 = query_athena(sql_ref, database="fund_ec2")
print(df8.to_string(index=False))
print()

# ── 9. Verificar total de rollbacks do dia INTEIRO (conferir 270/65) ──
print("=" * 80)
print("9) TOTAL GERAL ROLLBACKS HOJE (validar 270/65 do alerta)")
print("=" * 80)

sql_total = """
SELECT
    COUNT(*) AS rollback_count,
    COUNT(DISTINCT f.c_ecr_id) AS players_com_rollback
FROM fund_ec2.tbl_real_fund_txn f
JOIN ecr_ec2.tbl_ecr_flags fl ON f.c_ecr_id = fl.c_ecr_id
WHERE f.c_txn_type = 72
  AND f.c_txn_status = 'SUCCESS'
  AND NOT fl.c_test_user
  AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
"""
df9 = query_athena(sql_total, database="fund_ec2")
print(df9.to_string(index=False))
print()

print("=" * 80)
print("INVESTIGACAO PARTE 2 CONCLUIDA")
print("=" * 80)