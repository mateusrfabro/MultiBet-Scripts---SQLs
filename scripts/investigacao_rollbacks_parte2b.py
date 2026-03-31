"""
Investigacao parte 2b: queries independentes com tratamento de erro
"""
import sys, os, traceback
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from db.athena import query_athena

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)

IDS_STR = "122650591789884041, 545570261762335630, 300472041791201457, 551411774002817207, 557272641791363992, 818241774693292851"

def safe_query(label, sql, db="fund_ec2"):
    try:
        df = query_athena(sql, database=db)
        print(f"\n{'=' * 80}")
        print(label)
        print("=" * 80)
        print(df.to_string(index=False))
        print()
        return df
    except Exception as e:
        print(f"\n[ERRO] {label}: {e}")
        return None

# 5a. Perfil via ps_bi.dim_user (mais confiavel)
safe_query(
    "5) PERFIL JOGADORES (ps_bi.dim_user)",
    f"""
    SELECT
        u.ecr_id,
        u.external_id,
        u.is_test,
        u.ftd_date,
        u.registration_date
    FROM ps_bi.dim_user u
    WHERE u.ecr_id IN ({IDS_STR})
    """,
    db="ps_bi"
)

# 6. Timeline por hora
safe_query(
    "6) TIMELINE POR HORA — todos os 6 jogadores",
    f"""
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
)

# 7. Amostra bruta do player mais anomalo (545570261762335630)
safe_query(
    "7) TRANSACOES BRUTAS — player 545570261762335630 (183% rb/bet ratio)",
    """
    SELECT
        f.c_txn_type,
        CASE f.c_txn_type WHEN 27 THEN 'BET' WHEN 45 THEN 'WIN' WHEN 72 THEN 'ROLLBACK' ELSE CAST(f.c_txn_type AS VARCHAR) END AS tipo,
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
    LIMIT 40
    """
)

# 8. Referencia de rollbacks (ref_txn_id)
safe_query(
    "8) ROLLBACKS — TEM REFERENCIA A BET? (c_ref_txn_id)",
    f"""
    SELECT
        f.c_ecr_id,
        f.c_txn_type,
        CASE f.c_txn_type WHEN 27 THEN 'BET' WHEN 45 THEN 'WIN' WHEN 72 THEN 'ROLLBACK' ELSE CAST(f.c_txn_type AS VARCHAR) END AS tipo,
        COUNT(*) AS qtd,
        COUNT_IF(f.c_ref_txn_id IS NOT NULL AND f.c_ref_txn_id != 0) AS com_ref,
        COUNT_IF(f.c_ref_txn_id IS NULL OR f.c_ref_txn_id = 0) AS sem_ref
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_ecr_id IN ({IDS_STR})
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_product_id = 'CASINO'
      AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
    GROUP BY f.c_ecr_id, f.c_txn_type
    ORDER BY f.c_ecr_id, f.c_txn_type
    """
)

# 9. Total geral do dia (validar 270/65 do alerta)
safe_query(
    "9) TOTAL GERAL ROLLBACKS HOJE (validar 270/65 do alerta de 10:24)",
    """
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
)

# 10. Comparar com media de rollbacks nos ultimos 7 dias
safe_query(
    "10) MEDIA ROLLBACKS/DIA ULTIMOS 7 DIAS (benchmark)",
    """
    SELECT
        CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dia,
        COUNT(*) AS rollbacks,
        COUNT(DISTINCT f.c_ecr_id) AS players
    FROM fund_ec2.tbl_real_fund_txn f
    JOIN ecr_ec2.tbl_ecr_flags fl ON f.c_ecr_id = fl.c_ecr_id
    WHERE f.c_txn_type = 72
      AND f.c_txn_status = 'SUCCESS'
      AND NOT fl.c_test_user
      AND f.c_start_time >= TIMESTAMP '2026-03-21 03:00:00'
      AND f.c_start_time < TIMESTAMP '2026-03-28 03:00:00'
    GROUP BY CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
    ORDER BY dia
    """
)

print("\n" + "=" * 80)
print("INVESTIGACAO PARTE 2 CONCLUIDA")
print("=" * 80)
