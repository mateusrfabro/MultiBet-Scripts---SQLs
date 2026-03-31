"""
Investigacao parte 3: transacoes brutas + spike historico
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from db.athena import query_athena

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)
pd.set_option("display.max_colwidth", 30)

IDS_STR = "122650591789884041, 545570261762335630, 300472041791201457, 551411774002817207, 557272641791363992, 818241774693292851"

def safe_query(label, sql, db="fund_ec2"):
    try:
        df = query_athena(sql, database=db)
        print(f"\n{'=' * 100}")
        print(label)
        print("=" * 100)
        print(df.to_string(index=False))
        print()
        return df
    except Exception as e:
        print(f"\n[ERRO] {label}: {e}")
        return None

# 11. Transacoes brutas do player mais anomalo (SEM c_ref_txn_id)
safe_query(
    "11) TRANSACOES BRUTAS — player 545570261762335630 (Wild Bandito, 66 rollbacks em 2min)",
    """
    SELECT
        f.c_txn_type,
        CASE f.c_txn_type WHEN 27 THEN 'BET' WHEN 45 THEN 'WIN' WHEN 72 THEN 'ROLLBACK' ELSE CAST(f.c_txn_type AS VARCHAR) END AS tipo,
        f.c_game_id,
        f.c_amount_in_ecr_ccy / 100.0 AS valor_brl,
        f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS hora_brt,
        f.c_txn_id,
        f.c_product_id
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_ecr_id = 545570261762335630
      AND f.c_txn_status = 'SUCCESS'
      AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
    ORDER BY f.c_start_time
    LIMIT 50
    """
)

# 12. Verificar se alerta conta SEM product filter (possivel duplicacao)
safe_query(
    "12) ROLLBACKS COM vs SEM FILTRO PRODUCT_ID — mesmo player 545570261762335630",
    """
    SELECT
        f.c_product_id,
        COUNT(*) AS rollbacks,
        SUM(f.c_amount_in_ecr_ccy) / 100.0 AS valor_brl
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_ecr_id = 545570261762335630
      AND f.c_txn_type = 72
      AND f.c_txn_status = 'SUCCESS'
      AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
    GROUP BY f.c_product_id
    """
)

# 13. Spike de 127K rollbacks em 27/03 — por product_id
safe_query(
    "13) SPIKE 27/03 — 127K rollbacks: breakdown por product_id",
    """
    SELECT
        f.c_product_id,
        COUNT(*) AS rollbacks,
        COUNT(DISTINCT f.c_ecr_id) AS players
    FROM fund_ec2.tbl_real_fund_txn f
    JOIN ecr_ec2.tbl_ecr_flags fl ON f.c_ecr_id = fl.c_ecr_id
    WHERE f.c_txn_type = 72
      AND f.c_txn_status = 'SUCCESS'
      AND NOT fl.c_test_user
      AND f.c_start_time >= TIMESTAMP '2026-03-27 03:00:00'
      AND f.c_start_time < TIMESTAMP '2026-03-28 03:00:00'
    GROUP BY f.c_product_id
    """
)

# 14. Todos os 6 players: rollbacks por product_id (pega duplicacao?)
safe_query(
    "14) TODOS 6 PLAYERS: rollbacks por PRODUCT_ID hoje (pega duplicacao?)",
    f"""
    SELECT
        f.c_ecr_id,
        f.c_product_id,
        COUNT_IF(f.c_txn_type = 27) AS bets,
        COUNT_IF(f.c_txn_type = 45) AS wins,
        COUNT_IF(f.c_txn_type = 72) AS rollbacks
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_ecr_id IN ({IDS_STR})
      AND f.c_txn_status = 'SUCCESS'
      AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
    GROUP BY f.c_ecr_id, f.c_product_id
    ORDER BY f.c_ecr_id, f.c_product_id
    """
)

# 15. Comparar contagem COM filtro test user (JOIN flags) vs SEM para esses 6
safe_query(
    "15) CONTAGEM COM JOIN ECR_FLAGS (como faz o alerta) — esses 6 players",
    f"""
    SELECT
        f.c_ecr_id,
        COUNT(*) AS rollbacks_com_flag_filter,
        COALESCE(g.c_game_desc, 'N/A') AS top_game
    FROM fund_ec2.tbl_real_fund_txn f
    JOIN ecr_ec2.tbl_ecr_flags fl ON f.c_ecr_id = fl.c_ecr_id
    LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data g
        ON CAST(f.c_game_id AS VARCHAR) = CAST(g.c_game_id AS VARCHAR)
    WHERE f.c_ecr_id IN ({IDS_STR})
      AND f.c_txn_type = 72
      AND f.c_txn_status = 'SUCCESS'
      AND NOT fl.c_test_user
      AND CAST(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-28'
    GROUP BY f.c_ecr_id, g.c_game_desc
    ORDER BY rollbacks_com_flag_filter DESC
    """
)

print("\n" + "=" * 100)
print("INVESTIGACAO PARTE 3 CONCLUIDA")
print("=" * 100)
