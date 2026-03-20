"""Check rápido: campos de atribuição (utm/affiliate) também contaminados?"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db.athena import query_athena

sql = """
WITH contaminados AS (
    SELECT DISTINCT d.player_id
    FROM ps_bi.fct_deposits_daily d
    INNER JOIN ps_bi.dim_user u ON d.player_id = u.ecr_id
    WHERE d.success_count > 0
      AND u.has_ftd = 1 AND u.is_test = false
      AND CAST(u.ftd_date AS DATE) >= DATE '2026-03-08'
      AND CAST(u.ftd_date AS DATE) < current_date
      AND CAST(d.created_date AS DATE) < CAST(u.ftd_date AS DATE)
)
SELECT
    COUNT(*) AS total_contaminados,
    COUNT(u.utm_source) AS dim_com_utm,
    COUNT(CASE WHEN u.utm_source IS NULL THEN 1 END) AS dim_sem_utm,
    COUNT(u.affiliate_id) AS dim_com_affiliate,
    COUNT(b.c_affiliate_id) AS bi_com_affiliate,
    COUNT(CASE WHEN CAST(u.affiliate_id AS VARCHAR) != CAST(b.c_affiliate_id AS VARCHAR) THEN 1 END) AS affiliate_diverge,
    COUNT(CASE WHEN CAST(u.signup_datetime AS DATE) != CAST(b.c_sign_up_time AS DATE) THEN 1 END) AS signup_date_diverge
FROM contaminados c
JOIN ps_bi.dim_user u ON c.player_id = u.ecr_id
JOIN bireports_ec2.tbl_ecr b ON c.player_id = b.c_ecr_id
"""

print("Checando contaminacao de campos de atribuicao...")
df = query_athena(sql, database='ps_bi')
print()
for col in df.columns:
    print(f"  {col}: {df[col].iloc[0]}")
