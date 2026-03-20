"""
Extração pontual: Affiliate IDs 297657, 445431, 468114
Data: 19/03/2026 (consolidado, não separado por ID)
Métricas: Saques, REG, FTD, FTD Deposit, Dep Amount, GGR Cassino, GGR Sport, NGR

Fonte: Athena (ps_bi + bireports_ec2)
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

SQL = """
WITH
-- 1. Players dos 3 affiliate_ids (sem test users)
players AS (
    SELECT ecr_id,
           registration_date,
           has_ftd,
           ftd_date,
           ftd_amount_inhouse
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
      AND is_test = false
),

-- 2. REG: cadastros de 19/03/2026
reg AS (
    SELECT COUNT(*) AS reg
    FROM players
    WHERE registration_date = DATE '2026-03-19'
),

-- 3. FTD e valor FTD de 19/03/2026
ftd AS (
    SELECT
        COUNT(*) AS ftd,
        COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
    FROM players
    WHERE ftd_date = DATE '2026-03-19'
),

-- 4. Metricas financeiras de 19/03/2026 (bireports BI Summary - valores em centavos)
financeiro AS (
    SELECT
        COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS dep_amount,
        COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS saques,
        COALESCE(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_cassino,
        COALESCE(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
        COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0 AS bonus_cost
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN players p ON s.c_ecr_id = p.ecr_id
    WHERE s.c_created_date = DATE '2026-03-19'
)

SELECT
    ROUND(f.saques, 2) AS saques,
    r.reg,
    t.ftd,
    ROUND(t.ftd_deposit, 2) AS ftd_deposit,
    ROUND(f.dep_amount, 2) AS dep_amount,
    ROUND(f.ggr_cassino, 2) AS ggr_cassino,
    ROUND(f.ggr_sport, 2) AS ggr_sport,
    ROUND(f.ggr_cassino + f.ggr_sport - f.bonus_cost, 2) AS ngr
FROM financeiro f
CROSS JOIN reg r
CROSS JOIN ftd t
"""

SQL_BASE = """
SELECT COUNT(*) AS total_players
FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
  AND is_test = false
"""

print("Executando query principal no Athena...")
df = query_athena(SQL, database="ps_bi")
print()
print("=== RESULTADO: Affiliates 297657, 445431, 468114 — 19/03/2026 ===")
print()
for col in df.columns:
    print(f"  {col:20s}: {df.iloc[0][col]}")
print()

print("Consultando base de players...")
df2 = query_athena(SQL_BASE, database="ps_bi")
print(f"Base total de players desses affiliates: {df2.iloc[0, 0]}")
