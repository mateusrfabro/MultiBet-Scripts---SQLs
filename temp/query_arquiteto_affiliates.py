"""
Query do arquiteto (corrigida) — Affiliates 297657, 445431, 468114
Data: 19/03/2026

Correcao: ps_bi.dim_user usa 'ecr_id' (sem prefixo c_), nao 'c_ecr_id'
"""
import sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

# SQL do arquiteto com correcao: c_ecr_id -> ecr_id no base_players
SQL_ARQUITETO = """
WITH params AS (
    SELECT
        DATE('2026-03-19') AS target_date
),
-- FIX 1: ps_bi.dim_user usa 'ecr_id' (sem c_), affiliate_id e VARCHAR
base_players AS (
    SELECT DISTINCT ecr_id
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
      AND is_test = false
),
registrations AS (
    SELECT COUNT(*) AS total_reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = (SELECT target_date FROM params)
      AND CAST(c_affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
      AND c_test_user = false
),
-- FIX 2: affiliate_id e VARCHAR no ps_bi
ftd_metrics AS (
    SELECT
        COUNT(*) AS total_ftd,
        SUM(ftd_amount_inhouse) AS total_ftd_deposit
    FROM ps_bi.dim_user
    WHERE ftd_date = (SELECT target_date FROM params)
      AND CAST(affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
      AND is_test = false
),
financial_summary AS (
    SELECT
        SUM(c_co_success_amount) / 100.0 AS saques,
        SUM(c_deposit_success_amount) / 100.0 AS dep_amount,
        SUM(c_casino_realcash_bet_amount - c_casino_realcash_win_amount) / 100.0 AS ggr_cassino,
        SUM(c_sb_realcash_bet_amount - c_sb_realcash_win_amount) / 100.0 AS ggr_sport,
        SUM(c_bonus_issued_amount) / 100.0 AS bonus_cost
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
    WHERE c_created_date = (SELECT target_date FROM params)
      AND c_ecr_id IN (SELECT ecr_id FROM base_players)
)
SELECT
    f.saques,
    r.total_reg AS reg,
    ftd.total_ftd AS ftd,
    ftd.total_ftd_deposit AS ftd_deposit,
    f.dep_amount,
    f.ggr_cassino,
    f.ggr_sport,
    (f.ggr_cassino + f.ggr_sport - f.bonus_cost) AS ngr
FROM financial_summary f
CROSS JOIN registrations r
CROSS JOIN ftd_metrics ftd
"""

# Nosso SQL original (que ja rodou com sucesso)
SQL_NOSSO = """
WITH
players AS (
    SELECT ecr_id, registration_date, has_ftd, ftd_date, ftd_amount_inhouse
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
      AND is_test = false
),
reg AS (
    SELECT COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '2026-03-19'
      AND CAST(c_affiliate_id AS VARCHAR) IN ('297657', '445431', '468114')
      AND c_test_user = false
),
ftd AS (
    SELECT COUNT(*) AS ftd, COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
    FROM players
    WHERE ftd_date = DATE '2026-03-19'
),
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

print("=" * 70)
print("COMPARACAO: Query Arquiteto (corrigida) vs Query Nossa")
print("=" * 70)

print("\n[1] Rodando query do ARQUITETO...")
df_arq = query_athena(SQL_ARQUITETO, database="ps_bi")
print("OK!")

print("\n[2] Rodando NOSSA query...")
df_nos = query_athena(SQL_NOSSO, database="ps_bi")
print("OK!")

print("\n" + "-" * 70)
print(f"  {'Metrica':<20} {'Arquiteto':>14} {'Nossa':>14} {'Match?':>10}")
print(f"  {'-'*20} {'-'*14} {'-'*14} {'-'*10}")

cols_map = [
    ("Saques", "saques"),
    ("REG", "reg"),
    ("FTD", "ftd"),
    ("FTD Deposit", "ftd_deposit"),
    ("Dep Amount", "dep_amount"),
    ("GGR Cassino", "ggr_cassino"),
    ("GGR Sport", "ggr_sport"),
    ("NGR", "ngr"),
]

all_match = True
for label, col in cols_map:
    v_arq = df_arq.iloc[0][col]
    v_nos = df_nos.iloc[0][col]

    # Format numbers
    if isinstance(v_arq, (int, float)) and abs(float(v_arq)) > 10:
        s_arq = f"R${float(v_arq):>11,.2f}"
        s_nos = f"R${float(v_nos):>11,.2f}"
    else:
        s_arq = f"{v_arq:>14}"
        s_nos = f"{v_nos:>14}"

    # Check match
    diff = abs(float(v_arq or 0) - float(v_nos or 0))
    match = "OK" if diff < 0.02 else f"DIFF {diff:,.2f}"
    if diff >= 0.02:
        all_match = False

    print(f"  {label:<20} {s_arq} {s_nos} {match:>10}")

print(f"\n  {'RESULTADO':} {'QUERIES BATEM!' if all_match else 'DIVERGENCIAS ENCONTRADAS'}")
print("=" * 70)
