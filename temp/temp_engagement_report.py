import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova
from db.athena import query_athena

# TOP 5 mais engajados
print("=== TOP 5 JOGADORES MAIS ENGAJADOS ===")
rows = execute_supernova("""
    SELECT c_ecr_id, source, ftd_date, total_active_days, avg_bets_per_day,
           days_active_since_ftd, ROUND(total_ggr::numeric, 2) as ggr,
           last_active_date, days_since_last_active
    FROM multibet.fact_player_engagement_daily
    ORDER BY total_active_days DESC
    LIMIT 5
""", fetch=True)
print(f"{'ecr_id':<22} {'source':<15} {'FTD':<12} {'dias_atv':>8} {'bets/dia':>8} {'vida':>5} {'GGR':>12} {'ultimo':>12} {'inativo':>8}")
print("=" * 115)
for r in rows:
    print(f"{r[0]:<22} {r[1]:<15} {r[2]!s:<12} {r[3]:>8} {float(r[4]):>8.1f} {r[5]:>5}d R${float(r[6]):>10,.0f} {r[7]!s:<12} {r[8]:>6}d")

# Resumo por source
print()
print("=== ENGAGEMENT POR SOURCE ===")
rows2 = execute_supernova("""
    SELECT source,
           COUNT(*) as players,
           ROUND(AVG(total_active_days)::numeric, 1) as avg_days,
           ROUND(AVG(avg_bets_per_day)::numeric, 1) as avg_bets,
           ROUND(SUM(is_churned)::numeric / COUNT(*) * 100, 1) as churn_pct,
           ROUND(AVG(total_ggr)::numeric, 2) as avg_ggr
    FROM multibet.fact_player_engagement_daily
    GROUP BY source
    ORDER BY avg_days DESC
""", fetch=True)
print(f"{'Source':<25} {'Players':>8} {'Avg Days':>9} {'Bets/Dia':>9} {'Churn%':>8} {'Avg GGR':>12}")
print("-" * 75)
for r in rows2:
    print(f"{r[0]:<25} {r[1]:>8,} {float(r[2]):>9.1f} {float(r[3]):>9.1f} {float(r[4]):>7.1f}% R${float(r[5]):>10,.0f}")

# Bonus investigation
print()
print("=== INVESTIGACAO BONUS (fund_ec2) ===")
df = query_athena("""
    SELECT c_txn_type, c_op_type, COUNT(*) as cnt,
           ROUND(SUM(CAST(c_amount_in_ecr_ccy AS DOUBLE)) / 100.0, 2) as total_brl
    FROM tbl_real_fund_txn
    WHERE c_txn_type IN (5, 7, 14, 15, 19, 20, 30, 37, 39, 40, 53, 87, 88)
    GROUP BY 1, 2
    ORDER BY cnt DESC
""", database="fund_ec2")
print("Tipos de bonus encontrados:")
for _, r in df.iterrows():
    print(f"  type={int(r.iloc[0]):>3} op={r.iloc[1]} cnt={int(r.iloc[2]):>10,} total=R${r.iloc[3]:>14,.2f}")
