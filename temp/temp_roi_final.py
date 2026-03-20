import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

# Adicionar coluna source se nao existir e atualizar
execute_supernova("ALTER TABLE multibet.fact_attribution ADD COLUMN IF NOT EXISTS source VARCHAR(100);")
execute_supernova("UPDATE multibet.fact_attribution SET source = COALESCE((SELECT source FROM multibet.dim_marketing_mapping WHERE tracker_id = fact_attribution.c_tracker_id), 'unmapped_orphans')")
execute_supernova("UPDATE multibet.agg_cohort_acquisition SET source = COALESCE((SELECT source FROM multibet.dim_marketing_mapping WHERE tracker_id = agg_cohort_acquisition.c_tracker_id), 'unmapped_orphans')")

# Recriar view
execute_supernova("DROP VIEW IF EXISTS multibet.vw_cohort_roi CASCADE;")
execute_supernova("""
CREATE VIEW multibet.vw_cohort_roi AS
SELECT
    c.month_of_ftd, c.source,
    COUNT(*) AS qty_players,
    ROUND(AVG(c.ftd_amount)::numeric, 2) AS avg_ftd_amount,
    ROUND(AVG(c.ggr_d7)::numeric, 2) AS avg_ltv_d7,
    ROUND(AVG(c.ggr_d30)::numeric, 2) AS avg_ltv_d30,
    ROUND(SUM(c.ggr_d30)::numeric, 2) AS total_ggr_d30,
    ROUND(SUM(c.is_2nd_depositor)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS pct_2nd_deposit,
    s.monthly_spend,
    CASE WHEN s.monthly_spend > 0
         THEN ROUND(SUM(c.ggr_d30)::numeric / s.monthly_spend * 100, 2)
         ELSE NULL END AS roi_d30_pct
FROM multibet.agg_cohort_acquisition c
LEFT JOIN (
    SELECT TO_CHAR(a.dt, 'YYYY-MM') AS month_ref,
           COALESCE(m.source, 'unmapped_orphans') AS source,
           SUM(a.marketing_spend) AS monthly_spend
    FROM multibet.fact_attribution a
    LEFT JOIN multibet.dim_marketing_mapping m ON a.c_tracker_id = m.tracker_id
    GROUP BY 1, 2
) s ON c.month_of_ftd = s.month_ref AND c.source = s.source
GROUP BY c.month_of_ftd, c.source, s.monthly_spend
ORDER BY c.month_of_ftd DESC, total_ggr_d30 DESC
""")

print("=== ROI FINAL — Google Ads (445431 + 297657 consolidados) ===")
print()
rows = execute_supernova("""
    SELECT month_of_ftd, qty_players, avg_ltv_d30, total_ggr_d30,
           monthly_spend, roi_d30_pct, pct_2nd_deposit
    FROM multibet.vw_cohort_roi
    WHERE source = 'google_ads'
    ORDER BY month_of_ftd
""", fetch=True)
print(f"{'Safra':<8} {'Players':>8} {'LTV D30':>9} {'GGR D30':>13} {'Spend':>14} {'ROI D30':>9} {'2nd%':>6}")
print("="*72)
for r in rows:
    sp = f"R${float(r[4]):>11,.0f}" if r[4] else "        N/A"
    roi = f"{float(r[5]):.1f}%" if r[5] else "   N/A"
    print(f"{r[0]:<8} {r[1]:>8,} R${float(r[2]):>7,.0f} R${float(r[3]):>11,.0f} {sp} {roi:>9} {float(r[6]):.1f}%")

print()
print("=== COMPARATIVO TODAS SOURCES (ultimos 3 meses) ===")
rows2 = execute_supernova("""
    SELECT month_of_ftd, source, qty_players, avg_ltv_d30, total_ggr_d30,
           monthly_spend, roi_d30_pct
    FROM multibet.vw_cohort_roi
    WHERE month_of_ftd >= '2026-01'
    ORDER BY month_of_ftd, total_ggr_d30 DESC
""", fetch=True)
print(f"{'Safra':<8} {'Source':<25} {'Players':>8} {'LTV D30':>9} {'GGR D30':>13} {'Spend':>14} {'ROI%':>8}")
print("="*90)
for r in rows2:
    sp = f"R${float(r[5]):>11,.0f}" if r[5] else "        N/A"
    roi = f"{float(r[6]):.1f}%" if r[6] else "   N/A"
    print(f"{r[0]:<8} {r[1]:<25} {r[2]:>8,} R${float(r[3]):>7,.0f} R${float(r[4]):>11,.0f} {sp} {roi:>8}")
