import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

sql = """
    SELECT month_of_ftd, source, COUNT(*) AS players,
           ROUND(AVG(ggr_d30)::numeric, 2) AS avg_d30,
           ROUND(SUM(ggr_d30)::numeric, 2) AS total_d30,
           ROUND(SUM(is_2nd_depositor)::numeric / COUNT(*) * 100, 1) AS pct_2nd
    FROM multibet.agg_cohort_acquisition
    WHERE source IN ('google_ads','meta_ads','organic','multi_channel')
    GROUP BY 1, 2 ORDER BY 1, 2
"""
rows = execute_supernova(sql, fetch=True)
print('Safra    Source           Players  LTV D30      GGR D30 Total  2nd%')
print('='*75)
for r in rows:
    print(f'{r[0]}  {r[1]:<17} {r[2]:>7,}  R${float(r[3]):>8,.0f}  R${float(r[4]):>12,.0f}  {float(r[5]):.1f}%')

print()
print('=== GOOGLE ADS ROI por Safra (D30) ===')
sql2 = """
    SELECT c.month_of_ftd, COUNT(*) AS players,
           ROUND(SUM(c.ggr_d30)::numeric, 2) AS ggr_d30,
           s.spend,
           CASE WHEN s.spend > 0 THEN ROUND(SUM(c.ggr_d30)::numeric / s.spend * 100, 1) ELSE NULL END AS roi
    FROM multibet.agg_cohort_acquisition c
    LEFT JOIN (
        SELECT TO_CHAR(dt, 'YYYY-MM') AS m, ROUND(SUM(marketing_spend)::numeric, 2) AS spend
        FROM multibet.fact_attribution WHERE source = 'google_ads' GROUP BY 1
    ) s ON c.month_of_ftd = s.m
    WHERE c.source = 'google_ads'
    GROUP BY c.month_of_ftd, s.spend ORDER BY 1
"""
rows2 = execute_supernova(sql2, fetch=True)
print(f'Safra    Players    GGR D30          Spend           ROI D30')
print('='*65)
for r in rows2:
    sp = f'R${float(r[3]):>11,.0f}' if r[3] else '        N/A'
    roi = f'{float(r[4]):.1f}%' if r[4] else '  N/A'
    print(f'{r[0]}   {r[1]:>7,}  R${float(r[2]):>12,.0f}  {sp}  {roi}')
