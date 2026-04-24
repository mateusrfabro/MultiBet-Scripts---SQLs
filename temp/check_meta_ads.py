"""Verifica ultima carga do meta_ads + google_ads em multibet.fact_ad_spend."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

sql = """
SELECT ad_source,
       MAX(dt) AS max_dt,
       MAX(refreshed_at) AS ultima_carga,
       COUNT(*) AS linhas,
       COUNT(DISTINCT dt) AS dias
FROM multibet.fact_ad_spend
GROUP BY ad_source
ORDER BY ad_source
"""
rows = execute_supernova(sql, fetch=True)
print("\n=== ULTIMA CARGA POR FONTE ===")
print(f"{'fonte':<10} {'max_dt':<12} {'ultima_carga':<30} {'linhas':>8} {'dias':>6}")
for r in rows:
    print(f"{r[0]:<10} {str(r[1]):<12} {str(r[2]):<30} {r[3]:>8,} {r[4]:>6}")

# refreshed_at distintos ultimos 5 (pra ver cadencia de cron)
sql2 = """
SELECT ad_source, DATE_TRUNC('minute', refreshed_at) AS run_ts, COUNT(*) AS linhas
FROM multibet.fact_ad_spend
GROUP BY ad_source, DATE_TRUNC('minute', refreshed_at)
ORDER BY run_ts DESC
LIMIT 10
"""
rows2 = execute_supernova(sql2, fetch=True)
print("\n=== ULTIMAS 10 EXECUCOES (refreshed_at agrupado por minuto) ===")
print(f"{'fonte':<10} {'run_ts':<30} {'linhas':>8}")
for r in rows2:
    print(f"{r[0]:<10} {str(r[1]):<30} {r[2]:>8,}")
