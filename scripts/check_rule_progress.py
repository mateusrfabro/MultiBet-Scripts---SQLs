"""Verificar j_automation_rule_progress como bridge entre campanha e bonus."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.bigquery import query_bigquery

DS = "smartico-bq6.dwh_ext_24105"

# 1. Colunas do j_automation_rule_progress
print("=== j_automation_rule_progress COLUNAS ===")
sql = f"SELECT column_name FROM `{DS}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'j_automation_rule_progress' ORDER BY ordinal_position"
df = query_bigquery(sql)
for _, r in df.iterrows():
    print(f"  {r['column_name']}")

# 2. Amostra - link rule_id -> user
print("\n=== AMOSTRA (27/03) ===")
sql2 = f"""
SELECT rule_id, rule_step, user_ext_id, COUNT(*) as cnt
FROM `{DS}.j_automation_rule_progress`
WHERE DATE(fact_date, 'America/Sao_Paulo') = '2026-03-27'
GROUP BY rule_id, rule_step, user_ext_id
ORDER BY cnt DESC
LIMIT 15
"""
df2 = query_bigquery(sql2)
print(df2.to_string(index=False))

# 3. Quantos users por rule_id em marco?
print("\n=== USERS POR RULE (MARCO) ===")
sql3 = f"""
SELECT
    p.rule_id,
    r.rule_name,
    r.is_active,
    COUNT(DISTINCT p.user_ext_id) as users,
    COUNT(*) as events
FROM `{DS}.j_automation_rule_progress` p
LEFT JOIN `{DS}.dm_automation_rule` r ON r.rule_id = p.rule_id
WHERE DATE(p.fact_date, 'America/Sao_Paulo') BETWEEN '2026-03-01' AND '2026-03-30'
GROUP BY p.rule_id, r.rule_name, r.is_active
ORDER BY users DESC
LIMIT 20
"""
df3 = query_bigquery(sql3)
print(df3.to_string(index=False))

print("\nDONE")
