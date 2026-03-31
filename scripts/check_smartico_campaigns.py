"""Mapear campanhas reais (dm_automation_rule) vs bonus (j_bonuses.entity_id)."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.bigquery import query_bigquery

DS = "smartico-bq6.dwh_ext_24105"

# 1. Quantas campanhas ativas existem?
print("=== CAMPANHAS ATIVAS (dm_automation_rule) ===")
sql1 = f"""
SELECT rule_id, rule_name, is_active
FROM `{DS}.dm_automation_rule`
WHERE is_active = true
ORDER BY rule_name
LIMIT 30
"""
df1 = query_bigquery(sql1)
print(f"Total ativas: ver abaixo")
print(df1.to_string(index=False))

# 2. Quantas no total?
sql1b = f"SELECT COUNT(*) as total, COUNT(CASE WHEN is_active THEN 1 END) as ativas FROM `{DS}.dm_automation_rule`"
df1b = query_bigquery(sql1b)
print(f"\nTotal rules: {df1b.iloc[0,0]}, Ativas: {df1b.iloc[0,1]}")

# 3. activity_details - ver se tem automation_rule_id
print("\n=== ACTIVITY_DETAILS (j_bonuses) ===")
sql3 = f"""
SELECT
    entity_id,
    JSON_EXTRACT_SCALAR(activity_details, '$.campaign_name') as camp_name,
    JSON_EXTRACT_SCALAR(activity_details, '$.rule_name') as rule_name,
    JSON_EXTRACT_SCALAR(activity_details, '$.automation_id') as auto_id,
    JSON_EXTRACT_SCALAR(activity_details, '$.journey_id') as journey_id,
    label_bonus_template_id,
    COUNT(*) as cnt
FROM `{DS}.j_bonuses`
WHERE bonus_status_id = 3
  AND DATE(fact_date, 'America/Sao_Paulo') = '2026-03-27'
GROUP BY 1,2,3,4,5,6
ORDER BY cnt DESC
LIMIT 15
"""
df3 = query_bigquery(sql3)
print(df3.to_string(index=False))

# 4. dm_bonus_template - link bonus template -> campanha?
print("\n=== dm_bonus_template COLUNAS ===")
sql4 = f"SELECT column_name FROM `{DS}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'dm_bonus_template' ORDER BY ordinal_position"
df4 = query_bigquery(sql4)
for _, r in df4.iterrows():
    print(f"  {r['column_name']}")

print("\nDONE")
