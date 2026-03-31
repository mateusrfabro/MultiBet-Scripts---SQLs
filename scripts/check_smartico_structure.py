"""Mapear estrutura de campanhas no Smartico BigQuery."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.bigquery import query_bigquery

DS = "smartico-bq6.dwh_ext_24105"

# 1. Colunas j_bonuses
print("=== j_bonuses COLUNAS ===")
sql = f'SELECT column_name FROM `{DS}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = "j_bonuses" ORDER BY ordinal_position'
df = query_bigquery(sql)
for _, r in df.iterrows():
    print(f"  {r['column_name']}")

# 2. Colunas dm_automation_rule (campanhas)
print("\n=== dm_automation_rule COLUNAS ===")
sql2 = f'SELECT column_name FROM `{DS}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = "dm_automation_rule" ORDER BY ordinal_position'
df2 = query_bigquery(sql2)
for _, r in df2.iterrows():
    print(f"  {r['column_name']}")

# 3. Amostra de activity_details para ver campos disponiveis
print("\n=== activity_details AMOSTRA ===")
sql3 = f"""
SELECT DISTINCT activity_details
FROM `{DS}.j_bonuses`
WHERE bonus_status_id = 3
  AND DATE(fact_date, 'America/Sao_Paulo') = '2026-03-27'
LIMIT 5
"""
df3 = query_bigquery(sql3)
for _, r in df3.iterrows():
    print(f"  {r['activity_details'][:200]}")

# 4. Campos do activity_details - quais chaves existem?
print("\n=== activity_details KEYS ===")
sql4 = f"""
SELECT DISTINCT
    JSON_EXTRACT_SCALAR(activity_details, '$.campaign_name') as camp_name,
    JSON_EXTRACT_SCALAR(activity_details, '$.rule_name') as rule_name,
    JSON_EXTRACT_SCALAR(activity_details, '$.automation_rule_id') as rule_id,
    entity_id,
    label_bonus_template_id
FROM `{DS}.j_bonuses`
WHERE bonus_status_id = 3
  AND DATE(fact_date, 'America/Sao_Paulo') = '2026-03-27'
  AND entity_id IN (754, 23053, 2157323)
LIMIT 10
"""
df4 = query_bigquery(sql4)
print(df4.to_string(index=False))

# 5. dm_automation_rule - como as campanhas sao organizadas?
print("\n=== dm_automation_rule AMOSTRA ===")
sql5 = f"""
SELECT rule_id, rule_name, is_active, rule_type
FROM `{DS}.dm_automation_rule`
WHERE is_active = true
LIMIT 15
"""
df5 = query_bigquery(sql5)
print(df5.to_string(index=False))

print("\nDONE")
