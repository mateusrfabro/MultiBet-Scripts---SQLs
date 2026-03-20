import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from db.bigquery import query_bigquery

# Query consolidada: agrupar por Animal e Quest (5/15/25 FS), status 3 = resgatado
sql = """
WITH bonus_fortune AS (
  SELECT
    b.user_ext_id,
    bt.public_name,
    bt.label_bonus_template_id,
    b.bonus_status_id,
    -- Extrair animal
    CASE
      WHEN UPPER(bt.public_name) LIKE '%TIGER%' THEN 'Tiger'
      WHEN UPPER(bt.public_name) LIKE '%RABBIT%' THEN 'Rabbit'
      WHEN UPPER(bt.public_name) LIKE '%DRAGON%' THEN 'Dragon'
      WHEN UPPER(bt.public_name) LIKE '%OX%' THEN 'Ox'
      WHEN UPPER(bt.public_name) LIKE '%MOUSE%' THEN 'Mouse'
      WHEN UPPER(bt.public_name) LIKE '%SNAKE%' THEN 'Snake'
      ELSE 'Outro'
    END AS animal,
    -- Extrair quest (5, 15 ou 25 FS)
    CASE
      WHEN REGEXP_CONTAINS(bt.public_name, r'(?i)^0?5 Giros') THEN 'Q1_5FS'
      WHEN REGEXP_CONTAINS(bt.public_name, r'(?i)^15 Giros') THEN 'Q2_15FS'
      WHEN REGEXP_CONTAINS(bt.public_name, r'(?i)^25 Giros') THEN 'Q3_25FS'
      ELSE 'Outro'
    END AS quest
  FROM `smartico-bq6.dwh_ext_24105.j_bonuses` b
  JOIN `smartico-bq6.dwh_ext_24105.dm_bonus_template` bt
    ON bt.label_bonus_template_id = b.label_bonus_template_id
  WHERE (
    UPPER(bt.public_name) LIKE '%FORTUNE TIGER%'
    OR UPPER(bt.public_name) LIKE '%FORTUNE RABBIT%'
    OR UPPER(bt.public_name) LIKE '%FORTUNE OX%'
    OR UPPER(bt.public_name) LIKE '%FORTUNE DRAGON%'
    OR UPPER(bt.public_name) LIKE '%FORTUNE MOUSE%'
    OR UPPER(bt.public_name) LIKE '%FORTUNE SNAKE%'
  )
  AND REGEXP_CONTAINS(bt.public_name, r'(?i)^(0?5|15|25) Giros')
  AND b.bonus_status_id = 3  -- Resgatado/Usado
)
SELECT
  animal,
  quest,
  COUNT(DISTINCT user_ext_id) AS users_resgataram
FROM bonus_fortune
WHERE animal != 'Outro' AND quest != 'Outro'
GROUP BY animal, quest
ORDER BY
  CASE animal
    WHEN 'Tiger' THEN 1 WHEN 'Rabbit' THEN 2 WHEN 'Dragon' THEN 3
    WHEN 'Ox' THEN 4 WHEN 'Mouse' THEN 5 WHEN 'Snake' THEN 6
  END,
  quest
"""
df = query_bigquery(sql)

# Pivotar para formato tabela
import pandas as pd
pivot = df.pivot(index='animal', columns='quest', values='users_resgataram').fillna(0).astype(int)

# Garantir ordem dos animais e colunas
animal_order = ['Tiger', 'Rabbit', 'Dragon', 'Ox', 'Mouse', 'Snake']
quest_order = ['Q1_5FS', 'Q2_15FS', 'Q3_25FS']

for col in quest_order:
    if col not in pivot.columns:
        pivot[col] = 0
pivot = pivot[quest_order]

for animal in animal_order:
    if animal not in pivot.index:
        pivot.loc[animal] = 0
pivot = pivot.loc[animal_order]

# Totais
pivot.loc['Total'] = pivot.sum()

# Total FS entregues
total_fs = pivot.loc['Total', 'Q1_5FS'] * 5 + pivot.loc['Total', 'Q2_15FS'] * 15 + pivot.loc['Total', 'Q3_25FS'] * 25

# Unicos
sql_uniq = """
SELECT COUNT(DISTINCT b.user_ext_id) AS unicos
FROM `smartico-bq6.dwh_ext_24105.j_bonuses` b
JOIN `smartico-bq6.dwh_ext_24105.dm_bonus_template` bt
  ON bt.label_bonus_template_id = b.label_bonus_template_id
WHERE (
  UPPER(bt.public_name) LIKE '%FORTUNE TIGER%'
  OR UPPER(bt.public_name) LIKE '%FORTUNE RABBIT%'
  OR UPPER(bt.public_name) LIKE '%FORTUNE OX%'
  OR UPPER(bt.public_name) LIKE '%FORTUNE DRAGON%'
  OR UPPER(bt.public_name) LIKE '%FORTUNE MOUSE%'
  OR UPPER(bt.public_name) LIKE '%FORTUNE SNAKE%'
)
AND REGEXP_CONTAINS(bt.public_name, r'(?i)^(0?5|15|25) Giros')
AND b.bonus_status_id = 3
"""
df_u = query_bigquery(sql_uniq)
unicos = df_u['unicos'].iloc[0]

print("=" * 60)
print("Bonus Resgatados — Quests Multiverso (dados BigQuery)")
print("bonus_status_id = 3 (Resgatado/Usado)")
print("=" * 60)
print()
print(f"{'Animal':<8} {'Q1 - 5 FS':>10} {'Q2 - 15 FS':>11} {'Q3 - 25 FS':>11}")
print("-" * 42)
for animal in animal_order + ['Total']:
    q1 = pivot.loc[animal, 'Q1_5FS']
    q2 = pivot.loc[animal, 'Q2_15FS']
    q3 = pivot.loc[animal, 'Q3_25FS']
    print(f"{animal:<8} {q1:>10} {q2:>11} {q3:>11}")
print()
print(f"- Jogadores unicos que resgataram: {unicos}")
print(f"- Total Free Spins resgatados: {int(total_fs)} FS")