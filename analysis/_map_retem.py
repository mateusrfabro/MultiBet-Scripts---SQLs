"""Mapeamento de resource_ids RETEM → segmento + canal."""
import sys, pandas as pd
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.bigquery import query_bigquery

df = query_bigquery("""
SELECT
    jc.resource_id,
    r.resource_name,
    jc.label_provider_id,
    MIN(DATE(jc.fact_date)) AS primeira_data,
    COUNT(DISTINCT jc.user_ext_id) AS total_entregues
FROM `smartico-bq6.dwh_ext_24105.j_communication` jc
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_resource` r
    ON jc.resource_id = r.resource_id AND r.label_id = 24105
WHERE jc.label_id = 24105
  AND DATE(jc.fact_date) BETWEEN '2026-03-06' AND '2026-03-10'
  AND UPPER(COALESCE(r.resource_name, '')) LIKE '%RETEM%'
  AND jc.fact_type_id = 2
GROUP BY jc.resource_id, r.resource_name, jc.label_provider_id
ORDER BY primeira_data, r.resource_name, total_entregues DESC
""")

df.to_csv("analysis/output/mapeamento_retem_recursos.csv", index=False)

for _, row in df.iterrows():
    prov = row["label_provider_id"]
    if pd.isna(prov):
        canal = "Popup"
    elif int(prov) == 1536:
        canal = "WhatsApp"
    elif int(prov) == 611:
        canal = "Push"
    else:
        canal = f"Outro({prov})"
    print(f"{row['resource_id']:>10} | {canal:<10} | {row['primeira_data']} | {row['total_entregues']:>7} | {row['resource_name']}")

print(f"\nTotal: {len(df)} resource_ids")
