"""Diagnóstico: mapeamento ecr_id entre BQ (Smartico) e Redshift (Pragmatic)."""
import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.bigquery import query_bigquery
from db.redshift import query_redshift
import pandas as pd

# 1. Amostra de user_ext_id entregues na Ativação (BQ)
print("=== 1. user_ext_id de entregues — Ativação (BQ, 20 amostras) ===")
df_bq = query_bigquery("""
SELECT DISTINCT user_ext_id
FROM `smartico-bq6.dwh_ext_24105.j_communication`
WHERE label_id = 24105
  AND resource_id = 159256
  AND fact_type_id = 2
  AND user_ext_id IS NOT NULL
LIMIT 20
""")
print(df_bq.to_string())

# 2. Amostra FTDs no Redshift com c_label_id
print("\n=== 2. FTDs no Redshift — período 06-09/03 (20 amostras) ===")
df_rs = query_redshift("""
SELECT c_ecr_id, c_label_id, DATE(c_conversion_time) AS conv_date
FROM ecr.tbl_ecr_conversion_info
WHERE c_conversion_time >= '2026-03-06'
  AND c_conversion_time  < dateadd(day, 1, '2026-03-09')
LIMIT 20
""")
print(df_rs.to_string())

# 3. Distribuição de label_id na tabela FTD
print("\n=== 3. Label IDs em tbl_ecr_conversion_info (período) ===")
df_labels = query_redshift("""
SELECT c_label_id, COUNT(*) as cnt
FROM ecr.tbl_ecr_conversion_info
WHERE c_conversion_time >= '2026-03-06'
  AND c_conversion_time  < dateadd(day, 1, '2026-03-09')
GROUP BY c_label_id
ORDER BY cnt DESC
""")
print(df_labels.to_string())

# 4. Verificar se algum ecr_id da Ativação bate com o Redshift FTD
print("\n=== 4. Overlap: Ativação depositantes (BQ) x FTD (Redshift) ===")
df_dep_atv = query_bigquery("""
SELECT CAST(u.user_ext_id AS INT64) AS ecr_id
FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` d
JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON d.user_id = u.user_id
WHERE d.label_id = 24105
  AND DATE(d.event_time) BETWEEN '2026-03-06' AND '2026-03-09'
  AND (d.acc_is_rollback IS NULL OR d.acc_is_rollback = FALSE)
GROUP BY u.user_ext_id
""")
df_ftd_rs = query_redshift("""
SELECT c_ecr_id AS ecr_id
FROM ecr.tbl_ecr_conversion_info
WHERE c_conversion_time >= '2026-03-06'
  AND c_conversion_time  < dateadd(day, 1, '2026-03-09')
""")
df_ftd_rs["ecr_id"] = df_ftd_rs["ecr_id"].astype(int)
df_dep_atv["ecr_id"] = df_dep_atv["ecr_id"].astype(int)

overlap = df_dep_atv.merge(df_ftd_rs, on="ecr_id", how="inner")
print(f"Depositantes BQ período: {len(df_dep_atv)}")
print(f"FTDs Redshift período  : {len(df_ftd_rs)}")
print(f"Overlap (match)        : {len(overlap)}")

if len(overlap) == 0:
    print("\n⚠️  Nenhum ecr_id coincide. Amostras para comparação manual:")
    print("BQ  (primeiros 10):", sorted(df_dep_atv['ecr_id'].head(10).tolist()))
    print("RS  (primeiros 10):", sorted(df_ftd_rs['ecr_id'].head(10).tolist()))
