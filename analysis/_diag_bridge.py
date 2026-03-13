"""Diagnóstico: tabela bridge ecr.tbl_ecr — mapeamento c_ecr_id x c_external_id."""
import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.redshift import query_redshift

# 1. Colunas da tabela bridge
print("=== 1. Colunas de ecr.tbl_ecr ===")
df_cols = query_redshift("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'ecr' AND table_name = 'tbl_ecr'
ORDER BY ordinal_position
LIMIT 30
""")
print(df_cols.to_string())

# 2. Amostra de mapeamento — LIMIT 5 para ser rápido
print("\n=== 2. Amostra c_ecr_id x c_external_id (label multibet) ===")
df_sample = query_redshift("""
SELECT c_ecr_id, c_external_id, c_label_id
FROM ecr.tbl_ecr
WHERE c_label_id = 'multibet'
LIMIT 5
""")
print(df_sample.to_string())

# 3. Confirma que c_external_id (15 dígitos) bate com BQ user_ext_id
# Usando um ecr_id conhecido do Redshift: 131071768181502029
print("\n=== 3. c_external_id para c_ecr_id conhecido do bireports ===")
df_check = query_redshift("""
SELECT c_ecr_id, c_external_id
FROM ecr.tbl_ecr
WHERE c_ecr_id = 131071768181502029
LIMIT 1
""")
print(df_check.to_string())
