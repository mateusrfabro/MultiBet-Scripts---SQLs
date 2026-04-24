"""Explora ps_bi.dim_user pra ver se ja tem acquisition_source/utm nativos."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

# 1. Schema completo
sql1 = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'ps_bi' AND table_name = 'dim_user'
ORDER BY ordinal_position
"""
df = query_athena(sql1, database="ps_bi")
print("=== ps_bi.dim_user schema ===")
for _, r in df.iterrows():
    # destacar colunas potencialmente uteis
    hit = any(k in r['column_name'].lower() for k in ['utm', 'source', 'acqui', 'affil', 'track', 'campaign', 'signup', 'first'])
    marker = " <<<" if hit else ""
    print(f"  {r['column_name']:<35} {r['data_type']}{marker}")
