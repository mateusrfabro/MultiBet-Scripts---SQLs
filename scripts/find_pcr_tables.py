"""Localiza tabelas pcr/segmentacao no Super Nova DB."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova import execute_supernova

rows = execute_supernova("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_name ILIKE '%pcr%' OR table_name ILIKE '%segmen%' OR table_name ILIKE '%seg_pop%'
    ORDER BY table_schema, table_name
""", fetch=True)

print(f"Tabelas encontradas: {len(rows)}")
for r in rows:
    print(f"  {r[0]}.{r[1]}")
