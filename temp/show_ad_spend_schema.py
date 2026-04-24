"""Mostra schema atual de multibet.fact_ad_spend."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

sql = """
SELECT column_name, data_type,
       character_maximum_length AS max_len,
       is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'multibet' AND table_name = 'fact_ad_spend'
ORDER BY ordinal_position
"""
rows = execute_supernova(sql, fetch=True)
print(f"{'coluna':<20} {'tipo':<22} {'tamanho':<8} {'null':<5} default")
print("-" * 75)
for r in rows:
    print(f"{r[0]:<20} {r[1]:<22} {str(r[2] or '-'):<8} {r[3]:<5} {r[4] or '-'}")
