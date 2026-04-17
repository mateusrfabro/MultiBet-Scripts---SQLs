"""Parte 4 — validar silver_game_15min (PostgreSQL) como fonte horaria."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova
from db.athena import query_athena

def secao(t):
    print("\n" + "=" * 70); print(t); print("=" * 70)


# A. silver_game_15min - schema PostgreSQL
secao("A. silver_game_15min — schema (PostgreSQL Super Nova)")
rows = execute_supernova("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'multibet' AND table_name = 'silver_game_15min'
    ORDER BY ordinal_position
""", fetch=True)
for c, t in rows:
    print(f"  {c:<35} {t}")


# B. silver_game_15min - amostra recente
secao("B. silver_game_15min — amostra (5 linhas mais recentes)")
rows = execute_supernova("""
    SELECT * FROM multibet.silver_game_15min
    ORDER BY 1 DESC
    LIMIT 5
""", fetch=True)
for r in rows:
    print(f"  {r}")


# C. silver_game_activity (a outra opcao)
secao("C. silver_game_activity — schema")
rows = execute_supernova("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'multibet' AND table_name = 'silver_game_activity'
    ORDER BY ordinal_position
""", fetch=True)
for c, t in rows:
    print(f"  {c:<35} {t}")


# D. game_paid_15min view
secao("D. game_paid_15min VIEW — definicao")
rows = execute_supernova("""
    SELECT view_definition FROM information_schema.views
    WHERE table_schema = 'multibet' AND table_name = 'game_paid_15min'
""", fetch=True)
if rows:
    print(rows[0][0])


# E. fund_ec2 schema (Athena) — campo timestamp
secao("E. fund_ec2.tbl_real_fund_txn — campos timestamp (DESCRIBE)")
df = query_athena("DESCRIBE fund_ec2.tbl_real_fund_txn", database="fund_ec2")
print(df.head(60).to_string(index=False))
