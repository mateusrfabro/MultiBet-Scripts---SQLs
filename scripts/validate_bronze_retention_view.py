"""
Valida tabelas bronze no Super Nova DB e cria view de retenção semanal.
Gráfico: Active Player Retention vs Repeat Depositors
"""
import sys
sys.path.insert(0, ".")
from db.supernova import execute_supernova

# 1) Listar tabelas do schema multibet
print("=== TABELAS NO SCHEMA MULTIBET ===")
tables = execute_supernova("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'multibet'
    ORDER BY table_name
""", fetch=True)

for t in tables:
    print(f"  {t[1]:10s} | {t[0]}")

# 2) Checar se tabelas de depósito bronze existem
deposit_tables = [t[0] for t in tables]
needed = [
    'tbl_cashier_ecr_daily_payment_summary',  # ideal: agregado diário
    'tbl_cashier_deposit',                      # alternativa: transações
    'tbl_ecr',                                  # para test user filter
    'tbl_ecr_flags',                            # test user flag
]

print("\n=== TABELAS NECESSÁRIAS ===")
for n in needed:
    status = "ENCONTRADA" if n in deposit_tables else "NAO ENCONTRADA"
    print(f"  {n}: {status}")

# 3) Se daily summary existe, checar colunas e amostra
if 'tbl_cashier_ecr_daily_payment_summary' in deposit_tables:
    print("\n=== COLUNAS tbl_cashier_ecr_daily_payment_summary ===")
    cols = execute_supernova("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'multibet'
          AND table_name = 'tbl_cashier_ecr_daily_payment_summary'
        ORDER BY ordinal_position
    """, fetch=True)
    for c in cols:
        print(f"  {c[0]:45s} {c[1]}")

    print("\n=== AMOSTRA (5 rows) ===")
    sample = execute_supernova("""
        SELECT * FROM multibet.tbl_cashier_ecr_daily_payment_summary LIMIT 5
    """, fetch=True)
    for row in sample:
        print(f"  {row}")

    print("\n=== RANGE DE DATAS ===")
    dt_range = execute_supernova("""
        SELECT MIN(c_created_date), MAX(c_created_date), COUNT(*)
        FROM multibet.tbl_cashier_ecr_daily_payment_summary
    """, fetch=True)
    print(f"  De {dt_range[0][0]} até {dt_range[0][1]} | {dt_range[0][2]} registros")

elif 'tbl_cashier_deposit' in deposit_tables:
    print("\n=== COLUNAS tbl_cashier_deposit ===")
    cols = execute_supernova("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'multibet' AND table_name = 'tbl_cashier_deposit'
        ORDER BY ordinal_position
    """, fetch=True)
    for c in cols:
        print(f"  {c[0]:45s} {c[1]}")

    print("\n=== RANGE DE DATAS ===")
    dt_range = execute_supernova("""
        SELECT MIN(c_created_time), MAX(c_created_time), COUNT(*)
        FROM multibet.tbl_cashier_deposit
    """, fetch=True)
    print(f"  De {dt_range[0][0]} até {dt_range[0][1]} | {dt_range[0][2]} registros")
else:
    print("\nNENHUMA tabela de depósito bronze encontrada. ETL precisa rodar primeiro.")

# 4) Checar se já existe view de retenção
if 'vw_active_player_retention_weekly' in deposit_tables:
    print("\n=== VIEW JÁ EXISTE! Amostra: ===")
    sample = execute_supernova("""
        SELECT * FROM multibet.vw_active_player_retention_weekly
        ORDER BY semana DESC LIMIT 5
    """, fetch=True)
    for row in sample:
        print(f"  {row}")

print("\nDone.")