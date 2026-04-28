"""
Inspeciona schemas das tabelas pro Bloco 3 antes de codar.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from db.athena import query_athena

queries = {
    "fct_casino_activity_daily": "SHOW COLUMNS FROM ps_bi.fct_casino_activity_daily",
    "fct_casino_activity_hourly": "SHOW COLUMNS FROM ps_bi.fct_casino_activity_hourly",
    "fct_deposits_hourly": "SHOW COLUMNS FROM ps_bi.fct_deposits_hourly",
    "dim_game": "SHOW COLUMNS FROM ps_bi.dim_game",
    "fct_sports_book_activity_daily": "SHOW TABLES IN ps_bi LIKE 'fct_sports_book%'",
}

for name, sql in queries.items():
    print(f"\n=== {name} ===")
    try:
        df = query_athena(sql, database="ps_bi")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"ERRO: {e}")
