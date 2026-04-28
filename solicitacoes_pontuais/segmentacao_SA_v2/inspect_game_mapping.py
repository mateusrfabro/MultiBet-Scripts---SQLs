"""Inspeciona schemas e samples das 3 tabelas de game mapping."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from db.supernova import execute_supernova

for tbl in ("game_image_mapping", "dim_games_catalog"):
    print(f"\n=== multibet.{tbl} ===")
    rows = execute_supernova(
        f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'multibet' AND table_name = '{tbl}'
        ORDER BY ordinal_position;
        """, fetch=True,
    )
    for r in rows:
        print(f"  {r[0]:<35} {r[1]}")
    cnt = execute_supernova(f"SELECT COUNT(*) FROM multibet.{tbl}", fetch=True)
    print(f"  -> {cnt[0][0]:,} linhas")
    sample = execute_supernova(f"SELECT * FROM multibet.{tbl} LIMIT 3", fetch=True)
    cols = [r[0] for r in execute_supernova(
        f"""SELECT column_name FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='{tbl}'
            ORDER BY ordinal_position""", fetch=True)]
    print("  Sample 3 linhas:")
    for s in sample:
        print("    " + " | ".join(f"{c}={v}" for c, v in zip(cols, s) if v is not None))
