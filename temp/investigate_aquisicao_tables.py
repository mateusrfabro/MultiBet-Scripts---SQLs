"""Investiga tab_user_affiliate e silver_tab_user_ftd pra desenhar a view."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        for tbl in ["tab_user_affiliate", "silver_tab_user_ftd"]:
            cur.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema='multibet' AND table_name='{tbl}'
                ORDER BY ordinal_position
            """)
            cols = cur.fetchall()
            print(f"\n=== multibet.{tbl} ({len(cols)} colunas) ===")
            for c in cols: print(f"  {c[0]:<25} {c[1]}")
            cur.execute(f"SELECT COUNT(*), MIN(data_registro) FROM multibet.{tbl}" if tbl=="tab_user_affiliate" else f"SELECT COUNT(*), MIN(ftd_data) FROM multibet.{tbl}")
            r = cur.fetchone()
            print(f"  linhas={r[0]:,} | min_dt={r[1]}")
finally:
    conn.close()
    tunnel.stop()
