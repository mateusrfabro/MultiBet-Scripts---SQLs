"""Investiga o que temos pra montar fact_ad_results (REG/FTD por campaign_id)."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        # 1. multibet.trackings — schema primeiro
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='trackings'
            ORDER BY ordinal_position
        """)
        print("=== multibet.trackings schema ===")
        for c in cur.fetchall():
            print(f"  {c[0]:<25} {c[1]}")

        cur.execute("""
            SELECT COUNT(*), MIN(created_at), MAX(created_at),
                   COUNT(DISTINCT user_id) AS users
            FROM multibet.trackings
        """)
        r = cur.fetchone()
        print(f"\nlinhas={r[0]:,} | periodo={r[1]} a {r[2]} | users={r[3]:,}")

        # 2. dim_marketing_mapping — schema primeiro
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='dim_marketing_mapping'
            ORDER BY ordinal_position
        """)
        print("\n=== multibet.dim_marketing_mapping schema ===")
        cols_mapping = [c[0] for c in cur.fetchall()]
        for c in cols_mapping:
            print(f"  {c}")
        cur.execute("SELECT COUNT(*) FROM multibet.dim_marketing_mapping")
        print(f"linhas: {cur.fetchone()[0]:,}")
        # amostra
        cur.execute("SELECT * FROM multibet.dim_marketing_mapping LIMIT 3")
        print("amostra:")
        for row in cur.fetchall():
            print(" | ".join(str(v)[:40] for v in row))

        # 3. Existe ou nao dim_campaign_affiliate (mapping campaign_id das ads)
        cur.execute("""
            SELECT ad_source, COUNT(*), COUNT(affiliate_id)
            FROM multibet.dim_campaign_affiliate
            GROUP BY ad_source ORDER BY ad_source
        """)
        print("\n=== multibet.dim_campaign_affiliate ===")
        print(f"{'source':<12} {'linhas':>7} {'com_aff':>8}")
        for r in cur.fetchall():
            print(f"{r[0]:<12} {r[1]:>7,} {r[2]:>8,}")

        # 4. Amostra de dim_marketing_mapping pra entender relacionamento
        cur.execute("""
            SELECT * FROM multibet.dim_marketing_mapping LIMIT 5
        """)
        print("\n=== AMOSTRA dim_marketing_mapping (5 linhas) ===")
        cols = [d[0] for d in cur.description]
        print(" | ".join(cols))
        for row in cur.fetchall():
            print(" | ".join(str(v)[:30] for v in row))
finally:
    conn.close()
    tunnel.stop()
