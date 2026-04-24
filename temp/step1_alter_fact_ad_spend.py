"""
Passo 1: adicionar colunas page_views e reach em multibet.fact_ad_spend.
- Snapshot antes (count, sum) para garantir que nada sumiu
- ALTER TABLE ADD COLUMN IF NOT EXISTS (idempotente)
- Snapshot depois
- Lista schema final
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

SNAPSHOT_SQL = """
SELECT ad_source,
       COUNT(*) AS linhas,
       MIN(dt) AS dt_min,
       MAX(dt) AS dt_max,
       SUM(cost_brl)::numeric(14,2) AS spend_total,
       SUM(clicks) AS clicks,
       SUM(impressions) AS impressions
FROM multibet.fact_ad_spend
GROUP BY ad_source
ORDER BY ad_source
"""

ALTER_SQL = """
ALTER TABLE multibet.fact_ad_spend
    ADD COLUMN IF NOT EXISTS page_views BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reach BIGINT DEFAULT 0
"""

SCHEMA_SQL = """
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_schema = 'multibet' AND table_name = 'fact_ad_spend'
ORDER BY ordinal_position
"""

def print_snapshot(label, rows):
    print(f"\n=== SNAPSHOT {label} ===")
    print(f"{'source':<12} {'linhas':>7} {'dt_min':<12} {'dt_max':<12} {'spend':>15} {'clicks':>10} {'impressions':>14}")
    for r in rows:
        print(f"{r[0]:<12} {r[1]:>7,} {str(r[2]):<12} {str(r[3]):<12} "
              f"R$ {float(r[4] or 0):>12,.2f} {r[5]:>10,} {r[6]:>14,}")


tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        # 1. Snapshot antes
        cur.execute(SNAPSHOT_SQL)
        before = cur.fetchall()
        print_snapshot("ANTES", before)

        # 2. ALTER TABLE
        print("\n=== EXECUTANDO ALTER TABLE ===")
        cur.execute(ALTER_SQL)
        conn.commit()
        print("ALTER TABLE ADD COLUMN IF NOT EXISTS page_views, reach — OK")

        # 3. Snapshot depois
        cur.execute(SNAPSHOT_SQL)
        after = cur.fetchall()
        print_snapshot("DEPOIS", after)

        # 4. Schema final
        cur.execute(SCHEMA_SQL)
        cols = cur.fetchall()
        print("\n=== SCHEMA FINAL ===")
        for c in cols:
            marker = "  <-- NOVA" if c[0] in ("page_views", "reach") else ""
            print(f"  {c[0]:<18} {c[1]:<25} default={c[2] or '-'}{marker}")

        # 5. Validacao: numeros iguais antes/depois?
        print("\n=== VALIDACAO ===")
        ok = True
        for b, a in zip(before, after):
            if b != a:
                print(f"  DIVERGENCIA em {b[0]}: {b} -> {a}")
                ok = False
        if ok:
            print("  OK — nenhuma linha perdida, spend/clicks/impressions identicos")
finally:
    conn.close()
    tunnel.stop()
