"""Auditoria pipeline madrugada 29/04/2026 (via Super Nova DB)."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import timezone, timedelta
from db.supernova import execute_supernova

BRT = timezone(timedelta(hours=-3))


def to_brt(ts):
    if ts is None:
        return "NULL"
    if hasattr(ts, "tzinfo"):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(BRT).strftime("%Y-%m-%d %H:%M:%S BRT")
    return str(ts)


def section(title):
    print(f"\n{'=' * 80}\n{title}\n{'=' * 80}")


def get_columns(table_full):
    schema, table = table_full.split(".")
    rows = execute_supernova(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='{schema}' AND table_name='{table}'
        ORDER BY ordinal_position
    """, fetch=True)
    return [r[0] for r in rows]


# 1. PCR_ATUAL
section("1. multibet.pcr_atual (PCR cron 03:30 BRT)")
print(f"  Colunas: {get_columns('multibet.pcr_atual')}\n")

rows = execute_supernova("""
    SELECT COUNT(*), MAX(snapshot_date)
    FROM multibet.pcr_atual
""", fetch=True)
total, snap = rows[0]
print(f"  Total jogadores:     {total:,}")
print(f"  Ultimo snapshot:     {snap}")

# 2. Distribuicao por rating no ultimo snapshot
section("2. Distribuicao por rating (ultimo snapshot)")
rows = execute_supernova(f"""
    SELECT rating, COUNT(*) AS qtd
    FROM multibet.pcr_atual
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM multibet.pcr_atual)
    GROUP BY rating
    ORDER BY rating
""", fetch=True)
for r in rows:
    print(f"  {str(r[0]):<10} {r[1]:>10,}")

# 3. SEGMENTACAO_SA
section("3. multibet.segmentacao_sa_diaria (Segmentacao cron 04:00 BRT)")
print(f"  Colunas: {get_columns('multibet.segmentacao_sa_diaria')[:15]}...\n")

rows = execute_supernova("""
    SELECT
        snapshot_date,
        COUNT(*) AS total,
        SUM(CASE WHEN rating='S' THEN 1 ELSE 0 END) AS rating_s,
        SUM(CASE WHEN rating='A' THEN 1 ELSE 0 END) AS rating_a
    FROM multibet.segmentacao_sa_diaria
    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '5 days'
    GROUP BY snapshot_date
    ORDER BY snapshot_date DESC
""", fetch=True)

print(f"  {'Snapshot':<15} {'Total':>10} {'Rating S':>10} {'Rating A':>10}")
print(f"  {'-' * 50}")
for r in rows:
    print(f"  {str(r[0]):<15} {r[1]:>10,} {r[2]:>10,} {r[3]:>10,}")

# 4. Validacao explicita 29/04
section("4. Validacao 29/04/2026")
rows = execute_supernova("""
    SELECT
        EXISTS(SELECT 1 FROM multibet.pcr_atual WHERE snapshot_date='2026-04-29') AS pcr_29,
        EXISTS(SELECT 1 FROM multibet.segmentacao_sa_diaria WHERE snapshot_date='2026-04-29') AS seg_29,
        (SELECT COUNT(*) FROM multibet.pcr_atual WHERE snapshot_date='2026-04-29') AS pcr_count,
        (SELECT COUNT(*) FROM multibet.segmentacao_sa_diaria WHERE snapshot_date='2026-04-29') AS seg_count
""", fetch=True)
r = rows[0]
print(f"  PCR (cron 03:30)        rodou em 29/04? {'SIM' if r[0] else 'NAO'}  -- {r[2]:,} jogadores")
print(f"  Segmentacao (04:00)     rodou em 29/04? {'SIM' if r[1] else 'NAO'}  -- {r[3]:,} jogadores S+A")
