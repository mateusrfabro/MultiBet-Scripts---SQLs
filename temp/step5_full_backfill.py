"""
Passo 5: Full backfill de fact_ad_spend (Meta + Google) com colunas page_views/reach.
Estrategia: DELETE periodo+fonte + INSERT (idempotente) para todo o historico.
Valida snapshot antes/depois (cost_brl, clicks, impressions tem que bater com margem <1%).
"""
import sys, os, logging
from datetime import date, timedelta, datetime, timezone

os.environ["META_ADS_ACCESS_TOKEN"] = "EAASFqlKv054BRQredZAPZBOVxA3ztZBZC8C8ZB5oV0ZC1G9qGZB3YzFRZAXWnW6WtwhjYne3bdoO8Afo19en1tMrijMwF6h1mzwplbWwn6R0etsboWyJHqdeUzlWBS09DQjXQd6ttSJ6SW9wTCSK60ZCXnwa3vvhNaBmUKTy30XciUOg6EsrgTGrMayz6AgignNfondWM"

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.meta_ads import get_campaign_spend as meta_get, META_ADS_ACCOUNT_IDS
from db.google_ads import get_campaign_spend as google_get
from db.supernova import get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

EXCLUDE_META = {"act_846913941192022"}
ACCOUNTS_META = [a for a in META_ADS_ACCOUNT_IDS if a not in EXCLUDE_META]

INSERT_SQL = """
    INSERT INTO multibet.fact_ad_spend
        (dt, ad_source, campaign_id, campaign_name, channel_type,
         cost_brl, impressions, clicks, conversions,
         page_views, reach,
         affiliate_id, refreshed_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

SNAP_SQL = """
SELECT ad_source, COUNT(*) linhas, MIN(dt), MAX(dt),
       SUM(cost_brl)::numeric(14,2), SUM(clicks), SUM(impressions),
       SUM(page_views), SUM(reach)
FROM multibet.fact_ad_spend GROUP BY ad_source ORDER BY ad_source
"""

def print_snap(label, rows):
    log.info(f"=== {label} ===")
    log.info(f"  {'source':<12} {'linhas':>7} {'periodo':<26} {'spend':>14} {'clicks':>10} {'imps':>12} {'pv':>10} {'reach':>10}")
    for r in rows:
        per = f"{r[2]} a {r[3]}"
        log.info(f"  {r[0]:<12} {r[1]:>7,} {per:<26} R$ {float(r[4]):>11,.2f} {r[5]:>10,} {r[6]:>12,} {r[7]:>10,} {r[8]:>10,}")

# ============================================================
# Snapshot ANTES
# ============================================================
tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(SNAP_SQL)
        before = cur.fetchall()
    print_snap("SNAPSHOT ANTES", before)

    # ==========================================================
    # META BACKFILL — 14/01 a 23/04 (todo historico)
    # ==========================================================
    end_date = date.today() - timedelta(days=1)  # 2026-04-23
    start_meta = date(2026, 1, 14)
    log.info(f"\n>>> META backfill {start_meta} a {end_date}")

    meta_rows = []
    for acc in ACCOUNTS_META:
        try:
            rs = meta_get(start_date=start_meta, end_date=end_date, account_ids=[acc])
            meta_rows.extend(rs)
        except Exception as e:
            log.error(f"  FALHA {acc}: {e}")
    log.info(f"META API: {len(meta_rows)} linhas | spend R$ {sum(r['cost_brl'] for r in meta_rows):,.2f} | pv {sum(r.get('page_views',0) for r in meta_rows):,}")

    if meta_rows:
        now_utc = datetime.now(timezone.utc)
        records = [
            (r["date"], "meta", r["campaign_id"], r["campaign_name"][:500],
             r.get("account_name", "")[:50], r["cost_brl"], r["impressions"], r["clicks"],
             r["conversions"], r.get("page_views", 0), r.get("reach", 0), None, now_utc)
            for r in meta_rows
        ]
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM multibet.fact_ad_spend WHERE dt BETWEEN %s AND %s AND ad_source='meta'",
                (start_meta, end_date),
            )
            log.info(f"META DELETE: {cur.rowcount} linhas")
            psycopg2.extras.execute_batch(cur, INSERT_SQL, records, page_size=500)
            log.info(f"META INSERT: {len(records)} linhas")
        conn.commit()
    else:
        log.warning("META sem linhas — pulando persistencia")

    # ==========================================================
    # GOOGLE BACKFILL — mesmo periodo
    # ==========================================================
    log.info(f"\n>>> GOOGLE backfill {start_meta} a {end_date}")
    try:
        google_rows = google_get(start_date=start_meta, end_date=end_date)
    except Exception as e:
        log.error(f"  FALHA google: {e}")
        google_rows = []
    log.info(f"GOOGLE API: {len(google_rows)} linhas | spend R$ {sum(r['cost_brl'] for r in google_rows):,.2f}")

    if google_rows:
        # Carregar mapping affiliate (igual sync_google_ads_spend)
        with conn.cursor() as cur:
            cur.execute("SELECT campaign_id, affiliate_id FROM multibet.dim_campaign_affiliate WHERE ad_source = 'google_ads'")
            mapping = {row[0]: row[1] for row in cur.fetchall()}

        now_utc = datetime.now(timezone.utc)
        records = [
            (r["date"], "google_ads", r["campaign_id"], r["campaign_name"][:500],
             r.get("channel_type", ""), r["cost_brl"], r["impressions"], r["clicks"],
             r["conversions"], r.get("page_views", 0), r.get("reach", 0),
             mapping.get(r["campaign_id"]), now_utc)
            for r in google_rows
        ]
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM multibet.fact_ad_spend WHERE dt BETWEEN %s AND %s AND ad_source='google_ads'",
                (start_meta, end_date),
            )
            log.info(f"GOOGLE DELETE: {cur.rowcount} linhas")
            psycopg2.extras.execute_batch(cur, INSERT_SQL, records, page_size=500)
            log.info(f"GOOGLE INSERT: {len(records)} linhas")
        conn.commit()
    else:
        log.warning("GOOGLE sem linhas — pulando persistencia")

    # ==========================================================
    # Snapshot DEPOIS + validacao
    # ==========================================================
    with conn.cursor() as cur:
        cur.execute(SNAP_SQL)
        after = cur.fetchall()
    print_snap("SNAPSHOT DEPOIS", after)

    # Validacao: variacao % dos KPIs financeiros
    log.info("=== VARIACAO (antes -> depois) ===")
    log.info(f"  {'source':<12} {'linhas_delta':<15} {'spend_delta_%':<15} {'clicks_delta_%':<15}")
    for b, a in zip(before, after):
        if b[0] == a[0]:
            delta_lin = a[1] - b[1]
            delta_spend_pct = 100 * (float(a[4]) - float(b[4])) / float(b[4]) if float(b[4]) else 0
            delta_clicks_pct = 100 * (a[5] - b[5]) / b[5] if b[5] else 0
            log.info(f"  {a[0]:<12} {delta_lin:>+7,}         {delta_spend_pct:>+6.2f}%         {delta_clicks_pct:>+6.2f}%")
finally:
    conn.close()
    tunnel.stop()

log.info("BACKFILL COMPLETO")
