"""
Passo 4: Rodar sync_meta_spend para 1 dia (D-1) com token BM2 e conta sem permissao excluida.
Valida que page_views e reach sao preenchidos.
NAO mexe em outros periodos.
"""
import sys, os, logging
from datetime import date, timedelta, datetime, timezone

os.environ["META_ADS_ACCESS_TOKEN"] = "EAASFqlKv054BRQredZAPZBOVxA3ztZBZC8C8ZB5oV0ZC1G9qGZB3YzFRZAXWnW6WtwhjYne3bdoO8Afo19en1tMrijMwF6h1mzwplbWwn6R0etsboWyJHqdeUzlWBS09DQjXQd6ttSJ6SW9wTCSK60ZCXnwa3vvhNaBmUKTy30XciUOg6EsrgTGrMayz6AgignNfondWM"

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.meta_ads import get_campaign_spend, META_ADS_ACCOUNT_IDS
from db.supernova import get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

EXCLUDE = {"act_846913941192022"}
ACCOUNTS = [a for a in META_ADS_ACCOUNT_IDS if a not in EXCLUDE]

end_date = date.today() - timedelta(days=1)
start_date = end_date  # 1 dia so
log.info(f"Teste D-1: {start_date} (1 dia, {len(ACCOUNTS)} contas)")

# 1. API
all_rows = []
for acc in ACCOUNTS:
    try:
        rows = get_campaign_spend(start_date=start_date, end_date=end_date, account_ids=[acc])
        all_rows.extend(rows)
    except Exception as e:
        log.error(f"FALHA {acc}: {e}")

if not all_rows:
    log.error("Zero linhas — abortando")
    sys.exit(1)

# Sanity check: tem page_views preenchido?
total_pv = sum(r.get("page_views", 0) for r in all_rows)
total_reach = sum(r.get("reach", 0) for r in all_rows)
total_cost = sum(r["cost_brl"] for r in all_rows)
log.info(f"API: {len(all_rows)} linhas | cost R$ {total_cost:,.2f} | "
         f"page_views {total_pv:,} | reach {total_reach:,}")

# 2. INSERT (replica sync_meta_spend.py)
now_utc = datetime.now(timezone.utc)
insert_sql = """
    INSERT INTO multibet.fact_ad_spend
        (dt, ad_source, campaign_id, campaign_name, channel_type,
         cost_brl, impressions, clicks, conversions,
         page_views, reach,
         affiliate_id, refreshed_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
records = [
    (r["date"], "meta", r["campaign_id"], r["campaign_name"][:500],
     r.get("account_name", "")[:50], r["cost_brl"], r["impressions"], r["clicks"],
     r["conversions"], r.get("page_views", 0), r.get("reach", 0), None, now_utc)
    for r in all_rows
]

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM multibet.fact_ad_spend "
            "WHERE dt BETWEEN %s AND %s AND ad_source = 'meta'",
            (start_date, end_date),
        )
        log.info(f"DELETE {cur.rowcount} linhas antigas")
        psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        log.info(f"INSERT {len(records)} linhas")
    conn.commit()

    # Verificar
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dt, COUNT(*) linhas, SUM(cost_brl)::numeric(14,2) spend,
                   SUM(clicks) clicks, SUM(impressions) impressions,
                   SUM(page_views) page_views, SUM(reach) reach,
                   COUNT(*) FILTER (WHERE page_views > 0) linhas_com_pv
            FROM multibet.fact_ad_spend
            WHERE ad_source='meta' AND dt = %s
            GROUP BY dt
        """, (start_date,))
        r = cur.fetchone()
        log.info("=== RESULTADO D-1 ===")
        log.info(f"  dt: {r[0]} | linhas: {r[1]}")
        log.info(f"  cost: R$ {float(r[2]):,.2f} | clicks: {r[3]:,} | imps: {r[4]:,}")
        log.info(f"  page_views: {r[5]:,} | reach: {r[6]:,}")
        log.info(f"  linhas com page_views>0: {r[7]} de {r[1]}")

        # KPIs derivados (CPC, CTR, CPV)
        cost = float(r[2] or 0); clicks = r[3] or 0
        imps = r[4] or 0; pvs = r[5] or 0
        log.info(f"  CPC: R$ {cost/clicks:.2f}" if clicks else "  CPC: n/a")
        log.info(f"  CTR: {100*clicks/imps:.2f}%" if imps else "  CTR: n/a")
        log.info(f"  CPV: R$ {cost/pvs:.2f}" if pvs else "  CPV: n/a")
finally:
    conn.close()
    tunnel.stop()

log.info("TESTE D-1 CONCLUIDO")
