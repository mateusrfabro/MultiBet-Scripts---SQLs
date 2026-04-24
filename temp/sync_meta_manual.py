"""Sync manual fact_ad_spend (Meta) com token BM2 — recupera 20-22/04 (D-1)."""
import sys, os, logging
from datetime import date, timedelta, datetime, timezone

# Sobrescrever token ANTES de carregar modulo
os.environ["META_ADS_ACCESS_TOKEN"] = "EAASFqlKv054BRQredZAPZBOVxA3ztZBZC8C8ZB5oV0ZC1G9qGZB3YzFRZAXWnW6WtwhjYne3bdoO8Afo19en1tMrijMwF6h1mzwplbWwn6R0etsboWyJHqdeUzlWBS09DQjXQd6ttSJ6SW9wTCSK60ZCXnwa3vvhNaBmUKTy30XciUOg6EsrgTGrMayz6AgignNfondWM"

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.meta_ads import get_campaign_spend, META_ADS_ACCOUNT_IDS
from db.supernova import get_supernova_connection
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# Excluir conta sem permissao no token BM2 (Multibet sem BM)
EXCLUDE = {"act_846913941192022"}
ACCOUNTS = [a for a in META_ADS_ACCOUNT_IDS if a not in EXCLUDE]
log.info(f"Contas a sincronizar: {len(ACCOUNTS)}/{len(META_ADS_ACCOUNT_IDS)} "
         f"(excluida: {', '.join(EXCLUDE)} — sem permissao BM2)")

DAYS = 3
end_date = date.today() - timedelta(days=1)        # D-1 (regra obrigatoria)
start_date = end_date - timedelta(days=DAYS - 1)
log.info(f"Periodo: {start_date} a {end_date} ({DAYS} dias)")

# 1. Buscar API conta-a-conta (resiliente — uma conta nao derruba as outras)
all_rows = []
for acc in ACCOUNTS:
    try:
        rows = get_campaign_spend(start_date=start_date, end_date=end_date, account_ids=[acc])
        all_rows.extend(rows)
    except Exception as e:
        log.error(f"FALHA {acc}: {e}")

total_spend = sum(r["cost_brl"] for r in all_rows)
log.info(f"API total: {len(all_rows)} linhas | Spend: R$ {total_spend:,.2f}")

if not all_rows:
    log.error("Nenhum dado — abortando persistencia.")
    sys.exit(1)

# 2. Preparar records
now_utc = datetime.now(timezone.utc)
insert_sql = """
    INSERT INTO multibet.fact_ad_spend
        (dt, ad_source, campaign_id, campaign_name, channel_type,
         cost_brl, impressions, clicks, conversions,
         affiliate_id, refreshed_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
records = [
    (r["date"], "meta", r["campaign_id"], r["campaign_name"][:500],
     r.get("account_name", "")[:50], r["cost_brl"], r["impressions"], r["clicks"],
     r["conversions"], None, now_utc)
    for r in all_rows
]

# 3. DELETE periodo+fonte + INSERT (idempotente)
tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM multibet.fact_ad_spend "
            "WHERE dt BETWEEN %s AND %s AND ad_source = 'meta'",
            (start_date, end_date),
        )
        log.info(f"DELETE {cur.rowcount} registros antigos no periodo")
        psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
        log.info(f"INSERT {len(records)} registros novos")
    conn.commit()

    # 4. Verificar resultado na tabela
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dt, COUNT(*) AS linhas,
                   COUNT(DISTINCT campaign_id) AS campanhas,
                   SUM(cost_brl)::numeric(14,2) AS spend
            FROM multibet.fact_ad_spend
            WHERE ad_source = 'meta' AND dt BETWEEN %s AND %s
            GROUP BY dt ORDER BY dt
        """, (start_date, end_date))
        rows = cur.fetchall()
        log.info("=== POS-CARGA (fact_ad_spend WHERE ad_source='meta') ===")
        for r in rows:
            log.info(f"  {r[0]} | {r[1]:>3} linhas | {r[2]:>3} camp | R$ {float(r[3] or 0):>10,.2f}")
finally:
    conn.close()
    tunnel.stop()

log.info("CONCLUIDO")
