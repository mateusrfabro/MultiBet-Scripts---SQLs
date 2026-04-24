"""
Pipeline: sync_meta_spend (Meta Ads API -> Super Nova DB)
=========================================================
Puxa spend diario por campanha da Meta Ads API (Graph API)
e persiste em multibet.fact_ad_spend com ad_source = 'meta'.

Usa a mesma tabela multi-canal do Google Ads.

Contas MultiBet BRL configuradas no .env:
    META_ADS_ACCESS_TOKEN=EAA...
    META_ADS_ACCOUNT_IDS=act_123,act_456,...

Estrategia: DELETE periodo+fonte + INSERT (incremental, idempotente)

Execucao:
    python pipelines/sync_meta_spend.py                # ultimos 7 dias
    python pipelines/sync_meta_spend.py --days 30      # ultimos 30 dias
    python pipelines/sync_meta_spend.py --days 90      # carga historica

Agendamento sugerido: rodar diariamente apos meia-noite (BRT) com --days 3
"""

import sys
import os
import logging
import argparse
from datetime import date, timedelta, datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.meta_ads import get_campaign_spend
from db.supernova import execute_supernova, get_supernova_connection

import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

AD_SOURCE = "meta"


def sync(days: int = 7):
    """
    Puxa dados da Meta Ads API e insere no Super Nova DB.

    Estrategia: DELETE + INSERT para o periodo (idempotente).
    """
    end_date = date.today() - timedelta(days=1)  # D-1
    start_date = end_date - timedelta(days=days - 1)

    log.info(f"Periodo: {start_date} a {end_date} ({days} dias)")

    # 1. Buscar dados da API
    rows = get_campaign_spend(start_date=start_date, end_date=end_date)

    if not rows:
        log.warning("Nenhum dado retornado da Meta Ads API.")
        return

    # 2. Preparar records (campaign_id inclui account_id pra unicidade)
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
        (
            row["date"],
            AD_SOURCE,
            row["campaign_id"],
            row["campaign_name"][:500],
            row.get("account_name", "")[:50],  # channel_type = nome da conta
            row["cost_brl"],
            row["impressions"],
            row["clicks"],
            row["conversions"],
            row.get("page_views", 0),
            row.get("reach", 0),
            None,  # affiliate_id — mapear depois via dim_campaign_affiliate
            now_utc,
        )
        for row in rows
    ]

    # 3. Inserir no Super Nova DB (DELETE periodo + INSERT)
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM multibet.fact_ad_spend WHERE dt BETWEEN %s AND %s AND ad_source = %s",
                (start_date, end_date, AD_SOURCE),
            )
            deleted = cur.rowcount
            log.info(f"  Deletados {deleted} registros antigos do periodo")

            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
            log.info(f"  Inseridos {len(records)} registros")

        conn.commit()
    finally:
        conn.close()
        tunnel.stop()

    # 4. Resumo
    total_spend = sum(r["cost_brl"] for r in rows)
    total_clicks = sum(r["clicks"] for r in rows)
    total_conversions = sum(r["conversions"] for r in rows)
    campaigns = len(set(r["campaign_id"] for r in rows))
    accounts = len(set(r["account_id"] for r in rows))

    log.info(
        f"Sync concluido: {len(records)} linhas | "
        f"{accounts} contas | {campaigns} campanhas | "
        f"Spend: R$ {total_spend:,.2f} | "
        f"Clicks: {total_clicks:,} | "
        f"Conversions: {total_conversions:,.1f}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Meta Ads spend -> Super Nova DB")
    parser.add_argument(
        "--days", type=int, default=7,
        help="Numero de dias para sincronizar (default: 7)"
    )
    args = parser.parse_args()

    log.info("=== Pipeline sync_meta_spend ===")
    sync(days=args.days)
    log.info("=== Pipeline concluido ===")
