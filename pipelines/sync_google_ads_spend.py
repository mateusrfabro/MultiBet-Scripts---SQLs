"""
Pipeline: sync_google_ads_spend (Google Ads API -> Super Nova DB)
================================================================
Puxa spend diario por campanha da Google Ads API e persiste no
Super Nova DB em multibet.fact_google_ads_spend.

Colunas de destino:
    - dt (DATE): dia do gasto
    - campaign_id (VARCHAR): ID da campanha no Google Ads
    - campaign_name (VARCHAR): nome da campanha
    - channel_type (VARCHAR): tipo de canal (SEARCH, DISPLAY, etc.)
    - cost_brl (NUMERIC): valor gasto em BRL
    - impressions (INTEGER)
    - clicks (INTEGER)
    - conversions (NUMERIC)
    - affiliate_id (VARCHAR): mapeado via dim_campaign_affiliate
    - source (VARCHAR): ex: 'google_ads'
    - refreshed_at (TIMESTAMPTZ)

Estrategia: DELETE periodo + INSERT (incremental por faixa de datas)

Execucao:
    python pipelines/sync_google_ads_spend.py                # ultimos 7 dias
    python pipelines/sync_google_ads_spend.py --days 30      # ultimos 30 dias
    python pipelines/sync_google_ads_spend.py --days 90      # carga historica

Agendamento sugerido: rodar diariamente apos meia-noite (BRT) com --days 3
para garantir que D-1 esteja completo e cobrir reprocessamentos do Google.
"""

import sys
import os
import logging
import argparse
from datetime import date, timedelta, datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.google_ads import get_campaign_spend
from db.supernova import execute_supernova, get_supernova_connection

import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_google_ads_spend (
    dt                  DATE NOT NULL,
    campaign_id         VARCHAR(50) NOT NULL,
    campaign_name       VARCHAR(500),
    channel_type        VARCHAR(50),
    cost_brl            NUMERIC(18,2) DEFAULT 0,
    impressions         INTEGER DEFAULT 0,
    clicks              INTEGER DEFAULT 0,
    conversions         NUMERIC(18,2) DEFAULT 0,
    affiliate_id        VARCHAR(50),
    source              VARCHAR(50) DEFAULT 'google_ads',
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, campaign_id)
);
"""

DDL_INDEX_DT = """
CREATE INDEX IF NOT EXISTS idx_gads_spend_dt
ON multibet.fact_google_ads_spend (dt);
"""

DDL_INDEX_AFF = """
CREATE INDEX IF NOT EXISTS idx_gads_spend_affiliate
ON multibet.fact_google_ads_spend (affiliate_id);
"""

# Tabela de mapeamento campanha -> affiliate_id (manual, configuravel)
DDL_MAPPING = """
CREATE TABLE IF NOT EXISTS multibet.dim_campaign_affiliate (
    campaign_id         VARCHAR(50) PRIMARY KEY,
    campaign_name       VARCHAR(500),
    affiliate_id        VARCHAR(50),
    notes               VARCHAR(500),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);
"""

# View agregada por dia + affiliate (para o dashboard consumir)
DDL_VIEW = """
CREATE OR REPLACE VIEW multibet.vw_google_ads_spend_daily AS
SELECT
    g.dt,
    COALESCE(g.affiliate_id, 'nao_mapeado') AS affiliate_id,
    g.source,
    SUM(g.cost_brl) AS cost_brl,
    SUM(g.impressions) AS impressions,
    SUM(g.clicks) AS clicks,
    SUM(g.conversions) AS conversions,
    COUNT(DISTINCT g.campaign_id) AS campaigns
FROM multibet.fact_google_ads_spend g
GROUP BY g.dt, COALESCE(g.affiliate_id, 'nao_mapeado'), g.source
ORDER BY g.dt DESC, g.affiliate_id;
"""


def setup_tables():
    """Cria schema, tabelas e indices se nao existirem."""
    log.info("Verificando/criando tabelas...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX_DT)
    execute_supernova(DDL_INDEX_AFF)
    execute_supernova(DDL_MAPPING)
    execute_supernova(DDL_VIEW)
    log.info("Tabelas e view prontas.")


def load_campaign_mapping() -> dict:
    """Carrega mapeamento campaign_id -> affiliate_id."""
    rows = execute_supernova(
        "SELECT campaign_id, affiliate_id FROM multibet.dim_campaign_affiliate",
        fetch=True,
    )
    if not rows:
        return {}
    mapping = {r[0]: r[1] for r in rows}
    log.info(f"  Mapeamento campanha->affiliate: {len(mapping)} campanhas mapeadas")
    return mapping


def sync(days: int = 7):
    """
    Puxa dados da Google Ads API e insere no Super Nova DB.

    Estrategia: DELETE + INSERT para o periodo (idempotente).
    """
    end_date = date.today() - timedelta(days=1)  # D-1
    start_date = end_date - timedelta(days=days - 1)

    log.info(f"Periodo: {start_date} a {end_date} ({days} dias)")

    # 1. Buscar dados da API
    rows = get_campaign_spend(start_date=start_date, end_date=end_date)

    if not rows:
        log.warning("Nenhum dado retornado da Google Ads API.")
        return

    # 2. Carregar mapeamento campanha -> affiliate
    mapping = load_campaign_mapping()

    # 3. Enriquecer com affiliate_id
    for row in rows:
        row["affiliate_id"] = mapping.get(row["campaign_id"])
        row["source"] = "google_ads"

    # 4. Descobrir campanhas novas (sem mapeamento)
    unmapped = set()
    for row in rows:
        if row["affiliate_id"] is None:
            unmapped.add((row["campaign_id"], row["campaign_name"]))

    if unmapped:
        log.warning(
            f"  {len(unmapped)} campanhas sem affiliate_id mapeado! "
            f"Popule multibet.dim_campaign_affiliate."
        )
        for cid, cname in sorted(unmapped):
            log.warning(f"    - {cid}: {cname}")

    # 5. Inserir no Super Nova DB (DELETE periodo + INSERT)
    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.fact_google_ads_spend
            (dt, campaign_id, campaign_name, channel_type,
             cost_brl, impressions, clicks, conversions,
             affiliate_id, source, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            row["date"],
            row["campaign_id"],
            row["campaign_name"][:500],
            row.get("channel_type", ""),
            row["cost_brl"],
            row["impressions"],
            row["clicks"],
            row["conversions"],
            row["affiliate_id"],
            row["source"],
            now_utc,
        )
        for row in rows
    ]

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # Deletar periodo (idempotente — re-rodar nao duplica)
            cur.execute(
                "DELETE FROM multibet.fact_google_ads_spend WHERE dt BETWEEN %s AND %s",
                (start_date, end_date),
            )
            deleted = cur.rowcount
            log.info(f"  Deletados {deleted} registros antigos do periodo")

            # Inserir novos
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
            log.info(f"  Inseridos {len(records)} registros")

        conn.commit()
    finally:
        conn.close()
        tunnel.stop()

    # 6. Resumo
    total_spend = sum(r["cost_brl"] for r in rows)
    total_clicks = sum(r["clicks"] for r in rows)
    total_conversions = sum(r["conversions"] for r in rows)
    campaigns = len(set(r["campaign_id"] for r in rows))

    log.info(
        f"Sync concluido: {len(records)} linhas | "
        f"{campaigns} campanhas | "
        f"Spend: R$ {total_spend:,.2f} | "
        f"Clicks: {total_clicks:,} | "
        f"Conversions: {total_conversions:,.1f}"
    )

    if unmapped:
        log.info(
            f"\nACOES PENDENTES:\n"
            f"  - {len(unmapped)} campanhas sem affiliate_id\n"
            f"  - Popule multibet.dim_campaign_affiliate com:\n"
            f"    INSERT INTO multibet.dim_campaign_affiliate "
            f"(campaign_id, campaign_name, affiliate_id, notes)\n"
            f"    VALUES ('<campaign_id>', '<nome>', '<affiliate_id>', 'mapeamento manual');"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Google Ads spend -> Super Nova DB")
    parser.add_argument(
        "--days", type=int, default=7,
        help="Numero de dias para sincronizar (default: 7)"
    )
    args = parser.parse_args()

    log.info("=== Pipeline sync_google_ads_spend ===")
    setup_tables()
    sync(days=args.days)
    log.info("=== Pipeline concluido ===")
