"""
Pipeline: sync_google_ads_spend (Google Ads API -> Super Nova DB)
================================================================
Puxa spend diario por campanha da Google Ads API e persiste no
Super Nova DB em multibet.fact_ad_spend (tabela MULTI-CANAL).

Tabela de destino: multibet.fact_ad_spend
    - dt (DATE): dia do gasto
    - ad_source (VARCHAR): fonte do anuncio (google_ads, meta, tiktok, kwai)
    - campaign_id (VARCHAR): ID da campanha
    - campaign_name (VARCHAR): nome da campanha
    - channel_type (VARCHAR): tipo de canal (SEARCH, DISPLAY, PMAX, etc.)
    - cost_brl (NUMERIC): valor gasto em BRL
    - impressions (INTEGER)
    - clicks (INTEGER)
    - conversions (NUMERIC)
    - affiliate_id (VARCHAR): mapeado via dim_campaign_affiliate
    - refreshed_at (TIMESTAMPTZ)

Views criadas:
    - multibet.vw_ad_spend_daily: agregado dia + fonte + affiliate (com CPC e CTR)
    - multibet.vw_ad_spend_by_source: resumo geral por fonte (com CPA medio)

Estrategia: DELETE periodo+fonte + INSERT (incremental, idempotente)

Execucao:
    python pipelines/sync_google_ads_spend.py                # ultimos 7 dias
    python pipelines/sync_google_ads_spend.py --days 30      # ultimos 30 dias
    python pipelines/sync_google_ads_spend.py --days 90      # carga historica

Agendamento sugerido: rodar diariamente apos meia-noite (BRT) com --days 3
para garantir que D-1 esteja completo e cobrir reprocessamentos do Google.

Extensao multi-canal: para Meta/TikTok/Kwai, criar pipelines separados
(sync_meta_spend.py, etc.) que inserem na mesma fact_ad_spend com
ad_source diferente. As views consolidam automaticamente.
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

# ---------------------------------------------------------------------------
# Tabela MULTI-CANAL: fact_ad_spend
# Preparada para Google Ads, Meta, TikTok, Kwai e futuras fontes.
# A coluna `ad_source` diferencia cada plataforma.
# ---------------------------------------------------------------------------
DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_ad_spend (
    dt                  DATE NOT NULL,
    ad_source           VARCHAR(50) NOT NULL,      -- google_ads, meta, tiktok, kwai, etc.
    campaign_id         VARCHAR(100) NOT NULL,
    campaign_name       VARCHAR(500),
    channel_type        VARCHAR(50),               -- SEARCH, DISPLAY, PERFORMANCE_MAX, etc.
    cost_brl            NUMERIC(18,2) DEFAULT 0,
    impressions         INTEGER DEFAULT 0,
    clicks              INTEGER DEFAULT 0,
    conversions         NUMERIC(18,2) DEFAULT 0,
    page_views          BIGINT DEFAULT 0,          -- Meta: landing_page_view; Google: 0 (nativo nao expoe)
    reach               BIGINT DEFAULT 0,          -- Meta: reach unico; Google: 0
    affiliate_id        VARCHAR(50),
    refreshed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (dt, ad_source, campaign_id)
);
"""

DDL_INDEX_DT = """
CREATE INDEX IF NOT EXISTS idx_ad_spend_dt
ON multibet.fact_ad_spend (dt);
"""

DDL_INDEX_SOURCE = """
CREATE INDEX IF NOT EXISTS idx_ad_spend_source
ON multibet.fact_ad_spend (ad_source);
"""

DDL_INDEX_AFF = """
CREATE INDEX IF NOT EXISTS idx_ad_spend_affiliate
ON multibet.fact_ad_spend (affiliate_id);
"""

# Mapeamento campanha -> affiliate_id (multi-canal)
DDL_MAPPING = """
CREATE TABLE IF NOT EXISTS multibet.dim_campaign_affiliate (
    ad_source           VARCHAR(50) NOT NULL,      -- google_ads, meta, tiktok, kwai
    campaign_id         VARCHAR(100) NOT NULL,
    campaign_name       VARCHAR(500),
    affiliate_id        VARCHAR(50),
    notes               VARCHAR(500),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ad_source, campaign_id)
);
"""

# View agregada por dia + fonte + affiliate (multi-canal)
DDL_VIEW_DAILY = """
CREATE OR REPLACE VIEW multibet.vw_ad_spend_daily AS
SELECT
    g.dt,
    g.ad_source,
    COALESCE(g.affiliate_id, 'nao_mapeado') AS affiliate_id,
    SUM(g.cost_brl) AS cost_brl,
    SUM(g.impressions) AS impressions,
    SUM(g.clicks) AS clicks,
    SUM(g.conversions) AS conversions,
    COUNT(DISTINCT g.campaign_id) AS campaigns,
    CASE WHEN SUM(g.clicks) > 0
         THEN ROUND(SUM(g.cost_brl) / SUM(g.clicks), 2)
         ELSE 0 END AS cpc,
    CASE WHEN SUM(g.impressions) > 0
         THEN ROUND(100.0 * SUM(g.clicks) / SUM(g.impressions), 2)
         ELSE 0 END AS ctr_pct
FROM multibet.fact_ad_spend g
GROUP BY g.dt, g.ad_source, COALESCE(g.affiliate_id, 'nao_mapeado')
ORDER BY g.dt DESC, g.ad_source;
"""

# View consolidada por fonte (resumo geral multi-canal)
DDL_VIEW_SOURCE = """
CREATE OR REPLACE VIEW multibet.vw_ad_spend_by_source AS
SELECT
    g.ad_source,
    MIN(g.dt) AS dt_min,
    MAX(g.dt) AS dt_max,
    COUNT(DISTINCT g.dt) AS dias,
    SUM(g.cost_brl) AS cost_brl_total,
    SUM(g.impressions) AS impressions_total,
    SUM(g.clicks) AS clicks_total,
    SUM(g.conversions) AS conversions_total,
    COUNT(DISTINCT g.campaign_id) AS campaigns,
    CASE WHEN SUM(g.clicks) > 0
         THEN ROUND(SUM(g.cost_brl) / SUM(g.clicks), 2)
         ELSE 0 END AS cpc_medio,
    CASE WHEN SUM(g.conversions) > 0
         THEN ROUND(SUM(g.cost_brl) / SUM(g.conversions), 2)
         ELSE 0 END AS cpa_medio
FROM multibet.fact_ad_spend g
GROUP BY g.ad_source
ORDER BY cost_brl_total DESC;
"""


def setup_tables():
    """Cria schema, tabelas, indices e views se nao existirem."""
    log.info("Verificando/criando tabelas multi-canal...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX_DT)
    execute_supernova(DDL_INDEX_SOURCE)
    execute_supernova(DDL_INDEX_AFF)
    execute_supernova(DDL_MAPPING)
    execute_supernova(DDL_VIEW_DAILY)
    execute_supernova(DDL_VIEW_SOURCE)
    log.info("Tabelas e views multi-canal prontas.")


def load_campaign_mapping(ad_source: str = "google_ads") -> dict:
    """Carrega mapeamento campaign_id -> affiliate_id para uma fonte."""
    rows = execute_supernova(
        "SELECT campaign_id, affiliate_id FROM multibet.dim_campaign_affiliate WHERE ad_source = %s",
        params=(ad_source,),
        fetch=True,
    )
    if not rows:
        return {}
    mapping = {r[0]: r[1] for r in rows}
    log.info(f"  Mapeamento {ad_source} campanha->affiliate: {len(mapping)} campanhas mapeadas")
    return mapping


def sync(days: int = 7):
    """
    Puxa dados da Google Ads API e insere no Super Nova DB.

    Estrategia: DELETE + INSERT para o periodo (idempotente).
    """
    # Janela inclui D-0 por padrao (Google ajusta retroativamente ate ~72h).
    # Uso intraday: cron 5x/dia com --days 2 = D-1 + D-0, DELETE+INSERT idempotente.
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)

    log.info(f"Periodo: {start_date} a {end_date} ({days} dias, inclui D-0 intraday)")

    # 1. Buscar dados da API
    rows = get_campaign_spend(start_date=start_date, end_date=end_date)

    if not rows:
        log.warning("Nenhum dado retornado da Google Ads API.")
        return

    # 2. Carregar mapeamento campanha -> affiliate
    mapping = load_campaign_mapping()

    AD_SOURCE = "google_ads"

    # 3. Enriquecer com affiliate_id
    for row in rows:
        row["affiliate_id"] = mapping.get(row["campaign_id"])
        row["ad_source"] = AD_SOURCE

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
            row["ad_source"],
            row["campaign_id"],
            row["campaign_name"][:500],
            row.get("channel_type", ""),
            row["cost_brl"],
            row["impressions"],
            row["clicks"],
            row["conversions"],
            row.get("page_views", 0),
            row.get("reach", 0),
            row["affiliate_id"],
            now_utc,
        )
        for row in rows
    ]

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # Deletar periodo + fonte (idempotente — re-rodar nao duplica)
            cur.execute(
                "DELETE FROM multibet.fact_ad_spend WHERE dt BETWEEN %s AND %s AND ad_source = %s",
                (start_date, end_date, AD_SOURCE),
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
            f"(ad_source, campaign_id, campaign_name, affiliate_id, notes)\n"
            f"    VALUES ('google_ads', '<campaign_id>', '<nome>', '<affiliate_id>', 'mapeamento manual');"
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
