"""
Conector Google Ads API — extrai metricas de campanhas (spend, cliques, impressoes).

Uso:
    from db.google_ads import get_campaign_spend

Pré-requisitos:
    pip install google-ads
    Configurar credenciais no .env (ver SETUP_GOOGLE_ADS_API.md)

Referência:
    https://developers.google.com/google-ads/api/docs/start
"""

import os
import logging
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Credenciais (todas vem do .env)
# ---------------------------------------------------------------------------
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "4985069191")
# Se houver conta MCC gerenciando, informar aqui:
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")


def _get_client():
    """Cria e retorna um GoogleAdsClient configurado."""
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except ImportError:
        raise ImportError(
            "Biblioteca google-ads nao instalada. Rode:\n"
            "  pip install google-ads"
        )

    if not all([GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID,
                GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN]):
        raise ValueError(
            "Credenciais Google Ads incompletas no .env. "
            "Verifique: GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID, "
            "GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN"
        )

    config = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        config["login_customer_id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID

    return GoogleAdsClient.load_from_dict(config)


def get_campaign_spend(
    start_date: date = None,
    end_date: date = None,
    customer_id: str = None,
) -> list[dict]:
    """
    Extrai spend diario por campanha da Google Ads API.

    Args:
        start_date: Data inicio (default: 90 dias atras)
        end_date: Data fim (default: ontem D-1)
        customer_id: ID da conta Google Ads (default: .env)

    Returns:
        Lista de dicts com:
            - date: str (YYYY-MM-DD)
            - campaign_id: str
            - campaign_name: str
            - cost_brl: float (valor em BRL, ja convertido de micros)
            - impressions: int
            - clicks: int
            - conversions: float
    """
    if end_date is None:
        # D-0 (hoje) por padrao — Google ajusta invalid clicks retroativamente
        # ate ~72h, mas o ggr/spend intraday ja eh util pra dashboard fresh.
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=89)  # ultimos 90 dias

    cid = (customer_id or GOOGLE_ADS_CUSTOMER_ID).replace("-", "")

    client = _get_client()
    ga_service = client.get_service("GoogleAdsService")

    # GAQL (Google Ads Query Language)
    # cost_micros = valor em microunidades (1 BRL = 1.000.000 micros)
    query = f"""
        SELECT
            segments.date,
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions
        FROM campaign
        WHERE segments.date BETWEEN '{start_date.strftime("%Y-%m-%d")}'
                                AND '{end_date.strftime("%Y-%m-%d")}'
          AND campaign.status != 'REMOVED'
        ORDER BY segments.date DESC, metrics.cost_micros DESC
    """

    log.info(
        f"Google Ads API: buscando spend de {start_date} a {end_date} "
        f"(conta {cid})"
    )

    rows = []
    try:
        response = ga_service.search_stream(customer_id=cid, query=query)

        for batch in response:
            for row in batch.results:
                cost_brl = row.metrics.cost_micros / 1_000_000
                if cost_brl == 0 and row.metrics.impressions == 0:
                    continue  # pular campanhas sem atividade

                rows.append({
                    "date": row.segments.date,  # YYYY-MM-DD
                    "campaign_id": str(row.campaign.id),
                    "campaign_name": row.campaign.name,
                    "channel_type": row.campaign.advertising_channel_type.name,
                    "cost_brl": round(cost_brl, 2),
                    "impressions": row.metrics.impressions,
                    "clicks": row.metrics.clicks,
                    "conversions": round(row.metrics.conversions, 2),
                    # Google Ads API nao expoe landing_page_view nem reach unico nativamente
                    # em GAQL level=campaign. Para page_views, precisaria conversao custom
                    # importada do GA4. Mantemos 0 aqui; view consolidada usa somente Meta.
                    "page_views": 0,
                    "reach": 0,
                })

    except Exception as e:
        log.error(f"Erro na Google Ads API: {e}")
        raise

    total_spend = sum(r["cost_brl"] for r in rows)
    log.info(
        f"Google Ads API: {len(rows)} linhas retornadas | "
        f"Spend total: R$ {total_spend:,.2f}"
    )

    return rows


def get_daily_spend_summary(
    start_date: date = None,
    end_date: date = None,
    customer_id: str = None,
) -> list[dict]:
    """
    Retorna spend agregado por dia (sem quebra por campanha).

    Util para o caso mais simples: data + valor_gasto_total.

    Returns:
        Lista de dicts com: date, cost_brl, impressions, clicks, conversions
    """
    rows = get_campaign_spend(start_date, end_date, customer_id)

    # Agregar por data
    from collections import defaultdict
    daily = defaultdict(lambda: {
        "cost_brl": 0, "impressions": 0, "clicks": 0, "conversions": 0
    })

    for r in rows:
        d = daily[r["date"]]
        d["cost_brl"] += r["cost_brl"]
        d["impressions"] += r["impressions"]
        d["clicks"] += r["clicks"]
        d["conversions"] += r["conversions"]

    result = []
    for dt, metrics in sorted(daily.items()):
        result.append({
            "date": dt,
            "cost_brl": round(metrics["cost_brl"], 2),
            "impressions": metrics["impressions"],
            "clicks": metrics["clicks"],
            "conversions": round(metrics["conversions"], 2),
        })

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Testando conexao com Google Ads API...")
    print(f"Customer ID: {GOOGLE_ADS_CUSTOMER_ID}")

    try:
        rows = get_campaign_spend(
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() - timedelta(days=1),
        )
        print(f"\nResultado: {len(rows)} linhas nos ultimos 7 dias")
        for r in rows[:10]:
            print(
                f"  {r['date']} | {r['campaign_name'][:40]:40s} | "
                f"R$ {r['cost_brl']:>10,.2f} | "
                f"{r['clicks']:>6} cliques"
            )
        total = sum(r["cost_brl"] for r in rows)
        print(f"\n  TOTAL: R$ {total:,.2f}")
    except Exception as e:
        print(f"\nERRO: {e}")
        print("\nVerifique as credenciais no .env (ver SETUP_GOOGLE_ADS_API.md)")