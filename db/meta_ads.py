"""
Conector Meta Ads API — extrai metricas de campanhas (spend, cliques, impressoes).

Uso:
    from db.meta_ads import get_campaign_spend

Pre-requisitos:
    Token de System User do Business Manager com permissao ads_read.
    Configurar credenciais no .env:
        META_ADS_ACCESS_TOKEN=EAA...
        META_ADS_ACCOUNT_IDS=act_123,act_456  (separados por virgula)

Referencia:
    https://developers.facebook.com/docs/marketing-api/insights
"""

import os
import json
import logging
import urllib.request
import urllib.error
import urllib.parse
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Credenciais (todas vem do .env)
# ---------------------------------------------------------------------------
META_ADS_ACCESS_TOKEN = os.getenv("META_ADS_ACCESS_TOKEN")

# Contas MultiBet BRL (ACTIVE) — separadas por virgula no .env
META_ADS_ACCOUNT_IDS = [
    aid.strip()
    for aid in os.getenv("META_ADS_ACCOUNT_IDS", "").split(",")
    if aid.strip()
]

API_VERSION = "v21.0"


def _api_get(url: str) -> dict:
    """GET na Graph API com tratamento de erros."""
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            err = json.loads(body)
            msg = err["error"].get("message", body[:200])
            code = err["error"].get("code", "?")
            raise RuntimeError(f"Meta API erro {code}: {msg}")
        except (json.JSONDecodeError, KeyError):
            raise RuntimeError(f"Meta API HTTP {e.code}: {body[:200]}")


def get_campaign_spend(
    start_date: date = None,
    end_date: date = None,
    account_ids: list[str] = None,
    access_token: str = None,
) -> list[dict]:
    """
    Extrai spend diario por campanha de todas as contas Meta Ads.

    Args:
        start_date: Data inicio (default: 90 dias atras)
        end_date: Data fim (default: ontem D-1)
        account_ids: Lista de act_XXX (default: .env)
        access_token: Token (default: .env)

    Returns:
        Lista de dicts com:
            - date: str (YYYY-MM-DD)
            - account_id: str (act_XXX)
            - account_name: str
            - campaign_id: str
            - campaign_name: str
            - cost_brl: float
            - impressions: int
            - clicks: int
            - conversions: float (total de acoes de conversao)
    """
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=89)

    token = access_token or META_ADS_ACCESS_TOKEN
    accounts = account_ids or META_ADS_ACCOUNT_IDS

    if not token:
        raise ValueError("META_ADS_ACCESS_TOKEN nao configurado no .env")
    if not accounts:
        raise ValueError("META_ADS_ACCOUNT_IDS nao configurado no .env")

    all_rows = []

    for acc_id in accounts:
        log.info(f"  Meta Ads: buscando {acc_id} de {start_date} a {end_date}...")

        # Paginacao: a API retorna max 500 por pagina
        after = ""
        page = 0

        while True:
            time_range = urllib.parse.quote(json.dumps({"since": str(start_date), "until": str(end_date)}))
            url = (
                f"https://graph.facebook.com/{API_VERSION}/{acc_id}/insights"
                f"?access_token={token}"
                f"&fields=campaign_id,campaign_name,account_name,spend,impressions,"
                f"clicks,reach,actions"
                f"&level=campaign"
                f"&time_increment=1"
                f"&time_range={time_range}"
                f"&limit=500"
            )
            if after:
                url += f"&after={after}"

            data = _api_get(url)
            rows = data.get("data", [])

            for r in rows:
                spend = float(r.get("spend", 0))
                impressions = int(r.get("impressions", 0))
                clicks = int(r.get("clicks", 0))
                reach = int(r.get("reach", 0))

                # Conversoes + landing_page_view (KPI de tráfego — recomendado em 24/04 pelo
                # traffic-analyst: link_click infla 4.7x vs LP real, então usar landing_page_view).
                conversions = 0.0
                page_views = 0
                for action in (r.get("actions") or []):
                    atype = action.get("action_type", "")
                    if atype in (
                        "offsite_conversion.fb_pixel_purchase",
                        "offsite_conversion.fb_pixel_complete_registration",
                        "offsite_conversion.fb_pixel_lead",
                        "omni_complete_registration",
                        "complete_registration",
                    ):
                        conversions += float(action.get("value", 0))
                    elif atype == "landing_page_view":
                        page_views = int(float(action.get("value", 0)))

                if spend == 0 and impressions == 0:
                    continue

                all_rows.append({
                    "date": r.get("date_start", ""),
                    "account_id": acc_id,
                    "account_name": r.get("account_name", ""),
                    "campaign_id": r.get("campaign_id", ""),
                    "campaign_name": r.get("campaign_name", ""),
                    "cost_brl": round(spend, 2),
                    "impressions": impressions,
                    "clicks": clicks,
                    "conversions": round(conversions, 2),
                    "page_views": page_views,
                    "reach": reach,
                })

            # Paginacao
            paging = data.get("paging", {})
            cursors = paging.get("cursors", {})
            after = cursors.get("after", "")
            page += 1

            if not paging.get("next") or not after:
                break

        log.info(f"    {acc_id}: {len([r for r in all_rows if r['account_id'] == acc_id])} linhas")

    total_spend = sum(r["cost_brl"] for r in all_rows)
    log.info(
        f"Meta Ads API: {len(all_rows)} linhas totais | "
        f"Spend total: R$ {total_spend:,.2f}"
    )

    return all_rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Testando conexao com Meta Ads API...")
    print(f"Contas: {META_ADS_ACCOUNT_IDS}")

    try:
        rows = get_campaign_spend(
            start_date=date.today() - timedelta(days=7),
            end_date=date.today() - timedelta(days=1),
        )
        print(f"\nResultado: {len(rows)} linhas nos ultimos 7 dias")
        for r in rows[:10]:
            print(
                f"  {r['date']} | {r['account_name'][:20]:20s} | "
                f"{r['campaign_name'][:35]:35s} | "
                f"R$ {r['cost_brl']:>10,.2f} | "
                f"{r['clicks']:>6} cliques"
            )
        total = sum(r["cost_brl"] for r in rows)
        print(f"\n  TOTAL: R$ {total:,.2f}")
    except Exception as e:
        print(f"\nERRO: {e}")
