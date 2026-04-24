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
    python pipelines/sync_meta_spend.py                # ultimos 7 dias ate HOJE (D-0)
    python pipelines/sync_meta_spend.py --days 2       # intraday D-0 + D-1 (cron 5x/dia)
    python pipelines/sync_meta_spend.py --days 90      # carga historica

Janela: inclui D-0 (hoje) por padrao. Meta atualiza insights near-real-time
(~15min delay). Rodar varias vezes ao dia mantem dados frescos — DELETE+INSERT
sobrescreve a mesma janela de forma idempotente.

Agendamento sugerido no orquestrador:
    - Refresh token:  0 5 1 * *    (dia 1 de cada mes, 02:00 BRT)
    - Intraday 5x:    0 9,13,17,21,1 * * *  (06h/10h/14h/18h/22h BRT)
                      com --days 2 pra D-0 + D-1
"""

import sys
import os
import json
import logging
import argparse
import urllib.request
import urllib.error
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

# Limiar (dias) pra alertar expiracao proxima do token Meta.
# Cron do refresh_meta_token roda dia 1 do mes — 10d de folga cobre
# 1 falha inesperada do cron sem quebrar a extracao intraday.
TOKEN_EXPIRE_WARN_DAYS = 10


def _check_token_expiration() -> None:
    """
    Alerta se o token Meta esta perto de expirar.
    Roda no inicio de cada sync — barato (1 request, ~200ms).

    Nunca aborta a execucao (so loga) pra nao quebrar a carga do dia
    se o refresh_meta_token tiver falhado por motivo secundario. Exit
    code continua limpo; alerta vai pro log e pode ser coletado pelo
    orquestrador pra notificacao.
    """
    token = os.getenv("META_ADS_ACCESS_TOKEN")
    app_id = os.getenv("META_APP_ID")
    app_secret = os.getenv("META_APP_SECRET")

    if not (token and app_id and app_secret):
        # Sem app_id/app_secret nao da pra chamar debug_token —
        # silencioso, log curto (instala do pipeline pode ainda nao ter .env completo).
        log.debug("Skip token expiration check (app_id/app_secret ausentes)")
        return

    url = (
        "https://graph.facebook.com/v21.0/debug_token"
        f"?input_token={token}&access_token={app_id}|{app_secret}"
    )
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read()).get("data", {})
    except urllib.error.HTTPError as e:
        log.warning(f"Token expiration check falhou (HTTP {e.code}). Prosseguindo.")
        return
    except Exception as e:
        log.warning(f"Token expiration check falhou: {e}. Prosseguindo.")
        return

    if not data.get("is_valid", False):
        log.error(
            f"TOKEN META INVALIDO ou revogado — rode refresh_meta_token.py "
            f"ou peca novo token ao gestor. err_sub={data.get('error', {})}"
        )
        return

    exp = data.get("expires_at", 0)
    if not exp:
        log.info("Token Meta sem expiracao (System User permanente).")
        return

    exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc)
    days_left = (exp_dt - datetime.now(timezone.utc)).days
    log.info(f"Token Meta expira em {exp_dt.date()} ({days_left}d restantes)")

    if days_left <= 0:
        log.error("TOKEN META EXPIRADO — rode refresh_meta_token.py imediatamente.")
    elif days_left <= TOKEN_EXPIRE_WARN_DAYS:
        log.warning(
            f"TOKEN META PROXIMO DA EXPIRACAO ({days_left}d). "
            f"Confirme que refresh_meta_token rodou este mes."
        )


def sync(days: int = 7):
    """
    Puxa dados da Meta Ads API e insere no Super Nova DB.

    Estrategia: DELETE + INSERT para o periodo (idempotente).
    """
    # 0. Sanity check do token (loga warning/error mas nao aborta)
    _check_token_expiration()

    # Janela inclui HOJE (D-0) por default — Meta expoe insights near-real-time
    # (delay ~15min). Rodar 4-5x/dia mantem D-0 fresco via DELETE+INSERT idempotente.
    # Usar --days 2 pra cobrir D-1 + D-0; --days 3 pra cobrir ate D-2 (reprocessamentos).
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)

    log.info(f"Periodo: {start_date} a {end_date} ({days} dias, inclui D-0 intraday)")

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
