"""
Queries Athena para o Dashboard Google Ads Affiliates.

Todas as queries sao parametrizadas e seguem as regras do CLAUDE.md:
- Camada _ec2: valores em centavos (/100.0)
- Camada ps_bi: valores em BRL reais
- Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
- Test users: excluidos via is_test = false / c_test_user = false
- GGR: somente realcash (sem bonus)
- affiliate_id: VARCHAR no ps_bi — sempre comparar com strings
"""
import sys
import os
import logging
from datetime import date, timedelta
from functools import lru_cache
from time import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from db.athena import query_athena
from dashboards.google_ads.config import AFFILIATE_IDS, CACHE_TTL_SECONDS, TREND_DAYS

log = logging.getLogger(__name__)


def _aff_filter():
    """Retorna string SQL segura para filtro de affiliate_ids (sem input do usuario)."""
    # IDs vem do config.py (hardcoded), nao do request — sem risco de injection
    return "(" + ", ".join(f"'{aid}'" for aid in AFFILIATE_IDS) + ")"


# =========================================================================
# CACHE simples em memoria (TTL configuravel)
# =========================================================================
_cache = {}


def _cached(key, fn):
    """Executa fn() e cacheia resultado por CACHE_TTL_SECONDS."""
    now = time()
    if key in _cache:
        result, ts = _cache[key]
        if now - ts < CACHE_TTL_SECONDS:
            log.debug(f"Cache hit: {key}")
            return result
    log.info(f"Cache miss: {key} — consultando Athena...")
    result = fn()
    _cache[key] = (result, now)
    return result


def clear_cache():
    """Limpa cache manualmente (util para debug)."""
    _cache.clear()
    log.info("Cache limpo")


def get_cache_age():
    """Retorna idade do cache mais antigo em segundos, ou None se vazio."""
    if not _cache:
        return None
    oldest = min(ts for _, ts in _cache.values())
    return time() - oldest


# =========================================================================
# QUERY 1: Metricas consolidadas para um dia especifico
# =========================================================================
def _query_day_metrics(target_date: date) -> dict:
    """
    Retorna metricas consolidadas dos 3 affiliates para um dia.

    Fontes:
      - REG: bireports_ec2.tbl_ecr (conversao BRT explicita)
      - FTD/FTD Deposit: ps_bi.dim_user (ftd_date, ftd_amount_inhouse)
      - Financeiro: bireports_ec2.tbl_ecr_wise_daily_bi_summary (centavos/100)

    Retorna dict com: reg, ftd, ftd_deposit, dep_amount, saques,
                      ggr_cassino, ggr_sport, ngr, bonus_cost
    """
    dt = target_date.isoformat()
    aff = _aff_filter()

    # --- REG (bireports com BRT) ---
    sql_reg = f"""
    SELECT COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
          = DATE '{dt}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {aff}
      AND c_test_user = false
    """

    # --- FTD (ps_bi) ---
    sql_ftd = f"""
    SELECT
        COUNT(*) AS ftd,
        COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
    FROM ps_bi.dim_user
    WHERE CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
      AND CAST(affiliate_id AS VARCHAR) IN {aff}
      AND is_test = false
    """

    # --- Financeiro (bireports BI summary, centavos/100) ---
    sql_fin = f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
    )
    SELECT
        COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0    AS dep_amount,
        COALESCE(SUM(s.c_co_success_amount), 0) / 100.0         AS saques,
        COALESCE(SUM(s.c_casino_realcash_bet_amount
                    - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_cassino,
        COALESCE(SUM(s.c_sb_realcash_bet_amount
                    - s.c_sb_realcash_win_amount), 0) / 100.0     AS ggr_sport,
        COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0        AS bonus_cost,
        COUNT(DISTINCT s.c_ecr_id)                                AS players_ativos
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN base_players p ON s.c_ecr_id = p.ecr_id
    WHERE s.c_created_date = DATE '{dt}'
    """

    df_reg = query_athena(sql_reg, database="bireports_ec2")
    df_ftd = query_athena(sql_ftd, database="ps_bi")
    df_fin = query_athena(sql_fin, database="ps_bi")

    reg = int(df_reg.iloc[0]["reg"])
    ftd = int(df_ftd.iloc[0]["ftd"])
    ftd_deposit = float(df_ftd.iloc[0]["ftd_deposit"])

    r = df_fin.iloc[0]
    dep_amount = float(r["dep_amount"])
    saques = float(r["saques"])
    ggr_cassino = float(r["ggr_cassino"])
    ggr_sport = float(r["ggr_sport"])
    bonus_cost = float(r["bonus_cost"])

    return {
        "date": dt,
        "reg": reg,
        "ftd": ftd,
        "ftd_deposit": round(ftd_deposit, 2),
        "dep_amount": round(dep_amount, 2),
        "saques": round(saques, 2),
        "ggr_cassino": round(ggr_cassino, 2),
        "ggr_sport": round(ggr_sport, 2),
        "bonus_cost": round(bonus_cost, 2),
        "ngr": round(ggr_cassino + ggr_sport - bonus_cost, 2),
        "ggr_total": round(ggr_cassino + ggr_sport, 2),
        "players_ativos": int(r["players_ativos"]),
        "conv_pct": round(ftd / max(reg, 1) * 100, 1),
        "ticket_medio_ftd": round(ftd_deposit / max(ftd, 1), 2),
    }


# =========================================================================
# QUERY 2: Breakdown por affiliate ID para um dia
# =========================================================================
def _query_day_by_affiliate(target_date: date) -> list:
    """Retorna lista de dicts com metricas por affiliate para um dia."""
    dt = target_date.isoformat()
    aff = _aff_filter()

    sql = f"""
    WITH player_aff AS (
        SELECT ecr_id, CAST(affiliate_id AS VARCHAR) AS aff_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
    ),
    reg_by_aff AS (
        SELECT
            CAST(c_affiliate_id AS VARCHAR) AS aff_id,
            COUNT(*) AS reg
        FROM bireports_ec2.tbl_ecr
        WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
              = DATE '{dt}'
          AND CAST(c_affiliate_id AS VARCHAR) IN {aff}
          AND c_test_user = false
        GROUP BY CAST(c_affiliate_id AS VARCHAR)
    ),
    ftd_by_aff AS (
        SELECT
            CAST(affiliate_id AS VARCHAR) AS aff_id,
            COUNT(*) AS ftd,
            COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
        FROM ps_bi.dim_user
        WHERE CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
          AND CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
        GROUP BY CAST(affiliate_id AS VARCHAR)
    ),
    fin_by_aff AS (
        SELECT
            pa.aff_id,
            COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS dep_amount,
            COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS saques,
            COALESCE(SUM(s.c_casino_realcash_bet_amount
                        - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_cassino,
            COALESCE(SUM(s.c_sb_realcash_bet_amount
                        - s.c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
            COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0 AS bonus_cost
        FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
        JOIN player_aff pa ON s.c_ecr_id = pa.ecr_id
        WHERE s.c_created_date = DATE '{dt}'
        GROUP BY pa.aff_id
    )
    SELECT
        COALESCE(r.aff_id, COALESCE(f.aff_id, fi.aff_id)) AS affiliate_id,
        COALESCE(r.reg, 0) AS reg,
        COALESCE(f.ftd, 0) AS ftd,
        COALESCE(f.ftd_deposit, 0) AS ftd_deposit,
        COALESCE(fi.dep_amount, 0) AS dep_amount,
        COALESCE(fi.saques, 0) AS saques,
        COALESCE(fi.ggr_cassino, 0) AS ggr_cassino,
        COALESCE(fi.ggr_sport, 0) AS ggr_sport,
        COALESCE(fi.bonus_cost, 0) AS bonus_cost
    FROM reg_by_aff r
    FULL OUTER JOIN ftd_by_aff f ON r.aff_id = f.aff_id
    FULL OUTER JOIN fin_by_aff fi ON COALESCE(r.aff_id, f.aff_id) = fi.aff_id
    ORDER BY affiliate_id
    """

    df = query_athena(sql, database="ps_bi")
    rows = []
    for _, row in df.iterrows():
        ggr_c = float(row["ggr_cassino"])
        ggr_s = float(row["ggr_sport"])
        bonus = float(row["bonus_cost"])
        reg = int(row["reg"])
        ftd = int(row["ftd"])
        rows.append({
            "affiliate_id": str(row["affiliate_id"]),
            "reg": reg,
            "ftd": ftd,
            "ftd_deposit": round(float(row["ftd_deposit"]), 2),
            "dep_amount": round(float(row["dep_amount"]), 2),
            "saques": round(float(row["saques"]), 2),
            "ggr_cassino": round(ggr_c, 2),
            "ggr_sport": round(ggr_s, 2),
            "ngr": round(ggr_c + ggr_s - bonus, 2),
            "conv_pct": round(ftd / max(reg, 1) * 100, 1),
        })
    return rows


# =========================================================================
# QUERY 3: Serie temporal (ultimos N dias) para graficos de tendencia
# =========================================================================
def _query_trend(days: int = None) -> list:
    """Retorna lista de metricas diarias para os ultimos N dias."""
    if days is None:
        days = TREND_DAYS
    aff = _aff_filter()

    end_date = date.today() - timedelta(days=1)  # D-1 (hoje pode estar incompleto)
    start_date = end_date - timedelta(days=days - 1)

    sql = f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
    ),
    daily AS (
        SELECT
            s.c_created_date AS dt,
            COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS dep_amount,
            COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS saques,
            COALESCE(SUM(s.c_casino_realcash_bet_amount
                        - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_cassino,
            COALESCE(SUM(s.c_sb_realcash_bet_amount
                        - s.c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
            COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0 AS bonus_cost
        FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
        JOIN base_players p ON s.c_ecr_id = p.ecr_id
        WHERE s.c_created_date BETWEEN DATE '{start_date.isoformat()}'
                                   AND DATE '{end_date.isoformat()}'
        GROUP BY s.c_created_date
    )
    SELECT * FROM daily ORDER BY dt
    """

    df = query_athena(sql, database="ps_bi")
    rows = []
    for _, row in df.iterrows():
        ggr_c = float(row["ggr_cassino"])
        ggr_s = float(row["ggr_sport"])
        bonus = float(row["bonus_cost"])
        rows.append({
            "date": str(row["dt"]),
            "dep_amount": round(float(row["dep_amount"]), 2),
            "saques": round(float(row["saques"]), 2),
            "ggr_cassino": round(ggr_c, 2),
            "ggr_sport": round(ggr_s, 2),
            "ngr": round(ggr_c + ggr_s - bonus, 2),
            "ggr_total": round(ggr_c + ggr_s, 2),
        })
    return rows


# =========================================================================
# FUNCOES PUBLICAS (com cache)
# =========================================================================
def get_today_metrics() -> dict:
    """Metricas de hoje (pode estar incompleto)."""
    today = date.today()
    return _cached(f"day_{today}", lambda: _query_day_metrics(today))


def get_d1_metrics() -> dict:
    """Metricas de ontem (D-1)."""
    d1 = date.today() - timedelta(days=1)
    return _cached(f"day_{d1}", lambda: _query_day_metrics(d1))


def get_d7_metrics() -> dict:
    """Metricas de 7 dias atras."""
    d7 = date.today() - timedelta(days=7)
    return _cached(f"day_{d7}", lambda: _query_day_metrics(d7))


def get_today_by_affiliate() -> list:
    """Breakdown por affiliate de hoje."""
    today = date.today()
    return _cached(f"aff_{today}", lambda: _query_day_by_affiliate(today))


def get_d1_by_affiliate() -> list:
    """Breakdown por affiliate de ontem."""
    d1 = date.today() - timedelta(days=1)
    return _cached(f"aff_{d1}", lambda: _query_day_by_affiliate(d1))


def get_trend() -> list:
    """Serie temporal para graficos."""
    return _cached("trend", lambda: _query_trend())


def _calc_variation(current: float, previous: float) -> dict:
    """Calcula variacao percentual e direcao."""
    if previous == 0:
        pct = 0.0 if current == 0 else 100.0
    else:
        pct = round((current - previous) / abs(previous) * 100, 1)
    return {
        "pct": pct,
        "direction": "up" if pct > 0 else ("down" if pct < 0 else "neutral"),
    }


def get_dashboard_data() -> dict:
    """
    Retorna TODOS os dados necessarios para renderizar o dashboard.

    Chamada unica do frontend — consolida tudo em um JSON.
    """
    # Timestamp de INICIO da extracao (primeiro dado solicitado)
    # Mostra quando os dados "sao" — a foto do banco naquele instante
    from datetime import datetime
    query_started_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    today = get_today_metrics()
    d1 = get_d1_metrics()
    d7 = get_d7_metrics()
    trend = get_trend()
    by_affiliate = get_today_by_affiliate()

    # D-2 e D-8 para comparativos entre dias FECHADOS
    d2 = _cached(f"day_{date.today() - timedelta(days=2)}",
                 lambda: _query_day_metrics(date.today() - timedelta(days=2)))
    d8 = _cached(f"day_{date.today() - timedelta(days=8)}",
                 lambda: _query_day_metrics(date.today() - timedelta(days=8)))

    # Calcular variacoes entre dias FECHADOS (D-1 vs D-2, D-1 vs D-8)
    # NUNCA comparar hoje (parcial) com ontem (fechado) — nao faz sentido
    metrics_keys = [
        "reg", "ftd", "ftd_deposit", "dep_amount", "saques",
        "ggr_cassino", "ggr_sport", "ngr",
    ]

    # Comparativo principal: D-1 (fechado) vs D-2 (fechado)
    variations_d1 = {}
    for key in metrics_keys:
        variations_d1[key] = _calc_variation(d1[key], d2[key])

    # Comparativo semanal: D-1 (fechado) vs D-8 (mesmo dia semana passada)
    variations_d7 = {}
    for key in metrics_keys:
        variations_d7[key] = _calc_variation(d1[key], d8[key])

    # Gerar insights com base em D-1 vs D-2 (dias fechados)
    insights = _generate_insights(d1, d2, d8, by_affiliate)

    return {
        "today": today,
        "today_is_partial": True,
        "d1": d1,
        "d2": d2,
        "d7": d7,
        "variations_d1": variations_d1,
        "variations_d7": variations_d7,
        "trend": trend,
        "by_affiliate": by_affiliate,
        "insights": insights,
        "affiliate_ids": AFFILIATE_IDS,
        "cache_age_seconds": get_cache_age(),
        "query_started_at": query_started_at,
    }


# =========================================================================
# INSIGHTS AUTOMATICOS
# =========================================================================
def _generate_insights(today: dict, d1: dict, d7: dict, by_aff: list) -> list:
    """
    Gera insights em linguagem simples para a gestora.

    Regras:
    - Queda > 15% = alerta critico (vermelho)
    - Queda 5-15% = atencao (amarelo)
    - Subida > 10% = positivo (verde)
    - NGR negativo = alerta critico
    - Um affiliate com >60% do volume = risco de concentracao
    """
    insights = []

    # --- Alertas de variacao D-1 ---
    metric_labels = {
        "reg": "Novos cadastros",
        "ftd": "Primeiros depositos",
        "ftd_deposit": "Valor dos primeiros depositos",
        "dep_amount": "Total depositado",
        "saques": "Saques",
        "ggr_cassino": "Receita cassino",
        "ggr_sport": "Receita esportiva",
        "ngr": "Receita liquida (NGR)",
    }

    for key, label in metric_labels.items():
        if d1[key] == 0:
            continue
        pct = (today[key] - d1[key]) / abs(d1[key]) * 100

        if pct <= -15:
            insights.append({
                "level": "critical",
                "message": f"{label} caiu {abs(pct):.0f}% comparado com ontem. "
                           f"Verificar campanhas e orcamento.",
            })
        elif pct <= -5:
            insights.append({
                "level": "warning",
                "message": f"{label} caiu {abs(pct):.0f}% vs ontem. Monitorar.",
            })
        elif pct >= 10:
            insights.append({
                "level": "positive",
                "message": f"{label} subiu {pct:.0f}% vs ontem.",
            })

    # --- NGR negativo ---
    if today["ngr"] < 0:
        insights.append({
            "level": "critical",
            "message": f"NGR esta negativo (R$ {today['ngr']:,.2f}). "
                       f"A operacao esta no prejuizo hoje.",
        })

    # --- Conversao baixa ---
    if today["conv_pct"] < 25 and today["reg"] > 0:
        insights.append({
            "level": "warning",
            "message": f"Taxa de conversao REG→FTD esta em {today['conv_pct']:.1f}% "
                       f"(abaixo de 25%). Qualidade do trafego pode estar baixa.",
        })

    # --- Concentracao por affiliate ---
    if by_aff and today["ngr"] != 0:
        for aff in by_aff:
            share = abs(aff["ngr"]) / max(abs(today["ngr"]), 1) * 100
            if share > 60:
                insights.append({
                    "level": "warning",
                    "message": f"Affiliate {aff['affiliate_id']} concentra {share:.0f}% "
                               f"do NGR. Risco de dependencia.",
                })

    # --- Acumulado 7 dias ---
    if today.get("date") and d7.get("date"):
        insights.append({
            "level": "info",
            "message": f"Ticket medio FTD hoje: R$ {today['ticket_medio_ftd']:,.2f}. "
                       f"Conversao: {today['conv_pct']:.1f}%.",
        })

    return insights