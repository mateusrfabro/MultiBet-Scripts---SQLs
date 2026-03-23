"""
Queries horárias para comparativo justo: hoje até agora vs ontem mesmo horário.

Usa tabelas ps_bi hourly (fct_deposits_hourly, fct_casino_activity_hourly, etc.)
Valores já em BRL (sem dividir por 100). Test users já filtrados pelo dbt.
Horas em UTC — converter para BRT: hora_utc - 3 (simplificado, sem horário de verão).
"""
import sys
import os
import logging
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from db.athena import query_athena
from dashboards.google_ads.config import AFFILIATE_IDS, CACHE_TTL_SECONDS

log = logging.getLogger(__name__)

# Cache simples compartilhado
_cache_hourly = {}
from time import time as _time


def _cached_h(key, fn):
    now = _time()
    if key in _cache_hourly:
        result, ts = _cache_hourly[key]
        if now - ts < CACHE_TTL_SECONDS:
            return result
    result = fn()
    _cache_hourly[key] = (result, now)
    return result


def _aff_filter():
    return "(" + ", ".join(f"'{aid}'" for aid in AFFILIATE_IDS) + ")"


def _hora_corte_utc():
    """Retorna a hora de corte em UTC (hora BRT atual convertida para UTC).

    Exclui a hora atual (pode estar parcial) — usa hora_atual - 1.
    Ex: se agora = 14:30 BRT = 17:30 UTC → corte = 17 (horas 0-16 completas)
    """
    now_utc = datetime.utcnow()
    return now_utc.hour  # hora atual UTC (parcial) — filtramos < este valor


def get_hourly_comparison() -> dict:
    """
    Retorna comparativo hoje vs ontem usando tabelas horárias.

    Mesmo recorte temporal: horas 0 até (hora_atual - 1).
    Assim o comparativo é justo — não compara dia parcial com dia fechado.
    """
    return _cached_h("hourly_cmp", _query_hourly_comparison)


def _query_hourly_comparison() -> dict:
    """Executa queries horárias no Athena."""
    hoje = date.today().isoformat()
    ontem = (date.today() - timedelta(days=1)).isoformat()
    hora_corte = _hora_corte_utc()
    hora_corte_brt = max(hora_corte - 3, 0)
    aff = _aff_filter()

    log.info(f"Hourly comparison: hoje={hoje}, ontem={ontem}, corte UTC={hora_corte} (BRT ~{hora_corte_brt}h)")

    # --- Depósitos horários ---
    # --- REG e FTD por hora (via signup_datetime e ftd_datetime) ---
    sql_reg_ftd = f"""
    WITH
    reg_hoje AS (
        SELECT COUNT(*) AS reg
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
          AND CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{hoje}'
          AND HOUR(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') < {hora_corte_brt}
    ),
    reg_ontem AS (
        SELECT COUNT(*) AS reg
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
          AND CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{ontem}'
          AND HOUR(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') < {hora_corte_brt}
    ),
    ftd_hoje AS (
        SELECT COUNT(*) AS ftd, COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
          AND CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{hoje}'
          AND HOUR(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') < {hora_corte_brt}
    ),
    ftd_ontem AS (
        SELECT COUNT(*) AS ftd, COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
          AND CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{ontem}'
          AND HOUR(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') < {hora_corte_brt}
    )
    SELECT
        COALESCE(rh.reg, 0) AS reg_hoje,
        COALESCE(ro.reg, 0) AS reg_ontem,
        COALESCE(fh.ftd, 0) AS ftd_hoje,
        COALESCE(fo.ftd, 0) AS ftd_ontem,
        COALESCE(fh.ftd_deposit, 0) AS ftd_deposit_hoje,
        COALESCE(fo.ftd_deposit, 0) AS ftd_deposit_ontem
    FROM reg_hoje rh
    CROSS JOIN reg_ontem ro
    CROSS JOIN ftd_hoje fh
    CROSS JOIN ftd_ontem fo
    """

    sql_dep = f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
    ),
    dep_hoje AS (
        SELECT
            SUM(d.success_count) AS depositos,
            SUM(d.success_amount_local) AS valor
        FROM ps_bi.fct_deposits_hourly d
        JOIN base_players p ON d.player_id = p.ecr_id
        WHERE d.created_date = DATE '{hoje}'
          AND d.created_hour < {hora_corte}
    ),
    dep_ontem AS (
        SELECT
            SUM(d.success_count) AS depositos,
            SUM(d.success_amount_local) AS valor
        FROM ps_bi.fct_deposits_hourly d
        JOIN base_players p ON d.player_id = p.ecr_id
        WHERE d.created_date = DATE '{ontem}'
          AND d.created_hour < {hora_corte}
    )
    SELECT
        COALESCE(h.depositos, 0) AS dep_hoje_qtd,
        COALESCE(h.valor, 0) AS dep_hoje_valor,
        COALESCE(o.depositos, 0) AS dep_ontem_qtd,
        COALESCE(o.valor, 0) AS dep_ontem_valor
    FROM dep_hoje h
    CROSS JOIN dep_ontem o
    """

    # --- GGR Casino + Sport horário (separado por product_id) ---
    sql_ggr = f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
    ),
    ggr_hoje AS (
        SELECT
            COALESCE(SUM(CASE WHEN c.product_id = 'casino' THEN c.real_bet_amount_local - c.real_win_amount_local ELSE 0 END), 0) AS ggr_casino,
            COALESCE(SUM(CASE WHEN c.product_id = 'sports_book' THEN c.real_bet_amount_local - c.real_win_amount_local ELSE 0 END), 0) AS ggr_sport
        FROM ps_bi.fct_casino_activity_hourly c
        JOIN base_players p ON c.player_id = p.ecr_id
        WHERE c.activity_date = DATE '{hoje}'
          AND c.activity_hour < {hora_corte}
    ),
    ggr_ontem AS (
        SELECT
            COALESCE(SUM(CASE WHEN c.product_id = 'casino' THEN c.real_bet_amount_local - c.real_win_amount_local ELSE 0 END), 0) AS ggr_casino,
            COALESCE(SUM(CASE WHEN c.product_id = 'sports_book' THEN c.real_bet_amount_local - c.real_win_amount_local ELSE 0 END), 0) AS ggr_sport
        FROM ps_bi.fct_casino_activity_hourly c
        JOIN base_players p ON c.player_id = p.ecr_id
        WHERE c.activity_date = DATE '{ontem}'
          AND c.activity_hour < {hora_corte}
    ),
    bonus_hoje AS (
        SELECT COALESCE(SUM(b.amount_issued_local), 0) AS bonus
        FROM ps_bi.fct_bonus_activity_hourly b
        JOIN base_players p ON b.player_id = p.ecr_id
        WHERE b.activity_date = DATE '{hoje}' AND b.activity_hour < {hora_corte}
    ),
    bonus_ontem AS (
        SELECT COALESCE(SUM(b.amount_issued_local), 0) AS bonus
        FROM ps_bi.fct_bonus_activity_hourly b
        JOIN base_players p ON b.player_id = p.ecr_id
        WHERE b.activity_date = DATE '{ontem}' AND b.activity_hour < {hora_corte}
    )
    SELECT
        COALESCE(gh.ggr_casino, 0) AS ggr_casino_hoje,
        COALESCE(go.ggr_casino, 0) AS ggr_casino_ontem,
        COALESCE(gh.ggr_sport, 0) AS ggr_sport_hoje,
        COALESCE(go.ggr_sport, 0) AS ggr_sport_ontem,
        COALESCE(bh.bonus, 0) AS bonus_hoje,
        COALESCE(bo.bonus, 0) AS bonus_ontem
    FROM ggr_hoje gh
    CROSS JOIN ggr_ontem go
    CROSS JOIN bonus_hoje bh
    CROSS JOIN bonus_ontem bo
    """

    # --- Saques horários ---
    sql_saques = f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff}
          AND is_test = false
    ),
    saq_hoje AS (
        SELECT COALESCE(SUM(s.success_amount_local), 0) AS saques
        FROM ps_bi.fct_cashout_hourly s
        JOIN base_players p ON s.player_id = p.ecr_id
        WHERE s.created_date = DATE '{hoje}' AND s.created_hour < {hora_corte}
    ),
    saq_ontem AS (
        SELECT COALESCE(SUM(s.success_amount_local), 0) AS saques
        FROM ps_bi.fct_cashout_hourly s
        JOIN base_players p ON s.player_id = p.ecr_id
        WHERE s.created_date = DATE '{ontem}' AND s.created_hour < {hora_corte}
    )
    SELECT
        COALESCE(h.saques, 0) AS saques_hoje,
        COALESCE(o.saques, 0) AS saques_ontem
    FROM saq_hoje h CROSS JOIN saq_ontem o
    """

    # Executar queries
    df_rf = query_athena(sql_reg_ftd, database="ps_bi")
    df_dep = query_athena(sql_dep, database="ps_bi")
    df_ggr = query_athena(sql_ggr, database="ps_bi")
    df_saq = query_athena(sql_saques, database="ps_bi")

    rrf = df_rf.iloc[0]
    rd = df_dep.iloc[0]
    rg = df_ggr.iloc[0]
    rs = df_saq.iloc[0]

    def _var(hoje_val, ontem_val):
        if ontem_val == 0:
            pct = 0.0 if hoje_val == 0 else 100.0
        else:
            pct = round((hoje_val - ontem_val) / abs(ontem_val) * 100, 1)
        return {
            "pct": pct,
            "direction": "up" if pct > 0 else ("down" if pct < 0 else "neutral"),
        }

    dep_hoje = float(rd["dep_hoje_valor"])
    dep_ontem = float(rd["dep_ontem_valor"])
    ggr_cas_hoje = float(rg["ggr_casino_hoje"])
    ggr_cas_ontem = float(rg["ggr_casino_ontem"])
    ggr_spt_hoje = float(rg["ggr_sport_hoje"])
    ggr_spt_ontem = float(rg["ggr_sport_ontem"])
    bonus_hoje = float(rg["bonus_hoje"])
    bonus_ontem = float(rg["bonus_ontem"])
    saq_hoje = float(rs["saques_hoje"])
    saq_ontem = float(rs["saques_ontem"])

    # NGR = GGR Casino + GGR Sport - Bonus
    ngr_hoje = ggr_cas_hoje + ggr_spt_hoje - bonus_hoje
    ngr_ontem = ggr_cas_ontem + ggr_spt_ontem - bonus_ontem

    reg_hoje = int(rrf["reg_hoje"])
    reg_ontem = int(rrf["reg_ontem"])
    ftd_hoje = int(rrf["ftd_hoje"])
    ftd_ontem = int(rrf["ftd_ontem"])
    ftd_dep_hoje = float(rrf["ftd_deposit_hoje"])
    ftd_dep_ontem = float(rrf["ftd_deposit_ontem"])

    return {
        "hora_corte_utc": hora_corte,
        "hora_corte_brt": hora_corte_brt,
        "hoje": hoje,
        "ontem": ontem,
        "metrics": {
            "reg": {
                "hoje": reg_hoje,
                "ontem": reg_ontem,
                "var": _var(reg_hoje, reg_ontem),
            },
            "ftd": {
                "hoje": ftd_hoje,
                "ontem": ftd_ontem,
                "var": _var(ftd_hoje, ftd_ontem),
            },
            "ftd_deposit": {
                "hoje": round(ftd_dep_hoje, 2),
                "ontem": round(ftd_dep_ontem, 2),
                "var": _var(ftd_dep_hoje, ftd_dep_ontem),
            },
            "dep_amount": {
                "hoje": round(dep_hoje, 2),
                "ontem": round(dep_ontem, 2),
                "var": _var(dep_hoje, dep_ontem),
            },
            "dep_count": {
                "hoje": int(rd["dep_hoje_qtd"]),
                "ontem": int(rd["dep_ontem_qtd"]),
                "var": _var(int(rd["dep_hoje_qtd"]), int(rd["dep_ontem_qtd"])),
            },
            "ggr_cassino": {
                "hoje": round(ggr_cas_hoje, 2),
                "ontem": round(ggr_cas_ontem, 2),
                "var": _var(ggr_cas_hoje, ggr_cas_ontem),
            },
            "ggr_sport": {
                "hoje": round(ggr_spt_hoje, 2),
                "ontem": round(ggr_spt_ontem, 2),
                "var": _var(ggr_spt_hoje, ggr_spt_ontem),
            },
            "ngr": {
                "hoje": round(ngr_hoje, 2),
                "ontem": round(ngr_ontem, 2),
                "var": _var(ngr_hoje, ngr_ontem),
            },
            "saques": {
                "hoje": round(saq_hoje, 2),
                "ontem": round(saq_ontem, 2),
                "var": _var(saq_hoje, saq_ontem),
            },
        },
    }