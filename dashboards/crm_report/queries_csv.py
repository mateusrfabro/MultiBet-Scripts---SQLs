"""
Dashboard CRM Report v3 — Data Layer (CSV-based)
==================================================
Le dados dos CSVs extraidos por scripts/extract_crm_report_csvs.py

CSVS CONSUMIDOS (data/crm_report/):
  campaigns.csv          — campanhas logicas agrupadas + financeiro
  dispatch_costs.csv     — custos de disparo por canal/provedor

FONTES ORIGINAIS (documentado para handoff ao Gusta):
  1. BigQuery (Smartico CRM):
     - j_automation_rule_progress (automation_rule_id → users)
     - dm_automation_rule (rule_name, is_active)
     - dm_segment (segment_name)
     - j_communication (funil, disparos)
  2. Athena (ps_bi):
     - fct_player_activity_daily (GGR, NGR, deposits por user)
     - dim_user (external_id → user_key bridge)

CONTRATO (importado por app.py):
  get_all_dashboard_data()    → dict
  get_campaigns_for_export()  → list[dict]
  clear_cache()               → None

Reescrito: 01/04/2026 — Mateus F. (Squad Intelligence Engine)
"""
import os
import logging
from time import time

import pandas as pd

from dashboards.crm_report.config import (
    CACHE_TTL_SECONDS, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
)

log = logging.getLogger(__name__)

CSV_DIR = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "crm_report"
))


# =========================================================================
# CACHE + CSV LOADING
# =========================================================================
_cache = {}


def _cached(key, fn, ttl=None):
    now = time()
    if key in _cache:
        result, ts = _cache[key]
        if now - ts < (ttl or CACHE_TTL_SECONDS):
            return result
    result = fn()
    _cache[key] = (result, now)
    return result


def clear_cache():
    _cache.clear()
    log.info("Cache CRM limpo")


def _load_csv(name):
    """Carrega CSV com cache."""
    def _read():
        path = os.path.join(CSV_DIR, f"{name}.csv")
        if not os.path.exists(path):
            log.warning(f"CSV nao encontrado: {path}")
            return pd.DataFrame()
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
        log.info(f"CSV carregado: {name} ({len(df)} linhas)")
        return df
    return _cached(f"csv_{name}", _read, ttl=600)


# =========================================================================
# HELPERS
# =========================================================================
def _f(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _i(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val)


def _safe_div(a, b, default=0.0):
    try:
        if b and float(b) != 0:
            return float(a) / float(b)
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return default


# =========================================================================
# 1. KPIs
# =========================================================================
def get_kpis(date_from, date_to, campaign_type=None, alcance=None):
    """
    6 KPIs: Campanhas Ativas, Jogadores Impactados, GGR, NGR, Net Deposit, ROI CRM
    """
    df = _load_csv("campaigns")
    if df.empty:
        return {
            "campanhas_ativas": 0, "campanhas_total": 0, "players_ativos": 0,
            "ggr_total": 0, "ngr_total": 0, "turnover_total": 0,
            "depositos_total": 0, "saques_total": 0, "net_deposit_total": 0,
            "custo_bonus_total": 0, "custo_disparo_total": 0,
            "custo_crm_total": 0, "roi_medio": 0, "arpu": 0,
        }

    if campaign_type and campaign_type != "all":
        df = df[df["campaign_type"] == campaign_type]

    # Filtro alcance nos KPIs tambem
    if alcance and alcance != "all":
        if "is_targeted" in df.columns:
            is_dir = alcance == "Direcionada"
            df = df[df["is_targeted"] == is_dir]
        else:
            if alcance == "Direcionada":
                df = df[df["users"] < 50000]
            else:
                df = df[df["users"] >= 50000]

    n_total = len(df)
    n_active = int(df["status"].eq("ativa").sum()) if "status" in df.columns else 0

    # Jogadores: se filtro "Todos os alcances", so direcionadas. Senao, todas do filtro.
    if not alcance or alcance == "all":
        if "is_targeted" in df.columns:
            df_for_users = df[df["is_targeted"] == True]
        else:
            df_for_users = df[df["users"] < 50000]
    else:
        df_for_users = df
    users = _i(df_for_users["users"].sum()) if "users" in df_for_users.columns else 0

    # Financeiro (colunas opcionais, vem do Athena cross)
    fin_cols = {
        "total_ggr": "ggr_total", "ngr": "ngr_total",
        "casino_turnover": "turnover_total",
        "total_deposit": "depositos_total", "total_withdrawal": "saques_total",
        "net_deposit": "net_deposit_total", "bonus_cost": "custo_bonus_total",
    }
    kpis = {}
    for csv_col, kpi_name in fin_cols.items():
        if csv_col in df.columns:
            kpis[kpi_name] = round(_f(pd.to_numeric(df[csv_col], errors="coerce").sum()), 2)
        else:
            kpis[kpi_name] = 0

    # Dispatch costs
    df_disp = _load_csv("dispatch_costs")
    custo_disp = round(_f(df_disp["custo_total_brl"].sum()), 2) if not df_disp.empty and "custo_total_brl" in df_disp.columns else 0

    custo_crm = kpis.get("custo_bonus_total", 0) + custo_disp
    ngr = kpis.get("ngr_total", 0)

    return {
        "campanhas_ativas": n_active,
        "campanhas_total": n_total,
        "players_ativos": users,
        "ggr_total": kpis.get("ggr_total", 0),
        "ngr_total": ngr,
        "turnover_total": kpis.get("turnover_total", 0),
        "depositos_total": kpis.get("depositos_total", 0),
        "saques_total": kpis.get("saques_total", 0),
        "net_deposit_total": kpis.get("net_deposit_total", 0),
        "custo_bonus_total": kpis.get("custo_bonus_total", 0),
        "custo_disparo_total": custo_disp,
        "custo_crm_total": custo_crm,
        "roi_medio": round(_safe_div(ngr, custo_crm), 1),
        "arpu": round(_safe_div(ngr, users), 2),
    }


# =========================================================================
# 2. Tabela de campanhas (paginada)
# =========================================================================
def get_campaigns(date_from, date_to, campaign_type=None, alcance=None,
                  page=1, page_size=DEFAULT_PAGE_SIZE,
                  sort_by="users", sort_dir="DESC"):
    """
    Tabela de campanhas logicas. 1 linha por campanha agrupada.
    """
    page_size = min(page_size, MAX_PAGE_SIZE)
    df = _load_csv("campaigns")
    empty = {"data": [], "total": 0, "page": 1,
             "page_size": page_size, "total_pages": 1}

    if df.empty:
        return empty

    if campaign_type and campaign_type != "all":
        df = df[df["campaign_type"] == campaign_type]

    # Filtro alcance (Direcionada / Base Inteira)
    if alcance and alcance != "all":
        if "is_targeted" in df.columns:
            is_dir = alcance == "Direcionada"
            df = df[df["is_targeted"] == is_dir]
        else:
            if alcance == "Direcionada":
                df = df[df["users"] < 50000]
            else:
                df = df[df["users"] >= 50000]

    if df.empty:
        return empty

    # Sort
    sort_col = sort_by if sort_by in df.columns else "users"
    if sort_col in df.columns:
        asc = sort_dir.upper() == "ASC"
        df = df.sort_values(sort_col, ascending=asc, na_position="last")

    total = len(df)
    total_pages = max(1, -(-total // page_size))
    offset = (page - 1) * page_size
    page_df = df.iloc[offset:offset + page_size]

    data = []
    for _, r in page_df.iterrows():
        ggr = _f(r.get("total_ggr", 0))
        ngr = _f(r.get("ngr", 0))
        custo = _f(r.get("bonus_cost", 0))

        is_targeted = bool(r.get("is_targeted", True)) if "is_targeted" in r.index else _i(r.get("users", 0)) < 50000

        # Data inicio formatada DD/MM/YYYY
        first = _s(r.get("first_exec", ""))
        if first and "-" in first:
            parts = first.split("-")
            first_fmt = f"{parts[2]}/{parts[1]}/{parts[0]}" if len(parts) == 3 else first
        else:
            first_fmt = first

        data.append({
            "first_exec": first_fmt,
            "report_date": f"{_i(r.get('dias_ativa', 0))} dias",
            "campaign_id": _s(r.get("campaign_group", "")),
            "campaign_name": _s(r.get("campaign_group", "")),
            "campaign_type": _s(r.get("campaign_type", "")),
            "alcance": "Direcionada" if is_targeted else "Base Inteira",
            "canais_disparo": _s(r.get("canais_disparo", "-")),
            "segment_name": _s(r.get("segment_name", "")),
            "status": _s(r.get("status", "")),
            "oferecidos": _i(r.get("users", 0)),
            "completaram": _i(r.get("rules_count", 0)),
            "total_ggr": ggr,
            "ngr": ngr,
            "custo_bonus_brl": custo,
            "depositos_brl": _f(r.get("total_deposit", 0)),
            "net_deposit": _f(r.get("net_deposit", 0)),
            "roi": _f(r.get("roi", 0)),
            "fin_users": _i(r.get("fin_users", 0)),
        })

    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


# =========================================================================
# 3. Funil
# =========================================================================
def get_funnel(date_from, date_to, campaign_type=None, alcance=None):
    """Funil CRM real do BigQuery (j_communication fact_type_id 1-5)."""
    df = _load_csv("funnel_daily")

    if df.empty:
        return {
            "impactados": 0, "com_financeiro": 0, "ativados": 0,
            "monetizados": 0, "pct_financeiro": 0, "pct_ativados": 0,
            "pct_monetizados": 0, "segmentados": 0, "entregues": 0,
            "abertos": 0, "clicados": 0, "convertidos": 0,
            "completaram": 0, "pct_entregues": 0, "pct_abertos": 0,
            "pct_clicados": 0, "pct_convertidos": 0, "pct_completaram": 0,
        }

    # Totais do periodo
    env = _i(df["enviados"].sum())
    ent = _i(df["entregues"].sum())
    abe = _i(df["abertos"].sum())
    cli = _i(df["clicados"].sum())
    conv = _i(df["convertidos"].sum())
    u_env = _i(df["users_enviados"].sum())
    u_conv = _i(df["users_convertidos"].sum())
    # Dados extraidos BigQuery + Athena para campanhas direcionadas (02/04/2026)
    # TODO: automatizar no extract_crm_report_csvs.py
    u_bonus = 3228
    u_depositaram = 4015
    u_apostaram = 4019
    u_dep_e_apost = 3940
    base = env if env > 0 else 1

    return {
        "impactados": u_env,
        "com_financeiro": u_conv,
        "ativados": u_conv,
        "monetizados": u_conv,
        "pct_financeiro": round(u_conv / max(u_env, 1) * 100, 1),
        "pct_ativados": round(u_conv / max(u_env, 1) * 100, 1),
        "pct_monetizados": round(u_conv / max(u_env, 1) * 100, 1),
        "segmentados": env,
        "entregues": ent,
        "abertos": abe,
        "clicados": cli,
        "convertidos": conv,
        "completaram": u_conv,
        "users_bonus": u_bonus,
        "users_depositaram": u_depositaram,
        "users_apostaram": u_apostaram,
        "users_dep_e_apost": u_dep_e_apost,
        "pct_entregues": round(ent / base * 100, 1),
        "pct_abertos": round(abe / base * 100, 1),
        "pct_clicados": round(cli / base * 100, 1),
        "pct_convertidos": round(conv / base * 100, 1),
        "pct_completaram": round(u_conv / max(u_env, 1) * 100, 1),
    }


# =========================================================================
# 4. Funil por tipo
# =========================================================================
def get_funnel_by_type(date_from, date_to):
    """Users e GGR por tipo de campanha (para grafico de barras)."""
    df = _load_csv("campaigns")
    if df.empty:
        return []

    # Converter colunas numericas
    for c in ["users", "fin_users", "total_ggr", "ngr"]:
        if c in df.columns:
            df.loc[:, c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    agg = df.groupby("campaign_type").agg(
        oferecidos=("users", "sum"),
        fin_users=("fin_users", "sum") if "fin_users" in df.columns else ("users", lambda x: 0),
        campanhas=("campaign_group", "nunique"),
        ggr=("total_ggr", "sum") if "total_ggr" in df.columns else ("users", lambda x: 0),
    ).reset_index().sort_values("oferecidos", ascending=False)

    # Taxa conversao = fin_users / oferecidos (quem teve atividade financeira)
    agg["taxa_conversao"] = (agg["fin_users"] / agg["oferecidos"].replace(0, 1) * 100).round(1)

    return [
        {
            "campaign_type": str(r["campaign_type"]),
            "oferecidos": int(r["oferecidos"]),
            "completaram": int(r["fin_users"]),
            "campanhas": int(r["campanhas"]),
            "taxa_conversao": float(r["taxa_conversao"]),
            "ggr": float(r.get("ggr", 0)),
        }
        for _, r in agg.iterrows()
    ]


# =========================================================================
# 5. Volume diario (placeholder — dados single-day por ora)
# =========================================================================
def get_daily_volume(date_from, date_to, campaign_type=None):
    """Volume diario do funil CRM — todas as etapas."""
    df = _load_csv("funnel_daily")
    if df.empty:
        return []
    return [
        {
            "date": str(r.get("report_date", "")),
            "enviados": _i(r.get("enviados", 0)),
            "entregues": _i(r.get("entregues", 0)),
            "abertos": _i(r.get("abertos", 0)),
            "clicados": _i(r.get("clicados", 0)),
            "convertidos": _i(r.get("convertidos", 0)),
            # Compat com JS antigo
            "segmentados": _i(r.get("enviados", 0)),
            "oferecidos": _i(r.get("users_enviados", 0)),
            "completaram": _i(r.get("users_convertidos", 0)),
        }
        for _, r in df.iterrows()
    ]


# =========================================================================
# 6. Top jogos (placeholder — pendente cross Athena por user x jogo)
# =========================================================================
def get_top_games(date_from, date_to, limit=10):
    """Top jogos da base CRM impactada (fund_ec2)."""
    df = _load_csv("top_games")
    if df.empty:
        return {"games": [], "unique_players": 0}
    df = df.head(limit)
    for c in ["turnover_brl", "ggr_brl"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Users unicos = COUNT DISTINCT do Athena (salvo em top_games_meta.json)
    import json
    meta_path = os.path.join(CSV_DIR, "top_games_meta.json")
    try:
        with open(meta_path) as f:
            unique_players = json.load(f).get("unique_players", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        unique_players = 0

    games = [
        {
            "game_name": _s(r.get("game_name", "?")),
            "game_id": _s(r.get("game_id", "")),
            "users": _i(r.get("users", 0)),
            "turnover_brl": round(_f(r.get("turnover_brl", 0)), 2),
            "ggr_brl": round(_f(r.get("ggr_brl", 0)), 2),
            "rtp_pct": round((1 - _safe_div(r.get("ggr_brl", 0), r.get("turnover_brl", 0))) * 100, 1),
        }
        for _, r in df.iterrows()
    ]
    return {"games": games, "unique_players": unique_players}


# =========================================================================
# 7. VIP (placeholder)
# =========================================================================
def get_vip_analysis(date_from, date_to):
    """VIP por NGR: Elite >= R$10K, Key Account >= R$5K, High Value >= R$3K."""
    df = _load_csv("vip_summary")
    if df.empty:
        return []
    return [
        {
            "vip_tier": _s(r.get("vip_tier", "")),
            "users": _i(r.get("users", 0)),
            "ngr_total": round(_f(r.get("ngr_total", 0)), 2),
            "ngr_medio": round(_f(r.get("ngr_medio", 0)), 2),
            "apd": round(_f(r.get("apd", 0)), 1),
        }
        for _, r in df.iterrows()
    ]


# =========================================================================
# 8. Dispatch
# =========================================================================
def get_dispatch_budget(date_from, date_to):
    df = _load_csv("dispatch_costs")
    if df.empty:
        return []

    # Remover canais sem custo (popup, outro, desconhecido) — CRM pediu desconsiderar
    if "custo_unitario" in df.columns:
        df = df[pd.to_numeric(df["custo_unitario"], errors="coerce") > 0]

    # Ordenar por canal (SMS juntos, depois WhatsApp)
    df = df.sort_values(["channel", "provider"])

    return [
        {
            "channel": _s(r.get("channel", "")),
            "provider": _s(r.get("provider", "")),
            "custo_unitario": _f(r.get("custo_unitario", 0)),
            "total_sent": _i(r.get("total_sent", 0)),
            "custo_total_brl": _f(r.get("custo_total_brl", 0)),
        }
        for _, r in df.iterrows()
    ]


# =========================================================================
# 9. ROI por tipo
# =========================================================================
def get_roi_by_type(date_from, date_to):
    df = _load_csv("campaigns")
    if df.empty:
        return []

    for c in ["total_ggr", "ngr", "bonus_cost"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    agg = df.groupby("campaign_type").agg(
        ggr_total=("total_ggr", "sum") if "total_ggr" in df.columns else ("users", lambda x: 0),
        ngr_total=("ngr", "sum") if "ngr" in df.columns else ("users", lambda x: 0),
        custo_total=("bonus_cost", "sum") if "bonus_cost" in df.columns else ("users", lambda x: 0),
        users=("users", "sum"),
    ).reset_index().sort_values("ggr_total", ascending=False)

    return [
        {
            "campaign_type": str(r["campaign_type"]),
            "roi": round(_safe_div(r["ngr_total"], r["custo_total"]), 1),
            "custo_total": _f(r["custo_total"]),
            "ggr_total": _f(r["ggr_total"]),
            "taxa_conversao": 0,
        }
        for _, r in agg.iterrows()
    ]


# =========================================================================
# 10-11. Comparativo + Recovery (placeholders)
# =========================================================================
def get_comparison(campaign_type="RETEM"):
    return []

def get_recovery(date_from, date_to):
    return []


# =========================================================================
# CONSOLIDADO
# =========================================================================
def get_all_dashboard_data(date_from, date_to, campaign_type=None,
                           alcance=None, page=1, page_size=DEFAULT_PAGE_SIZE,
                           sort_by="users", sort_dir="DESC"):
    cache_key = (
        f"all_{date_from}_{date_to}_{campaign_type}_{alcance}"
        f"_{page}_{page_size}_{sort_by}_{sort_dir}"
    )

    def _fetch():
        return {
            "kpis": get_kpis(date_from, date_to, campaign_type, alcance),
            "campaigns": get_campaigns(
                date_from, date_to, campaign_type, alcance,
                page, page_size, sort_by, sort_dir,
            ),
            "funnel": get_funnel(date_from, date_to, campaign_type, alcance),
            "funnel_by_type": get_funnel_by_type(date_from, date_to),
            "daily_volume": get_daily_volume(date_from, date_to, campaign_type),
            "top_games": get_top_games(date_from, date_to),
            "vip_analysis": get_vip_analysis(date_from, date_to),
            "dispatch_budget": get_dispatch_budget(date_from, date_to),
            "roi_by_type": get_roi_by_type(date_from, date_to),
            "recovery": get_recovery(date_from, date_to),
        }

    return _cached(cache_key, _fetch)


# =========================================================================
# EXPORT CSV
# =========================================================================
def get_campaigns_for_export(date_from, date_to, campaign_type=None,
                              alcance=None):
    result = get_campaigns(
        date_from, date_to, campaign_type, alcance,
        page=1, page_size=9999,
    )
    return result["data"]
