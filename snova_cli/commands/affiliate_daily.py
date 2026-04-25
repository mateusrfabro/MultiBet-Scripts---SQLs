"""
Comando: affiliate-daily
Report D-1 (ou data arbitraria) de um ou mais affiliates.

Caso de uso: report diario para trafego/CRM consolidado (REG, FTD, FTD Deposit,
Dep Amount, GGR Cassino, GGR Sport, NGR, Saques). Equivale ao antigo
extract_affiliates_d1.py mas parametrizado.

Saida: tabela no console (formato WhatsApp-ready) e CSV com os numeros.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd

from db.athena import query_athena
from db.helpers import (
    FILTER_NOT_TEST_PSBI,
    to_brt_date,
    affiliate_in,
    fmt_brl,
    fmt_int,
    save_csv_with_legenda,
)

log = logging.getLogger(__name__)


def _sql_kpis(aff_ids: list[str], data: str) -> str:
    return f"""
    WITH p AS (
        SELECT ecr_id FROM ps_bi.dim_user
        WHERE {affiliate_in(aff_ids)}
          AND {FILTER_NOT_TEST_PSBI}
    )
    SELECT
        COALESCE(SUM(a.cashout_success_base), 0)                                      AS saques,
        COALESCE(SUM(a.deposit_success_base), 0)                                      AS dep_amount,
        COALESCE(SUM(a.casino_realbet_base) - SUM(a.casino_real_win_base), 0)         AS ggr_casino,
        COALESCE(SUM(a.sb_realbet_base) - SUM(a.sb_real_win_base), 0)                 AS ggr_sport,
        COALESCE(SUM(a.ngr_base), 0)                                                  AS ngr,
        COALESCE(SUM(a.deposit_success_count), 0)                                     AS qty_dep,
        COALESCE(SUM(a.cashout_success_count), 0)                                     AS qty_saques
    FROM ps_bi.fct_player_activity_daily a
    INNER JOIN p ON a.player_id = p.ecr_id
    WHERE a.activity_date = DATE '{data}'
    """


def _sql_reg_ftd(aff_ids: list[str], data: str) -> str:
    """REG e FTD do dia usando dim_user com conversao BRT."""
    return f"""
    SELECT
        COUNT(*) AS reg,
        COUNT_IF({to_brt_date('ftd_datetime')} = DATE '{data}') AS ftd,
        SUM(CASE WHEN {to_brt_date('ftd_datetime')} = DATE '{data}'
                 THEN ftd_amount_inhouse ELSE 0 END) AS ftd_deposit
    FROM ps_bi.dim_user
    WHERE {affiliate_in(aff_ids)}
      AND {FILTER_NOT_TEST_PSBI}
      AND {to_brt_date('signup_datetime')} = DATE '{data}'
    """


def _print_whatsapp_table(data: str, aff_ids: list[str], k: pd.Series, r: pd.Series) -> list[str]:
    """Print estilo WhatsApp: tabela Metrica|Valor com Dep/Net/P&L em bold."""
    header = f"Extracao Affiliates — {data}"
    sub = f"IDs: {', '.join(aff_ids)}"
    lines = []
    lines.append("=" * 50)
    lines.append(header)
    lines.append(sub)
    lines.append(f"{'  Metrica':<18}{'Valor':>18}")
    lines.append(f"  {'Saques':<16}{fmt_brl(k['saques']):>18}")
    lines.append(f"  {'REG':<16}{fmt_int(r['reg']):>18}")
    lines.append(f"  {'FTD':<16}{fmt_int(r['ftd']):>18}")
    lines.append(f"  {'FTD Deposit':<16}{fmt_brl(r['ftd_deposit']):>18}")
    lines.append(f"  {'Dep Amount':<16}{fmt_brl(k['dep_amount']):>18}")
    lines.append(f"  {'GGR Cassino':<16}{fmt_brl(k['ggr_casino']):>18}")
    lines.append(f"  {'GGR Sport':<16}{fmt_brl(k['ggr_sport']):>18}")
    lines.append(f"  {'NGR':<16}{fmt_brl(k['ngr']):>18}")
    lines.append("=" * 50)
    for l in lines:
        print(l)
    return lines


def run(affiliate_ids: list[str], data: str | None, output_dir: str) -> dict:
    """
    Gera report D-1 (ou data passada) para o(s) affiliate(s).

    Args:
        affiliate_ids: lista de affiliate_id
        data:          "YYYY-MM-DD" ou None (usa D-1 automaticamente)
        output_dir:    pasta onde salvar CSV

    Returns:
        dict com numeros extraidos + paths.
    """
    if data is None:
        data = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        log.info("Sem data informada - usando D-1 (%s)", data)

    log.info(">>> KPIs diarios %s (ps_bi.fct_player_activity_daily)", data)
    df_kpis = query_athena(_sql_kpis(affiliate_ids, data), database="ps_bi")
    log.info(">>> REG/FTD %s (ps_bi.dim_user com BRT)", data)
    df_reg = query_athena(_sql_reg_ftd(affiliate_ids, data), database="ps_bi")

    k = df_kpis.iloc[0]
    r = df_reg.iloc[0]

    lines = _print_whatsapp_table(data, affiliate_ids, k, r)

    # ---- persistencia em CSV (uma linha por affiliate-group por data) ----
    aff_str = "_".join(affiliate_ids)
    out_df = pd.DataFrame([{
        "data":          data,
        "affiliates":    ";".join(affiliate_ids),
        "reg":           int(r["reg"]),
        "ftd":           int(r["ftd"]),
        "ftd_deposit":   float(r["ftd_deposit"] or 0),
        "qty_dep":       int(k["qty_dep"]),
        "dep_amount":    float(k["dep_amount"]),
        "qty_saques":    int(k["qty_saques"]),
        "saques":        float(k["saques"]),
        "ggr_casino":    float(k["ggr_casino"]),
        "ggr_sport":     float(k["ggr_sport"]),
        "ngr":           float(k["ngr"]),
    }])

    csv_name = f"affiliate_daily_{aff_str}_{data}.csv"
    csv_path = os.path.join(output_dir, csv_name)
    csv_out, leg_out = save_csv_with_legenda(
        out_df,
        csv_path,
        titulo=f"AFFILIATE DAILY REPORT — {data}",
        columns_dict={
            "data":        "Data do report (YYYY-MM-DD, BRT)",
            "affiliates":  "Affiliate IDs consolidados (separados por ;)",
            "reg":         "Novos cadastros no dia (D-1)",
            "ftd":         "First Time Deposits no dia",
            "ftd_deposit": "Valor do FTD (BRL)",
            "qty_dep":     "Qtd de depositos confirmados no dia",
            "dep_amount":  "Valor depositado no dia (BRL)",
            "qty_saques":  "Qtd de saques confirmados no dia",
            "saques":      "Valor sacado no dia (BRL)",
            "ggr_casino":  "GGR Cassino = realbet - realwin casino (BRL)",
            "ggr_sport":   "GGR Sport = realbet - realwin sb (BRL)",
            "ngr":         "NGR = GGR - Bonus Turned Real (ps_bi.ngr_base)",
        },
        glossario={
            "REG":  "New Registered Customer",
            "FTD":  "First Time Deposit",
            "GGR":  "Gross Gaming Revenue (bet - win)",
            "NGR":  "Net Gaming Revenue (GGR - BTR)",
        },
        fonte="AWS Athena - ps_bi (fct_player_activity_daily + dim_user)",
        periodo=data,
        regras=[
            f"Affiliate(s): {', '.join(affiliate_ids)}",
            "Exclusao: players is_test = true",
            "REG/FTD: filtro por signup_datetime/ftd_datetime convertido para BRT",
            "NGR: usa ps_bi.ngr_base (proxy = GGR - bonus_issued)",
        ],
    )

    log.info("CSV salvo: %s", csv_out)
    log.info("Legenda:   %s", leg_out)

    return {
        "data":       data,
        "kpis":       k.to_dict(),
        "reg_ftd":    r.to_dict(),
        "csv":        csv_out,
        "legenda":    leg_out,
        "print_text": "\n".join(lines),
    }
