"""
Comando: affiliate-daily
Report D-1 (ou data arbitraria) de um ou mais affiliates.

Caso de uso: report diario para trafego/CRM consolidado (REG, FTD, FTD Deposit,
Dep Amount, GGR Cassino, GGR Sport, NGR proxy, Saques).

ATENCAO 24/04/2026: ps_bi.fct_player_activity_daily esta com gap (parou em
06/04). Comando migrado para usar bireports_ec2.tbl_ecr_wise_daily_bi_summary
(camada raw, atualizada em tempo real, valores em centavos -> dividir por 100).

Saida: tabela no console (formato WhatsApp-ready) + CSV com legenda + auditor.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd

from db.athena import query_athena
from db.auditor import AthenaAuditor
from db.helpers import (
    FILTER_NOT_TEST_PSBI,
    to_brt_date,
    affiliate_in,
    fmt_brl,
    fmt_int,
    save_csv_with_legenda,
)

log = logging.getLogger(__name__)


# =============================================================================
# Queries — fonte canonica: bireports_ec2 (raw, sem dependencia do dbt)
# =============================================================================

def _sql_kpis_bireports(aff_ids: list[str], data: str) -> str:
    """KPIs financeiros do dia via bireports_ec2 (centavos -> /100.0)."""
    return f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE {affiliate_in(aff_ids)}
          AND {FILTER_NOT_TEST_PSBI}
    )
    SELECT
        COALESCE(SUM(s.c_co_success_amount), 0) / 100.0                                    AS saques,
        COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0                               AS dep_amount,
        COALESCE(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount), 0) / 100.0  AS ggr_casino,
        COALESCE(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount), 0) / 100.0          AS ggr_sport,
        COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0                                  AS bonus_cost,
        COALESCE(SUM(s.c_deposit_success_count), 0)                                        AS qty_dep,
        COALESCE(SUM(s.c_co_success_count), 0)                                             AS qty_saques
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    INNER JOIN base_players p ON s.c_ecr_id = p.ecr_id
    WHERE s.c_created_date = DATE '{data}'
    """


def _sql_reg_ftd(aff_ids: list[str], data: str) -> str:
    """REG e FTD do dia via dim_user (camada DIM dbt — atualizada)."""
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


def _sql_audit_reg_ecr(aff_ids: list[str], data: str) -> str:
    """
    REG cross-check via ecr_ec2.tbl_ecr (raw cadastro).
    NOTA: ecr_ec2.tbl_ecr nao tem coluna c_test_user — divergencia esperada
    de ate ~3-15% vs ps_bi (test users incluidos + possiveis CDC duplicates).
    Usa DISTINCT c_ecr_id para mitigar CDC.
    """
    return f"""
    SELECT COUNT(DISTINCT c_ecr_id) AS reg_raw
    FROM ecr_ec2.tbl_ecr
    WHERE {affiliate_in(aff_ids, column='c_affiliate_id')}
      AND CAST(c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
    """


# =============================================================================
# Output
# =============================================================================

def _print_whatsapp_table(data: str, aff_ids: list[str], k: pd.Series, r: pd.Series) -> list[str]:
    """Print estilo WhatsApp: tabela Metrica|Valor (NGR proxy = GGR - bonus)."""
    ngr_proxy = float(k['ggr_casino'] or 0) + float(k['ggr_sport'] or 0) - float(k['bonus_cost'] or 0)
    lines = []
    lines.append("=" * 50)
    lines.append(f"Extracao Affiliates - {data}")
    lines.append(f"IDs: {', '.join(aff_ids)}")
    lines.append(f"{'  Metrica':<18}{'Valor':>18}")
    lines.append(f"  {'Saques':<16}{fmt_brl(k['saques']):>18}")
    lines.append(f"  {'REG':<16}{fmt_int(r['reg']):>18}")
    lines.append(f"  {'FTD':<16}{fmt_int(r['ftd']):>18}")
    lines.append(f"  {'FTD Deposit':<16}{fmt_brl(r['ftd_deposit']):>18}")
    lines.append(f"  {'Dep Amount':<16}{fmt_brl(k['dep_amount']):>18}")
    lines.append(f"  {'GGR Cassino':<16}{fmt_brl(k['ggr_casino']):>18}")
    lines.append(f"  {'GGR Sport':<16}{fmt_brl(k['ggr_sport']):>18}")
    lines.append(f"  {'Bonus Cost':<16}{fmt_brl(k['bonus_cost']):>18}")
    lines.append(f"  {'NGR (proxy)':<16}{fmt_brl(ngr_proxy):>18}")
    lines.append("=" * 50)
    for l in lines:
        print(l)
    return lines


# =============================================================================
# Entry point
# =============================================================================

def run(affiliate_ids: list[str], data: str | None, output_dir: str) -> dict:
    """
    Gera report D-1 (ou data passada) para o(s) affiliate(s).

    Fonte: bireports_ec2.tbl_ecr_wise_daily_bi_summary (raw, gap-resistant)
    """
    if data is None:
        data = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        log.info("Sem data informada - usando D-1 (%s)", data)

    log.info(">>> KPIs %s via bireports_ec2 (gap-resistant)", data)
    df_kpis = query_athena(_sql_kpis_bireports(affiliate_ids, data), database="bireports_ec2")
    log.info(">>> REG/FTD %s via ps_bi.dim_user (BRT)", data)
    df_reg = query_athena(_sql_reg_ftd(affiliate_ids, data), database="ps_bi")

    k = df_kpis.iloc[0]
    r = df_reg.iloc[0]

    # ---------------------------------------------------------------
    # Auditor: cross-check REG ps_bi vs ecr_ec2 (raw)
    # ---------------------------------------------------------------
    log.info(">>> AUDITOR: REG cross-check (ps_bi vs ecr_ec2)")
    df_audit_reg = query_athena(_sql_audit_reg_ecr(affiliate_ids, data), database="ecr_ec2")
    n_reg_psbi = int(r["reg"])
    n_reg_raw = int(df_audit_reg["reg_raw"].iloc[0])

    # Tolerancia mais ampla aqui: ecr_ec2 nao filtra test users, pode ter
    # divergencia natural ate ~15% vs ps_bi. >15% indica problema real.
    a = AthenaAuditor(divergencia_ok=5.0, divergencia_alerta=15.0)
    a.add_count("ps_bi.dim_user (DIM)", n_reg_psbi)
    a.add_count("ecr_ec2.tbl_ecr (RAW)", n_reg_raw)
    a.compare_counts(baseline_label="ps_bi.dim_user (DIM)")

    audit_lines = a.report()
    aprovado = a.is_approved()

    lines = _print_whatsapp_table(data, affiliate_ids, k, r)

    # ---------------------------------------------------------------
    # CSV + legenda
    # ---------------------------------------------------------------
    aff_str = "_".join(affiliate_ids)
    ngr_proxy = float(k['ggr_casino'] or 0) + float(k['ggr_sport'] or 0) - float(k['bonus_cost'] or 0)

    out_df = pd.DataFrame([{
        "data":          data,
        "affiliates":    ";".join(affiliate_ids),
        "reg":           int(r["reg"]),
        "reg_raw_ecr":   n_reg_raw,
        "ftd":           int(r["ftd"]),
        "ftd_deposit":   float(r["ftd_deposit"] or 0),
        "qty_dep":       int(k["qty_dep"]),
        "dep_amount":    float(k["dep_amount"]),
        "qty_saques":    int(k["qty_saques"]),
        "saques":        float(k["saques"]),
        "ggr_casino":    float(k["ggr_casino"]),
        "ggr_sport":     float(k["ggr_sport"]),
        "bonus_cost":    float(k["bonus_cost"]),
        "ngr_proxy":     ngr_proxy,
    }])

    csv_path = os.path.join(output_dir, f"affiliate_daily_{aff_str}_{data}.csv")
    csv_out, leg_out = save_csv_with_legenda(
        out_df,
        csv_path,
        titulo=f"AFFILIATE DAILY REPORT - {data}",
        columns_dict={
            "data":         "Data do report (YYYY-MM-DD, BRT)",
            "affiliates":   "Affiliate IDs consolidados (separados por ;)",
            "reg":          "Novos cadastros no dia (ps_bi.dim_user)",
            "reg_raw_ecr":  "Novos cadastros no dia via raw (ecr_ec2.tbl_ecr) - cross-check",
            "ftd":          "First Time Deposits no dia",
            "ftd_deposit":  "Valor do FTD (BRL)",
            "qty_dep":      "Qtd de depositos confirmados no dia",
            "dep_amount":   "Valor depositado no dia (BRL)",
            "qty_saques":   "Qtd de saques confirmados no dia",
            "saques":       "Valor sacado no dia (BRL)",
            "ggr_casino":   "GGR Cassino = realcash bet - realcash win (BRL)",
            "ggr_sport":    "GGR Sport = realcash bet - realcash win sb (BRL)",
            "bonus_cost":   "Bonus Issued (BRL)",
            "ngr_proxy":    "NGR (proxy) = GGR Cassino + GGR Sport - Bonus Issued",
        },
        glossario={
            "REG":   "New Registered Customer",
            "FTD":   "First Time Deposit",
            "GGR":   "Gross Gaming Revenue (bet - win)",
            "NGR":   "Net Gaming Revenue (proxy = GGR - Bonus Issued)",
            "BTR":   "Bonus Turned Real (formula canonica usaria isso ao inves de bonus_issued)",
        },
        fonte="AWS Athena - bireports_ec2.tbl_ecr_wise_daily_bi_summary (KPIs) + ps_bi.dim_user (REG/FTD)",
        periodo=data,
        regras=[
            f"Affiliate(s): {', '.join(affiliate_ids)}",
            "Exclusao: players is_test = true",
            "Fonte KPIs: bireports_ec2 (raw, atualizada em tempo real). Migrado de ps_bi.fct_player_activity_daily em 24/04 (gap dbt 18d).",
            "REG: filtro signup_datetime convertido BRT na ps_bi; cross-check com ecr_ec2.c_sign_up_time",
            "NGR aqui e PROXY (GGR - bonus_issued). Formula canonica seria GGR - BTR - RCA.",
        ],
        validacao=audit_lines,
    )

    log.info("CSV salvo: %s", csv_out)
    log.info("Auditor:   %s", "APROVADO" if aprovado else "ALERTA/REPROVADO")

    return {
        "data":              data,
        "kpis":              k.to_dict(),
        "reg_ftd":           r.to_dict(),
        "ngr_proxy":         ngr_proxy,
        "auditor_aprovado":  aprovado,
        "csv":               csv_out,
        "legenda":           leg_out,
        "print_text":        "\n".join(lines),
    }
