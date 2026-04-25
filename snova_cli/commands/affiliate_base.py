"""
Comando: affiliate-base
Extrai base lifetime de players de um ou mais affiliates.

Caso de uso: Rapha (CRM) pede "base total de um affiliate" para matching
no Smartico. Equivale ao antigo extract_affiliate_363722_pri_simoes.py mas
parametrizado e reusavel.

Saida: CSV + legenda padronizada.
"""
from __future__ import annotations

import logging
import os

from db.athena import query_athena
from db.auditor import AthenaAuditor
from db.helpers import (
    FILTER_NOT_TEST_PSBI,
    to_brt,
    affiliate_in,
    save_csv_with_legenda,
)

log = logging.getLogger(__name__)


def _sql_players(aff_ids: list[str]) -> str:
    """Players lifetime do(s) affiliate(s) com campos padrao CRM."""
    return f"""
    SELECT
        u.ecr_id,
        u.external_id                                                                         AS smartico_id,
        TRIM(
            COALESCE(u.first_name,'') || ' ' ||
            COALESCE(u.middle_name,'') || ' ' ||
            COALESCE(u.last_name,'')  || ' ' ||
            COALESCE(u.second_last_name,'')
        )                                                                                      AS nome_completo,
        u.screen_name,
        CAST(u.affiliate_id AS VARCHAR)                                                        AS affiliate_id,
        CAST({to_brt('u.signup_datetime')} AS VARCHAR)                                         AS signup_brt,
        u.tracker_id,
        u.country_code
    FROM ps_bi.dim_user u
    WHERE {affiliate_in(aff_ids)}
      AND {FILTER_NOT_TEST_PSBI}
      AND u.external_id IS NOT NULL
    ORDER BY u.signup_datetime ASC
    """


def _sql_audit_ecr(aff_ids: list[str]) -> str:
    return f"""
    SELECT COUNT(DISTINCT c_ecr_id) AS n
    FROM ecr_ec2.tbl_ecr
    WHERE {affiliate_in(aff_ids, column='c_affiliate_id')}
    """


def _sql_audit_banner(aff_ids: list[str]) -> str:
    return f"""
    SELECT COUNT(DISTINCT c_ecr_id) AS n
    FROM ecr_ec2.tbl_ecr_banner
    WHERE {affiliate_in(aff_ids, column='c_affiliate_id')}
    """


def run(affiliate_ids: list[str], output_dir: str, label: str | None = None) -> dict:
    """
    Executa a extracao e entrega para um ou mais affiliate_ids.

    Args:
        affiliate_ids: lista de affiliate_id (ex: ["363722"] ou ["363722","532570"])
        output_dir:    pasta onde salvar CSV + legenda
        label:         nome amigavel do affiliate (usa no arquivo). Se None, usa IDs.

    Returns:
        dict com paths de csv/legenda e status do auditor.
    """
    aff_str = "_".join(affiliate_ids)
    slug = label.lower().replace(" ", "_") if label else aff_str
    csv_name = f"affiliate_{aff_str}_{slug}_base_players_FINAL.csv"
    csv_path = os.path.join(output_dir, csv_name)
    titulo = f"BASE LIFETIME DE PLAYERS - AFFILIATE {aff_str}"
    if label:
        titulo += f" ({label})"

    # -----------------------------------------------------------------
    # Extracao
    # -----------------------------------------------------------------
    log.info(">>> EXTRACAO: players lifetime affiliate %s", aff_str)
    df = query_athena(_sql_players(affiliate_ids), database="ps_bi")
    log.info("    %d players extraidos", len(df))

    # -----------------------------------------------------------------
    # Auditor
    # -----------------------------------------------------------------
    log.info(">>> AUDITOR (cross-check 100%% Athena)")
    df_ecr = query_athena(_sql_audit_ecr(affiliate_ids), database="ecr_ec2")
    df_ban = query_athena(_sql_audit_banner(affiliate_ids), database="ecr_ec2")

    a = AthenaAuditor()
    a.add_count("ps_bi.dim_user", len(df))
    a.add_count("ecr_ec2.tbl_ecr", int(df_ecr["n"].iloc[0]))
    a.add_count("ecr_ec2.tbl_ecr_banner", int(df_ban["n"].iloc[0]))
    a.compare_counts(baseline_label="ps_bi.dim_user")
    a.check_unique("ps_bi.dim_user", df, "smartico_id")
    a.check_nulls(df, ["ecr_id", "smartico_id", "nome_completo"])

    audit_lines = a.report()

    # -----------------------------------------------------------------
    # Entrega: CSV + legenda
    # -----------------------------------------------------------------
    periodo = ""
    if not df.empty:
        periodo = f"{df['signup_brt'].min()[:10]} -> {df['signup_brt'].max()[:10]}"

    csv_out, leg_out = save_csv_with_legenda(
        df,
        csv_path,
        titulo=titulo,
        columns_dict={
            "ecr_id":        "Identificador interno do player (ID 18 digitos MultiBet)",
            "smartico_id":   "ID do player no Smartico (= ps_bi.dim_user.external_id). Campo de matching no CRM.",
            "nome_completo": "first_name + middle_name + last_name + second_last_name (espacos extras removidos)",
            "screen_name":   "Nickname/email do player na plataforma",
            "affiliate_id":  "ID do afiliado",
            "signup_brt":    "Data e hora de cadastro em BRT (UTC-3)",
            "tracker_id":    "ID do tracker/campanha (pode vir vazio)",
            "country_code":  "ISO 3166-1 alpha-2 (BR = Brasil)",
        },
        glossario={
            "Smartico ID":   "Equivale a user_ext_id no BigQuery/Smartico (chave de matching CRM)",
            "ecr_id":        "ID interno MultiBet, nao tem equivalente no Smartico",
        },
        fonte="AWS Athena - ps_bi.dim_user (camada BI dbt)",
        periodo=periodo,
        regras=[
            f"Affiliate(s): {', '.join(affiliate_ids)}",
            "Exclusao: players is_test = true",
            "Exclusao: players sem external_id (nao utilizaveis no CRM)",
            "Ordenacao: data de cadastro crescente",
        ],
        validacao=audit_lines,
        acao_sugerida=(
            "Usar 'smartico_id' como chave de matching no Smartico. "
            "Equivale a user_ext_id na plataforma."
        ),
    )

    log.info("=" * 70)
    log.info("ENTREGA: affiliate %s", aff_str + (f" ({label})" if label else ""))
    log.info("Players   : %d", len(df))
    log.info("CSV       : %s", csv_out)
    log.info("Legenda   : %s", leg_out)
    log.info("Auditor   : %s", "APROVADO" if a.is_approved() else "REPROVADO")
    log.info("=" * 70)

    return {
        "csv": csv_out,
        "legenda": leg_out,
        "n_players": len(df),
        "auditor_aprovado": a.is_approved(),
        "auditor_alerta": a.has_alert(),
    }
