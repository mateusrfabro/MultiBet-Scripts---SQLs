"""
Extrator de Report Diario — Affiliates Google Ads e Meta.

Detecta automaticamente se a data e intraday (dia corrente) ou D-1 (dia fechado)
e alterna fontes de dados para garantir precisao:
  - Intraday: BigQuery (real-time) para REG, FTD, FTD Deposit, Dep Amount
  - D-1:      Athena (bireports/ps_bi) para tudo (carga completa)
  - GGR/NGR/Saques: sempre Athena (sem fonte BQ real-time)

Uso:
    python scripts/extract_affiliates_report.py              # usa data padrao (hoje)
    python scripts/extract_affiliates_report.py 2026-03-31   # data especifica
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\MultiBet")

from db.athena import query_athena
from db.bigquery import query_bigquery
from datetime import date, datetime
import traceback
import pytz

# =====================================================================
# CONFIGURACAO
# =====================================================================
DATA = sys.argv[1] if len(sys.argv) > 1 else str(date.today())

# Detectar modo: intraday (hoje) ou dia fechado (D-1 ou anterior)
BRT = pytz.timezone("America/Sao_Paulo")
HOJE_BRT = datetime.now(BRT).strftime("%Y-%m-%d")
INTRADAY = (DATA == HOJE_BRT)

CANAIS = {
    "google": {
        "label": "Google Ads",
        "affiliates": "('297657', '445431', '468114')",
        "affiliates_bq": "(297657, 445431, 468114)",
        "ids_display": "297657, 445431, 468114",
    },
    "meta": {
        "label": "Meta Ads",
        "affiliates": "('532570', '532571', '464673')",
        "affiliates_bq": "(532570, 532571, 464673)",
        "ids_display": "532570, 532571, 464673",
    },
}


# =====================================================================
# QUERIES ATHENA — dia fechado (D-1), carga completa
# =====================================================================
def query_reg_athena(data, affiliates):
    """REG via bireports_ec2.tbl_ecr (BRT)."""
    return f"""
    SELECT COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {affiliates}
      AND c_test_user = false
    """


def query_ftd_athena(data, affiliates):
    """FTD same-day via bireports + ps_bi.dim_user (ftd_datetime)."""
    return f"""
    WITH regs AS (
        SELECT c_ecr_id
        FROM bireports_ec2.tbl_ecr
        WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
          AND CAST(c_affiliate_id AS VARCHAR) IN {affiliates}
          AND c_test_user = false
    )
    SELECT COUNT(*) AS ftd,
           COALESCE(SUM(u.ftd_amount_inhouse), 0) AS ftd_dep
    FROM regs r
    JOIN ps_bi.dim_user u ON r.c_ecr_id = u.ecr_id
    WHERE CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
    """


def query_financeiro(data, affiliates):
    """Financeiro via bireports_ec2.tbl_ecr_wise_daily_bi_summary (centavos /100).
    GGR usa sub-fund isolation (somente realcash, sem bonus)."""
    return f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {affiliates}
          AND is_test = false
    )
    SELECT
        COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS dep_amount,
        COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS saques,
        COALESCE(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_cassino,
        COALESCE(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
        COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0 AS bonus_cost
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN base_players p ON s.c_ecr_id = p.ecr_id
    WHERE s.c_created_date = DATE '{data}'
    """


def query_reg_ecr_ec2(data, affiliates):
    """REG via ecr_ec2.tbl_ecr — mais atualizada intraday que bireports."""
    return f"""
    SELECT COUNT(*) AS reg_ecr
    FROM ecr_ec2.tbl_ecr
    WHERE CAST(c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {affiliates}
    """


# =====================================================================
# QUERIES BIGQUERY — real-time (intraday)
# =====================================================================
def query_reg_bigquery(data, affiliates_bq):
    """REG real-time via BigQuery j_user."""
    return f"""
    SELECT COUNT(DISTINCT user_ext_id) AS reg_bq
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{data}'
      AND core_affiliate_id IN {affiliates_bq}
    """


def query_ftd_bigquery(data, affiliates_bq):
    """FTD same-day via BigQuery j_user (acc_last_deposit_date).
    Corrigido 31/03/2026: usar DATE() = data, nao IS NOT NULL."""
    return f"""
    SELECT COUNT(DISTINCT user_ext_id) AS ftd_bq
    FROM `smartico-bq6.dwh_ext_24105.j_user`
    WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{data}'
      AND core_affiliate_id IN {affiliates_bq}
      AND DATE(acc_last_deposit_date, "America/Sao_Paulo") = '{data}'
    """


def query_ftd_deposit_bigquery(data, affiliates_bq):
    """FTD Deposit real-time: valor total dos depositos de jogadores que
    registraram E depositaram no mesmo dia. Fonte: tr_acc_deposit_approved.
    Join j_user.user_id = tr.user_id (campo correto, nao user_ext_id)."""
    return f"""
    SELECT
        COUNT(DISTINCT j.user_ext_id) AS ftd_count,
        ROUND(SUM(t.acc_last_deposit_amount), 2) AS ftd_deposit_total
    FROM `smartico-bq6.dwh_ext_24105.j_user` j
    JOIN `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` t
        ON j.user_id = t.user_id
    WHERE DATE(j.core_registration_date, 'America/Sao_Paulo') = '{data}'
      AND j.core_affiliate_id IN {affiliates_bq}
      AND DATE(t.event_time, 'America/Sao_Paulo') = '{data}'
    """


def query_dep_total_bigquery(data, affiliates_bq):
    """Dep Amount total real-time: todos os depositos aprovados dos jogadores
    dos affiliates no dia (nao apenas FTD). Fonte: tr_acc_deposit_approved."""
    return f"""
    SELECT
        COUNT(DISTINCT t.user_id) AS depositantes_unicos,
        ROUND(SUM(t.acc_last_deposit_amount), 2) AS dep_amount_total
    FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` t
    JOIN `smartico-bq6.dwh_ext_24105.j_user` j ON j.user_id = t.user_id
    WHERE j.core_affiliate_id IN {affiliates_bq}
      AND DATE(t.event_time, 'America/Sao_Paulo') = '{data}'
    """


# =====================================================================
# FORMATACAO E EXECUCAO
# =====================================================================
def fmt(valor):
    """Formata valor monetario no padrao BR."""
    if valor < 0:
        return f"-R$ {abs(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def run_canal(canal, cfg):
    modo = "INTRADAY (real-time)" if INTRADAY else "DIA FECHADO (D-1)"
    hora_brt = datetime.now(BRT).strftime("%H:%M BRT")

    print(f"\n{'='*60}")
    print(f"EXTRACAO {cfg['label'].upper()} — {DATA}")
    print(f"Modo: {modo}" + (f" | Atualizado as {hora_brt}" if INTRADAY else ""))
    print(f"Affiliates: {cfg['ids_display']}")
    print(f"{'='*60}")

    # -----------------------------------------------------------------
    # REG
    # -----------------------------------------------------------------
    if INTRADAY:
        # Primario: BigQuery (real-time)
        df_reg_bq = query_bigquery(query_reg_bigquery(DATA, cfg["affiliates_bq"]))
        reg = int(df_reg_bq["reg_bq"].iloc[0])
        reg_fonte = "BigQuery (real-time)"
        # Cross-validation: ecr_ec2
        try:
            df_ecr = query_athena(query_reg_ecr_ec2(DATA, cfg["affiliates"]), database="ecr_ec2")
            reg_ecr = int(df_ecr["reg_ecr"].iloc[0])
        except Exception:
            reg_ecr = "N/A"
        # bireports (delay)
        try:
            df_bi = query_athena(query_reg_athena(DATA, cfg["affiliates"]), database="bireports_ec2")
            reg_bi = int(df_bi["reg"].iloc[0])
        except Exception:
            reg_bi = "N/A"
    else:
        # Primario: bireports (carga completa)
        df_bi = query_athena(query_reg_athena(DATA, cfg["affiliates"]), database="bireports_ec2")
        reg = int(df_bi["reg"].iloc[0])
        reg_bi = reg
        reg_fonte = "bireports_ec2"
        # Cross-validation
        try:
            df_ecr = query_athena(query_reg_ecr_ec2(DATA, cfg["affiliates"]), database="ecr_ec2")
            reg_ecr = int(df_ecr["reg_ecr"].iloc[0])
        except Exception:
            reg_ecr = "N/A"
        try:
            df_reg_bq = query_bigquery(query_reg_bigquery(DATA, cfg["affiliates_bq"]))
            reg_bq_val = int(df_reg_bq["reg_bq"].iloc[0])
        except Exception:
            reg_bq_val = "N/A"

    # -----------------------------------------------------------------
    # FTD + FTD Deposit
    # -----------------------------------------------------------------
    if INTRADAY:
        # Primario: BigQuery (real-time)
        df_ftd_bq = query_bigquery(query_ftd_bigquery(DATA, cfg["affiliates_bq"]))
        ftd = int(df_ftd_bq["ftd_bq"].iloc[0])
        ftd_fonte = "BigQuery (real-time)"

        # FTD Deposit: BigQuery tr_acc_deposit_approved
        try:
            df_ftd_dep = query_bigquery(query_ftd_deposit_bigquery(DATA, cfg["affiliates_bq"]))
            ftd_dep = float(df_ftd_dep["ftd_deposit_total"].iloc[0])
            ftd_dep_fonte = "BigQuery (real-time)"
        except Exception:
            ftd_dep = 0.0
            ftd_dep_fonte = "ERRO BigQuery"

        # Cross-validation: ps_bi
        try:
            df_psbi = query_athena(query_ftd_athena(DATA, cfg["affiliates"]), database="bireports_ec2")
            ftd_psbi = int(df_psbi["ftd"].iloc[0])
            ftd_dep_psbi = float(df_psbi["ftd_dep"].iloc[0])
        except Exception:
            ftd_psbi = "N/A"
            ftd_dep_psbi = "N/A"
    else:
        # Primario: ps_bi (carga completa)
        df_psbi = query_athena(query_ftd_athena(DATA, cfg["affiliates"]), database="bireports_ec2")
        ftd = int(df_psbi["ftd"].iloc[0])
        ftd_dep = float(df_psbi["ftd_dep"].iloc[0])
        ftd_fonte = "ps_bi.dim_user"
        ftd_dep_fonte = "ps_bi.dim_user"
        ftd_psbi = ftd
        ftd_dep_psbi = ftd_dep
        # Cross-validation: BigQuery
        try:
            df_ftd_bq = query_bigquery(query_ftd_bigquery(DATA, cfg["affiliates_bq"]))
            ftd_bq_val = int(df_ftd_bq["ftd_bq"].iloc[0])
        except Exception:
            ftd_bq_val = "N/A"

    # -----------------------------------------------------------------
    # Dep Amount (total, nao so FTD)
    # -----------------------------------------------------------------
    if INTRADAY:
        # BigQuery real-time para Dep Amount
        try:
            df_dep_bq = query_bigquery(query_dep_total_bigquery(DATA, cfg["affiliates_bq"]))
            dep = float(df_dep_bq["dep_amount_total"].iloc[0])
            dep_fonte = "BigQuery (real-time)"
        except Exception:
            dep = 0.0
            dep_fonte = "ERRO BigQuery"
    else:
        dep_fonte = "bireports_ec2"
        # dep ja vem do query_financeiro abaixo

    # -----------------------------------------------------------------
    # Financeiro: Saques, GGR, Bonus, NGR — sempre Athena
    # -----------------------------------------------------------------
    df3 = query_athena(query_financeiro(DATA, cfg["affiliates"]), database="ps_bi")
    dep_athena = float(df3["dep_amount"].iloc[0])
    saq = float(df3["saques"].iloc[0])
    ggr_c = float(df3["ggr_cassino"].iloc[0])
    ggr_s = float(df3["ggr_sport"].iloc[0])
    bonus = float(df3["bonus_cost"].iloc[0])
    ngr = ggr_c + ggr_s - bonus

    if not INTRADAY:
        dep = dep_athena

    # -----------------------------------------------------------------
    # Test users
    # -----------------------------------------------------------------
    try:
        df_test = query_athena(f"""
        SELECT COUNT(*) AS test_users
        FROM bireports_ec2.tbl_ecr
        WHERE CAST(c_affiliate_id AS VARCHAR) IN {cfg["affiliates"]}
          AND c_test_user = true
        """, database="bireports_ec2")
        test_users = int(df_test["test_users"].iloc[0])
    except Exception:
        test_users = "N/A"

    # -----------------------------------------------------------------
    # Conversao
    # -----------------------------------------------------------------
    conv = f"{(ftd / reg * 100):.1f}%" if reg > 0 else "N/A"

    # -----------------------------------------------------------------
    # Output
    # -----------------------------------------------------------------
    print(f"\n{'Metrica':<16} {'Valor':>14}  {'Fonte'}")
    print(f"{'-'*60}")
    print(f"{'REG':<16} {reg:>14}  {reg_fonte}")
    print(f"{'FTD':<16} {ftd:>14}  {ftd_fonte}")
    print(f"{'Conversao':<16} {conv:>14}  FTD/REG")
    print(f"{'FTD Deposit':<16} {fmt(ftd_dep):>14}  {ftd_dep_fonte}")
    print(f"{'Dep Amount':<16} {fmt(dep):>14}  {dep_fonte}")
    print(f"{'Saques':<16} {fmt(saq):>14}  bireports_ec2")
    print(f"{'GGR Cassino':<16} {fmt(ggr_c):>14}  bireports_ec2")
    print(f"{'GGR Sport':<16} {fmt(ggr_s):>14}  bireports_ec2")
    print(f"{'Bonus Cost':<16} {fmt(bonus):>14}  bireports_ec2")
    print(f"{'NGR':<16} {fmt(ngr):>14}  calculado")

    # Validacao cruzada
    print(f"\nValidacao cruzada:")
    if INTRADAY:
        print(f"  REG: BigQuery={reg} | ecr_ec2={reg_ecr} | bireports={reg_bi}")
        cv_ftd = f"ps_bi={ftd_psbi}" if isinstance(ftd_psbi, int) else f"ps_bi=N/A"
        print(f"  FTD: BigQuery={ftd} | {cv_ftd} (ps_bi delay dbt)")
        if isinstance(ftd_dep_psbi, float):
            print(f"  FTD Dep: BigQuery={fmt(ftd_dep)} | ps_bi={fmt(ftd_dep_psbi)} (delay)")
        print(f"  Dep Amount: BigQuery={fmt(dep)} | bireports={fmt(dep_athena)} (delay)")
    else:
        reg_bq_display = reg_bq_val if 'reg_bq_val' in dir() else "N/A"
        ftd_bq_display = ftd_bq_val if 'ftd_bq_val' in dir() else "N/A"
        match_reg = "OK" if isinstance(reg_bq_display, int) and abs(reg - reg_bq_display) <= 5 else "DIVERGE"
        match_ftd = "OK" if isinstance(ftd_bq_display, int) and abs(ftd - ftd_bq_display) <= 5 else "DIVERGE"
        print(f"  REG: bireports={reg} | ecr_ec2={reg_ecr} | BigQuery={reg_bq_display} ({match_reg})")
        print(f"  FTD: ps_bi={ftd} | BigQuery={ftd_bq_display} ({match_ftd})")

    if ftd > reg:
        print(f"  *** ALERTA: FTD ({ftd}) > REG ({reg}) — verificar logica!")
    print(f"  Test users: {test_users} confirmados (excluidos das queries)")

    if INTRADAY:
        print(f"\n  ** DADOS PARCIAIS — dia em andamento. GGR/NGR/Saques via Athena (delay ETL).")


def run():
    modo = "INTRADAY" if INTRADAY else "DIA FECHADO"
    print(f"\n*** MODO: {modo} — Data: {DATA} ***")
    if INTRADAY:
        print(f"*** BigQuery = fonte primaria (real-time) | Athena = cross-validation ***")
    else:
        print(f"*** Athena = fonte primaria (carga completa) | BigQuery = cross-validation ***")

    for canal, cfg in CANAIS.items():
        try:
            run_canal(canal, cfg)
        except Exception as e:
            print(f"\nERRO {cfg['label']}: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    run()
