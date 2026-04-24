"""
Extrator de Report Diario — Affiliates Google Ads e Meta.
100% Athena — cross-validation entre camadas raw/bronze (_ec2) e BI (bireports/ps_bi).

Modo automatico:
  - D-1 (dia fechado): bireports/ps_bi como fonte primaria, _ec2 como cross-validation
  - Intraday (dia corrente): ecr_ec2/cashier_ec2 como primario (mais atualizados),
    bireports como CV (pode ter delay ETL)

Uso:
    python scripts/extract_affiliates_report.py              # default D-1 (ontem BRT)
    python scripts/extract_affiliates_report.py 2026-04-06   # data especifica
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\MultiBet")

from db.athena import query_athena
from datetime import datetime, timedelta
import traceback
import pytz

# =====================================================================
# CONFIGURACAO
# =====================================================================
BRT = pytz.timezone("America/Sao_Paulo")
HOJE_BRT = datetime.now(BRT).strftime("%Y-%m-%d")

# Default: D-1 (ontem BRT) — dados fechados sao mais confiaveis
if len(sys.argv) > 1:
    DATA = sys.argv[1]
else:
    ontem = datetime.now(BRT) - timedelta(days=1)
    DATA = ontem.strftime("%Y-%m-%d")

INTRADAY = (DATA == HOJE_BRT)

CANAIS = {
    "google": {
        "label": "Google Ads",
        "affiliates": "('297657', '445431', '468114')",
        "ids_display": "297657, 445431, 468114",
    },
    "meta": {
        "label": "Meta Ads",
        "affiliates": "('464673', '532090', '532571', '532570')",
        "ids_display": "464673, 532090, 532571, 532570",
    },
    "tiktok": {
        "label": "TikTok Ads",
        "affiliates": "('477668')",
        "ids_display": "477668",
    },
}


# =====================================================================
# QUERIES — CAMADA BI (bireports/ps_bi) — fonte primaria D-1
# =====================================================================
def query_reg_bireports(data, affiliates):
    """REG via bireports_ec2.tbl_ecr (camada BI, BRT)."""
    return f"""
    SELECT COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {affiliates}
      AND c_test_user = false
    """


def query_ftd_psbi(data, affiliates):
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


# =====================================================================
# QUERIES — CAMADA RAW/BRONZE (_ec2) — cross-validation
# =====================================================================
def query_reg_ecr_ec2(data, affiliates):
    """REG via ecr_ec2.tbl_ecr — camada raw/bronze (sem filtro test_user)."""
    return f"""
    SELECT COUNT(*) AS reg_ecr
    FROM ecr_ec2.tbl_ecr
    WHERE CAST(c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {affiliates}
    """


def query_ftd_cashier(data, affiliates):
    """FTD cross-validation via cashier_ec2 (camada raw).
    Conta jogadores que registraram no dia E fizeram deposito confirmado no mesmo dia."""
    return f"""
    WITH regs AS (
        SELECT c_ecr_id
        FROM ecr_ec2.tbl_ecr
        WHERE CAST(c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
          AND CAST(c_affiliate_id AS VARCHAR) IN {affiliates}
    )
    SELECT COUNT(DISTINCT d.c_ecr_id) AS ftd_cashier
    FROM cashier_ec2.tbl_cashier_deposit d
    JOIN regs r ON d.c_ecr_id = r.c_ecr_id
    WHERE d.c_txn_status = 'txn_confirmed_success'
      AND CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
    """


def query_dep_cashier(data, affiliates):
    """Dep Amount cross-validation via cashier_ec2 (camada raw, centavos /100).
    Total de depositos confirmados dos jogadores dos affiliates no dia."""
    return f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {affiliates}
          AND is_test = false
    )
    SELECT
        COALESCE(SUM(d.c_confirmed_amount_in_ecr_ccy), 0) / 100.0 AS dep_cashier
    FROM cashier_ec2.tbl_cashier_deposit d
    JOIN base_players p ON d.c_ecr_id = p.ecr_id
    WHERE d.c_txn_status = 'txn_confirmed_success'
      AND CAST(d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{data}'
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
    modo = "INTRADAY (parcial)" if INTRADAY else "DIA FECHADO (D-1)"
    hora_brt = datetime.now(BRT).strftime("%H:%M BRT")

    print(f"\n{'='*60}")
    print(f"EXTRACAO {cfg['label'].upper()} — {DATA}")
    print(f"Modo: {modo}" + (f" | Atualizado as {hora_brt}" if INTRADAY else ""))
    print(f"Affiliates: {cfg['ids_display']}")
    print(f"Fonte: 100% Athena (camadas BI + raw/bronze)")
    print(f"{'='*60}")

    # -----------------------------------------------------------------
    # REG — primario: bireports (D-1) ou ecr_ec2 (intraday)
    # CV: a outra camada
    # -----------------------------------------------------------------
    if INTRADAY:
        # Intraday: ecr_ec2 como primario (mais atualizado)
        df_ecr = query_athena(query_reg_ecr_ec2(DATA, cfg["affiliates"]), database="ecr_ec2")
        reg = int(df_ecr["reg_ecr"].iloc[0])
        reg_ecr = reg
        reg_fonte = "ecr_ec2 (raw)"
        try:
            df_bi = query_athena(query_reg_bireports(DATA, cfg["affiliates"]), database="bireports_ec2")
            reg_bi = int(df_bi["reg"].iloc[0])
        except Exception:
            reg_bi = "N/A"
    else:
        # D-1: bireports como primario (carga completa)
        df_bi = query_athena(query_reg_bireports(DATA, cfg["affiliates"]), database="bireports_ec2")
        reg = int(df_bi["reg"].iloc[0])
        reg_bi = reg
        reg_fonte = "bireports_ec2 (BI)"
        try:
            df_ecr = query_athena(query_reg_ecr_ec2(DATA, cfg["affiliates"]), database="ecr_ec2")
            reg_ecr = int(df_ecr["reg_ecr"].iloc[0])
        except Exception:
            reg_ecr = "N/A"

    # -----------------------------------------------------------------
    # FTD + FTD Deposit — primario: ps_bi | CV: cashier_ec2
    # -----------------------------------------------------------------
    df_ftd = query_athena(query_ftd_psbi(DATA, cfg["affiliates"]), database="bireports_ec2")
    ftd = int(df_ftd["ftd"].iloc[0])
    ftd_dep = float(df_ftd["ftd_dep"].iloc[0])
    ftd_fonte = "ps_bi.dim_user"
    ftd_dep_fonte = "ps_bi.dim_user"

    # CV: cashier_ec2 (raw)
    try:
        df_ftd_cv = query_athena(query_ftd_cashier(DATA, cfg["affiliates"]), database="cashier_ec2")
        ftd_cashier = int(df_ftd_cv["ftd_cashier"].iloc[0])
    except Exception as e:
        ftd_cashier = f"N/A ({e.__class__.__name__})"

    # -----------------------------------------------------------------
    # Financeiro: Dep, Saques, GGR, Bonus, NGR — bireports
    # -----------------------------------------------------------------
    df3 = query_athena(query_financeiro(DATA, cfg["affiliates"]), database="ps_bi")
    dep = float(df3["dep_amount"].iloc[0])
    saq = float(df3["saques"].iloc[0])
    ggr_c = float(df3["ggr_cassino"].iloc[0])
    ggr_s = float(df3["ggr_sport"].iloc[0])
    bonus = float(df3["bonus_cost"].iloc[0])
    ngr = ggr_c + ggr_s - bonus
    net_dep = dep - saq
    pl = ngr  # P&L = NGR (GGR total - Bonus)

    # CV: cashier_ec2 para Dep Amount
    try:
        df_dep_cv = query_athena(query_dep_cashier(DATA, cfg["affiliates"]), database="cashier_ec2")
        dep_cashier = float(df_dep_cv["dep_cashier"].iloc[0])
    except Exception as e:
        dep_cashier = f"N/A ({e.__class__.__name__})"

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
    print(f"{'Dep Amount':<16} {fmt(dep):>14}  bireports_ec2")
    print(f"{'Saques':<16} {fmt(saq):>14}  bireports_ec2")
    print(f"{'Net Deposit':<16} {fmt(net_dep):>14}  Dep - Saques")
    print(f"{'GGR Cassino':<16} {fmt(ggr_c):>14}  bireports_ec2")
    print(f"{'GGR Sport':<16} {fmt(ggr_s):>14}  bireports_ec2")
    print(f"{'Bonus Cost':<16} {fmt(bonus):>14}  bireports_ec2")
    print(f"{'P&L (NGR)':<16} {fmt(pl):>14}  GGR - Bonus")

    # Validacao cruzada (Athena: BI vs raw/bronze)
    print(f"\nValidacao cruzada (BI vs raw/bronze):")

    # REG
    if INTRADAY:
        print(f"  REG: ecr_ec2={reg_ecr} (primario) | bireports={reg_bi} (delay ETL)")
    else:
        match_reg = ""
        if isinstance(reg_ecr, int):
            diff_reg = abs(reg - reg_ecr)
            match_reg = "OK" if diff_reg <= 5 else f"DIVERGE ({diff_reg})"
        print(f"  REG: bireports={reg_bi} | ecr_ec2={reg_ecr} ({match_reg})")

    # FTD
    match_ftd = ""
    if isinstance(ftd_cashier, int):
        diff_ftd = abs(ftd - ftd_cashier)
        match_ftd = "OK" if diff_ftd <= 5 else f"DIVERGE ({diff_ftd})"
    print(f"  FTD: ps_bi={ftd} | cashier_ec2={ftd_cashier} ({match_ftd})")

    # Dep
    if isinstance(dep_cashier, float):
        diff_dep = abs(dep - dep_cashier)
        pct = f"{(diff_dep / dep * 100):.1f}%" if dep > 0 else "N/A"
        match_dep = "OK" if dep == 0 or diff_dep < dep * 0.05 else f"DIVERGE ({pct})"
        print(f"  Dep: bireports={fmt(dep)} | cashier_ec2={fmt(dep_cashier)} ({match_dep})")
    else:
        print(f"  Dep: bireports={fmt(dep)} | cashier_ec2={dep_cashier}")

    if ftd > reg:
        print(f"  *** ALERTA: FTD ({ftd}) > REG ({reg}) — verificar logica!")
    print(f"  Test users: {test_users} confirmados (excluidos das queries)")

    if INTRADAY:
        print(f"\n  ** DADOS PARCIAIS — dia em andamento. Metricas podem ter delay ETL.")


def run():
    modo = "INTRADAY" if INTRADAY else "DIA FECHADO"
    print(f"\n*** MODO: {modo} — Data: {DATA} ***")
    print(f"*** Fonte: 100% Athena — BI (bireports/ps_bi) + raw/bronze (_ec2) ***")
    if INTRADAY:
        print(f"*** ATENCAO: Modo intraday sem BigQuery — possivel delay ETL ***")

    for canal, cfg in CANAIS.items():
        try:
            run_canal(canal, cfg)
        except Exception as e:
            print(f"\nERRO {cfg['label']}: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    run()
