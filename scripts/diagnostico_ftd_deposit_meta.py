"""
Diagnostico FTD Deposit Meta Ads — 31/03/2026
Investiga:
1. Freshness do bireports
2. FTD Deposit via BigQuery (real-time)
3. Depositos totais por affiliate
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\MultiBet")

from db.athena import query_athena
from db.bigquery import query_bigquery
import traceback

DATA = "2026-03-31"
AFFILIATES_ATHENA = "('532570', '532571', '464673')"
AFFILIATES_BQ = "(532570, 532571, 464673)"


def check_freshness():
    print("=" * 60)
    print("1. FRESHNESS BIREPORTS (bi_summary)")
    print("=" * 60)
    sql = f"""
    SELECT
        MAX(c_created_date) AS ultimo_date,
        COUNT(*) AS total_registros_31mar
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
    WHERE c_created_date = DATE '{DATA}'
    """
    df = query_athena(sql, database="bireports_ec2")
    print(df.to_string())

    print("\n" + "=" * 60)
    print("2. FRESHNESS tbl_ecr (ultimo signup BRT)")
    print("=" * 60)
    sql2 = f"""
    SELECT
        MAX(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_signup_brt,
        COUNT(*) AS total_regs_31mar
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
    """
    df2 = query_athena(sql2, database="bireports_ec2")
    print(df2.to_string())


def check_psbi_diagnostic():
    print("\n" + "=" * 60)
    print("3. DIAGNOSTICO ps_bi.dim_user (cobertura REGs do dia)")
    print("=" * 60)
    sql = f"""
    WITH regs AS (
        SELECT c_ecr_id
        FROM bireports_ec2.tbl_ecr
        WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
          AND CAST(c_affiliate_id AS VARCHAR) IN {AFFILIATES_ATHENA}
          AND c_test_user = false
    )
    SELECT
        COUNT(*) AS total_regs,
        COUNT(u.ecr_id) AS match_dimuser,
        COUNT(u.ftd_datetime) AS com_ftd,
        COUNT(CASE WHEN CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}' THEN 1 END) AS ftd_same_day,
        COALESCE(SUM(CASE WHEN CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}' THEN u.ftd_amount_inhouse END), 0) AS ftd_dep_psbi
    FROM regs r
    LEFT JOIN ps_bi.dim_user u ON r.c_ecr_id = u.ecr_id
    """
    df = query_athena(sql, database="bireports_ec2")
    print(df.to_string())


def check_bigquery_deposits():
    print("\n" + "=" * 60)
    print("4. FTD DEPOSIT via BigQuery (tr_acc_deposit_approved)")
    print("=" * 60)
    sql = f"""
    SELECT
        COUNT(DISTINCT j.user_ext_id) AS ftd_count,
        ROUND(SUM(t.amount), 2) AS ftd_deposit_total
    FROM `smartico-bq6.dwh_ext_24105.j_user` j
    JOIN `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` t
        ON j.user_ext_id = t.user_ext_id
    WHERE DATE(j.core_registration_date, 'America/Sao_Paulo') = '{DATA}'
      AND j.core_affiliate_id IN {AFFILIATES_BQ}
      AND DATE(t.event_date, 'America/Sao_Paulo') = '{DATA}'
    """
    df = query_bigquery(sql)
    print(df.to_string())

    print("\n" + "=" * 60)
    print("5. DEPOSITOS TOTAIS Meta 31/03 (todos jogadores, nao so FTD)")
    print("=" * 60)
    sql2 = f"""
    SELECT
        COUNT(DISTINCT t.user_ext_id) AS depositantes_unicos,
        COUNT(*) AS total_depositos,
        ROUND(SUM(t.amount), 2) AS total_deposited
    FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` t
    JOIN `smartico-bq6.dwh_ext_24105.j_user` j ON t.user_ext_id = j.user_ext_id
    WHERE j.core_affiliate_id IN {AFFILIATES_BQ}
      AND DATE(t.event_date, 'America/Sao_Paulo') = '{DATA}'
    """
    df2 = query_bigquery(sql2)
    print(df2.to_string())

    print("\n" + "=" * 60)
    print("6. DEPOSITOS POR AFFILIATE Meta 31/03")
    print("=" * 60)
    sql3 = f"""
    SELECT
        j.core_affiliate_id,
        COUNT(DISTINCT t.user_ext_id) AS depositantes,
        COUNT(*) AS qtd_depositos,
        ROUND(SUM(t.amount), 2) AS total_dep
    FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` t
    JOIN `smartico-bq6.dwh_ext_24105.j_user` j ON t.user_ext_id = j.user_ext_id
    WHERE j.core_affiliate_id IN {AFFILIATES_BQ}
      AND DATE(t.event_date, 'America/Sao_Paulo') = '{DATA}'
    GROUP BY j.core_affiliate_id
    ORDER BY total_dep DESC
    """
    df3 = query_bigquery(sql3)
    print(df3.to_string())


def check_dep_bireports():
    print("\n" + "=" * 60)
    print("7. DEP AMOUNT via bireports (comparar com BigQuery)")
    print("=" * 60)
    sql = f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {AFFILIATES_ATHENA}
          AND is_test = false
    )
    SELECT
        COUNT(DISTINCT p.ecr_id) AS jogadores_com_movimento,
        COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS dep_amount_bireports,
        COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS saques_bireports
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN base_players p ON s.c_ecr_id = p.ecr_id
    WHERE s.c_created_date = DATE '{DATA}'
    """
    df = query_athena(sql, database="bireports_ec2")
    print(df.to_string())


if __name__ == "__main__":
    try:
        check_freshness()
    except Exception as e:
        print(f"ERRO freshness: {e}")
        traceback.print_exc()

    try:
        check_psbi_diagnostic()
    except Exception as e:
        print(f"ERRO psbi: {e}")
        traceback.print_exc()

    try:
        check_bigquery_deposits()
    except Exception as e:
        print(f"ERRO bigquery: {e}")
        traceback.print_exc()

    try:
        check_dep_bireports()
    except Exception as e:
        print(f"ERRO dep bireports: {e}")
        traceback.print_exc()
