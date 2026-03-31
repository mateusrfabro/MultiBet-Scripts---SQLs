"""
Investigacao: Divergencia FTD BigQuery (326) vs ps_bi (74) — Meta Ads
Data: 2026-03-31

OBJETIVO: Identificar causa raiz e recomendar fonte correta para FTD.

HIPOTESE PRINCIPAL:
A query BigQuery usa `acc_last_deposit_date IS NOT NULL` que verifica se o jogador
ja depositou ALGUMA VEZ, nao se o primeiro deposito foi no MESMO DIA do registro.
Isso infla o numero (326 inclui quem registrou hoje e depositou em qualquer data).
O ps_bi (74) verifica ftd_datetime same-day e deve ser mais preciso.

QUERIES DE DIAGNOSTICO:
1. BigQuery — FTD corrigido (same-day deposit)
2. BigQuery — Verificar campos de deposito disponiveis
3. BigQuery — Quebra: deposito same-day vs outro dia vs nunca depositou
4. Athena — Quantos REGs do dia estao no dim_user e com ftd_datetime
5. BigQuery — Verificar se existe campo acc_first_deposit_date
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\MultiBet")

from db.athena import query_athena
from db.bigquery import query_bigquery
import traceback

DATA = "2026-03-31"
AFFILIATES_BQ = "(532570, 532571, 464673)"
AFFILIATES_ATHENA = "('532570', '532571', '464673')"


def run():
    print("=" * 70)
    print(f"INVESTIGACAO DIVERGENCIA FTD — Meta Ads — {DATA}")
    print("BigQuery reportou 326 FTD | ps_bi reportou 74 FTD")
    print("=" * 70)

    # ---------------------------------------------------------------
    # ETAPA 1: Verificar campos de deposito disponiveis no BigQuery
    # ---------------------------------------------------------------
    print("\n--- ETAPA 1: Campos de deposito disponiveis no BigQuery ---")
    try:
        sql_campos = """
        SELECT
            column_name,
            data_type
        FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'j_user'
          AND (
            LOWER(column_name) LIKE '%deposit%'
            OR LOWER(column_name) LIKE '%ftd%'
            OR LOWER(column_name) LIKE '%first%dep%'
          )
        ORDER BY column_name
        """
        df = query_bigquery(sql_campos)
        print(f"\nColunas relacionadas a deposito em j_user:")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA 2: Query BigQuery ORIGINAL (reproduzir o 326)
    # ---------------------------------------------------------------
    print("\n--- ETAPA 2: Query BigQuery ORIGINAL (deve retornar ~326) ---")
    try:
        sql_original = f"""
        SELECT COUNT(DISTINCT user_ext_id) AS ftd_bq_original
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
          AND acc_last_deposit_date IS NOT NULL
        """
        df = query_bigquery(sql_original)
        print(f"FTD BigQuery (query original): {df['ftd_bq_original'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA 3: Query BigQuery CORRIGIDA — same-day deposit
    # ---------------------------------------------------------------
    print("\n--- ETAPA 3: BigQuery CORRIGIDO — acc_last_deposit_date same-day ---")
    try:
        sql_corrigido = f"""
        SELECT COUNT(DISTINCT user_ext_id) AS ftd_bq_sameday
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
          AND DATE(acc_last_deposit_date, "America/Sao_Paulo") = '{DATA}'
        """
        df = query_bigquery(sql_corrigido)
        print(f"FTD BigQuery (same-day acc_last_deposit): {df['ftd_bq_sameday'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA 4: BigQuery — Verificar se existe acc_first_deposit_date
    # ---------------------------------------------------------------
    print("\n--- ETAPA 4: BigQuery — Testar campo acc_first_deposit_date ---")
    try:
        # Primeiro: ver se o campo existe tentando uma query com LIMIT 1
        sql_first = f"""
        SELECT
            user_ext_id,
            core_registration_date,
            acc_first_deposit_date,
            acc_last_deposit_date
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
        LIMIT 5
        """
        df = query_bigquery(sql_first)
        print("Campo acc_first_deposit_date EXISTE!")
        print(df.to_string(index=False))

        # Se existe, contar FTD usando first_deposit same-day
        sql_ftd_first = f"""
        SELECT COUNT(DISTINCT user_ext_id) AS ftd_bq_first_deposit_sameday
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
          AND DATE(acc_first_deposit_date, "America/Sao_Paulo") = '{DATA}'
        """
        df2 = query_bigquery(sql_ftd_first)
        print(f"\nFTD BigQuery (same-day acc_FIRST_deposit): {df2['ftd_bq_first_deposit_sameday'].iloc[0]}")

    except Exception as e:
        print(f"Campo acc_first_deposit_date NAO existe ou erro: {e}")

    # ---------------------------------------------------------------
    # ETAPA 5: BigQuery — Quebra por categoria de deposito
    # ---------------------------------------------------------------
    print("\n--- ETAPA 5: BigQuery — Quebra REGs do dia por status de deposito ---")
    try:
        sql_quebra = f"""
        SELECT
            COUNT(DISTINCT user_ext_id) AS total_regs,
            COUNT(DISTINCT CASE
                WHEN acc_last_deposit_date IS NULL THEN user_ext_id
            END) AS sem_deposito,
            COUNT(DISTINCT CASE
                WHEN acc_last_deposit_date IS NOT NULL
                AND DATE(acc_last_deposit_date, "America/Sao_Paulo") = '{DATA}'
                THEN user_ext_id
            END) AS deposito_mesmo_dia,
            COUNT(DISTINCT CASE
                WHEN acc_last_deposit_date IS NOT NULL
                AND DATE(acc_last_deposit_date, "America/Sao_Paulo") < '{DATA}'
                THEN user_ext_id
            END) AS deposito_dia_anterior,
            COUNT(DISTINCT CASE
                WHEN acc_last_deposit_date IS NOT NULL
                AND DATE(acc_last_deposit_date, "America/Sao_Paulo") > '{DATA}'
                THEN user_ext_id
            END) AS deposito_dia_futuro
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
        """
        df = query_bigquery(sql_quebra)
        print(f"Total REGs do dia: {df['total_regs'].iloc[0]}")
        print(f"  Sem deposito: {df['sem_deposito'].iloc[0]}")
        print(f"  Deposito MESMO dia: {df['deposito_mesmo_dia'].iloc[0]}")
        print(f"  Deposito dia ANTERIOR: {df['deposito_dia_anterior'].iloc[0]}")
        print(f"  Deposito dia FUTURO: {df['deposito_dia_futuro'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA 6: Athena — Diagnostico dim_user para REGs do dia
    # ---------------------------------------------------------------
    print("\n--- ETAPA 6: Athena — REGs do dia no dim_user (ps_bi) ---")
    try:
        sql_dimuser = f"""
        -- Quantos REGs do dia estao no dim_user?
        WITH regs AS (
            SELECT c_ecr_id
            FROM bireports_ec2.tbl_ecr
            WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
              AND CAST(c_affiliate_id AS VARCHAR) IN {AFFILIATES_ATHENA}
              AND c_test_user = false
        )
        SELECT
            COUNT(*) AS total_regs,
            COUNT(u.ecr_id) AS regs_com_match_dimuser,
            COUNT(u.ftd_datetime) AS regs_com_ftd_datetime,
            COUNT(CASE WHEN CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}' THEN 1 END) AS ftd_same_day,
            COUNT(CASE WHEN CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) < DATE '{DATA}' THEN 1 END) AS ftd_dia_anterior,
            COUNT(CASE WHEN u.ftd_datetime IS NULL THEN 1 END) AS sem_ftd_datetime
        FROM regs r
        LEFT JOIN ps_bi.dim_user u ON r.c_ecr_id = u.ecr_id
        """
        df = query_athena(sql_dimuser, database="bireports_ec2")
        print(f"Total REGs (bireports_ec2): {df['total_regs'].iloc[0]}")
        print(f"  Com match no dim_user: {df['regs_com_match_dimuser'].iloc[0]}")
        print(f"  Com ftd_datetime preenchido: {df['regs_com_ftd_datetime'].iloc[0]}")
        print(f"  FTD same-day (= {DATA}): {df['ftd_same_day'].iloc[0]}")
        print(f"  FTD dia anterior: {df['ftd_dia_anterior'].iloc[0]}")
        print(f"  Sem ftd_datetime (NULL): {df['sem_ftd_datetime'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA 7: Athena — REG count via ecr_ec2 (fonte intraday)
    # ---------------------------------------------------------------
    print("\n--- ETAPA 7: Athena — REG via ecr_ec2 (intraday) ---")
    try:
        sql_ecr = f"""
        SELECT COUNT(*) AS reg_ecr
        FROM ecr_ec2.tbl_ecr
        WHERE CAST(c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
          AND CAST(c_affiliate_id AS VARCHAR) IN {AFFILIATES_ATHENA}
        """
        df = query_athena(sql_ecr, database="ecr_ec2")
        print(f"REG ecr_ec2 (sem filtro test_user): {df['reg_ecr'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA 8: BigQuery — Verificar se acc_first_deposit_date difere de acc_last_deposit_date
    # ---------------------------------------------------------------
    print("\n--- ETAPA 8: BigQuery — Comparar first vs last deposit para REGs do dia ---")
    try:
        sql_compare = f"""
        SELECT
            COUNT(DISTINCT user_ext_id) AS total_com_deposito,
            COUNT(DISTINCT CASE
                WHEN acc_first_deposit_date IS NOT NULL
                AND acc_last_deposit_date IS NOT NULL
                AND DATE(acc_first_deposit_date) = DATE(acc_last_deposit_date)
                THEN user_ext_id
            END) AS first_eq_last,
            COUNT(DISTINCT CASE
                WHEN acc_first_deposit_date IS NOT NULL
                AND acc_last_deposit_date IS NOT NULL
                AND DATE(acc_first_deposit_date) != DATE(acc_last_deposit_date)
                THEN user_ext_id
            END) AS first_diff_last,
            COUNT(DISTINCT CASE
                WHEN acc_first_deposit_date IS NULL
                AND acc_last_deposit_date IS NOT NULL
                THEN user_ext_id
            END) AS has_last_no_first
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
          AND acc_last_deposit_date IS NOT NULL
        """
        df = query_bigquery(sql_compare)
        print(f"Total com deposito: {df['total_com_deposito'].iloc[0]}")
        print(f"  first_deposit = last_deposit (mesma data): {df['first_eq_last'].iloc[0]}")
        print(f"  first_deposit != last_deposit: {df['first_diff_last'].iloc[0]}")
        print(f"  Tem last_deposit mas NAO tem first_deposit: {df['has_last_no_first'].iloc[0]}")
    except Exception as e:
        # Se acc_first_deposit_date nao existir, pular
        print(f"ERRO (acc_first_deposit_date pode nao existir): {e}")

    # ---------------------------------------------------------------
    # RESUMO E RECOMENDACAO
    # ---------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RESUMO DA INVESTIGACAO")
    print("=" * 70)
    print("""
CAUSA RAIZ PROVAVEL:
A query BigQuery original usa `acc_last_deposit_date IS NOT NULL` que
conta QUALQUER jogador que ja depositou, nao apenas os que fizeram o
PRIMEIRO deposito no MESMO dia do registro.

DEFINICAO CORRETA DE FTD (same-day conversion):
FTD = jogador que se REGISTROU no dia E fez o PRIMEIRO deposito no MESMO dia.

CORRECOES NECESSARIAS:
1. Se `acc_first_deposit_date` existe no BigQuery:
   Usar `DATE(acc_first_deposit_date, "America/Sao_Paulo") = '{DATA}'`

2. Se apenas `acc_last_deposit_date` esta disponivel:
   Usar `DATE(acc_last_deposit_date, "America/Sao_Paulo") = '{DATA}'`
   (menos preciso — conta quem depositou no dia, nao necessariamente o primeiro)

3. Para reports D-1 (consolidados):
   ps_bi.dim_user.ftd_datetime e a fonte mais confiavel (verifica primeiro deposito)

RECOMENDACAO:
- Intraday: BigQuery com acc_first_deposit_date (se existir) same-day
- D-1 consolidado: ps_bi.dim_user.ftd_datetime (fonte canonica)
- Divergencia esperada: ate 5% (delay de sincronizacao entre fontes)
- NUNCA usar `acc_last_deposit_date IS NOT NULL` sem filtro de data!
""")


if __name__ == "__main__":
    run()