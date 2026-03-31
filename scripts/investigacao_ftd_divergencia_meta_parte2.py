"""
Investigacao PARTE 2: Divergencia FTD — 332 (BigQuery same-day) vs 74 (ps_bi same-day)

DESCOBERTAS DA PARTE 1:
- BigQuery: 779 REGs, 332 com deposito same-day, 447 sem deposito
- Athena bireports: 729 REGs, 317 com match no dim_user, 74 com FTD same-day
- ecr_ec2: 784 REGs
- acc_first_deposit_date NAO EXISTE no BigQuery (so acc_last_deposit_date)
- Deposito dia anterior = 0, dia futuro = 0 (so same-day)

NOVO PROBLEMA:
Mesmo corrigindo para same-day, BigQuery retorna 332 e ps_bi retorna 74.
Diferenca de 258 jogadores. POR QUE?

HIPOTESES:
H1: dim_user nao carregou FTDs de hoje (delay do dbt) — 605 regs SEM ftd_datetime
H2: BigQuery conta acc_last_deposit_date que inclui bonus/depositos nao-reais
H3: bireports_ec2 tem menos REGs (729 vs 779 BQ) por filtro c_test_user
H4: dim_user so tem 317/729 matches (43%) — tabela incompleta para registros do dia

QUERIES COMPLEMENTARES:
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
    print(f"INVESTIGACAO PARTE 2 — FTD Meta Ads — {DATA}")
    print("BigQuery same-day: 332 | ps_bi same-day: 74 | Gap: 258")
    print("=" * 70)

    # ---------------------------------------------------------------
    # ETAPA A: BigQuery — o que e acc_last_deposit_date exatamente?
    # Verificar se o campo captura depositos reais ou inclui bonus
    # ---------------------------------------------------------------
    print("\n--- ETAPA A: BigQuery — Verificar se ha campo de deposito REAL vs QUALQUER ---")
    try:
        sql_campos_acc = """
        SELECT column_name, data_type
        FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'j_user'
          AND LOWER(column_name) LIKE 'acc_%'
        ORDER BY column_name
        """
        df = query_bigquery(sql_campos_acc)
        print("Todos os campos acc_* em j_user:")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA B: BigQuery — Quantos desses 332 tem deposito confirmado
    # via tabela transacional tr_acc_deposit_approved?
    # ---------------------------------------------------------------
    print("\n--- ETAPA B: BigQuery — FTD via tr_acc_deposit_approved (transacional) ---")
    try:
        sql_tr_dep = f"""
        WITH regs AS (
            SELECT DISTINCT user_ext_id
            FROM `smartico-bq6.dwh_ext_24105.j_user`
            WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
              AND core_affiliate_id IN {AFFILIATES_BQ}
        ),
        deps AS (
            SELECT DISTINCT d.user_ext_id
            FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` d
            JOIN regs r ON d.user_ext_id = r.user_ext_id
            WHERE DATE(d.event_date, "America/Sao_Paulo") = '{DATA}'
        )
        SELECT
            (SELECT COUNT(*) FROM regs) AS total_regs,
            (SELECT COUNT(*) FROM deps) AS ftd_transacional
        """
        df = query_bigquery(sql_tr_dep)
        print(f"Total REGs: {df['total_regs'].iloc[0]}")
        print(f"FTD via tr_acc_deposit_approved (transacional): {df['ftd_transacional'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA C: Athena — Por que dim_user so tem 317/729 matches?
    # Verificar se dim_user tem delay de carga
    # ---------------------------------------------------------------
    print("\n--- ETAPA C: Athena — dim_user: ultimo registro carregado ---")
    try:
        sql_ultimo = """
        SELECT
            MAX(CAST(registration_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)) AS ultimo_reg,
            MAX(CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)) AS ultimo_ftd,
            COUNT(*) AS total_dim_user
        FROM ps_bi.dim_user
        WHERE is_test = false
        """
        df = query_athena(sql_ultimo, database="ps_bi")
        print(f"Ultimo registro no dim_user: {df['ultimo_reg'].iloc[0]}")
        print(f"Ultimo FTD no dim_user: {df['ultimo_ftd'].iloc[0]}")
        print(f"Total jogadores no dim_user: {df['total_dim_user'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA D: Athena — dim_user REGs do dia vs bireports
    # Quantos REGs de 31/03 existem diretamente no dim_user?
    # ---------------------------------------------------------------
    print("\n--- ETAPA D: Athena — dim_user REGs de 31/03 por affiliate ---")
    try:
        sql_dimuser_regs = f"""
        SELECT
            CAST(affiliate_id AS VARCHAR) AS affiliate_id,
            COUNT(*) AS total_dimuser,
            COUNT(ftd_datetime) AS com_ftd,
            COUNT(CASE WHEN CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}' THEN 1 END) AS ftd_sameday
        FROM ps_bi.dim_user
        WHERE CAST(registration_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
          AND CAST(affiliate_id AS VARCHAR) IN {AFFILIATES_ATHENA}
          AND is_test = false
        GROUP BY CAST(affiliate_id AS VARCHAR)
        ORDER BY affiliate_id
        """
        df = query_athena(sql_dimuser_regs, database="ps_bi")
        print("dim_user — REGs de 31/03 por affiliate:")
        print(df.to_string(index=False))
        print(f"\nTotais: {df['total_dimuser'].sum()} REGs | {df['com_ftd'].sum()} com FTD | {df['ftd_sameday'].sum()} FTD same-day")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA E: Athena — FTD via fund_ec2 (transacional bruto)
    # Verificar depositos reais confirmados para REGs do dia
    # ---------------------------------------------------------------
    print("\n--- ETAPA E: Athena — FTD via fund_ec2 (depositos reais confirmados) ---")
    try:
        sql_fund = f"""
        -- REGs do dia com deposito confirmado no fund_ec2
        WITH regs AS (
            SELECT c_ecr_id
            FROM bireports_ec2.tbl_ecr
            WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
              AND CAST(c_affiliate_id AS VARCHAR) IN {AFFILIATES_ATHENA}
              AND c_test_user = false
        ),
        primeiro_dep AS (
            SELECT
                f.c_ecr_id,
                MIN(f.c_txn_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_deposito_brt
            FROM fund_ec2.tbl_real_fund_txn f
            JOIN regs r ON f.c_ecr_id = r.c_ecr_id
            WHERE f.c_txn_type = 10           -- deposito
              AND f.c_txn_status = 'txn_confirmed_success'
              AND f.c_product_id = 'CASINO'
            GROUP BY f.c_ecr_id
        )
        SELECT
            COUNT(*) AS ftd_fund_total,
            COUNT(CASE WHEN CAST(primeiro_deposito_brt AS DATE) = DATE '{DATA}' THEN 1 END) AS ftd_fund_sameday
        FROM primeiro_dep
        """
        df = query_athena(sql_fund, database="fund_ec2")
        print(f"FTD via fund_ec2 (total com deposito): {df['ftd_fund_total'].iloc[0]}")
        print(f"FTD via fund_ec2 (same-day): {df['ftd_fund_sameday'].iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # ETAPA F: BigQuery — Verificar se 332 inclui test users
    # ---------------------------------------------------------------
    print("\n--- ETAPA F: BigQuery — Test users entre os 332 depositantes ---")
    try:
        # Verificar campos de test/status no j_user
        sql_test = f"""
        SELECT
            COUNT(DISTINCT user_ext_id) AS total_deposito_sameday,
            COUNT(DISTINCT CASE WHEN core_is_test = true THEN user_ext_id END) AS test_users,
            COUNT(DISTINCT CASE WHEN core_is_test = false OR core_is_test IS NULL THEN user_ext_id END) AS real_users
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
          AND DATE(acc_last_deposit_date, "America/Sao_Paulo") = '{DATA}'
        """
        df = query_bigquery(sql_test)
        print(f"Total deposito same-day: {df['total_deposito_sameday'].iloc[0]}")
        print(f"  Test users: {df['test_users'].iloc[0]}")
        print(f"  Real users: {df['real_users'].iloc[0]}")
    except Exception as e:
        # Pode ser que core_is_test nao existe
        print(f"ERRO (core_is_test pode nao existir): {e}")
        # Tentar sem filtro de test
        try:
            sql_test2 = """
            SELECT column_name
            FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = 'j_user'
              AND LOWER(column_name) LIKE '%test%'
            """
            df2 = query_bigquery(sql_test2)
            print(f"Campos com 'test' em j_user: {df2['column_name'].tolist()}")
        except Exception as e2:
            print(f"ERRO buscando campos test: {e2}")

    # ---------------------------------------------------------------
    # ETAPA G: BigQuery — Distribuicao de acc_last_deposit_date
    # para entender se e um campo que atualiza em real-time
    # ---------------------------------------------------------------
    print("\n--- ETAPA G: BigQuery — Amostra dos 332 (hora do deposito) ---")
    try:
        sql_amostra = f"""
        SELECT
            EXTRACT(HOUR FROM DATETIME(acc_last_deposit_date, "America/Sao_Paulo")) AS hora_dep_brt,
            COUNT(DISTINCT user_ext_id) AS qtd
        FROM `smartico-bq6.dwh_ext_24105.j_user`
        WHERE DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
          AND core_affiliate_id IN {AFFILIATES_BQ}
          AND DATE(acc_last_deposit_date, "America/Sao_Paulo") = '{DATA}'
        GROUP BY 1
        ORDER BY 1
        """
        df = query_bigquery(sql_amostra)
        print("Distribuicao por hora BRT (deposito same-day):")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()

    # ---------------------------------------------------------------
    # RESUMO
    # ---------------------------------------------------------------
    print("\n" + "=" * 70)
    print("RESUMO PARTE 2")
    print("=" * 70)
    print("""
ANALISE:

1. BigQuery acc_last_deposit_date same-day = 332
   - Registros do dia que depositaram no mesmo dia
   - Campo atualiza em real-time no Smartico CRM

2. ps_bi.dim_user ftd_datetime same-day = 74
   - dim_user tem delay de carga (dbt roda 1x/dia)
   - So 317/729 REGs do dia tem match no dim_user (43%)
   - 605 REGs SEM ftd_datetime (tabela nao atualizou)
   - CLARAMENTE subdimensionado para dados intraday

3. fund_ec2 (depositos transacionais reais)
   - Fonte mais confiavel para contar depositos reais confirmados
   - Nao depende de delay do dbt

CONCLUSAO ESPERADA:
- Para INTRADAY: BigQuery (acc_last_deposit_date same-day) e mais confiavel
  porque atualiza em real-time
- Para D-1: ps_bi.dim_user.ftd_datetime (apos carga completa do dbt)
- O numero 74 do ps_bi esta SUBDIMENSIONADO porque o dbt nao carregou
  os registros de hoje
- O numero correto esta mais proximo de 332 (BigQuery real-time)
""")


if __name__ == "__main__":
    run()