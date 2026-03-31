"""
Investigação de dados para Report CRM Diário — 3 fontes em paralelo
BigQuery (Smartico) + Athena (Data Lake) + Super Nova DB (PostgreSQL)

Objetivo: mapear EXATAMENTE o que temos e o que NÃO temos para cada bloco da task.
"""

import sys
import os
import json
import traceback
from datetime import datetime

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")

# ============================================================
# HELPERS
# ============================================================
def safe_query(func, sql, label, **kwargs):
    """Executa query com tratamento de erro."""
    print(f"\n{'='*60}")
    print(f"[{label}]")
    print(f"{'='*60}")
    try:
        result = func(sql, **kwargs)
        if hasattr(result, 'to_string'):
            print(f"Linhas: {len(result)}")
            print(result.to_string(max_rows=20, max_colwidth=60))
        elif isinstance(result, list):
            print(f"Registros: {len(result)}")
            for r in result[:10]:
                print(r)
        else:
            print(result)
        return result
    except Exception as e:
        print(f"ERRO: {e}")
        traceback.print_exc()
        return None


# ============================================================
# PARTE 1: BIGQUERY (SMARTICO CRM)
# ============================================================
def investigar_bigquery():
    print("\n" + "#"*70)
    print("# PARTE 1: BIGQUERY (SMARTICO CRM)")
    print("#"*70)

    from db.bigquery import query_bigquery
    DS = "`smartico-bq6.dwh_ext_24105`"

    # 1a. Listar TODAS as views/tabelas disponíveis
    safe_query(query_bigquery, f"""
    SELECT table_name, table_type, row_count, size_bytes
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.TABLES`
    ORDER BY table_name
    """, "BQ: Tabelas disponíveis no dataset")

    # 1b. j_communication — funil CRM
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'j_communication'
    ORDER BY ordinal_position
    """, "BQ: Schema j_communication")

    safe_query(query_bigquery, f"""
    SELECT activity_type_id, COUNT(*) AS qty,
           MIN(fact_date) AS min_date, MAX(fact_date) AS max_date
    FROM {DS}.j_communication
    WHERE fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY activity_type_id
    ORDER BY qty DESC
    """, "BQ: j_communication — activity_type_id (últimos 30 dias)")

    safe_query(query_bigquery, f"""
    SELECT fact_type_id, COUNT(*) AS qty
    FROM {DS}.j_communication
    WHERE fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY fact_type_id
    ORDER BY fact_type_id
    """, "BQ: j_communication — fact_type_id (1-5)")

    safe_query(query_bigquery, f"""
    SELECT label_provider_id, COUNT(*) AS qty
    FROM {DS}.j_communication
    WHERE fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
      AND label_provider_id IS NOT NULL
    GROUP BY label_provider_id
    ORDER BY qty DESC
    LIMIT 20
    """, "BQ: j_communication — label_provider_id (custos)")

    # 1c. j_bonuses — bônus e campanhas
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'j_bonuses'
    ORDER BY ordinal_position
    """, "BQ: Schema j_bonuses")

    safe_query(query_bigquery, f"""
    SELECT bonus_status_id, COUNT(*) AS qty,
           COUNT(DISTINCT entity_id) AS entities,
           COUNT(DISTINCT user_ext_id) AS users,
           SUM(CAST(bonus_cost_value AS FLOAT64)) AS total_cost
    FROM {DS}.j_bonuses
    WHERE fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY bonus_status_id
    ORDER BY bonus_status_id
    """, "BQ: j_bonuses — status + custo (últimos 30 dias)")

    safe_query(query_bigquery, f"""
    SELECT entity_id,
           COUNT(*) AS qty,
           COUNT(DISTINCT user_ext_id) AS users,
           MIN(fact_date) AS min_date,
           MAX(fact_date) AS max_date
    FROM {DS}.j_bonuses
    WHERE fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
      AND bonus_status_id = 3
    GROUP BY entity_id
    ORDER BY qty DESC
    LIMIT 30
    """, "BQ: j_bonuses — top entity_ids com claims (últimos 30 dias)")

    # 1d. j_automation_rule_progress — progresso de quests
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'j_automation_rule_progress'
    ORDER BY ordinal_position
    """, "BQ: Schema j_automation_rule_progress")

    safe_query(query_bigquery, f"""
    SELECT COUNT(*) AS total
    FROM {DS}.j_automation_rule_progress
    WHERE fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    """, "BQ: j_automation_rule_progress — volume 30 dias")

    # 1e. dm_bonus_template — nomes e tipos de templates
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'dm_bonus_template'
    ORDER BY ordinal_position
    """, "BQ: Schema dm_bonus_template")

    safe_query(query_bigquery, f"""
    SELECT * FROM {DS}.dm_bonus_template LIMIT 10
    """, "BQ: dm_bonus_template — amostra")

    # 1f. j_user — dados de segmentação do jogador
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'j_user'
    ORDER BY ordinal_position
    """, "BQ: Schema j_user")

    safe_query(query_bigquery, f"""
    SELECT
        COUNT(*) AS total_users,
        COUNT(core_level_id) AS has_level,
        COUNT(DISTINCT core_level_id) AS distinct_levels,
        COUNT(core_segment_id) AS has_segment,
        COUNT(DISTINCT core_segment_id) AS distinct_segments
    FROM {DS}.j_user
    """, "BQ: j_user — campos de segmentação")

    # 1g. dm_automation_rule — regras de automação (campanhas)
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'dm_automation_rule'
    ORDER BY ordinal_position
    """, "BQ: Schema dm_automation_rule")

    safe_query(query_bigquery, f"""
    SELECT * FROM {DS}.dm_automation_rule LIMIT 10
    """, "BQ: dm_automation_rule — amostra")

    # 1h. dm_segment — segmentos
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'dm_segment'
    ORDER BY ordinal_position
    """, "BQ: Schema dm_segment")

    safe_query(query_bigquery, f"""
    SELECT * FROM {DS}.dm_segment LIMIT 20
    """, "BQ: dm_segment — amostra")

    # 1i. dm_level — níveis VIP
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'dm_level'
    ORDER BY ordinal_position
    """, "BQ: Schema dm_level")

    safe_query(query_bigquery, f"""
    SELECT * FROM {DS}.dm_level LIMIT 20
    """, "BQ: dm_level — amostra (VIP tiers)")

    # 1j. dm_providers_sms — custos de SMS
    safe_query(query_bigquery, f"""
    SELECT table_name
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '%provider%' OR table_name LIKE '%sms%' OR table_name LIKE '%channel%'
    """, "BQ: Tabelas relacionadas a providers/SMS/canal")

    # 1k. Opt-in — como identificar
    safe_query(query_bigquery, f"""
    SELECT column_name, data_type
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'j_user'
      AND (column_name LIKE '%opt%' OR column_name LIKE '%consent%' OR column_name LIKE '%subscribe%'
           OR column_name LIKE '%notification%' OR column_name LIKE '%push%')
    """, "BQ: j_user — campos de opt-in/consent")

    # 1l. Metas de campanha — tabelas possíveis
    safe_query(query_bigquery, f"""
    SELECT table_name
    FROM `smartico-bq6.dwh_ext_24105.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '%goal%' OR table_name LIKE '%target%' OR table_name LIKE '%kpi%' OR table_name LIKE '%meta%'
    """, "BQ: Tabelas de metas/goals")


# ============================================================
# PARTE 2: ATHENA (DATA LAKE)
# ============================================================
def investigar_athena():
    print("\n" + "#"*70)
    print("# PARTE 2: ATHENA (DATA LAKE)")
    print("#"*70)

    from db.athena import query_athena

    # 2a. ps_bi.fct_player_activity_daily — colunas
    safe_query(query_athena, """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'ps_bi' AND table_name = 'fct_player_activity_daily'
    ORDER BY ordinal_position
    """, "Athena: Schema ps_bi.fct_player_activity_daily", database="ps_bi")

    # 2b. ps_bi.fct_player_activity_daily — amostra recente
    safe_query(query_athena, """
    SELECT activity_date, COUNT(*) AS rows, COUNT(DISTINCT user_id) AS users
    FROM ps_bi.fct_player_activity_daily
    WHERE activity_date >= date_add('day', -7, current_date)
    GROUP BY activity_date
    ORDER BY activity_date DESC
    """, "Athena: ps_bi.fct_player_activity_daily — volume 7 dias", database="ps_bi")

    # 2c. ps_bi.fct_casino_activity_daily — colunas
    safe_query(query_athena, """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'ps_bi' AND table_name = 'fct_casino_activity_daily'
    ORDER BY ordinal_position
    """, "Athena: Schema ps_bi.fct_casino_activity_daily", database="ps_bi")

    # 2d. ps_bi.dim_user — campos de segmentação
    safe_query(query_athena, """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'ps_bi' AND table_name = 'dim_user'
    ORDER BY ordinal_position
    """, "Athena: Schema ps_bi.dim_user", database="ps_bi")

    # 2e. ps_bi.dim_user — VIP / segmentação
    safe_query(query_athena, """
    SELECT
        COUNT(*) AS total,
        COUNT(DISTINCT vip_level) AS vip_levels,
        COUNT(DISTINCT affiliate_id) AS affiliates,
        COUNT(external_id) AS has_external_id,
        COUNT(ftd_date) AS has_ftd
    FROM ps_bi.dim_user
    WHERE is_test = false
    """, "Athena: ps_bi.dim_user — campos VIP/segmentação", database="ps_bi")

    # 2f. ps_bi.dim_game — cobertura
    safe_query(query_athena, """
    SELECT COUNT(*) AS total_games, COUNT(DISTINCT vendor_name) AS vendors
    FROM ps_bi.dim_game
    """, "Athena: ps_bi.dim_game — cobertura", database="ps_bi")

    # 2g. bireports_ec2.tbl_ecr_wise_daily_bi_summary — top colunas GGR
    safe_query(query_athena, """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'bireports_ec2' AND table_name = 'tbl_ecr_wise_daily_bi_summary'
      AND (column_name LIKE '%ggr%' OR column_name LIKE '%bet%' OR column_name LIKE '%win%'
           OR column_name LIKE '%deposit%' OR column_name LIKE '%session%'
           OR column_name LIKE '%login%' OR column_name LIKE '%turnover%')
    ORDER BY ordinal_position
    """, "Athena: bireports_ec2 daily_bi_summary — colunas financeiras", database="bireports_ec2")

    # 2h. bonus_ec2 — tabelas de bônus disponíveis
    safe_query(query_athena, """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'bonus_ec2'
    ORDER BY table_name
    """, "Athena: bonus_ec2 — tabelas disponíveis", database="bonus_ec2")


# ============================================================
# PARTE 3: SUPER NOVA DB (POSTGRESQL)
# ============================================================
def investigar_supernova():
    print("\n" + "#"*70)
    print("# PARTE 3: SUPER NOVA DB (POSTGRESQL)")
    print("#"*70)

    from db.supernova import execute_supernova

    # 3a. Schemas disponíveis
    safe_query(execute_supernova, """
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    ORDER BY schema_name
    """, "SuperNova: Schemas disponíveis", fetch=True)

    # 3b. Tabelas no schema multibet
    safe_query(execute_supernova, """
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'multibet'
    ORDER BY table_name
    """, "SuperNova: Tabelas schema multibet", fetch=True)

    # 3c. Para cada tabela multibet — contagem
    result = execute_supernova("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'multibet' AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """, fetch=True)
    if result:
        for (tbl,) in result:
            safe_query(execute_supernova,
                f"SELECT COUNT(*) AS cnt FROM multibet.{tbl}",
                f"SuperNova: multibet.{tbl} — contagem", fetch=True)

    # 3d. Schemas com tabelas bronze (public, bronze, staging, raw)
    for schema in ['public', 'bronze', 'staging', 'raw', 'silver', 'gold']:
        safe_query(execute_supernova, f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        ORDER BY table_name
        LIMIT 30
        """, f"SuperNova: Tabelas schema '{schema}'", fetch=True)

    # 3e. fact_crm_daily_performance — estrutura
    safe_query(execute_supernova, """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'multibet' AND table_name = 'fact_crm_daily_performance'
    ORDER BY ordinal_position
    """, "SuperNova: Schema fact_crm_daily_performance", fetch=True)

    # 3f. fact_crm_daily_performance — dados existentes
    safe_query(execute_supernova, """
    SELECT campanha_id, campanha_name, period, period_start, period_end
    FROM multibet.fact_crm_daily_performance
    ORDER BY updated_at DESC
    LIMIT 20
    """, "SuperNova: fact_crm_daily_performance — dados existentes", fetch=True)


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print(f"Início: {datetime.now()}")
    print(f"Investigação CRM Report — 3 fontes de dados")

    try:
        investigar_bigquery()
    except Exception as e:
        print(f"\nERRO FATAL BigQuery: {e}")
        traceback.print_exc()

    try:
        investigar_athena()
    except Exception as e:
        print(f"\nERRO FATAL Athena: {e}")
        traceback.print_exc()

    try:
        investigar_supernova()
    except Exception as e:
        print(f"\nERRO FATAL Super Nova DB: {e}")
        traceback.print_exc()

    print(f"\nFim: {datetime.now()}")