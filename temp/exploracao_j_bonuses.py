"""
Exploração da view j_bonuses (BigQuery / Smartico CRM).

Objetivo: identificar os entity_id mais frequentes dos últimos 30 dias
e descobrir qual evento marca o "Cumprimento da Condição" do bônus.

Colunas reais da j_bonuses (validado 2026-03-16):
  fact_date, bonus_id, label_id, user_id, user_ext_id, crm_brand_id,
  bonus_status_id, label_bonus_template_id, source_product_id,
  engagement_uid, redeem_date, bonus_meta, source_product_ref_id,
  activity_details, bonus_internal_meta, http_calls, root_audience_id,
  error_code, error_message, entity_id, bonus_cost_value

Uso:
    python exploracao_j_bonuses.py
"""

import sys
import logging

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
log = logging.getLogger(__name__)

DATASET = "`smartico-bq6.dwh_ext_24105`"


def explorar_entity_ids_distintos():
    """
    Query 1 — Todos os entity_id distintos com bonus_status_id e contagem.

    Mostra o mapa completo de entity_id × bonus_status_id para
    identificar o significado de cada combinação.
    """

    sql = f"""
    -- Mapa completo: entity_id × bonus_status_id (últimos 30 dias)
    SELECT
        b.entity_id,
        b.bonus_status_id,
        COUNT(*)                          AS total_registros,
        COUNT(DISTINCT b.user_ext_id)     AS jogadores_unicos,
        MIN(b.fact_date)                  AS primeira_ocorrencia,
        MAX(b.fact_date)                  AS ultima_ocorrencia
    FROM {DATASET}.j_bonuses b
    WHERE b.fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY b.entity_id, b.bonus_status_id
    ORDER BY total_registros DESC
    """

    log.info("Executando Query 1 — entity_id × bonus_status_id distintos...")
    df = query_bigquery(sql)
    log.info(f"Retornadas {len(df)} linhas.")

    print("\n" + "=" * 80)
    print("MAPA: entity_id × bonus_status_id (j_bonuses — últimos 30 dias)")
    print("=" * 80)
    print(df.to_string(index=False))
    return df


def explorar_top_entity_ids():
    """
    Query 2 — Top 10 entity_id mais frequentes, cruzando com
    dm_bonus_template para trazer o nome do template.
    """

    sql = f"""
    -- Top 10 entity_id com nome do template (últimos 30 dias)
    SELECT
        b.entity_id,
        b.bonus_status_id,
        b.label_bonus_template_id,
        t.label_name                      AS nome_template,
        COUNT(*)                          AS total_registros,
        COUNT(DISTINCT b.user_ext_id)     AS jogadores_unicos
    FROM {DATASET}.j_bonuses b
    LEFT JOIN {DATASET}.dm_bonus_template t
        ON b.label_bonus_template_id = t.label_bonus_template_id
    WHERE b.fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY
        b.entity_id,
        b.bonus_status_id,
        b.label_bonus_template_id,
        t.label_name
    ORDER BY total_registros DESC
    LIMIT 10
    """

    log.info("Executando Query 2 — Top 10 entity_id com template...")
    df = query_bigquery(sql)
    log.info(f"Retornadas {len(df)} linhas.")

    print("\n" + "=" * 80)
    print("TOP 10 entity_id × template (j_bonuses — últimos 30 dias)")
    print("=" * 80)
    print(df.to_string(index=False))
    return df


def explorar_bonus_meta_amostra():
    """
    Query 3 — Amostra de bonus_meta e bonus_internal_meta para
    entender o conteúdo JSON e extrair significados dos entity_id.
    """

    sql = f"""
    -- Amostra: entity_id com bonus_meta (últimos 7 dias)
    -- O campo bonus_internal_meta costuma ter info como
    -- "given_by_product_name", "label_bonus_template_id" etc.
    SELECT
        b.entity_id,
        b.bonus_status_id,
        b.label_bonus_template_id,
        b.bonus_meta,
        b.bonus_internal_meta,
        b.activity_details
    FROM {DATASET}.j_bonuses b
    WHERE b.fact_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    LIMIT 10
    """

    log.info("Executando Query 3 — Amostra de bonus_meta...")
    df = query_bigquery(sql)
    log.info(f"Retornadas {len(df)} linhas.")

    print("\n" + "=" * 80)
    print("AMOSTRA: bonus_meta / bonus_internal_meta (últimos 7 dias)")
    print("=" * 80)
    for i, row in df.iterrows():
        print(f"\n--- Registro {i+1} ---")
        print(f"  entity_id:               {row['entity_id']}")
        print(f"  bonus_status_id:         {row['bonus_status_id']}")
        print(f"  label_bonus_template_id: {row['label_bonus_template_id']}")
        print(f"  bonus_meta:              {str(row['bonus_meta'])[:200]}")
        print(f"  bonus_internal_meta:     {str(row['bonus_internal_meta'])[:300]}")
        print(f"  activity_details:        {str(row['activity_details'])[:200]}")

    return df


def explorar_dm_bonus_template():
    """
    Query 4 — Verifica colunas da dm_bonus_template para acertar o join.
    """

    sql = f"""
    SELECT * FROM {DATASET}.dm_bonus_template LIMIT 3
    """

    log.info("Executando Query 4 — Amostra dm_bonus_template...")
    df = query_bigquery(sql)
    log.info(f"Colunas dm_bonus_template: {list(df.columns)}")

    print("\n" + "=" * 80)
    print("COLUNAS dm_bonus_template")
    print("=" * 80)
    for col in df.columns:
        print(f"  • {col} ({df[col].dtype})")
    return df


if __name__ == "__main__":
    print("=" * 80)
    print("EXPLORAÇÃO j_bonuses — BigQuery Smartico")
    print("Objetivo: identificar entity_id = 'Cumprimento da Condição'")
    print("=" * 80)

    try:
        # 1. Verifica estrutura da dm_bonus_template (para o join)
        df_template = explorar_dm_bonus_template()

        # 2. Mapa completo entity_id × bonus_status_id
        df_distintos = explorar_entity_ids_distintos()

        # 3. Top 10 com nome do template
        df_top = explorar_top_entity_ids()

        # 4. Amostra de bonus_meta para contexto
        df_meta = explorar_bonus_meta_amostra()

        # Resumo
        print("\n" + "=" * 80)
        print("COMO INTERPRETAR OS RESULTADOS")
        print("=" * 80)
        print("""
1. entity_id: identifica o template/campanha específica do bônus.
   Valores altos (ex: 1002272) são IDs de templates Smartico.

2. bonus_status_id: indica o ESTADO do bônus no ciclo de vida.
   Valores típicos no Smartico:
     1 = Issued (bônus criado/atribuído)
     2 = Activated (jogador ativou)
     3 = Redeemed/Claimed (jogador resgatou — CUMPRIU CONDIÇÃO)
     4 = Completed (wagering cumprido)
     5 = Expired
     6 = Canceled
     7 = Failed

3. O 'Cumprimento da Condição' provavelmente corresponde a
   bonus_status_id = 3 (Redeemed) ou 4 (Completed).

4. Verifique o bonus_internal_meta — ele costuma conter o JSON
   com detalhes como 'given_by_product_name' e 'redeem_automatically'.
        """)

    except Exception as e:
        log.error(f"Erro na exploração: {e}", exc_info=True)
        sys.exit(1)
