"""
Pipeline: CRM Daily Performance (v2 — Isolamento de Coorte)
=============================================================
Produtização do script temp_check_users.py.
Calcula métricas ANTES / DURANTE / DEPOIS para campanhas CRM e persiste
na tabela multibet.fact_crm_daily_performance (Super Nova DB).

IMPORTANTE: v2 implementa isolamento de coorte por entity_id.
  - Extrai user_ids da campanha no BigQuery (j_bonuses)
  - Traduz user_ext_id → c_ecr_id via ecr.tbl_ecr no Redshift
  - Filtra bireports.tbl_ecr_wise_daily_bi_summary APENAS para a coorte
  - Para coortes > 5.000 users: usa temp table + INNER JOIN

Fontes:
  - BigQuery (Smartico j_bonuses): coorte de users + BTR
  - Redshift (ecr.tbl_ecr): bridge de IDs (external → internal)
  - Redshift (bireports.tbl_ecr_wise_daily_bi_summary): GGR, depósitos, RCA, APD, sessões
  - BigQuery (Smartico j_communication): funil CRM

Destino:
  - Super Nova DB → multibet.fact_crm_daily_performance (JSONB)

Regras de negócio:
  - DURING é dinâmico: min(campanha_end, ontem)
  - BEFORE = baseline M-1 (mesmo intervalo no mês anterior)
  - BTR vem SOMENTE do Smartico (bônus efetivamente creditados)
  - NGR = GGR - BTR_Smartico - Royalties
  - ROI = NGR_Incremental / CustoTotal
  - Custos por canal: WhatsApp R$0,16 | SMS DisparoPro R$0,045 |
                      SMS Comtele R$0,063 | Push R$0,060

Uso:
    # Campanha com entity_id (backfill automático)
    python pipelines/crm_daily_performance.py \\
        --campanha-id ENTITY_754 \\
        --campanha-name "AUTO_MAP_754" \\
        --start 2026-01-01 --end 2026-03-16 \\
        --entity-id 754

    # Modo programático
    from pipelines.crm_daily_performance import run_pipeline
    run_pipeline(
        campanha_id="ENTITY_754",
        campanha_name="AUTO_MAP_754",
        campanha_start="2026-01-01",
        campanha_end="2026-03-16",
        entity_id="754",
    )
"""

import argparse
import json
import logging
import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd
from dateutil.relativedelta import relativedelta

# Garante que o diretório raiz do projeto está no path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.redshift import query_redshift, get_connection as get_redshift_connection
from db.bigquery import query_bigquery
from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# =============================================================================
# CONSTANTES — Custos de disparo por canal
# =============================================================================
# Tabela oficial de custos por provedor (confirmada com CRM)
# BigQuery: j_communication.label_provider_id → dm_providers_sms
CUSTO_POR_PROVEDOR = {
    # SMS
    "sms_disparopro":  0.045,   # provider_id=1536 (DisparoPro SMS)
    "sms_pushfy":      0.060,   # provider_id=1545 (PushFY SMS)
    "sms_comtele":     0.063,   # provider_id=1268 (Multibet Comtele)
    "sms_outros":      0.045,   # fallback para SMS sem provider
    # Push
    "push":            0.060,   # activity_type_id=40 (PushFY)
    # WhatsApp
    "whatsapp":        0.16,    # activity_type_id=61 (WhatsApp Loyalty)
}

# Mapa provider_id → chave de custo
PROVIDER_ID_MAP = {
    1536: "sms_disparopro",   # DisparoPro SMS
    1545: "sms_pushfy",       # PushFY SMS
    1268: "sms_comtele",      # Multibet Comtele
}


# =============================================================================
# DDL — Criação da tabela (idempotente)
# =============================================================================
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.fact_crm_daily_performance (
    id                  SERIAL PRIMARY KEY,
    campanha_id         VARCHAR(100)  NOT NULL,
    campanha_name       VARCHAR(255),
    campanha_start      DATE          NOT NULL,
    campanha_end        DATE          NOT NULL,
    period              VARCHAR(10)   NOT NULL,
    period_start        DATE          NOT NULL,
    period_end          DATE          NOT NULL,
    funil               JSONB         DEFAULT '{}'::JSONB,
    financeiro          JSONB         DEFAULT '{}'::JSONB,
    comparativo         JSONB         DEFAULT '{}'::JSONB,
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   DEFAULT NOW(),
    CONSTRAINT uq_campanha_period UNIQUE (campanha_id, period)
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_fact_crm_campanha ON multibet.fact_crm_daily_performance (campanha_id);",
    "CREATE INDEX IF NOT EXISTS idx_fact_crm_financeiro_gin ON multibet.fact_crm_daily_performance USING GIN (financeiro);",
    "CREATE INDEX IF NOT EXISTS idx_fact_crm_comparativo_gin ON multibet.fact_crm_daily_performance USING GIN (comparativo);",
]

# Placeholder De-Para: tabela para futuras traduções de nomes (Raphael)
DDL_FRIENDLY_NAMES = """
CREATE TABLE IF NOT EXISTS multibet.dim_crm_friendly_names (
    entity_id       VARCHAR(100)  PRIMARY KEY,
    friendly_name   VARCHAR(255)  NOT NULL,
    categoria       VARCHAR(100),        -- ex: 'RETEM', 'MULTIVERSO', 'WELCOME'
    responsavel     VARCHAR(100),        -- ex: 'Raphael', 'CRM Team'
    created_at      TIMESTAMPTZ   DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   DEFAULT NOW()
);
"""


def setup_table():
    """Cria schema, tabelas e índices no Super Nova DB (idempotente)."""
    log.info("Verificando/criando tabelas no Super Nova DB...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_FRIENDLY_NAMES)
    for ddl in DDL_INDEXES:
        execute_supernova(ddl)
    log.info("Tabelas prontas.")


# =============================================================================
# HELPERS
# =============================================================================
def _parse_date(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()


def _decimal_to_float(val):
    """Converte Decimal para float (seguro para JSON)."""
    if isinstance(val, Decimal):
        return float(val)
    return val


def _df_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """Converte todas as colunas numéricas de Decimal para float."""
    for col in df.columns:
        df[col] = df[col].apply(_decimal_to_float)
    return df


# =============================================================================
# JANELAS TEMPORAIS — Automação BEFORE / DURING / AFTER
# =============================================================================
def calcular_janelas(
    campanha_start: str,
    campanha_end: str,
    dias_pos_campanha: int = 3,
) -> dict:
    """
    Calcula as janelas temporais dinâmicas.

    Regras:
      - BEFORE = baseline M-1 (mesmo intervalo, mês anterior)
      - DURING = campanha_start até min(campanha_end, ontem)
      - AFTER  = campanha_end+1 até min(campanha_end+dias_pos, ontem)
                 (só existe se campanha já encerrou)

    Returns:
        dict com chaves 'BEFORE', 'DURING', e opcionalmente 'AFTER',
        cada uma com (dt_start, dt_end).
    """
    dt_start = _parse_date(campanha_start)
    dt_end = _parse_date(campanha_end)
    ontem = date.today() - timedelta(days=1)

    # Se campanha ainda não começou, aborta
    if dt_start > ontem:
        log.warning("Campanha ainda não iniciou (start > ontem). Abortando.")
        return {}

    # DURING dinâmico: acumula até ontem ou campanha_end (o que vier antes)
    dt_during_end = min(dt_end, ontem)

    # BEFORE: baseline M-1 (mesmo intervalo no mês anterior)
    bl_start = dt_start - relativedelta(months=1)
    bl_end = dt_end - relativedelta(months=1)

    janelas = {
        "BEFORE": (bl_start, bl_end),
        "DURING": (dt_start, dt_during_end),
    }

    # AFTER: só se campanha já encerrou
    campanha_encerrada = dt_end < ontem
    if campanha_encerrada:
        dt_after_start = dt_end + timedelta(days=1)
        dt_after_end = min(dt_end + timedelta(days=dias_pos_campanha), ontem)
        janelas["AFTER"] = (dt_after_start, dt_after_end)

    for period, (d1, d2) in janelas.items():
        log.info(f"  {period}: {d1} a {d2}")

    return janelas


# =============================================================================
# PASSO 0: Isolamento de Coorte — extrair user_ids da campanha
# =============================================================================
LIMITE_TEMP_TABLE = 5000  # acima disso, usa temp table no Redshift

def extrair_coorte_bigquery(entity_id: str) -> list[str]:
    """
    ISOLAMENTO DE COORTE: extrai todos os user_ext_id únicos que receberam
    bônus (bonus_status_id=3) na entity_id especificada.

    Args:
        entity_id: ID da campanha/entity no Smartico

    Returns:
        Lista de user_ext_id (strings) da coorte
    """
    sql = f"""
    SELECT DISTINCT CAST(user_ext_id AS STRING) AS user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
    WHERE entity_id = {entity_id}
      AND bonus_status_id = 3
      AND user_ext_id IS NOT NULL
    """

    log.info(f"Extraindo coorte do BigQuery (entity_id={entity_id})...")
    df = query_bigquery(sql)
    user_ids = df["user_ext_id"].tolist()
    log.info(f"  Coorte: {len(user_ids)} usuarios unicos.")
    return user_ids


def bridge_ids_redshift(user_ext_ids: list[str]) -> list[int]:
    """
    BRIDGE DE IDs: traduz user_ext_id (Smartico external) para c_ecr_id
    (Redshift internal) via ecr.tbl_ecr.

    Smartico user_ext_id = ecr.tbl_ecr.c_external_id

    Args:
        user_ext_ids: lista de IDs externos (strings)

    Returns:
        Lista de c_ecr_id (int) internos do Redshift
    """
    if not user_ext_ids:
        return []

    # Redshift aceita IN com até ~10.000 valores; para mais, usar temp table
    ids_csv = ",".join(user_ext_ids)

    sql = f"""
    SELECT c_ecr_id
    FROM ecr.tbl_ecr
    WHERE c_external_id IN ({ids_csv})
    """

    log.info(f"Bridge IDs: traduzindo {len(user_ext_ids)} user_ext_id -> c_ecr_id...")
    df = query_redshift(sql)
    ecr_ids = [int(x) for x in df["c_ecr_id"].tolist()]
    log.info(f"  Bridge: {len(ecr_ids)} c_ecr_id encontrados (de {len(user_ext_ids)} externos).")
    return ecr_ids


# =============================================================================
# PARTE 1: Redshift — GGR, Depósitos, RCA, APD, Sessões (SURGICAL JOIN)
# =============================================================================
BATCH_INSERT_SIZE = 1000  # IDs por batch no INSERT INTO temp table

def extrair_financeiro_redshift(janelas: dict, ecr_ids: list[int] | None = None) -> pd.DataFrame:
    """
    Consulta bireports.tbl_ecr_wise_daily_bi_summary no Redshift.
    Usa SURGICAL JOIN: CREATE TEMP TABLE cohort_ids + batch INSERT + INNER JOIN.

    Args:
        janelas: dict com períodos e datas
        ecr_ids: lista de c_ecr_id da coorte (None = sem filtro, modo global)

    Returns:
        DataFrame com colunas por período (BEFORE/DURING/AFTER).
    """
    # Montar CASE WHEN dinâmico
    case_lines = []
    for period, (d1, d2) in janelas.items():
        case_lines.append(
            f"WHEN b.c_created_date BETWEEN '{d1}'::DATE AND '{d2}'::DATE THEN '{period}'"
        )
    case_sql = "\n                ".join(case_lines)

    # Range total para filtro eficiente
    all_dates = [d for d1, d2 in janelas.values() for d in (d1, d2)]
    min_dt = min(all_dates)
    max_dt = max(all_dates)

    # Query principal de agregação
    join_clause = "INNER JOIN cohort_ids c ON b.c_ecr_id = c.c_ecr_id" if ecr_ids else ""

    sql_agg = f"""
    SELECT * FROM (
        SELECT
            CASE
                {case_sql}
            END AS period,

            COUNT(DISTINCT b.c_ecr_id) AS total_users,

            -- Depositos (centavos / 100 = BRL)
            SUM(b.c_deposit_success_amount) / 100.0 AS depositos_brl,
            SUM(b.c_deposit_success_count)          AS depositos_qtd,

            -- GGR = apostas - wins (todos os produtos)
            SUM(
                (COALESCE(b.c_casino_bet_amount, 0) - COALESCE(b.c_casino_win_amount, 0))
              + (COALESCE(b.c_sb_bet_amount, 0)     - COALESCE(b.c_sb_win_amount, 0))
              + (COALESCE(b.c_bt_bet_amount, 0)     - COALESCE(b.c_bt_win_amount, 0))
              + (COALESCE(b.c_bingo_bet_amount, 0)  - COALESCE(b.c_bingo_win_amount, 0))
            ) / 100.0 AS ggr_brl,

            -- RCA = royalties + jackpot contribution
            SUM(COALESCE(b.c_royalty_amount, 0) + COALESCE(b.c_jackpot_contribution_amount, 0))
            / 100.0 AS rca_brl,

            -- APD (Average Play Days)
            ROUND(
                SUM(CASE WHEN (
                    COALESCE(b.c_casino_bet_amount, 0) + COALESCE(b.c_sb_bet_amount, 0)
                  + COALESCE(b.c_bt_bet_amount, 0) + COALESCE(b.c_bingo_bet_amount, 0)
                ) > 0 THEN 1 ELSE 0 END) * 1.0
                / NULLIF(COUNT(DISTINCT b.c_ecr_id), 0)
            , 2) AS avg_play_days,

            -- Sessoes
            SUM(b.c_login_count) AS total_sessions

        FROM bireports.tbl_ecr_wise_daily_bi_summary b
        {join_clause}
        WHERE b.c_created_date BETWEEN '{min_dt}'::DATE AND '{max_dt}'::DATE
        GROUP BY 1
    ) t
    WHERE period IS NOT NULL
    ORDER BY CASE period WHEN 'BEFORE' THEN 1 WHEN 'DURING' THEN 2 WHEN 'AFTER' THEN 3 END
    """

    log.info("Consultando Redshift (GGR, depositos, RCA, APD, sessoes)...")

    if ecr_ids:
        # SURGICAL JOIN: temp table + batch INSERT + INNER JOIN
        log.info(f"  Surgical Join: {len(ecr_ids)} IDs via TEMP TABLE + INNER JOIN...")
        conn = get_redshift_connection()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                # 1. Criar temp table
                cur.execute("CREATE TEMP TABLE cohort_ids (c_ecr_id BIGINT)")

                # 2. Batch INSERT (blocos de BATCH_INSERT_SIZE)
                for i in range(0, len(ecr_ids), BATCH_INSERT_SIZE):
                    batch = ecr_ids[i:i + BATCH_INSERT_SIZE]
                    values = ",".join(f"({eid})" for eid in batch)
                    cur.execute(f"INSERT INTO cohort_ids VALUES {values}")

                log.info(f"  Temp table carregada ({len(ecr_ids)} IDs em {(len(ecr_ids) // BATCH_INSERT_SIZE) + 1} batches).")

                # 3. Query de agregação com INNER JOIN
                cur.execute(sql_agg)
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()

                # 4. Limpar temp table
                cur.execute("DROP TABLE IF EXISTS cohort_ids")
        finally:
            conn.close()

        df = pd.DataFrame(rows, columns=cols)
    else:
        # Modo global (sem coorte)
        log.info("  Sem filtro de coorte (modo global).")
        df = query_redshift(sql_agg)

    df = _df_to_float(df)
    log.info(f"  Redshift retornou {len(df)} periodos.")
    return df


# =============================================================================
# PARTE 2: BigQuery/Smartico — BTR real (bonus_status_id = 3)
# =============================================================================
def extrair_btr_bigquery(janelas: dict, entity_id: str | None = None) -> pd.DataFrame:
    """
    Consulta j_bonuses no BigQuery para BTR (custo de bônus efetivamente creditados).
    Filtra por bonus_status_id = 3 (creditado/completado).
    Se entity_id fornecido, filtra APENAS para essa campanha.
    """
    case_lines = []
    for period, (d1, d2) in janelas.items():
        case_lines.append(
            f"WHEN DATE(fact_date) BETWEEN '{d1}' AND '{d2}' THEN '{period}'"
        )
    case_sql = "\n                ".join(case_lines)

    all_dates = [d for d1, d2 in janelas.values() for d in (d1, d2)]
    min_dt = min(all_dates)
    max_dt = max(all_dates)

    entity_filter = f"AND entity_id = {entity_id}" if entity_id else ""

    sql = f"""
    SELECT
        period,
        SUM(bonus_cost_value) AS btr_brl
    FROM (
        SELECT
            CASE
                {case_sql}
            END AS period,
            bonus_cost_value
        FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
        WHERE bonus_status_id = 3
          AND DATE(fact_date) BETWEEN '{min_dt}' AND '{max_dt}'
          {entity_filter}
    )
    WHERE period IS NOT NULL
    GROUP BY 1
    """

    log.info(f"Consultando BigQuery/Smartico (BTR real, entity_id={entity_id or 'ALL'})...")
    df = query_bigquery(sql)
    log.info(f"  BigQuery BTR retornou {len(df)} periodos.")
    return df


# =============================================================================
# PARTE 3: BigQuery/Smartico — Funil CRM (comunicações + engajamentos)
# =============================================================================
def extrair_funil_bigquery(janelas: dict) -> dict:
    """
    Consulta j_communication e j_engagements no BigQuery para métricas do funil CRM.

    Funil:
      1. Enviadas (fact_type_id = 1 em j_communication)
      2. Entregues (fact_type_id = 2 em j_communication)
      3. Abertas (fact_type_id = 3 em j_communication)
      4. Clicadas (fact_type_id = 4 em j_communication)
      5. Convertidas (fact_type_id = 5 em j_communication — depósito pós-click)

    Canais extraídos das automations para cálculo de custo.

    Returns:
        dict com chave = period, valor = dict de métricas de funil
    """
    case_lines = []
    for period, (d1, d2) in janelas.items():
        case_lines.append(
            f"WHEN DATE(fact_date) BETWEEN '{d1}' AND '{d2}' THEN '{period}'"
        )
    case_sql = "\n                ".join(case_lines)

    all_dates = [d for d1, d2 in janelas.values() for d in (d1, d2)]
    min_dt = min(all_dates)
    max_dt = max(all_dates)

    # Query 1: Funil de comunicação por fact_type_id
    sql_comm = f"""
    SELECT
        period,
        COUNTIF(fact_type_id = 1) AS comunicacoes_enviadas,
        COUNTIF(fact_type_id = 2) AS comunicacoes_entregues,
        COUNTIF(fact_type_id = 3) AS comunicacoes_abertas,
        COUNTIF(fact_type_id = 4) AS comunicacoes_clicadas,
        COUNTIF(fact_type_id = 5) AS comunicacoes_convertidas
    FROM (
        SELECT
            CASE
                {case_sql}
            END AS period,
            fact_type_id
        FROM `smartico-bq6.dwh_ext_24105.j_communication`
        WHERE DATE(fact_date) BETWEEN '{min_dt}' AND '{max_dt}'
          AND label_id = 24105
    )
    WHERE period IS NOT NULL
    GROUP BY 1
    """

    log.info("Consultando BigQuery/Smartico (funil CRM - j_communication)...")
    df_comm = query_bigquery(sql_comm)

    # Query 2: Disparos por canal + provedor (para cálculo de custo diferenciado)
    # SMS diferenciado por label_provider_id:
    #   1536 = DisparoPro (R$0,045) | 1545 = PushFY SMS (R$0,060) | 1268 = Comtele (R$0,063)
    sql_canais = f"""
    SELECT
        period,
        COUNTIF(activity_type_id = 61) AS disparos_whatsapp,
        COUNTIF(activity_type_id = 60 AND label_provider_id = 1536) AS disparos_sms_disparopro,
        COUNTIF(activity_type_id = 60 AND label_provider_id = 1545) AS disparos_sms_pushfy,
        COUNTIF(activity_type_id = 60 AND label_provider_id = 1268) AS disparos_sms_comtele,
        COUNTIF(activity_type_id = 60 AND label_provider_id NOT IN (1536, 1545, 1268)) AS disparos_sms_outros,
        COUNTIF(activity_type_id = 40) AS disparos_push,
        COUNTIF(activity_type_id = 50) AS disparos_email
    FROM (
        SELECT
            CASE
                {case_sql}
            END AS period,
            activity_type_id,
            label_provider_id
        FROM `smartico-bq6.dwh_ext_24105.j_communication`
        WHERE DATE(fact_date) BETWEEN '{min_dt}' AND '{max_dt}'
          AND label_id = 24105
          AND fact_type_id = 1  -- apenas enviadas
          AND activity_type_id IN (61, 60, 40, 50)  -- WA, SMS, Push, Email
    )
    WHERE period IS NOT NULL
    GROUP BY 1
    """

    log.info("Consultando BigQuery/Smartico (disparos por canal + provedor)...")
    df_canais = query_bigquery(sql_canais)

    # Montar dict por período
    funil_por_periodo = {}
    for _, row in df_comm.iterrows():
        p = row["period"]
        funil_por_periodo[p] = {
            "comunicacoes_enviadas":    int(row.get("comunicacoes_enviadas", 0)),
            "comunicacoes_entregues":   int(row.get("comunicacoes_entregues", 0)),
            "comunicacoes_abertas":     int(row.get("comunicacoes_abertas", 0)),
            "comunicacoes_clicadas":    int(row.get("comunicacoes_clicadas", 0)),
            "comunicacoes_convertidas": int(row.get("comunicacoes_convertidas", 0)),
        }

    # Adicionar disparos por canal com detalhamento de provedor SMS
    for _, row in df_canais.iterrows():
        p = row["period"]
        if p not in funil_por_periodo:
            funil_por_periodo[p] = {}
        funil_por_periodo[p]["canais"] = {
            "whatsapp":         int(row.get("disparos_whatsapp", 0)),
            "sms_disparopro":   int(row.get("disparos_sms_disparopro", 0)),
            "sms_pushfy":       int(row.get("disparos_sms_pushfy", 0)),
            "sms_comtele":      int(row.get("disparos_sms_comtele", 0)),
            "sms_outros":       int(row.get("disparos_sms_outros", 0)),
            "push":             int(row.get("disparos_push", 0)),
            "email":            int(row.get("disparos_email", 0)),
        }

    log.info(f"  Funil CRM extraido para {len(funil_por_periodo)} periodos.")
    return funil_por_periodo


# =============================================================================
# PARTE 4: Cálculo de Custos e ROI
# =============================================================================
def calcular_custos(funil: dict) -> dict:
    """
    Calcula o custo total de disparo diferenciado por provedor.

    Custos unitários (tabela oficial):
      - SMS DisparoPro (provider_id=1536): R$ 0,045
      - SMS PushFY (provider_id=1545):     R$ 0,060
      - SMS Comtele (provider_id=1268):    R$ 0,063
      - Push (activity_type_id=40):        R$ 0,060
      - WhatsApp (activity_type_id=61):    R$ 0,16

    Returns:
        dict com custos detalhados por provedor e custo_total
    """
    canais = funil.get("canais", {})

    custo_whatsapp      = canais.get("whatsapp", 0)       * CUSTO_POR_PROVEDOR["whatsapp"]
    custo_sms_disparopro = canais.get("sms_disparopro", 0) * CUSTO_POR_PROVEDOR["sms_disparopro"]
    custo_sms_pushfy    = canais.get("sms_pushfy", 0)      * CUSTO_POR_PROVEDOR["sms_pushfy"]
    custo_sms_comtele   = canais.get("sms_comtele", 0)     * CUSTO_POR_PROVEDOR["sms_comtele"]
    custo_sms_outros    = canais.get("sms_outros", 0)      * CUSTO_POR_PROVEDOR["sms_outros"]
    custo_push          = canais.get("push", 0)             * CUSTO_POR_PROVEDOR["push"]

    custo_sms_total = custo_sms_disparopro + custo_sms_pushfy + custo_sms_comtele + custo_sms_outros
    custo_total = custo_whatsapp + custo_sms_total + custo_push

    return {
        "custo_whatsapp":        round(custo_whatsapp, 2),
        "custo_sms_disparopro":  round(custo_sms_disparopro, 2),
        "custo_sms_pushfy":      round(custo_sms_pushfy, 2),
        "custo_sms_comtele":     round(custo_sms_comtele, 2),
        "custo_sms_total":       round(custo_sms_total, 2),
        "custo_push":            round(custo_push, 2),
        "custo_total":           round(custo_total, 2),
    }


def calcular_comparativo(df_financeiro: pd.DataFrame, funil_por_periodo: dict) -> dict:
    """
    Calcula métricas comparativas:
      - NGR incremental = NGR_DURING - NGR_BEFORE
      - % Atingimento de Meta (Meta = NGR_BEFORE, baseline M-1)
      - Custos de disparo por canal
      - ROI = NGR_Incremental / CustoTotal

    Returns:
        dict com métricas comparativas (para o período DURING)
    """
    ngr_before = df_financeiro.loc[
        df_financeiro["period"] == "BEFORE", "ngr_brl"
    ].values
    ngr_during = df_financeiro.loc[
        df_financeiro["period"] == "DURING", "ngr_brl"
    ].values

    ngr_inc = None
    ngr_var_pct = None
    meta_atingimento_pct = None
    if len(ngr_before) > 0 and len(ngr_during) > 0:
        ngr_inc = round(float(ngr_during[0] - ngr_before[0]), 2)
        if ngr_before[0] != 0:
            ngr_var_pct = round(
                float(ngr_inc / abs(ngr_before[0]) * 100), 2
            )
            # Meta = NGR_BEFORE (baseline M-1)
            # % Atingimento = NGR_DURING / NGR_BEFORE * 100
            meta_atingimento_pct = round(
                float(ngr_during[0] / abs(ngr_before[0]) * 100), 2
            )

    # Custos: usa disparos do período DURING
    funil_during = funil_por_periodo.get("DURING", {})
    custos = calcular_custos(funil_during)

    # ROI = NGR_Incremental / CustoTotal
    roi = None
    if ngr_inc is not None and custos["custo_total"] > 0:
        roi = round(ngr_inc / custos["custo_total"], 2)

    comp = {
        "ngr_incremental":       ngr_inc,
        "ngr_variacao_pct":      ngr_var_pct,
        "meta_atingimento_pct":  meta_atingimento_pct,
        "roi":                   roi,
        **custos,
    }

    log.info(f"  NGR Incremental:  R$ {ngr_inc}")
    log.info(f"  Meta Atingimento: {meta_atingimento_pct}%")
    log.info(f"  Custo Total:      R$ {custos['custo_total']}")
    log.info(f"  ROI:              {roi}")

    return comp


# =============================================================================
# PERSISTÊNCIA — UPSERT no Super Nova DB
# =============================================================================
# UPSERT com LEFT JOIN na dim_crm_friendly_names para resolver nome amigável
# Se friendly_name existir na tabela de-para, usa ele; senão, usa o campanha_name passado
UPSERT_SQL = """
INSERT INTO multibet.fact_crm_daily_performance
    (campanha_id, campanha_name, campanha_start, campanha_end,
     period, period_start, period_end,
     funil, financeiro, comparativo,
     created_at, updated_at)
VALUES
    (%s,
     COALESCE(
         (SELECT friendly_name FROM multibet.dim_crm_friendly_names WHERE entity_id = %s),
         %s
     ),
     %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
ON CONFLICT (campanha_id, period)
DO UPDATE SET
    campanha_name  = COALESCE(
        (SELECT friendly_name FROM multibet.dim_crm_friendly_names WHERE entity_id = EXCLUDED.campanha_id),
        EXCLUDED.campanha_name
    ),
    campanha_start = EXCLUDED.campanha_start,
    campanha_end   = EXCLUDED.campanha_end,
    period_start   = EXCLUDED.period_start,
    period_end     = EXCLUDED.period_end,
    funil          = EXCLUDED.funil,
    financeiro     = EXCLUDED.financeiro,
    comparativo    = EXCLUDED.comparativo,
    updated_at     = NOW()
;
"""


def persistir(
    campanha_id: str,
    campanha_name: str,
    campanha_start: str,
    campanha_end: str,
    janelas: dict,
    df_financeiro: pd.DataFrame,
    funil_por_periodo: dict,
    comparativo: dict,
    entity_id: str | None = None,
):
    """
    Faz UPSERT de cada período no Super Nova DB.
    Usa LEFT JOIN com dim_crm_friendly_names para resolver nome amigável.
    """
    log.info("Persistindo no Super Nova DB...")

    # Entity ID para lookup na dim_crm_friendly_names
    lookup_entity = entity_id or campanha_id

    # Montar mapa financeiro por período
    fin_map = {}
    for _, row in df_financeiro.iterrows():
        p = row["period"]
        fin_map[p] = {
            "total_users":    int(row.get("total_users", 0)),
            "depositos_brl":  round(float(row.get("depositos_brl", 0)), 2),
            "depositos_qtd":  int(row.get("depositos_qtd", 0)),
            "ggr_brl":        round(float(row.get("ggr_brl", 0)), 2),
            "btr_brl":        round(float(row.get("btr_brl", 0)), 2),
            "rca_brl":        round(float(row.get("rca_brl", 0)), 2),
            "ngr_brl":        round(float(row.get("ngr_brl", 0)), 2),
            "avg_play_days":  round(float(row.get("avg_play_days", 0)), 2),
            "total_sessions": int(row.get("total_sessions", 0)),
        }

    records = []
    for period, (d1, d2) in janelas.items():
        funil_json = json.dumps(
            funil_por_periodo.get(period, {}), ensure_ascii=False
        )
        fin_json = json.dumps(
            fin_map.get(period, {}), ensure_ascii=False
        )
        # Comparativo só vai no DURING
        comp_json = json.dumps(
            comparativo if period == "DURING" else {}, ensure_ascii=False
        )

        # Parâmetros: campanha_id, entity_id (lookup), campanha_name (fallback),
        #             campanha_start, campanha_end, period, period_start, period_end,
        #             funil, financeiro, comparativo
        records.append((
            campanha_id,
            lookup_entity,    # para lookup na dim_crm_friendly_names
            campanha_name,    # fallback se não encontrar friendly_name
            campanha_start,
            campanha_end,
            period,
            str(d1),
            str(d2),
            funil_json,
            fin_json,
            comp_json,
        ))

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for rec in records:
                cur.execute(UPSERT_SQL, rec)
        conn.commit()
        log.info(f"  {len(records)} registros upserted com sucesso.")
    except Exception as e:
        conn.rollback()
        log.error(f"Erro ao persistir: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


# =============================================================================
# ORQUESTRAÇÃO PRINCIPAL
# =============================================================================
def run_pipeline(
    campanha_id: str,
    campanha_name: str,
    campanha_start: str,
    campanha_end: str,
    dias_pos_campanha: int = 3,
    entity_id: str | None = None,
    skip_setup: bool = False,
    quiet: bool = False,
) -> pd.DataFrame | None:
    """
    Orquestra o pipeline completo com isolamento de coorte.

    Args:
        entity_id:  ID da campanha no Smartico. Se fornecido, ativa isolamento.
        skip_setup: Se True, pula setup_table() (chamar 1x antes do loop).
        quiet:      Se True, não imprime resumo no terminal (backfill em massa).

    Returns:
        DataFrame com métricas financeiras consolidadas, ou None se sem dados.
    """
    log.info(f"  Pipeline: {campanha_id} | entity={entity_id or 'global'}")

    # --- 0. Isolamento de Coorte ---
    ecr_ids = None
    if entity_id:
        user_ext_ids = extrair_coorte_bigquery(entity_id)
        if not user_ext_ids:
            log.warning(f"Coorte vazia para entity_id={entity_id}. Pulando.")
            return None
        ecr_ids = bridge_ids_redshift(user_ext_ids)
        if not ecr_ids:
            log.warning(f"Bridge vazio para entity_id={entity_id}. Pulando.")
            return None

    # --- 1. Janelas temporais ---
    janelas = calcular_janelas(campanha_start, campanha_end, dias_pos_campanha)
    if not janelas:
        return None

    # --- 2. Financeiro (Redshift) — FILTRADO pela coorte ---
    df_rs = extrair_financeiro_redshift(janelas, ecr_ids=ecr_ids)
    if df_rs.empty:
        log.warning("Redshift retornou vazio.")
        return None

    # --- 3. BTR (BigQuery) — FILTRADO por entity_id ---
    df_btr = extrair_btr_bigquery(janelas, entity_id=entity_id)
    btr_map = {}
    if not df_btr.empty:
        btr_map = dict(zip(df_btr["period"], df_btr["btr_brl"].astype(float)))

    df_rs["btr_brl"] = df_rs["period"].map(btr_map).fillna(0.0)
    df_rs["ngr_brl"] = df_rs["ggr_brl"] - df_rs["btr_brl"] - df_rs["rca_brl"]

    # --- 4. Funil CRM (BigQuery) ---
    funil_por_periodo = extrair_funil_bigquery(janelas)

    # --- 5. Comparativo ---
    comparativo = calcular_comparativo(df_rs, funil_por_periodo)

    # --- 6. Persistir ---
    if not skip_setup:
        setup_table()
    persistir(
        campanha_id=campanha_id,
        campanha_name=campanha_name,
        campanha_start=campanha_start,
        campanha_end=campanha_end,
        janelas=janelas,
        df_financeiro=df_rs,
        funil_por_periodo=funil_por_periodo,
        comparativo=comparativo,
        entity_id=entity_id,
    )

    if not quiet:
        _print_resumo(df_rs, funil_por_periodo, comparativo, campanha_id)

    return df_rs


# =============================================================================
# EXIBIÇÃO — Resumo no terminal
# =============================================================================
def _fmt_brl(valor) -> str:
    """Formata float para R$ brasileiro (ex: R$ 6.470.334,62)."""
    try:
        v = float(valor)
        sinal = "-" if v < 0 else ""
        return f"{sinal}R$ {abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(valor)


def _fmt_int(valor) -> str:
    """Formata inteiro com separador de milhar BR (ex: 84.007)."""
    try:
        return f"{int(valor):,}".replace(",", ".")
    except (TypeError, ValueError):
        return str(valor)


def _print_resumo(
    df: pd.DataFrame,
    funil: dict,
    comparativo: dict,
    campanha_id: str,
):
    """Imprime resumo formatado no terminal."""
    print(f"\n{'='*80}")
    print(f"  CRM DAILY PERFORMANCE — {campanha_id}")
    print(f"  BTR fonte: Smartico (j_bonuses, bonus_status_id=3)")
    print(f"{'='*80}\n")

    labels = {
        "BEFORE": "ANTES (baseline M-1)",
        "DURING": "DURANTE (campanha)",
        "AFTER":  "DEPOIS (pós-campanha)",
    }

    for _, row in df.iterrows():
        p = row["period"]
        print(f"  --- {labels.get(p, p)} ---")
        print(f"  Usuarios unicos:  {_fmt_int(row['total_users']):>10s}")
        print(f"  Depositos:        {_fmt_brl(row['depositos_brl']):>18s}  ({_fmt_int(row['depositos_qtd'])} txns)")
        print(f"  GGR:              {_fmt_brl(row['ggr_brl']):>18s}")
        print(f"  BTR (Smartico):   {_fmt_brl(row['btr_brl']):>18s}")
        print(f"  RCA (Royalties):  {_fmt_brl(row['rca_brl']):>18s}")
        print(f"  NGR:              {_fmt_brl(row['ngr_brl']):>18s}")
        print(f"  APD:              {float(row['avg_play_days']):>10.2f} dias")
        print(f"  Sessoes (logins): {_fmt_int(row['total_sessions']):>10s}")

        # Funil CRM
        f = funil.get(p, {})
        if f:
            print(f"  --- Funil CRM ---")
            print(f"  Enviadas:         {_fmt_int(f.get('comunicacoes_enviadas', 0)):>10s}")
            print(f"  Entregues:        {_fmt_int(f.get('comunicacoes_entregues', 0)):>10s}")
            print(f"  Abertas:          {_fmt_int(f.get('comunicacoes_abertas', 0)):>10s}")
            print(f"  Clicadas:         {_fmt_int(f.get('comunicacoes_clicadas', 0)):>10s}")
            print(f"  Convertidas:      {_fmt_int(f.get('comunicacoes_convertidas', 0)):>10s}")
            canais = f.get("canais", {})
            if canais:
                print(f"  Canais: WA={canais.get('whatsapp',0)} | SMS={canais.get('sms',0)} | Push={canais.get('push',0)}")
        print()

    # Comparativo
    if comparativo.get("ngr_incremental") is not None:
        print(f"  {'='*50}")
        print(f"  COMPARATIVO (DURING vs BEFORE)")
        print(f"  Meta = NGR_BEFORE (baseline M-1)")
        print(f"  {'='*50}")
        print(f"  NGR Incremental:  {_fmt_brl(comparativo['ngr_incremental'])}")
        if comparativo.get("ngr_variacao_pct") is not None:
            sinal = "+" if comparativo["ngr_variacao_pct"] > 0 else ""
            print(f"  Variacao:         {sinal}{comparativo['ngr_variacao_pct']}%")
        if comparativo.get("meta_atingimento_pct") is not None:
            print(f"  Meta Atingimento: {comparativo['meta_atingimento_pct']}%")
        print(f"  Custo WhatsApp:       {_fmt_brl(comparativo['custo_whatsapp'])}")
        print(f"  Custo SMS DisparoPro: {_fmt_brl(comparativo.get('custo_sms_disparopro', 0))}")
        print(f"  Custo SMS PushFY:     {_fmt_brl(comparativo.get('custo_sms_pushfy', 0))}")
        print(f"  Custo SMS Comtele:    {_fmt_brl(comparativo.get('custo_sms_comtele', 0))}")
        print(f"  Custo SMS Total:      {_fmt_brl(comparativo.get('custo_sms_total', 0))}")
        print(f"  Custo Push:           {_fmt_brl(comparativo['custo_push'])}")
        print(f"  Custo Total:          {_fmt_brl(comparativo['custo_total'])}")
        if comparativo.get("roi") is not None:
            print(f"  ROI:              {comparativo['roi']}x")
        print()


# =============================================================================
# CLI
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Pipeline CRM Daily Performance — ANTES/DURANTE/DEPOIS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python pipelines/crm_daily_performance.py \\
      --campanha-id RETEM_2026_02 \\
      --campanha-name "Campanha RETEM Fevereiro" \\
      --start 2026-02-01 --end 2026-02-07

  python pipelines/crm_daily_performance.py \\
      --campanha-id MULTIVERSO_2026_03 \\
      --campanha-name "Campanha Multiverso Marco" \\
      --start 2026-03-13 --end 2026-03-20
        """,
    )
    parser.add_argument("--campanha-id", required=True, help="ID da campanha (ex: RETEM_2026_02)")
    parser.add_argument("--campanha-name", required=True, help="Nome da campanha")
    parser.add_argument("--start", required=True, help="Data inicio (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="Data fim (YYYY-MM-DD)")
    parser.add_argument("--dias-pos", type=int, default=3, help="Dias pos-campanha (default: 3)")
    parser.add_argument("--entity-id", default=None, help="Entity ID do Smartico (ativa isolamento de coorte)")

    args = parser.parse_args()

    run_pipeline(
        campanha_id=args.campanha_id,
        campanha_name=args.campanha_name,
        campanha_start=args.start,
        campanha_end=args.end,
        dias_pos_campanha=args.dias_pos,
        entity_id=args.entity_id,
    )


if __name__ == "__main__":
    main()
