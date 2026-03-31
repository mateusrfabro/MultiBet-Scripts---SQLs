"""
Pipeline: CRM Report Daily (v3 -- Athena + BigQuery, sem Redshift)
===================================================================
Report de Performance CRM Diario completo. Substitui crm_daily_performance.py
que dependia do Redshift (descontinuado).

FONTES:
  - BigQuery (Smartico CRM):
      - dm_automation_rule: campanhas ativas e recentes
      - dm_segment: nome dos segmentos
      - j_communication: funil CRM (fact_type_id 1-5) + disparos por canal
      - j_bonuses: custos de bonus e coorte de users
  - Athena (Iceberg Data Lake):
      - ps_bi.dim_user: bridge de IDs (external_id = BigQuery user_ext_id)
      - ps_bi.fct_player_activity_daily: metricas financeiras por player/dia (BRL)
      - ps_bi.fct_casino_activity_daily: top jogos da coorte
      - bireports_ec2.tbl_ecr_wise_daily_bi_summary: fallback financeiro (centavos)

DESTINO:
  - Super Nova DB (PostgreSQL) schema multibet:
      - crm_campaign_daily: 1 linha por campanha x dia
      - crm_campaign_segment_daily: quebra por segmento
      - crm_campaign_game_daily: quebra por jogo
      - crm_campaign_comparison: antes/durante/depois
      - crm_dispatch_budget: orcamento disparos
      - crm_vip_group_daily: VIP groups
      - crm_recovery_daily: recuperacao

REGRAS APLICADAS:
  - Athena timestamps em UTC -> converter para BRT (AT TIME ZONE)
  - ps_bi ja esta em BRL reais (NAO dividir por 100)
  - bireports_ec2 valores em centavos -> /100.0
  - SEMPRE filtrar test users (is_test = false no ps_bi)
  - Duplo filtro entity_id + label_bonus_template_id para bonus
  - GGR = Bets - Rollbacks(72) - Wins
  - Usar D-1 para entregas consolidadas
  - activity_type_id 64 = WhatsApp (NAO 61)
  - Consultar MEMORY.md antes de cada task

USO:
    # Rodar para D-1 (padrao)
    python pipelines/crm_report_daily.py

    # Rodar para data especifica
    python pipelines/crm_report_daily.py --date 2026-03-27

    # Backfill de N dias
    python pipelines/crm_report_daily.py --days 7

    # Dry-run (nao persiste no banco)
    python pipelines/crm_report_daily.py --dry-run

    # Combinar opcoes
    python pipelines/crm_report_daily.py --days 7 --dry-run

AUTOR: Mateus F. (Squad Intelligence Engine)
DATA: 28/03/2026
"""

import argparse
import json
import logging
import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

import pandas as pd

# Garante que o diretorio raiz do projeto esta no path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.bigquery import query_bigquery
from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("crm_report_daily")


# ===========================================================================
# CONSTANTES
# ===========================================================================

# Dataset BigQuery Smartico
BQ_DATASET = "smartico-bq6.dwh_ext_24105"

# Custos unitarios por provedor de disparo (confirmados com CRM)
CUSTO_POR_PROVEDOR = {
    "sms_disparopro": 0.045,   # provider_id=1536 (DisparoPro SMS)
    "sms_pushfy":     0.060,   # provider_id=1545 (PushFY SMS)
    "sms_comtele":    0.063,   # provider_id=1268 (Multibet Comtele)
    "sms_outros":     0.045,   # fallback para SMS sem provider conhecido
    "push":           0.060,   # activity_type_id=30 ou 40 (PushFY)
    "whatsapp":       0.160,   # activity_type_id=64 (WhatsApp Loyalty)
}

# Mapa label_provider_id -> chave de custo
PROVIDER_ID_MAP = {
    1536: "sms_disparopro",
    1545: "sms_pushfy",
    1268: "sms_comtele",
}

# Mapa activity_type_id -> canal legivel
ACTIVITY_CHANNEL_MAP = {
    50: "popup",
    60: "SMS",
    64: "WhatsApp",       # CORRECAO: era 61 no pipeline antigo
    30: "push",
    40: "push_notification",
    31: "inbox",
}

# Classificacao de campaign_type por padrao no rule_name
CAMPAIGN_TYPE_PATTERNS = [
    ("[RETEM]",          "RETEM"),
    ("DailyFS",          "DailyFS"),
    ("Daily Free Spin",  "DailyFS"),
    ("Cashback",         "Cashback"),
    ("Torneio",          "Torneio"),
    ("Tournament",       "Torneio"),
    ("Freebet",          "Freebet"),
    ("Free Bet",         "Freebet"),
]

# Faixas VIP por NGR acumulado (BRL)
VIP_TIERS = [
    ("Elite",        10000),   # NGR >= R$ 10.000
    ("Key Account",   5000),   # NGR >= R$  5.000
    ("High Value",    3000),   # NGR >= R$  3.000
    ("Standard",         0),   # Demais
]

# Batch size para IN clauses (Athena tem limite de ~15k parametros)
BATCH_SIZE_IN_CLAUSE = 5000


# ===========================================================================
# HELPERS
# ===========================================================================

def _parse_date(d: str) -> date:
    """Converte string YYYY-MM-DD para date."""
    return datetime.strptime(d, "%Y-%m-%d").date()


def _decimal_to_float(val):
    """Converte Decimal para float (seguro para JSON)."""
    if isinstance(val, Decimal):
        return float(val)
    return val


def _df_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """Converte todas as colunas de Decimal para float."""
    for col in df.columns:
        df[col] = df[col].apply(_decimal_to_float)
    return df


def _classify_campaign_type(rule_name: str) -> str:
    """Classifica o tipo de campanha pelo padrao no rule_name."""
    if not rule_name:
        return "Outro"
    upper = rule_name.upper()
    for pattern, ctype in CAMPAIGN_TYPE_PATTERNS:
        if pattern.upper() in upper:
            return ctype
    return "Outro"


def _classify_channel(activity_type_id: int) -> str:
    """Mapeia activity_type_id para nome do canal."""
    return ACTIVITY_CHANNEL_MAP.get(activity_type_id, f"unknown_{activity_type_id}")


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


def _safe_div(numerator, denominator, default=0.0) -> float:
    """Divisao segura que retorna default se denominador e zero."""
    try:
        if denominator and float(denominator) != 0:
            return float(numerator) / float(denominator)
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return default


def _chunk_list(lst: list, chunk_size: int) -> list[list]:
    """Divide lista em chunks de tamanho maximo chunk_size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


# ===========================================================================
# DDL — Criacao das tabelas destino (idempotente)
# ===========================================================================
# Nota: as tabelas ja devem estar criadas pela DDL oficial.
# Este DDL serve como fallback para ambientes novos ou de teste.

DDL_STATEMENTS = [
    "CREATE SCHEMA IF NOT EXISTS multibet;",

    # --- crm_campaign_daily: 1 linha por campanha x dia ---
    """
    CREATE TABLE IF NOT EXISTS multibet.crm_campaign_daily (
        id                  SERIAL PRIMARY KEY,
        report_date      DATE          NOT NULL,
        rule_id             INTEGER       NOT NULL,
        rule_name           VARCHAR(500),
        campaign_type       VARCHAR(50),
        channel             VARCHAR(50),
        segment_id          INTEGER,
        segment_name        VARCHAR(500),
        is_active           BOOLEAN       DEFAULT true,
        bo_user_email       VARCHAR(255),

        -- Funil CRM
        enviados            INTEGER       DEFAULT 0,
        entregues           INTEGER       DEFAULT 0,
        abertos             INTEGER       DEFAULT 0,
        clicados            INTEGER       DEFAULT 0,
        convertidos         INTEGER       DEFAULT 0,

        -- Bonus
        cumpriram_condicao  INTEGER       DEFAULT 0,
        custo_bonus_brl     NUMERIC(14,2) DEFAULT 0,

        -- Financeiro da coorte (ps_bi, BRL)
        coorte_users        INTEGER       DEFAULT 0,
        casino_ggr          NUMERIC(14,2) DEFAULT 0,
        sportsbook_ggr      NUMERIC(14,2) DEFAULT 0,
        total_ggr           NUMERIC(14,2) DEFAULT 0,
        total_deposit       NUMERIC(14,2) DEFAULT 0,
        total_withdrawal    NUMERIC(14,2) DEFAULT 0,
        net_deposit         NUMERIC(14,2) DEFAULT 0,
        casino_turnover     NUMERIC(14,2) DEFAULT 0,
        sportsbook_turnover NUMERIC(14,2) DEFAULT 0,
        login_count         INTEGER       DEFAULT 0,

        -- Custos de disparo
        custo_disparo_brl   NUMERIC(14,2) DEFAULT 0,
        custo_detalhe       JSONB         DEFAULT '{}'::JSONB,

        -- KPIs derivados
        roi                 NUMERIC(10,4),
        cpa                 NUMERIC(14,2),
        arpu                NUMERIC(14,2),

        created_at          TIMESTAMPTZ   DEFAULT NOW(),
        updated_at          TIMESTAMPTZ   DEFAULT NOW(),

        CONSTRAINT uq_crm_campaign_daily UNIQUE (report_date, rule_id)
    );
    """,

    # --- crm_campaign_segment_daily: quebra por segmento ---
    """
    CREATE TABLE IF NOT EXISTS multibet.crm_campaign_segment_daily (
        id                  SERIAL PRIMARY KEY,
        report_date      DATE          NOT NULL,
        rule_id             INTEGER       NOT NULL,
        segment_id          INTEGER,
        segment_name        VARCHAR(500),

        enviados            INTEGER       DEFAULT 0,
        convertidos         INTEGER       DEFAULT 0,
        coorte_users        INTEGER       DEFAULT 0,
        total_ggr           NUMERIC(14,2) DEFAULT 0,
        total_deposit       NUMERIC(14,2) DEFAULT 0,
        custo_bonus_brl     NUMERIC(14,2) DEFAULT 0,

        created_at          TIMESTAMPTZ   DEFAULT NOW(),
        updated_at          TIMESTAMPTZ   DEFAULT NOW(),

        CONSTRAINT uq_crm_segment_daily UNIQUE (report_date, rule_id, segment_id)
    );
    """,

    # --- crm_campaign_game_daily: top jogos por campanha ---
    """
    CREATE TABLE IF NOT EXISTS multibet.crm_campaign_game_daily (
        id                  SERIAL PRIMARY KEY,
        report_date      DATE          NOT NULL,
        rule_id             INTEGER       NOT NULL,
        game_id             VARCHAR(100),
        game_name           VARCHAR(500),

        users               INTEGER       DEFAULT 0,
        turnover_brl        NUMERIC(14,2) DEFAULT 0,
        ggr_brl             NUMERIC(14,2) DEFAULT 0,

        created_at          TIMESTAMPTZ   DEFAULT NOW(),
        updated_at          TIMESTAMPTZ   DEFAULT NOW(),

        CONSTRAINT uq_crm_game_daily UNIQUE (report_date, rule_id, game_id)
    );
    """,

    # --- crm_campaign_comparison: antes/durante/depois ---
    """
    CREATE TABLE IF NOT EXISTS multibet.crm_campaign_comparison (
        id                  SERIAL PRIMARY KEY,
        rule_id             INTEGER       NOT NULL,
        period              VARCHAR(10)   NOT NULL,
        period_start        DATE          NOT NULL,
        period_end          DATE          NOT NULL,

        coorte_users        INTEGER       DEFAULT 0,
        total_ggr           NUMERIC(14,2) DEFAULT 0,
        total_deposit       NUMERIC(14,2) DEFAULT 0,
        net_deposit         NUMERIC(14,2) DEFAULT 0,
        casino_turnover     NUMERIC(14,2) DEFAULT 0,
        login_count         INTEGER       DEFAULT 0,

        ggr_delta           NUMERIC(14,2),
        ggr_delta_pct       NUMERIC(10,4),

        created_at          TIMESTAMPTZ   DEFAULT NOW(),
        updated_at          TIMESTAMPTZ   DEFAULT NOW(),

        CONSTRAINT uq_crm_comparison UNIQUE (rule_id, period)
    );
    """,

    # --- crm_dispatch_budget: orcamento de disparos ---
    """
    CREATE TABLE IF NOT EXISTS multibet.crm_dispatch_budget (
        id                  SERIAL PRIMARY KEY,
        report_date      DATE          NOT NULL,
        channel             VARCHAR(50)   NOT NULL,
        provider            VARCHAR(100),

        total_sent          INTEGER       DEFAULT 0,
        custo_unitario      NUMERIC(10,4) DEFAULT 0,
        custo_total_brl     NUMERIC(14,2) DEFAULT 0,

        created_at          TIMESTAMPTZ   DEFAULT NOW(),
        updated_at          TIMESTAMPTZ   DEFAULT NOW(),

        CONSTRAINT uq_crm_dispatch_budget UNIQUE (report_date, channel, provider)
    );
    """,

    # --- crm_vip_group_daily: metricas por faixa VIP ---
    """
    CREATE TABLE IF NOT EXISTS multibet.crm_vip_group_daily (
        id                  SERIAL PRIMARY KEY,
        report_date      DATE          NOT NULL,
        rule_id             INTEGER       NOT NULL,
        vip_tier            VARCHAR(50)   NOT NULL,

        coorte_users        INTEGER       DEFAULT 0,
        total_ggr           NUMERIC(14,2) DEFAULT 0,
        total_deposit       NUMERIC(14,2) DEFAULT 0,
        casino_turnover     NUMERIC(14,2) DEFAULT 0,

        created_at          TIMESTAMPTZ   DEFAULT NOW(),
        updated_at          TIMESTAMPTZ   DEFAULT NOW(),

        CONSTRAINT uq_crm_vip_daily UNIQUE (report_date, rule_id, vip_tier)
    );
    """,

    # --- crm_recovery_daily: metricas de recuperacao ---
    """
    CREATE TABLE IF NOT EXISTS multibet.crm_recovery_daily (
        id                  SERIAL PRIMARY KEY,
        report_date      DATE          NOT NULL,
        rule_id             INTEGER       NOT NULL,

        inativos_alvo       INTEGER       DEFAULT 0,
        reengajados         INTEGER       DEFAULT 0,
        depositaram         INTEGER       DEFAULT 0,
        tempo_medio_reengajamento_dias NUMERIC(10,2),
        churn_d7            INTEGER       DEFAULT 0,
        churn_d7_pct        NUMERIC(10,4),

        created_at          TIMESTAMPTZ   DEFAULT NOW(),
        updated_at          TIMESTAMPTZ   DEFAULT NOW(),

        CONSTRAINT uq_crm_recovery_daily UNIQUE (report_date, rule_id)
    );
    """,
]

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_crm_cd_date ON multibet.crm_campaign_daily (report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_cd_rule ON multibet.crm_campaign_daily (rule_id);",
    "CREATE INDEX IF NOT EXISTS idx_crm_csd_date ON multibet.crm_campaign_segment_daily (report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_cgd_date ON multibet.crm_campaign_game_daily (report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_cc_rule ON multibet.crm_campaign_comparison (rule_id);",
    "CREATE INDEX IF NOT EXISTS idx_crm_db_date ON multibet.crm_dispatch_budget (report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_vip_date ON multibet.crm_vip_group_daily (report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_rec_date ON multibet.crm_recovery_daily (report_date);",
]


def setup_tables():
    """Cria schema, tabelas e indices no Super Nova DB (idempotente)."""
    log.info("SETUP: Verificando/criando tabelas no Super Nova DB...")
    for ddl in DDL_STATEMENTS:
        execute_supernova(ddl)
    for idx in DDL_INDEXES:
        execute_supernova(idx)
    log.info("SETUP: Tabelas prontas.")


# ===========================================================================
# STEP 1: Descobrir campanhas ativas (BigQuery)
# ===========================================================================

def step1_discover_campaigns(target_date: date) -> pd.DataFrame:
    """
    Consulta dm_automation_rule + dm_segment no BigQuery para listar
    campanhas CRM ativas e recentes (atualizadas nos ultimos 7 dias).

    Classifica campaign_type pelo padrao no rule_name e channel pelo
    activity_type_id.

    Args:
        target_date: data de referencia (geralmente D-1)

    Returns:
        DataFrame com colunas: rule_id, rule_name, is_active, segment_id,
        segment_name, activity_type_id, bo_user_email, campaign_type, channel
    """
    log.info("STEP 1: Descobrindo campanhas ativas no BigQuery...")

    sql = f"""
    SELECT
        r.rule_id,
        r.rule_name,
        r.is_active,
        r.segment_id,
        s.segment_name,
        r.activity_type_id,
        r.bo_user_email
    FROM `{BQ_DATASET}.dm_automation_rule` r
    LEFT JOIN `{BQ_DATASET}.dm_segment` s
        ON r.segment_id = s.segment_id
    WHERE r.is_active = true
       OR r.update_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    ORDER BY r.rule_id
    """

    df = query_bigquery(sql)

    if df.empty:
        log.warning("STEP 1: Nenhuma campanha encontrada.")
        return df

    # Classificar campaign_type e channel
    df["campaign_type"] = df["rule_name"].apply(_classify_campaign_type)
    df["channel"] = df["activity_type_id"].apply(_classify_channel)

    log.info(
        f"STEP 1: {len(df)} campanhas encontradas "
        f"({df['is_active'].sum()} ativas, "
        f"{len(df['campaign_type'].unique())} tipos)."
    )

    return df


# ===========================================================================
# STEP 2: Extrair funil de conversao (BigQuery)
# ===========================================================================

def step2_extract_funnel(target_date: date) -> pd.DataFrame:
    """
    Extrai funil de conversao por campanha (engagement_uid) do BigQuery.
    Mapeia fact_type_id:
      1 = segmentados (enviados)
      2 = msg_entregues
      3 = msg_abertos
      4 = msg_clicados
      5 = convertidos

    Args:
        target_date: data de referencia (D-1). Filtra fact_date desse dia.

    Returns:
        DataFrame com colunas: engagement_uid, enviados, entregues,
        abertos, clicados, convertidos, users_enviados
    """
    log.info(f"STEP 2: Extraindo funil de conversao para {target_date}...")

    # fact_date no BigQuery usa TIMESTAMP; filtramos pelo dia inteiro
    sql = f"""
    SELECT
        engagement_uid,
        COUNTIF(fact_type_id = 1) AS enviados,
        COUNTIF(fact_type_id = 2) AS entregues,
        COUNTIF(fact_type_id = 3) AS abertos,
        COUNTIF(fact_type_id = 4) AS clicados,
        COUNTIF(fact_type_id = 5) AS convertidos,
        COUNT(DISTINCT CASE WHEN fact_type_id = 1 THEN user_ext_id END) AS users_enviados
    FROM `{BQ_DATASET}.j_communication`
    WHERE DATE(fact_date) = '{target_date}'
    GROUP BY engagement_uid
    """

    df = query_bigquery(sql)

    log.info(
        f"STEP 2: Funil extraido para {len(df)} campanhas. "
        f"Total enviados: {df['enviados'].sum() if not df.empty else 0}."
    )

    return df


# ===========================================================================
# STEP 3: Extrair bonus e custos (BigQuery)
# ===========================================================================

def step3_extract_bonuses(target_date: date) -> pd.DataFrame:
    """
    Extrai bonus creditados (claimed) por campanha (entity_id) do BigQuery.
    Filtra bonus_status_id = 3 (Claimed/Creditado).

    REGRA: Duplo filtro entity_id + label_bonus_template_id quando
    disponivel, para evitar contagem inflada (caso Multiverso/RETEM).
    Nesta versao simplificada, filtramos por entity_id que e o grouping
    natural do CRM.

    Args:
        target_date: data de referencia

    Returns:
        DataFrame com colunas: entity_id, cumpriram_condicao,
        custo_bonus_brl, users_bonus (distinct)
    """
    log.info(f"STEP 3: Extraindo bonus creditados para {target_date}...")

    sql = f"""
    SELECT
        CAST(entity_id AS STRING) AS entity_id,
        COUNT(DISTINCT user_ext_id) AS cumpriram_condicao,
        SUM(CAST(bonus_cost_value AS FLOAT64)) AS custo_bonus_brl,
        COUNT(DISTINCT user_ext_id) AS users_bonus
    FROM `{BQ_DATASET}.j_bonuses`
    WHERE bonus_status_id = 3
      AND DATE(fact_date) = '{target_date}'
      AND entity_id IS NOT NULL
    GROUP BY entity_id
    """

    df = query_bigquery(sql)

    log.info(
        f"STEP 3: Bonus extraidos para {len(df)} campanhas. "
        f"Total custo: R$ {df['custo_bonus_brl'].sum():.2f}" if not df.empty else
        "STEP 3: Nenhum bonus encontrado."
    )

    return df


# ===========================================================================
# STEP 4: Extrair custos de disparo (BigQuery)
# ===========================================================================

def step4_extract_dispatch_costs(target_date: date) -> pd.DataFrame:
    """
    Extrai volume de disparos por canal e provedor para calcular custos.

    Custos por provedor (confirmados com CRM):
      - 1536 (DisparoPro SMS): R$ 0,045
      - 1545 (PushFY SMS):     R$ 0,060
      - 1268 (Comtele SMS):    R$ 0,063
      - WhatsApp (act=64):     R$ 0,16
      - Push (act=30/40):      R$ 0,060

    Args:
        target_date: data de referencia

    Returns:
        DataFrame com colunas: channel, provider, total_sent,
        custo_unitario, custo_total_brl
    """
    log.info(f"STEP 4: Extraindo custos de disparo para {target_date}...")

    sql = f"""
    SELECT
        activity_type_id,
        label_provider_id,
        COUNT(*) AS total_sent
    FROM `{BQ_DATASET}.j_communication`
    WHERE fact_type_id = 1
      AND DATE(fact_date) = '{target_date}'
    GROUP BY activity_type_id, label_provider_id
    """

    df_raw = query_bigquery(sql)

    if df_raw.empty:
        log.warning("STEP 4: Nenhum disparo encontrado.")
        return pd.DataFrame(columns=[
            "channel", "provider", "total_sent",
            "custo_unitario", "custo_total_brl",
        ])

    # Mapear para canal + provedor + custo
    rows = []
    for _, r in df_raw.iterrows():
        act_id = int(r["activity_type_id"]) if pd.notna(r["activity_type_id"]) else 0
        prov_id = int(r["label_provider_id"]) if pd.notna(r["label_provider_id"]) else 0
        total = int(r["total_sent"])

        channel = _classify_channel(act_id)

        # Determinar provedor e custo unitario
        if act_id == 60:  # SMS
            prov_key = PROVIDER_ID_MAP.get(prov_id, "sms_outros")
            provider = prov_key
            custo_unit = CUSTO_POR_PROVEDOR.get(prov_key, 0.045)
        elif act_id == 64:  # WhatsApp
            provider = "whatsapp_loyalty"
            custo_unit = CUSTO_POR_PROVEDOR["whatsapp"]
        elif act_id in (30, 40):  # Push
            provider = "pushfy"
            custo_unit = CUSTO_POR_PROVEDOR["push"]
        else:
            provider = f"other_{prov_id}"
            custo_unit = 0.0

        rows.append({
            "channel": channel,
            "provider": provider,
            "total_sent": total,
            "custo_unitario": custo_unit,
            "custo_total_brl": round(total * custo_unit, 2),
        })

    df_costs = pd.DataFrame(rows)

    total_custo = df_costs["custo_total_brl"].sum()
    total_envios = df_costs["total_sent"].sum()
    log.info(
        f"STEP 4: {_fmt_int(total_envios)} disparos, "
        f"custo total: {_fmt_brl(total_custo)}."
    )

    return df_costs


# ===========================================================================
# STEP 5: Extrair coorte de users e metricas financeiras (Athena)
# ===========================================================================

def _get_cohort_user_ext_ids(target_date: date) -> dict[str, list[str]]:
    """
    Extrai a coorte de user_ext_ids por campanha (engagement_uid)
    a partir do j_communication do BigQuery.

    Retorna dict: engagement_uid -> lista de user_ext_id
    """
    log.info(f"STEP 5a: Extraindo coortes de users por campanha ({target_date})...")

    sql = f"""
    SELECT
        CAST(engagement_uid AS STRING) AS engagement_uid,
        CAST(user_ext_id AS STRING) AS user_ext_id
    FROM `{BQ_DATASET}.j_communication`
    WHERE fact_type_id = 1
      AND DATE(fact_date) = '{target_date}'
      AND user_ext_id IS NOT NULL
    """

    df = query_bigquery(sql)

    if df.empty:
        log.warning("STEP 5a: Nenhum user encontrado nas coortes.")
        return {}

    # Agrupar por engagement_uid
    cohorts = {}
    for uid, group in df.groupby("engagement_uid"):
        cohorts[str(uid)] = group["user_ext_id"].unique().tolist()

    total_users = sum(len(v) for v in cohorts.values())
    log.info(
        f"STEP 5a: {len(cohorts)} campanhas, "
        f"{_fmt_int(total_users)} users unicos no total."
    )

    return cohorts


def _build_all_ext_ids(cohorts: dict[str, list[str]]) -> list[str]:
    """Consolida todos os user_ext_ids unicos de todas as coortes."""
    all_ids = set()
    for ids in cohorts.values():
        all_ids.update(ids)
    return list(all_ids)


def step5_extract_financials(
    target_date: date,
    cohorts: dict[str, list[str]],
) -> pd.DataFrame:
    """
    Para a coorte consolidada de users, busca metricas financeiras no Athena
    via ps_bi.fct_player_activity_daily.

    Bridge de IDs: BigQuery user_ext_id = Athena ps_bi.dim_user.external_id

    REGRAS:
      - ps_bi ja esta em BRL reais (NAO dividir por 100)
      - is_test = false para excluir test users
      - Timestamps em UTC; activity_date e DATE (sem timezone)

    Args:
        target_date: data de referencia (D-1)
        cohorts: dict engagement_uid -> [user_ext_id]

    Returns:
        DataFrame com external_id + metricas financeiras do dia
    """
    all_ext_ids = _build_all_ext_ids(cohorts)

    if not all_ext_ids:
        log.warning("STEP 5: Coorte vazia, pulando financeiro.")
        return pd.DataFrame()

    log.info(
        f"STEP 5: Extraindo financeiro para {_fmt_int(len(all_ext_ids))} "
        f"users unicos no Athena (ps_bi) em {target_date}..."
    )

    # Para coortes grandes, processamos em batches para evitar
    # ultrapassar o limite de IN clause do Athena
    all_results = []

    for chunk_idx, chunk in enumerate(_chunk_list(all_ext_ids, BATCH_SIZE_IN_CLAUSE)):
        # Formata IDs como strings entre aspas para a IN clause
        ids_csv = ",".join(f"'{eid}'" for eid in chunk)

        sql = f"""
        SELECT
            d.external_id,
            p.activity_date,
            COALESCE(p.casino_ggr, 0)              AS casino_ggr,
            COALESCE(p.sportsbook_ggr, 0)           AS sportsbook_ggr,
            COALESCE(p.total_deposit_amount, 0)      AS total_deposit,
            COALESCE(p.total_withdrawal_amount, 0)   AS total_withdrawal,
            COALESCE(p.casino_turnover, 0)           AS casino_turnover,
            COALESCE(p.sportsbook_turnover, 0)       AS sportsbook_turnover,
            COALESCE(p.login_count, 0)               AS login_count
        FROM ps_bi.fct_player_activity_daily p
        JOIN ps_bi.dim_user d ON d.user_id = p.user_id
        WHERE d.is_test = false
          AND CAST(d.external_id AS VARCHAR) IN ({ids_csv})
          AND p.activity_date = DATE '{target_date}'
        """

        df_chunk = query_athena(sql, database="ps_bi")
        all_results.append(df_chunk)

        if len(_chunk_list(all_ext_ids, BATCH_SIZE_IN_CLAUSE)) > 1:
            log.info(
                f"  Batch {chunk_idx + 1}/{len(_chunk_list(all_ext_ids, BATCH_SIZE_IN_CLAUSE))}: "
                f"{len(df_chunk)} linhas."
            )

    if not all_results:
        return pd.DataFrame()

    df = pd.concat(all_results, ignore_index=True)
    df = _df_to_float(df)

    log.info(
        f"STEP 5: Financeiro extraido para {len(df)} player-days. "
        f"Casino GGR total: {_fmt_brl(df['casino_ggr'].sum())}."
    )

    return df


def _aggregate_financials_per_campaign(
    df_fin: pd.DataFrame,
    cohorts: dict[str, list[str]],
) -> dict[str, dict]:
    """
    Agrega metricas financeiras por campanha com base na coorte.

    Returns:
        dict: engagement_uid -> {casino_ggr, sportsbook_ggr, total_ggr,
              total_deposit, total_withdrawal, net_deposit, casino_turnover,
              sportsbook_turnover, login_count, coorte_users}
    """
    if df_fin.empty:
        return {}

    result = {}

    for uid, ext_ids in cohorts.items():
        ext_ids_set = set(str(e) for e in ext_ids)

        # Filtrar DataFrame para esta coorte
        mask = df_fin["external_id"].astype(str).isin(ext_ids_set)
        df_camp = df_fin[mask]

        if df_camp.empty:
            result[uid] = {
                "coorte_users": 0, "casino_ggr": 0, "sportsbook_ggr": 0,
                "total_ggr": 0, "total_deposit": 0, "total_withdrawal": 0,
                "net_deposit": 0, "casino_turnover": 0,
                "sportsbook_turnover": 0, "login_count": 0,
            }
            continue

        casino_ggr = float(df_camp["casino_ggr"].sum())
        sb_ggr = float(df_camp["sportsbook_ggr"].sum())
        total_dep = float(df_camp["total_deposit"].sum())
        total_wd = float(df_camp["total_withdrawal"].sum())

        result[uid] = {
            "coorte_users": int(df_camp["external_id"].nunique()),
            "casino_ggr": round(casino_ggr, 2),
            "sportsbook_ggr": round(sb_ggr, 2),
            "total_ggr": round(casino_ggr + sb_ggr, 2),
            "total_deposit": round(total_dep, 2),
            "total_withdrawal": round(total_wd, 2),
            "net_deposit": round(total_dep - total_wd, 2),
            "casino_turnover": round(float(df_camp["casino_turnover"].sum()), 2),
            "sportsbook_turnover": round(float(df_camp["sportsbook_turnover"].sum()), 2),
            "login_count": int(df_camp["login_count"].sum()),
        }

    return result


# ===========================================================================
# STEP 6: Top jogos da coorte (Athena)
# ===========================================================================

def step6_extract_top_games(
    target_date: date,
    all_ext_ids: list[str],
    top_n: int = 20,
) -> pd.DataFrame:
    """
    Extrai os top jogos (por turnover) da coorte no dia, usando
    ps_bi.fct_casino_activity_daily.

    NOTA: ps_bi.dim_game tem cobertura incompleta (0.2%, ~414 jogos).
    Para ranking completo de vendors, usar bireports_ec2. Mas para
    jogos da coorte (contexto CRM), ps_bi e suficiente na maioria.

    Args:
        target_date: data de referencia
        all_ext_ids: lista consolidada de user_ext_ids
        top_n: quantidade de jogos no ranking

    Returns:
        DataFrame com game_id, game_name, users, turnover_brl, ggr_brl
    """
    if not all_ext_ids:
        log.warning("STEP 6: Coorte vazia, pulando top jogos.")
        return pd.DataFrame()

    log.info(f"STEP 6: Extraindo top {top_n} jogos da coorte ({target_date})...")

    # Processar em batch se coorte for muito grande
    # Para top jogos, usamos apenas o primeiro batch (amostra representativa)
    # ou consolidamos todos
    sample_ids = all_ext_ids[:BATCH_SIZE_IN_CLAUSE]
    ids_csv = ",".join(f"'{eid}'" for eid in sample_ids)

    sql = f"""
    SELECT
        c.game_id,
        COALESCE(g.game_name, CAST(c.game_id AS VARCHAR)) AS game_name,
        COUNT(DISTINCT d.external_id) AS users,
        SUM(COALESCE(c.casino_turnover, 0)) AS turnover_brl,
        SUM(COALESCE(c.casino_ggr, 0)) AS ggr_brl
    FROM ps_bi.fct_casino_activity_daily c
    JOIN ps_bi.dim_user d ON d.user_id = c.user_id
    LEFT JOIN ps_bi.dim_game g ON g.game_id = c.game_id
    WHERE d.is_test = false
      AND CAST(d.external_id AS VARCHAR) IN ({ids_csv})
      AND c.activity_date = DATE '{target_date}'
    GROUP BY c.game_id, COALESCE(g.game_name, CAST(c.game_id AS VARCHAR))
    ORDER BY turnover_brl DESC
    LIMIT {top_n}
    """

    df = query_athena(sql, database="ps_bi")
    df = _df_to_float(df)

    if not df.empty:
        log.info(
            f"STEP 6: Top {len(df)} jogos extraidos. "
            f"#1: {df.iloc[0]['game_name']} "
            f"(turnover: {_fmt_brl(df.iloc[0]['turnover_brl'])})."
        )
    else:
        log.warning("STEP 6: Nenhum jogo encontrado para a coorte.")

    return df


# ===========================================================================
# STEP 7: Comparativo antes/durante/depois (Athena)
# ===========================================================================

def step7_extract_comparison(
    target_date: date,
    all_ext_ids: list[str],
    dias_antes: int = 30,
    dias_depois: int = 3,
) -> dict[str, dict]:
    """
    Calcula metricas comparativas em 3 janelas para a coorte:
      - BEFORE: baseline = mesma quantidade de dias ANTES da campanha
      - DURING: dia da campanha (target_date)
      - AFTER:  D+1 a D+dias_depois (se disponivel)

    Para o report diario, simplificamos:
      - BEFORE = D-31 a D-1 (M-1 como baseline)
      - DURING = target_date (o dia corrente do report)
      - AFTER  = nao disponivel no dia (sera preenchido D+3)

    NOTA: a logica completa de BEFORE/DURING/AFTER por campanha esta no
    pipeline crm_daily_performance.py (v2). Aqui usamos uma versao
    simplificada orientada a dia.

    Args:
        target_date: data de referencia
        all_ext_ids: lista consolidada de user_ext_ids
        dias_antes: janela de baseline em dias
        dias_depois: janela pos-campanha em dias

    Returns:
        dict com chaves 'BEFORE', 'DURING' e opcionalmente 'AFTER',
        cada uma com metricas agregadas da coorte
    """
    if not all_ext_ids:
        log.warning("STEP 7: Coorte vazia, pulando comparativo.")
        return {}

    log.info(f"STEP 7: Calculando comparativo para coorte ({target_date})...")

    # Janelas
    before_start = target_date - timedelta(days=dias_antes)
    before_end = target_date - timedelta(days=1)
    during_start = target_date
    during_end = target_date

    # AFTER: so se ja passou o prazo (target_date + dias_depois < hoje)
    after_start = target_date + timedelta(days=1)
    after_end = target_date + timedelta(days=dias_depois)
    ontem = date.today() - timedelta(days=1)
    include_after = after_end <= ontem

    # Construir periodos
    periods = {
        "BEFORE": (before_start, before_end),
        "DURING": (during_start, during_end),
    }
    if include_after:
        periods["AFTER"] = (after_start, min(after_end, ontem))

    # Sample da coorte (para performance)
    sample_ids = all_ext_ids[:BATCH_SIZE_IN_CLAUSE]
    ids_csv = ",".join(f"'{eid}'" for eid in sample_ids)

    # Range total de datas
    all_dates = [d for s, e in periods.values() for d in (s, e)]
    min_dt = min(all_dates)
    max_dt = max(all_dates)

    # Construir CASE WHEN
    case_lines = []
    for period_name, (d1, d2) in periods.items():
        case_lines.append(
            f"WHEN p.activity_date BETWEEN DATE '{d1}' AND DATE '{d2}' "
            f"THEN '{period_name}'"
        )
    case_sql = "\n            ".join(case_lines)

    sql = f"""
    SELECT
        period,
        COUNT(DISTINCT d.external_id)         AS coorte_users,
        SUM(COALESCE(p.casino_ggr, 0)
          + COALESCE(p.sportsbook_ggr, 0))    AS total_ggr,
        SUM(COALESCE(p.total_deposit_amount, 0))  AS total_deposit,
        SUM(COALESCE(p.total_deposit_amount, 0)
          - COALESCE(p.total_withdrawal_amount, 0)) AS net_deposit,
        SUM(COALESCE(p.casino_turnover, 0))   AS casino_turnover,
        SUM(COALESCE(p.login_count, 0))       AS login_count
    FROM (
        SELECT
            CASE
                {case_sql}
            END AS period,
            p.*
        FROM ps_bi.fct_player_activity_daily p
        WHERE p.activity_date BETWEEN DATE '{min_dt}' AND DATE '{max_dt}'
    ) p
    JOIN ps_bi.dim_user d ON d.user_id = p.user_id
    WHERE d.is_test = false
      AND CAST(d.external_id AS VARCHAR) IN ({ids_csv})
      AND period IS NOT NULL
    GROUP BY period
    """

    df = query_athena(sql, database="ps_bi")
    df = _df_to_float(df)

    result = {}
    for _, row in df.iterrows():
        p = row["period"]
        result[p] = {
            "period_start": str(periods[p][0]),
            "period_end": str(periods[p][1]),
            "coorte_users": int(row.get("coorte_users", 0)),
            "total_ggr": round(float(row.get("total_ggr", 0)), 2),
            "total_deposit": round(float(row.get("total_deposit", 0)), 2),
            "net_deposit": round(float(row.get("net_deposit", 0)), 2),
            "casino_turnover": round(float(row.get("casino_turnover", 0)), 2),
            "login_count": int(row.get("login_count", 0)),
        }

    # Calcular deltas DURING vs BEFORE
    if "BEFORE" in result and "DURING" in result:
        before_ggr = result["BEFORE"]["total_ggr"]
        during_ggr = result["DURING"]["total_ggr"]

        # Normalizar: BEFORE e range de N dias, DURING e 1 dia
        # Para comparar justo, calculamos media diaria do BEFORE
        before_days = (before_end - before_start).days + 1
        before_daily_avg = before_ggr / before_days if before_days > 0 else 0

        delta = round(during_ggr - before_daily_avg, 2)
        delta_pct = round(
            _safe_div(delta, abs(before_daily_avg)) * 100, 4
        )

        result["DURING"]["ggr_delta"] = delta
        result["DURING"]["ggr_delta_pct"] = delta_pct

    for period_name, data in result.items():
        log.info(f"  {period_name}: GGR={_fmt_brl(data['total_ggr'])}, "
                 f"Users={_fmt_int(data['coorte_users'])}")

    return result


# ===========================================================================
# STEP 8: VIP Groups (calculado a partir dos dados financeiros)
# ===========================================================================

def step8_calculate_vip_groups(
    df_fin: pd.DataFrame,
    cohorts: dict[str, list[str]],
) -> dict[str, list[dict]]:
    """
    Classifica users de cada campanha em faixas VIP por NGR acumulado.

    Faixas:
      - Elite:       NGR >= R$ 10.000
      - Key Account: NGR >= R$  5.000
      - High Value:  NGR >= R$  3.000
      - Standard:    Demais

    Args:
        df_fin: DataFrame financeiro do Step 5
        cohorts: dict engagement_uid -> [user_ext_id]

    Returns:
        dict: engagement_uid -> [{"vip_tier": ..., "coorte_users": ...,
              "total_ggr": ..., "total_deposit": ..., "casino_turnover": ...}]
    """
    if df_fin.empty:
        return {}

    log.info("STEP 8: Calculando VIP groups por campanha...")

    result = {}

    for uid, ext_ids in cohorts.items():
        ext_ids_set = set(str(e) for e in ext_ids)
        mask = df_fin["external_id"].astype(str).isin(ext_ids_set)
        df_camp = df_fin[mask]

        if df_camp.empty:
            result[uid] = []
            continue

        # Calcular GGR por player (proxy para NGR neste contexto diario)
        df_player = df_camp.groupby("external_id").agg({
            "casino_ggr": "sum",
            "sportsbook_ggr": "sum",
            "total_deposit": "sum",
            "casino_turnover": "sum",
        }).reset_index()

        df_player["total_ggr"] = df_player["casino_ggr"] + df_player["sportsbook_ggr"]

        # Classificar em tiers
        tiers = []
        for tier_name, min_val in VIP_TIERS:
            if tier_name == "Standard":
                tier_mask = df_player["total_ggr"] < VIP_TIERS[-2][1]  # Abaixo do tier anterior
            else:
                tier_mask = df_player["total_ggr"] >= min_val
                # Excluir os que ja foram classificados em tier superior
                for higher_name, higher_val in VIP_TIERS:
                    if higher_val > min_val:
                        tier_mask = tier_mask & (df_player["total_ggr"] < higher_val)
                    if higher_name == tier_name:
                        break

            df_tier = df_player[tier_mask]

            if not df_tier.empty:
                tiers.append({
                    "vip_tier": tier_name,
                    "coorte_users": len(df_tier),
                    "total_ggr": round(float(df_tier["total_ggr"].sum()), 2),
                    "total_deposit": round(float(df_tier["total_deposit"].sum()), 2),
                    "casino_turnover": round(float(df_tier["casino_turnover"].sum()), 2),
                })

        result[uid] = tiers

    total_vip = sum(
        sum(t["coorte_users"] for t in tiers if t["vip_tier"] != "Standard")
        for tiers in result.values()
    )
    log.info(f"STEP 8: {_fmt_int(total_vip)} users em tiers VIP (Elite+Key+High).")

    return result


# ===========================================================================
# STEP 9: Recovery / Reengajamento (BigQuery + Athena)
# ===========================================================================

def step9_extract_recovery(
    target_date: date,
    cohorts: dict[str, list[str]],
) -> dict[str, dict]:
    """
    Identifica metricas de recuperacao para campanhas de tipo RETEM/Recuperacao.
    Compara users inativos que foram alvos vs. os que reengajaram (depositaram).

    Simplificacao: no contexto diario, contamos quantos da coorte estavam
    inativos (sem login nos ultimos 7 dias) e quantos depositaram no target_date.

    Args:
        target_date: data de referencia
        cohorts: dict engagement_uid -> [user_ext_id]

    Returns:
        dict: engagement_uid -> {inativos_alvo, reengajados, depositaram, ...}
    """
    all_ext_ids = _build_all_ext_ids(cohorts)

    if not all_ext_ids:
        return {}

    log.info(f"STEP 9: Calculando metricas de recuperacao ({target_date})...")

    # Janela de inatividade: 7 dias antes do target
    inativo_start = target_date - timedelta(days=7)
    inativo_end = target_date - timedelta(days=1)

    sample_ids = all_ext_ids[:BATCH_SIZE_IN_CLAUSE]
    ids_csv = ",".join(f"'{eid}'" for eid in sample_ids)

    # Verificar quem teve login nos 7 dias anteriores
    sql = f"""
    WITH coorte AS (
        SELECT
            d.external_id,
            d.user_id
        FROM ps_bi.dim_user d
        WHERE d.is_test = false
          AND CAST(d.external_id AS VARCHAR) IN ({ids_csv})
    ),
    atividade_recente AS (
        SELECT
            c.external_id,
            SUM(COALESCE(p.login_count, 0)) AS logins_7d,
            SUM(COALESCE(p.total_deposit_amount, 0)) AS depositos_7d
        FROM coorte c
        LEFT JOIN ps_bi.fct_player_activity_daily p
            ON p.user_id = c.user_id
            AND p.activity_date BETWEEN DATE '{inativo_start}' AND DATE '{inativo_end}'
        GROUP BY c.external_id
    ),
    atividade_hoje AS (
        SELECT
            c.external_id,
            SUM(COALESCE(p.login_count, 0)) AS logins_hoje,
            SUM(COALESCE(p.total_deposit_amount, 0)) AS deposito_hoje
        FROM coorte c
        LEFT JOIN ps_bi.fct_player_activity_daily p
            ON p.user_id = c.user_id
            AND p.activity_date = DATE '{target_date}'
        GROUP BY c.external_id
    )
    SELECT
        ar.external_id,
        COALESCE(ar.logins_7d, 0) AS logins_7d,
        COALESCE(ah.logins_hoje, 0) AS logins_hoje,
        COALESCE(ah.deposito_hoje, 0) AS deposito_hoje
    FROM atividade_recente ar
    LEFT JOIN atividade_hoje ah ON ah.external_id = ar.external_id
    """

    df_recovery = query_athena(sql, database="ps_bi")
    df_recovery = _df_to_float(df_recovery)

    if df_recovery.empty:
        return {}

    # Calcular por campanha
    result = {}
    for uid, ext_ids in cohorts.items():
        ext_ids_set = set(str(e) for e in ext_ids)
        mask = df_recovery["external_id"].astype(str).isin(ext_ids_set)
        df_camp = df_recovery[mask]

        if df_camp.empty:
            result[uid] = {
                "inativos_alvo": 0, "reengajados": 0,
                "depositaram": 0, "churn_d7": 0, "churn_d7_pct": 0,
            }
            continue

        # Inativos = sem login nos 7 dias anteriores
        inativos = df_camp[df_camp["logins_7d"] == 0]
        inativos_alvo = len(inativos)

        # Reengajados = inativos que logaram hoje
        reengajados = len(inativos[inativos["logins_hoje"] > 0])

        # Depositaram = inativos que depositaram hoje
        depositaram = len(inativos[inativos["deposito_hoje"] > 0])

        # Churn D+7: dos que foram alvos, quantos nao logaram
        # (simplificado: contamos inativos que continuam sem login)
        churn_d7 = inativos_alvo - reengajados
        churn_d7_pct = round(_safe_div(churn_d7, inativos_alvo) * 100, 4)

        result[uid] = {
            "inativos_alvo": inativos_alvo,
            "reengajados": reengajados,
            "depositaram": depositaram,
            "churn_d7": churn_d7,
            "churn_d7_pct": churn_d7_pct,
        }

    total_reeng = sum(v["reengajados"] for v in result.values())
    log.info(f"STEP 9: {_fmt_int(total_reeng)} users reengajados no total.")

    return result


# ===========================================================================
# STEP 10: Persistir no Super Nova DB
# ===========================================================================

def step10_persist_campaign_daily(
    target_date: date,
    df_campaigns: pd.DataFrame,
    df_funnel: pd.DataFrame,
    df_bonuses: pd.DataFrame,
    financials_per_campaign: dict[str, dict],
    df_dispatch_costs: pd.DataFrame,
    dry_run: bool = False,
):
    """
    Persiste os dados consolidados na tabela crm_campaign_daily.
    Usa ON CONFLICT DO UPDATE para idempotencia.
    """
    if df_campaigns.empty:
        log.warning("STEP 10: Nenhuma campanha para persistir.")
        return

    log.info(
        f"STEP 10: Persistindo {len(df_campaigns)} campanhas em "
        f"crm_campaign_daily ({target_date})..."
    )

    # Preparar lookup de funil por engagement_uid (rule_id como string)
    funnel_map = {}
    if not df_funnel.empty:
        for _, row in df_funnel.iterrows():
            funnel_map[str(row["engagement_uid"])] = {
                "enviados": int(row.get("enviados", 0)),
                "entregues": int(row.get("entregues", 0)),
                "abertos": int(row.get("abertos", 0)),
                "clicados": int(row.get("clicados", 0)),
                "convertidos": int(row.get("convertidos", 0)),
            }

    # Preparar lookup de bonus por entity_id
    bonus_map = {}
    if not df_bonuses.empty:
        for _, row in df_bonuses.iterrows():
            bonus_map[str(row["entity_id"])] = {
                "cumpriram_condicao": int(row.get("cumpriram_condicao", 0)),
                "custo_bonus_brl": round(float(row.get("custo_bonus_brl", 0)), 2),
            }

    # Custo total de disparo
    custo_disparo_total = df_dispatch_costs["custo_total_brl"].sum() if not df_dispatch_costs.empty else 0
    custo_detalhe = {}
    if not df_dispatch_costs.empty:
        for _, row in df_dispatch_costs.iterrows():
            key = f"{row['channel']}_{row['provider']}"
            custo_detalhe[key] = {
                "sent": int(row["total_sent"]),
                "custo": round(float(row["custo_total_brl"]), 2),
            }

    # Construir records
    records = []
    for _, camp in df_campaigns.iterrows():
        rule_id = int(camp["rule_id"])
        rule_id_str = str(rule_id)

        funnel = funnel_map.get(rule_id_str, {})
        bonus = bonus_map.get(rule_id_str, {})
        fin = financials_per_campaign.get(rule_id_str, {})

        # KPIs derivados
        coorte_users = fin.get("coorte_users", 0)
        total_ggr = fin.get("total_ggr", 0)
        custo_bonus = bonus.get("custo_bonus_brl", 0)

        # ROI = (GGR - Custo Bonus - Custo Disparo) / (Custo Bonus + Custo Disparo)
        custo_total = custo_bonus + custo_disparo_total
        roi = round(_safe_div(total_ggr - custo_total, custo_total), 4) if custo_total > 0 else None

        # CPA = Custo Total / Convertidos
        convertidos = funnel.get("convertidos", 0)
        cpa = round(_safe_div(custo_total, convertidos), 2) if convertidos > 0 else None

        # ARPU = GGR / Users ativos
        arpu = round(_safe_div(total_ggr, coorte_users), 2) if coorte_users > 0 else None

        records.append((
            str(target_date),                              # report_date
            rule_id,                                       # rule_id
            camp.get("rule_name", ""),                     # rule_name
            camp.get("campaign_type", "Outro"),            # campaign_type
            camp.get("channel", ""),                       # channel
            camp.get("segment_id"),                        # segment_id
            camp.get("segment_name", ""),                  # segment_name
            bool(camp.get("is_active", True)),             # is_active
            camp.get("bo_user_email", ""),                 # bo_user_email
            funnel.get("enviados", 0),                     # enviados
            funnel.get("entregues", 0),                    # entregues
            funnel.get("abertos", 0),                      # abertos
            funnel.get("clicados", 0),                     # clicados
            funnel.get("convertidos", 0),                  # convertidos
            bonus.get("cumpriram_condicao", 0),            # cumpriram_condicao
            custo_bonus,                                   # custo_bonus_brl
            coorte_users,                                  # coorte_users
            fin.get("casino_ggr", 0),                      # casino_ggr
            fin.get("sportsbook_ggr", 0),                  # sportsbook_ggr
            total_ggr,                                     # total_ggr
            fin.get("total_deposit", 0),                   # total_deposit
            fin.get("total_withdrawal", 0),                # total_withdrawal
            fin.get("net_deposit", 0),                     # net_deposit
            fin.get("casino_turnover", 0),                 # casino_turnover
            fin.get("sportsbook_turnover", 0),             # sportsbook_turnover
            fin.get("login_count", 0),                     # login_count
            round(custo_disparo_total, 2),                 # custo_disparo_brl
            json.dumps(custo_detalhe, ensure_ascii=False), # custo_detalhe
            roi,                                           # roi
            cpa,                                           # cpa
            arpu,                                          # arpu
        ))

    if dry_run:
        log.info(f"  DRY-RUN: {len(records)} registros preparados (nao persistidos).")
        for rec in records[:3]:
            log.info(f"    rule_id={rec[1]}, name={rec[2][:50]}, "
                     f"ggr={_fmt_brl(rec[19])}, enviados={rec[9]}")
        if len(records) > 3:
            log.info(f"    ... e mais {len(records) - 3} registros.")
        return

    # UPSERT
    upsert_sql = """
    INSERT INTO multibet.crm_campaign_daily (
        report_date, rule_id, rule_name, campaign_type, channel,
        segment_id, segment_name, is_active, bo_user_email,
        enviados, entregues, abertos, clicados, convertidos,
        cumpriram_condicao, custo_bonus_brl,
        coorte_users, casino_ggr, sportsbook_ggr, total_ggr,
        total_deposit, total_withdrawal, net_deposit,
        casino_turnover, sportsbook_turnover, login_count,
        custo_disparo_brl, custo_detalhe,
        roi, cpa, arpu,
        created_at, updated_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        NOW(), NOW()
    )
    ON CONFLICT (report_date, rule_id)
    DO UPDATE SET
        rule_name           = EXCLUDED.rule_name,
        campaign_type       = EXCLUDED.campaign_type,
        channel             = EXCLUDED.channel,
        segment_id          = EXCLUDED.segment_id,
        segment_name        = EXCLUDED.segment_name,
        is_active           = EXCLUDED.is_active,
        bo_user_email       = EXCLUDED.bo_user_email,
        enviados            = EXCLUDED.enviados,
        entregues           = EXCLUDED.entregues,
        abertos             = EXCLUDED.abertos,
        clicados            = EXCLUDED.clicados,
        convertidos         = EXCLUDED.convertidos,
        cumpriram_condicao  = EXCLUDED.cumpriram_condicao,
        custo_bonus_brl     = EXCLUDED.custo_bonus_brl,
        coorte_users        = EXCLUDED.coorte_users,
        casino_ggr          = EXCLUDED.casino_ggr,
        sportsbook_ggr      = EXCLUDED.sportsbook_ggr,
        total_ggr           = EXCLUDED.total_ggr,
        total_deposit       = EXCLUDED.total_deposit,
        total_withdrawal    = EXCLUDED.total_withdrawal,
        net_deposit         = EXCLUDED.net_deposit,
        casino_turnover     = EXCLUDED.casino_turnover,
        sportsbook_turnover = EXCLUDED.sportsbook_turnover,
        login_count         = EXCLUDED.login_count,
        custo_disparo_brl   = EXCLUDED.custo_disparo_brl,
        custo_detalhe       = EXCLUDED.custo_detalhe,
        roi                 = EXCLUDED.roi,
        cpa                 = EXCLUDED.cpa,
        arpu                = EXCLUDED.arpu,
        updated_at          = NOW()
    ;
    """

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for rec in records:
                cur.execute(upsert_sql, rec)
        conn.commit()
        log.info(f"  crm_campaign_daily: {len(records)} registros upserted.")
    except Exception as e:
        conn.rollback()
        log.error(f"  Erro ao persistir crm_campaign_daily: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


def step10_persist_games(
    target_date: date,
    df_games: pd.DataFrame,
    cohorts: dict[str, list[str]],
    dry_run: bool = False,
):
    """
    Persiste top jogos na tabela crm_campaign_game_daily.
    Como os jogos sao da coorte consolidada, insere para cada campanha.
    """
    if df_games.empty:
        log.info("STEP 10 (games): Nenhum jogo para persistir.")
        return

    # Para simplificar, associamos os top jogos a todas as campanhas
    # (os jogos sao da coorte consolidada)
    rule_ids = list(cohorts.keys())

    if dry_run:
        log.info(
            f"  DRY-RUN: {len(df_games)} jogos x {len(rule_ids)} campanhas "
            f"preparados (nao persistidos)."
        )
        return

    upsert_sql = """
    INSERT INTO multibet.crm_campaign_game_daily (
        report_date, rule_id, game_id, game_name,
        users, turnover_brl, ggr_brl,
        created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (report_date, rule_id, game_id)
    DO UPDATE SET
        game_name    = EXCLUDED.game_name,
        users        = EXCLUDED.users,
        turnover_brl = EXCLUDED.turnover_brl,
        ggr_brl      = EXCLUDED.ggr_brl,
        updated_at   = NOW()
    ;
    """

    tunnel, conn = get_supernova_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            for rule_id in rule_ids:
                for _, game in df_games.iterrows():
                    cur.execute(upsert_sql, (
                        str(target_date),
                        int(rule_id) if rule_id.isdigit() else 0,
                        str(game.get("game_id", "")),
                        str(game.get("game_name", ""))[:500],
                        int(game.get("users", 0)),
                        round(float(game.get("turnover_brl", 0)), 2),
                        round(float(game.get("ggr_brl", 0)), 2),
                    ))
                    count += 1
        conn.commit()
        log.info(f"  crm_campaign_game_daily: {count} registros upserted.")
    except Exception as e:
        conn.rollback()
        log.error(f"  Erro ao persistir crm_campaign_game_daily: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


def step10_persist_dispatch_budget(
    target_date: date,
    df_dispatch: pd.DataFrame,
    dry_run: bool = False,
):
    """Persiste custos de disparo na tabela crm_dispatch_budget."""
    if df_dispatch.empty:
        log.info("STEP 10 (dispatch): Nenhum disparo para persistir.")
        return

    if dry_run:
        log.info(
            f"  DRY-RUN: {len(df_dispatch)} linhas de disparo "
            f"preparadas (nao persistidas)."
        )
        return

    upsert_sql = """
    INSERT INTO multibet.crm_dispatch_budget (
        report_date, channel, provider,
        total_sent, custo_unitario, custo_total_brl,
        created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (report_date, channel, provider)
    DO UPDATE SET
        total_sent      = EXCLUDED.total_sent,
        custo_unitario  = EXCLUDED.custo_unitario,
        custo_total_brl = EXCLUDED.custo_total_brl,
        updated_at      = NOW()
    ;
    """

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for _, row in df_dispatch.iterrows():
                cur.execute(upsert_sql, (
                    str(target_date),
                    str(row["channel"]),
                    str(row["provider"]),
                    int(row["total_sent"]),
                    round(float(row["custo_unitario"]), 4),
                    round(float(row["custo_total_brl"]), 2),
                ))
        conn.commit()
        log.info(f"  crm_dispatch_budget: {len(df_dispatch)} registros upserted.")
    except Exception as e:
        conn.rollback()
        log.error(f"  Erro ao persistir crm_dispatch_budget: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


def step10_persist_vip_groups(
    target_date: date,
    vip_groups: dict[str, list[dict]],
    dry_run: bool = False,
):
    """Persiste VIP groups na tabela crm_vip_group_daily."""
    if not vip_groups:
        log.info("STEP 10 (VIP): Nenhum VIP group para persistir.")
        return

    if dry_run:
        total = sum(len(tiers) for tiers in vip_groups.values())
        log.info(f"  DRY-RUN: {total} linhas VIP preparadas (nao persistidas).")
        return

    upsert_sql = """
    INSERT INTO multibet.crm_vip_group_daily (
        report_date, rule_id, vip_tier,
        coorte_users, total_ggr, total_deposit, casino_turnover,
        created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (report_date, rule_id, vip_tier)
    DO UPDATE SET
        coorte_users    = EXCLUDED.coorte_users,
        total_ggr       = EXCLUDED.total_ggr,
        total_deposit   = EXCLUDED.total_deposit,
        casino_turnover = EXCLUDED.casino_turnover,
        updated_at      = NOW()
    ;
    """

    tunnel, conn = get_supernova_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            for uid, tiers in vip_groups.items():
                rule_id = int(uid) if uid.isdigit() else 0
                for tier in tiers:
                    cur.execute(upsert_sql, (
                        str(target_date),
                        rule_id,
                        tier["vip_tier"],
                        tier["coorte_users"],
                        tier["total_ggr"],
                        tier["total_deposit"],
                        tier["casino_turnover"],
                    ))
                    count += 1
        conn.commit()
        log.info(f"  crm_vip_group_daily: {count} registros upserted.")
    except Exception as e:
        conn.rollback()
        log.error(f"  Erro ao persistir crm_vip_group_daily: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


def step10_persist_comparison(
    comparison: dict[str, dict],
    cohorts: dict[str, list[str]],
    dry_run: bool = False,
):
    """Persiste comparativo na tabela crm_campaign_comparison."""
    if not comparison:
        log.info("STEP 10 (comparison): Nenhum comparativo para persistir.")
        return

    if dry_run:
        log.info(
            f"  DRY-RUN: {len(comparison)} periodos de comparativo "
            f"preparados (nao persistidos)."
        )
        return

    upsert_sql = """
    INSERT INTO multibet.crm_campaign_comparison (
        rule_id, period, period_start, period_end,
        coorte_users, total_ggr, total_deposit, net_deposit,
        casino_turnover, login_count,
        ggr_delta, ggr_delta_pct,
        created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (rule_id, period)
    DO UPDATE SET
        period_start    = EXCLUDED.period_start,
        period_end      = EXCLUDED.period_end,
        coorte_users    = EXCLUDED.coorte_users,
        total_ggr       = EXCLUDED.total_ggr,
        total_deposit   = EXCLUDED.total_deposit,
        net_deposit     = EXCLUDED.net_deposit,
        casino_turnover = EXCLUDED.casino_turnover,
        login_count     = EXCLUDED.login_count,
        ggr_delta       = EXCLUDED.ggr_delta,
        ggr_delta_pct   = EXCLUDED.ggr_delta_pct,
        updated_at      = NOW()
    ;
    """

    # Usar rule_id=0 para comparativo consolidado (todas as campanhas)
    # Em futuras versoes, pode-se iterar por campanha
    rule_id = 0

    tunnel, conn = get_supernova_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            for period_name, data in comparison.items():
                cur.execute(upsert_sql, (
                    rule_id,
                    period_name,
                    data.get("period_start", ""),
                    data.get("period_end", ""),
                    data.get("coorte_users", 0),
                    data.get("total_ggr", 0),
                    data.get("total_deposit", 0),
                    data.get("net_deposit", 0),
                    data.get("casino_turnover", 0),
                    data.get("login_count", 0),
                    data.get("ggr_delta"),
                    data.get("ggr_delta_pct"),
                ))
                count += 1
        conn.commit()
        log.info(f"  crm_campaign_comparison: {count} registros upserted.")
    except Exception as e:
        conn.rollback()
        log.error(f"  Erro ao persistir crm_campaign_comparison: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


def step10_persist_recovery(
    target_date: date,
    recovery: dict[str, dict],
    dry_run: bool = False,
):
    """Persiste metricas de recuperacao na tabela crm_recovery_daily."""
    if not recovery:
        log.info("STEP 10 (recovery): Nenhuma metrica de recuperacao para persistir.")
        return

    if dry_run:
        log.info(
            f"  DRY-RUN: {len(recovery)} registros de recuperacao "
            f"preparados (nao persistidos)."
        )
        return

    upsert_sql = """
    INSERT INTO multibet.crm_recovery_daily (
        report_date, rule_id,
        inativos_alvo, reengajados, depositaram,
        churn_d7, churn_d7_pct,
        created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    ON CONFLICT (report_date, rule_id)
    DO UPDATE SET
        inativos_alvo   = EXCLUDED.inativos_alvo,
        reengajados     = EXCLUDED.reengajados,
        depositaram     = EXCLUDED.depositaram,
        churn_d7        = EXCLUDED.churn_d7,
        churn_d7_pct    = EXCLUDED.churn_d7_pct,
        updated_at      = NOW()
    ;
    """

    tunnel, conn = get_supernova_connection()
    count = 0
    try:
        with conn.cursor() as cur:
            for uid, data in recovery.items():
                rule_id = int(uid) if uid.isdigit() else 0
                cur.execute(upsert_sql, (
                    str(target_date),
                    rule_id,
                    data.get("inativos_alvo", 0),
                    data.get("reengajados", 0),
                    data.get("depositaram", 0),
                    data.get("churn_d7", 0),
                    data.get("churn_d7_pct", 0),
                ))
                count += 1
        conn.commit()
        log.info(f"  crm_recovery_daily: {count} registros upserted.")
    except Exception as e:
        conn.rollback()
        log.error(f"  Erro ao persistir crm_recovery_daily: {e}")
        raise
    finally:
        conn.close()
        tunnel.stop()


# ===========================================================================
# ORQUESTRACAO PRINCIPAL
# ===========================================================================

def run_pipeline(
    target_date: date,
    dry_run: bool = False,
    skip_setup: bool = False,
) -> dict:
    """
    Orquestra o pipeline CRM Report Daily completo.

    Fluxo:
      1. Descobrir campanhas ativas (BigQuery)
      2. Extrair funil de conversao (BigQuery)
      3. Extrair bonus e custos (BigQuery)
      4. Extrair custos de disparo (BigQuery)
      5. Extrair coorte + metricas financeiras (BigQuery + Athena)
      6. Extrair top jogos da coorte (Athena)
      7. Calcular comparativo antes/durante/depois (Athena)
      8. Calcular VIP groups (Python, dados do Step 5)
      9. Calcular metricas de recuperacao (Athena)
     10. Persistir em todas as tabelas (Super Nova DB)

    Cada step e independente: se um falhar, os demais continuam.
    O erro e logado e o step e pulado.

    Args:
        target_date: data de referencia (geralmente D-1)
        dry_run: se True, nao persiste no banco
        skip_setup: se True, pula criacao de tabelas

    Returns:
        dict com status de cada step e dados extraidos
    """
    log.info("=" * 80)
    log.info(f"  CRM REPORT DAILY — {target_date}")
    log.info(f"  Modo: {'DRY-RUN' if dry_run else 'PRODUCAO'}")
    log.info("=" * 80)

    result = {
        "target_date": str(target_date),
        "dry_run": dry_run,
        "steps": {},
    }

    # --- Setup (idempotente) ---
    if not skip_setup and not dry_run:
        try:
            setup_tables()
            result["steps"]["setup"] = "OK"
        except Exception as e:
            log.error(f"SETUP falhou: {e}")
            result["steps"]["setup"] = f"ERRO: {e}"
            # Setup falhando e critico; aborta
            return result

    # --- STEP 1: Campanhas ativas ---
    df_campaigns = pd.DataFrame()
    try:
        df_campaigns = step1_discover_campaigns(target_date)
        result["steps"]["step1_campaigns"] = f"OK ({len(df_campaigns)} campanhas)"
    except Exception as e:
        log.error(f"STEP 1 falhou: {e}")
        result["steps"]["step1_campaigns"] = f"ERRO: {e}"

    if df_campaigns.empty:
        log.warning("Nenhuma campanha encontrada. Pipeline encerrado.")
        return result

    # --- STEP 2: Funil ---
    df_funnel = pd.DataFrame()
    try:
        df_funnel = step2_extract_funnel(target_date)
        result["steps"]["step2_funnel"] = f"OK ({len(df_funnel)} campanhas com funil)"
    except Exception as e:
        log.error(f"STEP 2 falhou: {e}")
        result["steps"]["step2_funnel"] = f"ERRO: {e}"

    # --- STEP 3: Bonus ---
    df_bonuses = pd.DataFrame()
    try:
        df_bonuses = step3_extract_bonuses(target_date)
        result["steps"]["step3_bonuses"] = f"OK ({len(df_bonuses)} campanhas com bonus)"
    except Exception as e:
        log.error(f"STEP 3 falhou: {e}")
        result["steps"]["step3_bonuses"] = f"ERRO: {e}"

    # --- STEP 4: Custos de disparo ---
    df_dispatch = pd.DataFrame()
    try:
        df_dispatch = step4_extract_dispatch_costs(target_date)
        result["steps"]["step4_dispatch"] = f"OK ({len(df_dispatch)} linhas de custo)"
    except Exception as e:
        log.error(f"STEP 4 falhou: {e}")
        result["steps"]["step4_dispatch"] = f"ERRO: {e}"

    # --- STEP 5: Coorte + Financeiro ---
    cohorts = {}
    df_financials = pd.DataFrame()
    financials_per_campaign = {}
    try:
        cohorts = _get_cohort_user_ext_ids(target_date)
        if cohorts:
            df_financials = step5_extract_financials(target_date, cohorts)
            financials_per_campaign = _aggregate_financials_per_campaign(
                df_financials, cohorts
            )
        result["steps"]["step5_financials"] = (
            f"OK ({len(cohorts)} coortes, {len(df_financials)} player-days)"
        )
    except Exception as e:
        log.error(f"STEP 5 falhou: {e}")
        result["steps"]["step5_financials"] = f"ERRO: {e}"

    # --- STEP 6: Top jogos ---
    df_games = pd.DataFrame()
    try:
        all_ext_ids = _build_all_ext_ids(cohorts)
        if all_ext_ids:
            df_games = step6_extract_top_games(target_date, all_ext_ids)
        result["steps"]["step6_games"] = f"OK ({len(df_games)} jogos)"
    except Exception as e:
        log.error(f"STEP 6 falhou: {e}")
        result["steps"]["step6_games"] = f"ERRO: {e}"

    # --- STEP 7: Comparativo ---
    comparison = {}
    try:
        all_ext_ids = _build_all_ext_ids(cohorts)
        if all_ext_ids:
            comparison = step7_extract_comparison(target_date, all_ext_ids)
        result["steps"]["step7_comparison"] = f"OK ({len(comparison)} periodos)"
    except Exception as e:
        log.error(f"STEP 7 falhou: {e}")
        result["steps"]["step7_comparison"] = f"ERRO: {e}"

    # --- STEP 8: VIP Groups ---
    vip_groups = {}
    try:
        vip_groups = step8_calculate_vip_groups(df_financials, cohorts)
        result["steps"]["step8_vip"] = f"OK ({len(vip_groups)} campanhas com VIP)"
    except Exception as e:
        log.error(f"STEP 8 falhou: {e}")
        result["steps"]["step8_vip"] = f"ERRO: {e}"

    # --- STEP 9: Recovery ---
    recovery = {}
    try:
        recovery = step9_extract_recovery(target_date, cohorts)
        result["steps"]["step9_recovery"] = f"OK ({len(recovery)} campanhas)"
    except Exception as e:
        log.error(f"STEP 9 falhou: {e}")
        result["steps"]["step9_recovery"] = f"ERRO: {e}"

    # --- STEP 10: Persistir ---
    try:
        step10_persist_campaign_daily(
            target_date, df_campaigns, df_funnel, df_bonuses,
            financials_per_campaign, df_dispatch, dry_run=dry_run,
        )
        result["steps"]["step10_campaign_daily"] = "OK"
    except Exception as e:
        log.error(f"STEP 10 (campaign_daily) falhou: {e}")
        result["steps"]["step10_campaign_daily"] = f"ERRO: {e}"

    try:
        step10_persist_games(target_date, df_games, cohorts, dry_run=dry_run)
        result["steps"]["step10_games"] = "OK"
    except Exception as e:
        log.error(f"STEP 10 (games) falhou: {e}")
        result["steps"]["step10_games"] = f"ERRO: {e}"

    try:
        step10_persist_dispatch_budget(target_date, df_dispatch, dry_run=dry_run)
        result["steps"]["step10_dispatch"] = "OK"
    except Exception as e:
        log.error(f"STEP 10 (dispatch) falhou: {e}")
        result["steps"]["step10_dispatch"] = f"ERRO: {e}"

    try:
        step10_persist_vip_groups(target_date, vip_groups, dry_run=dry_run)
        result["steps"]["step10_vip"] = "OK"
    except Exception as e:
        log.error(f"STEP 10 (vip) falhou: {e}")
        result["steps"]["step10_vip"] = f"ERRO: {e}"

    try:
        step10_persist_comparison(comparison, cohorts, dry_run=dry_run)
        result["steps"]["step10_comparison"] = "OK"
    except Exception as e:
        log.error(f"STEP 10 (comparison) falhou: {e}")
        result["steps"]["step10_comparison"] = f"ERRO: {e}"

    try:
        step10_persist_recovery(target_date, recovery, dry_run=dry_run)
        result["steps"]["step10_recovery"] = "OK"
    except Exception as e:
        log.error(f"STEP 10 (recovery) falhou: {e}")
        result["steps"]["step10_recovery"] = f"ERRO: {e}"

    # --- Resumo ---
    _print_summary(result, df_campaigns, df_funnel, df_dispatch,
                   financials_per_campaign, comparison)

    return result


# ===========================================================================
# RESUMO NO TERMINAL
# ===========================================================================

def _print_summary(
    result: dict,
    df_campaigns: pd.DataFrame,
    df_funnel: pd.DataFrame,
    df_dispatch: pd.DataFrame,
    financials: dict,
    comparison: dict,
):
    """Imprime resumo formatado no terminal."""
    target = result["target_date"]
    mode = "DRY-RUN" if result["dry_run"] else "PRODUCAO"

    print(f"\n{'=' * 80}")
    print(f"  CRM REPORT DAILY — RESUMO ({target}, {mode})")
    print(f"{'=' * 80}\n")

    # Steps status
    print("  STATUS DOS STEPS:")
    for step_name, status in result.get("steps", {}).items():
        icon = "[OK]" if status.startswith("OK") else "[!!]"
        print(f"    {icon} {step_name}: {status}")
    print()

    # Campanhas
    if not df_campaigns.empty:
        print(f"  CAMPANHAS: {len(df_campaigns)} total")
        for ctype, count in df_campaigns["campaign_type"].value_counts().items():
            print(f"    {ctype}: {count}")
        print()

    # Funil consolidado
    if not df_funnel.empty:
        total_env = int(df_funnel["enviados"].sum())
        total_conv = int(df_funnel["convertidos"].sum())
        conv_rate = _safe_div(total_conv, total_env) * 100
        print(f"  FUNIL CONSOLIDADO:")
        print(f"    Enviados:    {_fmt_int(total_env)}")
        print(f"    Entregues:   {_fmt_int(int(df_funnel['entregues'].sum()))}")
        print(f"    Abertos:     {_fmt_int(int(df_funnel['abertos'].sum()))}")
        print(f"    Clicados:    {_fmt_int(int(df_funnel['clicados'].sum()))}")
        print(f"    Convertidos: {_fmt_int(total_conv)} ({conv_rate:.1f}%)")
        print()

    # Custos de disparo
    if not df_dispatch.empty:
        total_custo = df_dispatch["custo_total_brl"].sum()
        total_envios = df_dispatch["total_sent"].sum()
        print(f"  CUSTOS DE DISPARO:")
        print(f"    Total enviados: {_fmt_int(int(total_envios))}")
        print(f"    Custo total:    {_fmt_brl(total_custo)}")
        for _, row in df_dispatch.iterrows():
            print(
                f"    {row['channel']}/{row['provider']}: "
                f"{_fmt_int(int(row['total_sent']))} envios, "
                f"{_fmt_brl(row['custo_total_brl'])}"
            )
        print()

    # Financeiro consolidado
    if financials:
        total_ggr = sum(f.get("total_ggr", 0) for f in financials.values())
        total_dep = sum(f.get("total_deposit", 0) for f in financials.values())
        total_users = sum(f.get("coorte_users", 0) for f in financials.values())
        print(f"  FINANCEIRO CONSOLIDADO (coorte CRM):")
        print(f"    Users ativos:  {_fmt_int(total_users)}")
        print(f"    GGR total:     {_fmt_brl(total_ggr)}")
        print(f"    Depositos:     {_fmt_brl(total_dep)}")
        print()

    # Comparativo
    if comparison:
        print(f"  COMPARATIVO:")
        for period_name, data in comparison.items():
            print(
                f"    {period_name}: GGR={_fmt_brl(data.get('total_ggr', 0))}, "
                f"Users={_fmt_int(data.get('coorte_users', 0))}"
            )
            if "ggr_delta" in data and data["ggr_delta"] is not None:
                sinal = "+" if data["ggr_delta"] > 0 else ""
                print(
                    f"      Delta: {sinal}{_fmt_brl(data['ggr_delta'])} "
                    f"({sinal}{data.get('ggr_delta_pct', 0):.2f}%)"
                )
        print()

    print(f"{'=' * 80}\n")


# ===========================================================================
# CLI
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline CRM Report Daily — "
            "Extrai BigQuery + Athena, persiste no Super Nova DB"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  # Rodar para D-1 (padrao)
  python pipelines/crm_report_daily.py

  # Rodar para data especifica
  python pipelines/crm_report_daily.py --date 2026-03-27

  # Backfill de 7 dias
  python pipelines/crm_report_daily.py --days 7

  # Dry-run (nao persiste)
  python pipelines/crm_report_daily.py --dry-run

  # Combinar: backfill + dry-run
  python pipelines/crm_report_daily.py --days 7 --dry-run
        """,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Data de referencia YYYY-MM-DD (default: D-1 = ontem)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Quantidade de dias para processar (backfill). Default: 1 (apenas D-1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Modo dry-run: extrai dados mas NAO persiste no banco.",
    )

    args = parser.parse_args()

    # Determinar data(s) alvo
    if args.date:
        end_date = _parse_date(args.date)
    else:
        end_date = date.today() - timedelta(days=1)  # D-1

    # Backfill: processar args.days dias retroativos
    dates_to_process = []
    for i in range(args.days):
        dt = end_date - timedelta(days=i)
        dates_to_process.append(dt)

    # Processar do mais antigo para o mais recente
    dates_to_process.sort()

    log.info(f"Processando {len(dates_to_process)} dia(s): "
             f"{dates_to_process[0]} a {dates_to_process[-1]}")

    # Setup apenas uma vez
    skip_setup = False
    results = []

    for i, dt in enumerate(dates_to_process):
        log.info(f"\n--- Dia {i + 1}/{len(dates_to_process)}: {dt} ---")

        try:
            result = run_pipeline(
                target_date=dt,
                dry_run=args.dry_run,
                skip_setup=skip_setup,
            )
            results.append(result)
        except Exception as e:
            log.error(f"Pipeline falhou para {dt}: {e}")
            results.append({"target_date": str(dt), "error": str(e)})

        # Setup so roda uma vez
        skip_setup = True

    # Resumo final de backfill (se mais de 1 dia)
    if len(dates_to_process) > 1:
        ok_count = sum(
            1 for r in results
            if "error" not in r
        )
        print(f"\n{'=' * 80}")
        print(f"  BACKFILL CONCLUIDO: {ok_count}/{len(dates_to_process)} dias processados")
        print(f"{'=' * 80}\n")


if __name__ == "__main__":
    main()
