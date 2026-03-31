"""
DDL: Tabelas do Report de Performance CRM Diario
=================================================
Schema: multibet (Super Nova DB PostgreSQL)

Tabelas criadas:
  1. crm_campaign_daily         — 1 linha por campanha x dia (principal)
  2. crm_campaign_segment_daily — quebra por segmento/produto/ticket
  3. crm_campaign_game_daily    — quebra por jogo (top games)
  4. crm_campaign_comparison    — antes/durante/depois
  5. crm_dispatch_budget        — orcamento de disparos por canal
  6. crm_vip_group_daily        — analise por grupo VIP
  7. crm_recovery_daily         — usuarios de recuperacao
  8. crm_player_vip_tier        — classificacao VIP calculada (Elite/Key/High)

Uso:
    python pipelines/ddl_crm_report.py
"""

import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ============================================================
# 1. TABELA PRINCIPAL — 1 linha por campanha x dia
# ============================================================
DDL_CAMPAIGN_DAILY = """
CREATE TABLE IF NOT EXISTS multibet.crm_campaign_daily (
    id                          SERIAL PRIMARY KEY,
    report_date                 DATE NOT NULL,
    campaign_id                 VARCHAR(100) NOT NULL,
    campaign_name               VARCHAR(255),
    campaign_type               VARCHAR(50),
    channel                     VARCHAR(50),
    segment_name                VARCHAR(255),
    status                      VARCHAR(20) DEFAULT 'ativa',
    campaign_start              DATE,
    campaign_end                DATE,

    -- Funil de conversao
    segmentados                 INT DEFAULT 0,
    msg_entregues               INT DEFAULT 0,
    msg_abertos                 INT DEFAULT 0,
    msg_clicados                INT DEFAULT 0,
    convertidos                 INT DEFAULT 0,
    apostaram                   INT DEFAULT 0,
    cumpriram_condicao          INT DEFAULT 0,
    tempo_medio_conversao_horas NUMERIC(10,2),

    -- Opt-in
    optin_apostaram             INT DEFAULT 0,
    optin_nao_apostaram         INT DEFAULT 0,
    economia_optin_brl          NUMERIC(14,2) DEFAULT 0,

    -- Financeiro Geral
    turnover_total_brl          NUMERIC(14,2) DEFAULT 0,
    ggr_brl                     NUMERIC(14,2) DEFAULT 0,
    ggr_pct                     NUMERIC(6,2) DEFAULT 0,
    ngr_brl                     NUMERIC(14,2) DEFAULT 0,
    ngr_pct                     NUMERIC(6,2) DEFAULT 0,
    net_deposit_brl             NUMERIC(14,2) DEFAULT 0,
    depositos_brl               NUMERIC(14,2) DEFAULT 0,
    saques_brl                  NUMERIC(14,2) DEFAULT 0,

    -- Financeiro Cassino
    turnover_casino_brl         NUMERIC(14,2) DEFAULT 0,
    ggr_casino_brl              NUMERIC(14,2) DEFAULT 0,

    -- Financeiro Sportsbook
    turnover_sports_brl         NUMERIC(14,2) DEFAULT 0,
    ggr_sports_brl              NUMERIC(14,2) DEFAULT 0,

    -- ROI e custos
    custo_bonus_brl             NUMERIC(14,2) DEFAULT 0,
    custo_disparos_brl          NUMERIC(14,2) DEFAULT 0,
    custo_total_brl             NUMERIC(14,2) DEFAULT 0,
    cpa_medio_brl               NUMERIC(10,2) DEFAULT 0,
    roi                         NUMERIC(8,4),

    -- Disparos por canal
    disparos_sms                INT DEFAULT 0,
    disparos_whatsapp           INT DEFAULT 0,
    disparos_push               INT DEFAULT 0,
    disparos_popup              INT DEFAULT 0,
    disparos_email              INT DEFAULT 0,
    disparos_inbox              INT DEFAULT 0,

    -- Meta (input manual CRM)
    meta_conversao_pct          NUMERIC(6,2),
    meta_atingida               BOOLEAN,

    created_at                  TIMESTAMPTZ DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_campaign_daily UNIQUE (report_date, campaign_id)
);
"""

# ============================================================
# 2. QUEBRA POR SEGMENTO
# ============================================================
DDL_SEGMENT_DAILY = """
CREATE TABLE IF NOT EXISTS multibet.crm_campaign_segment_daily (
    id                  SERIAL PRIMARY KEY,
    report_date         DATE NOT NULL,
    campaign_id         VARCHAR(100) NOT NULL,
    segment_type        VARCHAR(50) NOT NULL,
    product_preference  VARCHAR(50) NOT NULL DEFAULT '',
    ticket_tier         VARCHAR(20) NOT NULL DEFAULT '',

    users               INT DEFAULT 0,
    apostaram           INT DEFAULT 0,
    turnover_brl        NUMERIC(14,2) DEFAULT 0,
    ggr_brl             NUMERIC(14,2) DEFAULT 0,
    depositos_brl       NUMERIC(14,2) DEFAULT 0,

    created_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_segment_daily
        UNIQUE (report_date, campaign_id, segment_type, product_preference, ticket_tier)
);
"""

# ============================================================
# 3. QUEBRA POR JOGO
# ============================================================
DDL_GAME_DAILY = """
CREATE TABLE IF NOT EXISTS multibet.crm_campaign_game_daily (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    campaign_id     VARCHAR(100) NOT NULL,
    game_id         VARCHAR(50),
    game_name       VARCHAR(255),
    vendor_name     VARCHAR(100),

    users           INT DEFAULT 0,
    turnover_brl    NUMERIC(14,2) DEFAULT 0,
    ggr_brl         NUMERIC(14,2) DEFAULT 0,
    rtp_pct         NUMERIC(6,2),

    created_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_game_daily UNIQUE (report_date, campaign_id, game_id)
);
"""

# ============================================================
# 4. COMPARATIVO ANTES / DURANTE / DEPOIS
# ============================================================
DDL_COMPARISON = """
CREATE TABLE IF NOT EXISTS multibet.crm_campaign_comparison (
    id              SERIAL PRIMARY KEY,
    campaign_id     VARCHAR(100) NOT NULL,
    period          VARCHAR(10) NOT NULL,
    period_start    DATE,
    period_end      DATE,

    users           INT DEFAULT 0,
    depositos_brl   NUMERIC(14,2) DEFAULT 0,
    ggr_brl         NUMERIC(14,2) DEFAULT 0,
    ngr_brl         NUMERIC(14,2) DEFAULT 0,
    sessoes         INT DEFAULT 0,
    apd             NUMERIC(6,2) DEFAULT 0,

    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_comparison UNIQUE (campaign_id, period)
);
"""

# ============================================================
# 5. ORCAMENTO DE DISPAROS
# ============================================================
DDL_DISPATCH_BUDGET = """
CREATE TABLE IF NOT EXISTS multibet.crm_dispatch_budget (
    id                  SERIAL PRIMARY KEY,
    month_ref           DATE NOT NULL,
    channel             VARCHAR(50) NOT NULL,
    provider            VARCHAR(100) NOT NULL DEFAULT '',
    cost_per_unit       NUMERIC(6,4) NOT NULL,
    total_sent          INT DEFAULT 0,
    total_cost_brl      NUMERIC(14,2) DEFAULT 0,
    budget_monthly_brl  NUMERIC(14,2),
    budget_pct_used     NUMERIC(6,2),
    projection_eom_brl  NUMERIC(14,2),

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_dispatch_budget UNIQUE (month_ref, channel, provider)
);
"""

# ============================================================
# 6. ANALISE VIP (Elite / Key Account / High Value)
# ============================================================
DDL_VIP_GROUP = """
CREATE TABLE IF NOT EXISTS multibet.crm_vip_group_daily (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    campaign_id     VARCHAR(100) NOT NULL,
    vip_group       VARCHAR(30) NOT NULL,

    users           INT DEFAULT 0,
    ngr_brl         NUMERIC(14,2) DEFAULT 0,
    apd             NUMERIC(6,2) DEFAULT 0,
    overlap_count   INT DEFAULT 0,

    created_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_vip_daily UNIQUE (report_date, campaign_id, vip_group)
);
"""

# ============================================================
# 7. RECUPERACAO
# ============================================================
DDL_RECOVERY = """
CREATE TABLE IF NOT EXISTS multibet.crm_recovery_daily (
    id                                  SERIAL PRIMARY KEY,
    report_date                         DATE NOT NULL,
    campaign_id                         VARCHAR(100) NOT NULL,
    channel                             VARCHAR(50) NOT NULL DEFAULT '',

    inativos_impactados                 INT DEFAULT 0,
    reengajados                         INT DEFAULT 0,
    depositaram                         INT DEFAULT 0,
    depositos_brl                       NUMERIC(14,2) DEFAULT 0,
    tempo_medio_reengajamento_horas     NUMERIC(10,2),
    churn_d7_pct                        NUMERIC(6,2),

    created_at                          TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_recovery_daily
        UNIQUE (report_date, campaign_id, channel)
);
"""

# ============================================================
# 8. CLASSIFICACAO VIP CALCULADA
# Recalculada periodicamente com base no NGR do jogador
# Tiers conforme task:
#   Elite:       NGR >= R$ 10.000
#   Key Account: NGR >= R$ 5.000 e < R$ 10.000
#   High Value:  NGR >= R$ 3.000 e < R$ 5.000
# ============================================================
DDL_PLAYER_VIP_TIER = """
CREATE TABLE IF NOT EXISTS multibet.crm_player_vip_tier (
    id              SERIAL PRIMARY KEY,
    ecr_id          BIGINT NOT NULL,
    external_id     VARCHAR(50),
    vip_tier        VARCHAR(30) NOT NULL,
    ngr_periodo_brl NUMERIC(14,2) DEFAULT 0,
    periodo_inicio  DATE,
    periodo_fim     DATE,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_crm_player_vip UNIQUE (ecr_id, periodo_inicio)
);
"""

# ============================================================
# INDICES
# ============================================================
DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_crm_cd_date ON multibet.crm_campaign_daily(report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_cd_type ON multibet.crm_campaign_daily(campaign_type);",
    "CREATE INDEX IF NOT EXISTS idx_crm_cd_campaign ON multibet.crm_campaign_daily(campaign_id);",
    "CREATE INDEX IF NOT EXISTS idx_crm_sd_date ON multibet.crm_campaign_segment_daily(report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_gd_date ON multibet.crm_campaign_game_daily(report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_comp_campaign ON multibet.crm_campaign_comparison(campaign_id);",
    "CREATE INDEX IF NOT EXISTS idx_crm_db_month ON multibet.crm_dispatch_budget(month_ref);",
    "CREATE INDEX IF NOT EXISTS idx_crm_vip_date ON multibet.crm_vip_group_daily(report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_rec_date ON multibet.crm_recovery_daily(report_date);",
    "CREATE INDEX IF NOT EXISTS idx_crm_pvip_tier ON multibet.crm_player_vip_tier(vip_tier);",
    "CREATE INDEX IF NOT EXISTS idx_crm_pvip_ecr ON multibet.crm_player_vip_tier(ecr_id);",
]


def setup_tables():
    """Cria todas as tabelas e indices (idempotente)."""
    log.info("Criando tabelas do Report CRM no Super Nova DB...")

    tables = [
        ("crm_campaign_daily", DDL_CAMPAIGN_DAILY),
        ("crm_campaign_segment_daily", DDL_SEGMENT_DAILY),
        ("crm_campaign_game_daily", DDL_GAME_DAILY),
        ("crm_campaign_comparison", DDL_COMPARISON),
        ("crm_dispatch_budget", DDL_DISPATCH_BUDGET),
        ("crm_vip_group_daily", DDL_VIP_GROUP),
        ("crm_recovery_daily", DDL_RECOVERY),
        ("crm_player_vip_tier", DDL_PLAYER_VIP_TIER),
    ]

    for name, ddl in tables:
        try:
            execute_supernova(ddl)
            log.info(f"  OK: multibet.{name}")
        except Exception as e:
            log.error(f"  ERRO: multibet.{name} — {e}")

    log.info("Criando indices...")
    for idx_sql in DDL_INDEXES:
        try:
            execute_supernova(idx_sql)
        except Exception as e:
            log.warning(f"  Index warning: {e}")

    log.info("DDL concluida.")


if __name__ == "__main__":
    setup_tables()