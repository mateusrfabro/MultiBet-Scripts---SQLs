"""
Pipeline: Player Credit Rating (PCR) — Persistencia Diaria v1.0
================================================================
Calcula o rating PCR (E, D, C, B, A, S) para jogadores ativos nos ultimos 90 dias
e persiste no Super Nova DB (PostgreSQL) com snapshot diario.

Fonte:  ps_bi.fct_player_activity_daily + ps_bi.dim_user (Athena)
Destino: multibet.pcr_ratings (Super Nova DB)
View:   multibet.pcr_atual (snapshot mais recente, filtro automatico)

Colunas adicionais vs pcr_scoring.py:
  - snapshot_date: data do calculo (DATE) — informativo, so mantem o ultimo
  - c_category:    status da conta do jogador (ecr_status do dim_user)

Estrategia: DELETE WHERE snapshot_date + INSERT (preserva historico, idempotente no dia).
View pcr_atual filtra MAX(snapshot_date) automaticamente.

Uso:
    python pipelines/pcr_pipeline.py              # roda e grava no banco
    python pipelines/pcr_pipeline.py --dry-run    # apenas CSV, sem PostgreSQL

Cron EC2: 06:30 UTC (03:30 BRT) — diario
"""

import sys
import os
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

# Adiciona raiz do projeto ao path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIGURACAO
# ============================================================
JANELA_DIAS = 90
OUTPUT_DIR = "output"
BRT = timezone(timedelta(hours=-3))
SNAPSHOT_DATE = datetime.now(BRT).strftime("%Y-%m-%d")
# Estrategia DELETE WHERE snapshot_date + INSERT — preserva historico para diff/backtest

# Thresholds do rating NEW (limbo benigno) — regra Castrin 27/04/2026.
# NEW = jogador na janela de onboarding: nao recebe regua agressiva (nem corte
# de bonus, nem bonus alto). Recebe acoes especificas de onboarding (matching FTD,
# T+24h/T+48h/T+7d). Sai do NEW por >=30 dias de cadastro OU >=3 depositos totais.
# Antes (v1.3): days_active<7 AND num_deposits<2 (43% da base virava NEW).
# Agora (v2.0): days_since_registration<30 AND num_deposits<3.
NEW_REGISTRATION_DAYS = 30
NEW_DEPOSITS_THRESHOLD = 3

# ============================================================
# DDL — Tabela + View
# ============================================================
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.pcr_ratings (
    snapshot_date       DATE            NOT NULL,
    player_id           BIGINT          NOT NULL,
    external_id         BIGINT,
    rating              VARCHAR(10)     NOT NULL,  -- v1.3: ampliado de VARCHAR(2) pra aceitar 'NEW' (novatos)
    pvs                 NUMERIC(8, 2)   NOT NULL,
    ggr_total           NUMERIC(15, 2),
    ngr_total           NUMERIC(15, 2),
    total_deposits      NUMERIC(15, 2),
    total_cashouts      NUMERIC(15, 2),
    num_deposits        INTEGER,
    days_active         INTEGER,
    recency_days        INTEGER,
    product_type        VARCHAR(10),
    casino_rounds       BIGINT,
    sport_bets          BIGINT,
    bonus_issued        NUMERIC(15, 2),
    bonus_ratio         NUMERIC(8, 4),
    wd_ratio            NUMERIC(8, 4),
    net_deposit         NUMERIC(15, 2),
    margem_ggr          NUMERIC(8, 4),
    ggr_por_dia         NUMERIC(15, 2),
    affiliate_id        VARCHAR(300),
    c_category          VARCHAR(50),
    registration_date   DATE,
    created_at          TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, player_id)
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pcr_snapshot ON multibet.pcr_ratings (snapshot_date DESC);",
    "CREATE INDEX IF NOT EXISTS idx_pcr_rating ON multibet.pcr_ratings (snapshot_date, rating);",
    "CREATE INDEX IF NOT EXISTS idx_pcr_category ON multibet.pcr_ratings (snapshot_date, c_category);",
]

# v1.3 (20/04/2026): ampliar coluna rating em bases que ja existem pra aceitar 'NEW'.
# Idempotente: PostgreSQL permite ALTER TYPE pra VARCHAR maior sem perda de dados.
DDL_ALTER_RATING_V13 = (
    "ALTER TABLE multibet.pcr_ratings ALTER COLUMN rating TYPE VARCHAR(10);"
)

DDL_VIEW = """
CREATE OR REPLACE VIEW multibet.pcr_atual AS
SELECT *
FROM multibet.pcr_ratings
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM multibet.pcr_ratings);
"""

DDL_VIEW_RESUMO = """
CREATE OR REPLACE VIEW multibet.pcr_resumo AS
SELECT
    snapshot_date,
    rating,
    COUNT(*)                        AS jogadores,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY snapshot_date), 1) AS pct_base,
    ROUND(SUM(ggr_total), 2)       AS ggr_total,
    ROUND(AVG(ggr_total), 2)       AS ggr_medio,
    ROUND(AVG(total_deposits), 2)  AS deposito_medio,
    ROUND(AVG(num_deposits), 1)    AS num_dep_medio,
    ROUND(AVG(days_active), 1)     AS dias_ativos_medio,
    ROUND(AVG(recency_days), 1)    AS recencia_media,
    ROUND(AVG(pvs), 2)             AS pvs_medio
FROM multibet.pcr_ratings
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM multibet.pcr_ratings)
GROUP BY snapshot_date, rating
ORDER BY
    CASE rating
        WHEN 'S'   THEN 1
        WHEN 'A'   THEN 2
        WHEN 'B'   THEN 3
        WHEN 'C'   THEN 4
        WHEN 'D'   THEN 5
        WHEN 'E'   THEN 6
        WHEN 'NEW' THEN 7  -- v1.3: novatos no final da lista (bucket separado)
    END;
"""


# ============================================================
# EXTRACAO
# ============================================================
def extrair_metricas_jogadores() -> pd.DataFrame:
    """
    Extrai metricas por jogador dos ultimos 90 dias usando ps_bi.
    Inclui ecr_status (c_category) para filtro do Head.
    """
    log.info(f"Extraindo metricas de jogadores (ultimos {JANELA_DIAS} dias)...")

    sql = f"""
    -- PCR Pipeline: metricas por jogador (ultimos {JANELA_DIAS} dias)
    -- Fonte: ps_bi.fct_player_activity_daily + dim_user
    -- Valores em BRL (pre-agregado pelo dbt)
    --
    -- DEFINICAO DE ATIVO (v1.1):
    --   Ativo = quem APOSTOU (casino ou sportsbook) OU DEPOSITOU no periodo.
    --   Login sozinho NAO conta. Bonus emitido sem aposta NAO conta.

    WITH player_metrics AS (
        SELECT
            f.player_id,

            -- GGR e NGR
            COALESCE(SUM(f.ggr_base), 0) AS ggr_total,
            COALESCE(SUM(f.ngr_base), 0) AS ngr_total,

            -- Casino
            COALESCE(SUM(f.casino_realbet_base), 0)  AS casino_bet,
            COALESCE(SUM(f.casino_real_win_base), 0)  AS casino_win,
            COALESCE(SUM(f.casino_realbet_count), 0)  AS casino_rounds,

            -- Sportsbook
            COALESCE(SUM(f.sb_realbet_base), 0)  AS sport_bet,
            COALESCE(SUM(f.sb_real_win_base), 0) AS sport_win,
            COALESCE(SUM(f.sb_realbet_count), 0) AS sport_bets,

            -- Depositos
            COALESCE(SUM(f.deposit_success_count), 0) AS num_deposits,
            COALESCE(SUM(f.deposit_success_base), 0)  AS total_deposits,

            -- Saques
            COALESCE(SUM(f.cashout_success_count), 0) AS num_cashouts,
            COALESCE(SUM(f.cashout_success_base), 0)  AS total_cashouts,

            -- Atividade
            COUNT(DISTINCT f.activity_date) AS days_active,
            MAX(f.activity_date) AS last_active_date,
            MIN(f.activity_date) AS first_active_date,

            -- Bonus
            COALESCE(SUM(f.bonus_issued_base), 0)     AS bonus_issued,
            COALESCE(SUM(f.bonus_turnedreal_base), 0)  AS bonus_turned_real,

            -- Turnover total (casino + sport)
            COALESCE(SUM(f.casino_realbet_base), 0)
                + COALESCE(SUM(f.sb_realbet_base), 0) AS turnover_total

        FROM ps_bi.fct_player_activity_daily f
        WHERE f.activity_date >= CURRENT_DATE - INTERVAL '{JANELA_DIAS}' DAY
          AND f.activity_date < CURRENT_DATE  -- exclui D-0 (parcial)
        GROUP BY f.player_id
        -- FILTRO CRITICO: somente quem apostou ou depositou
        HAVING COALESCE(SUM(f.casino_realbet_count), 0) > 0
            OR COALESCE(SUM(f.sb_realbet_count), 0) > 0
            OR COALESCE(SUM(f.deposit_success_count), 0) > 0
    )
    SELECT
        m.*,
        u.external_id,
        u.registration_date,
        u.affiliate_id,
        u.is_test,
        -- c_category: status da conta (real_user, closed, suspended, fraud, etc.)
        -- Fonte: bireports_ec2.tbl_ecr (48 cols, mais rico que ecr_ec2)
        ecr_bi.c_category,

        -- Recencia: dias desde ultima atividade
        DATE_DIFF('day', CAST(m.last_active_date AS DATE), CURRENT_DATE) AS recency_days,

        -- Tipo de produto
        CASE
            WHEN m.casino_rounds > 0 AND m.sport_bets > 0 THEN 'MISTO'
            WHEN m.casino_rounds > 0 THEN 'CASINO'
            WHEN m.sport_bets > 0 THEN 'SPORT'
            ELSE 'OUTRO'
        END AS product_type

    FROM player_metrics m
    LEFT JOIN ps_bi.dim_user u ON m.player_id = u.ecr_id
    LEFT JOIN (
        -- Dedup deterministico: pega a linha MAIS RECENTE por c_category_updated_time.
        -- FIX auditoria 21/04/2026: antes era `ORDER BY c_ecr_id DESC` (arbitrario,
        -- podia pegar status desatualizado). Agora ordena pela data de mudanca de
        -- status (c_category_updated_time), assim o c_category reflete o estado
        -- atual do jogador (ex: real_user -> closed -> fraud pega 'fraud').
        SELECT c_ecr_id, c_category
        FROM (
            SELECT c_ecr_id, c_category,
                   ROW_NUMBER() OVER (
                       PARTITION BY c_ecr_id
                       ORDER BY c_category_updated_time DESC NULLS LAST, c_ecr_id DESC
                   ) AS rn
            FROM bireports_ec2.tbl_ecr
        )
        WHERE rn = 1
    ) ecr_bi ON m.player_id = ecr_bi.c_ecr_id
    WHERE (u.is_test = false OR u.is_test IS NULL)
    """

    df = query_athena(sql, database="ps_bi")
    log.info(f"  -> {len(df):,} jogadores extraidos (pre-filtro c_category)")

    # Log breakdown completo ANTES do filtro (observabilidade A1 — 21/04/2026)
    if "c_category" in df.columns:
        cat_dist = df["c_category"].value_counts(dropna=False)
        log.info("  -> Breakdown c_category (antes do filtro):")
        for cat, qtd in cat_dist.items():
            pct = qtd / len(df) * 100
            log.info(f"       {str(cat):<20} {qtd:>8,} ({pct:>5.1f}%)")

    # REGRA CRITICA (GUIA_REPRODUCAO_PCR.md, regra #1, Castrin 27/04/2026):
    # NAO filtrar c_category. Mantem TODAS as categorias (real_user, play_user,
    # rg_closed, fraud, closed, rg_cool_off) pra coerencia metodologica entre rodadas.
    # c_category vai como coluna informativa no output. Filtro de campanha/bonus
    # e responsabilidade do CRM no consumo, nao do PCR descritivo.
    log.info(f"  -> Sem filtro de c_category (regra #1 do guia): {len(df):,} jogadores mantidos")

    # Dias desde cadastro (regra NEW v2.0 — substitui days_active na separacao de novatos)
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    hoje = pd.Timestamp.now(tz=df["registration_date"].dt.tz).normalize()
    df["days_since_registration"] = (hoje - df["registration_date"]).dt.days
    df["days_since_registration"] = df["days_since_registration"].fillna(9999).astype(int)

    return df


# ============================================================
# SCORING
# ============================================================
def normalizar_percentil(series: pd.Series, inverter: bool = False) -> pd.Series:
    """Normaliza para 0-100 usando percentil rank."""
    ranks = series.rank(pct=True, method="average") * 100
    if inverter:
        ranks = 100 - ranks
    return ranks.clip(0, 100)


def calcular_pvs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Player Value Score (PVS) v1.3 — pesos do GUIA_REPRODUCAO_PCR.md (Castrin, 27/04/2026).

    Pesos v1.3 (9 componentes ponderados, soma teorica 100):
      +25 GGR | +15 Deposito | +12 Recencia | +10 Margem GGR/Turnover
      +10 Num Deps | +8 Dias Ativos | +5 Mix Produto | +5 Taxa Atividade
      -10 Bonus Penalizador

    Reverte v2.0 (rebalanceio 21/04 pra "65% peso valor direto") porque o guia
    do Castrin (27/04) descreve v1.3 como a versao adotada pelo time. Manter
    coerencia com HTML estrategia + scripts 01_aplicar_modelo_v2 do Castrin.
    """
    log.info("Calculando Player Value Score (PVS) v1.3...")
    result = df.copy()

    result["score_ggr"] = normalizar_percentil(result["ggr_total"])
    result["score_deposit"] = normalizar_percentil(result["total_deposits"])
    result["score_recencia"] = normalizar_percentil(result["recency_days"], inverter=True)

    result["margem_ggr"] = np.where(
        result["turnover_total"] > 0,
        result["ggr_total"] / result["turnover_total"],
        0,
    )
    result["score_margem"] = normalizar_percentil(result["margem_ggr"])
    result["score_num_dep"] = normalizar_percentil(result["num_deposits"])
    result["score_dias_ativos"] = normalizar_percentil(result["days_active"])

    result["score_mix"] = (
        result["product_type"]
        .map({"MISTO": 100, "CASINO": 40, "SPORT": 40, "OUTRO": 0})
        .fillna(0)
    )

    result["taxa_atividade"] = (result["days_active"] / JANELA_DIAS).clip(0, 1)
    result["score_atividade"] = result["taxa_atividade"] * 100

    result["bonus_ratio"] = np.where(
        result["total_deposits"] > 0,
        result["bonus_issued"] / result["total_deposits"],
        0,
    )
    result["score_bonus_pen"] = normalizar_percentil(result["bonus_ratio"])

    # PVS v1.3 — pesos do guia oficial Castrin (GUIA_REPRODUCAO_PCR.md, secao 2.1)
    result["pvs"] = (
        result["score_ggr"]           * 0.25   # GGR Total
        + result["score_deposit"]     * 0.15   # Deposito Total
        + result["score_recencia"]    * 0.12   # Recencia
        + result["score_margem"]      * 0.10   # Margem GGR/Turnover
        + result["score_num_dep"]     * 0.10   # Numero de Depositos
        + result["score_dias_ativos"] * 0.08   # Dias Ativos
        + result["score_mix"]         * 0.05   # Mix de Produto
        + result["score_atividade"]   * 0.05   # Taxa de Atividade
        - result["score_bonus_pen"]   * 0.10   # Penalizacao de Bonus
    ).clip(0, 100)

    log.info(
        f"  -> PVS: min={result['pvs'].min():.1f}, "
        f"median={result['pvs'].median():.1f}, "
        f"max={result['pvs'].max():.1f}"
    )
    return result


def atribuir_rating(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rating PCR escala E-S + NEW (v2.0 — regra Castrin 27/04/2026):
      NEW = limbo benigno (cadastro<30d AND deps<3) — fora do ranking PVS
            Recebe acoes especificas de onboarding (matching FTD, T+24h/T+48h/T+7d).
            Nao recebe regua agressiva (nem corte de bonus, nem bonus alto).
            Sai do NEW por >=30 dias de cadastro OU >=3 depositos.
      E   = Bottom 25% dos maduros
      D   = 25-50%
      C   = 50-75%
      B   = 75-92%
      A   = 92-99%
      S   = Top 1%

    Motivo da regra NEW v2.0 (Castrin 27/04/2026):
      Janela de onboarding precisa ser maior pra acomodar jornadas de boas-vindas
      maduras (T+24h/T+48h/T+7d/matching FTD). Threshold anterior (7d ativos)
      derrubava o jogador no ranking E-S muito cedo, antes da regua de onboarding
      terminar. Novo criterio (30d cadastro / 3 deps) usa tempo de conta como
      proxy mais estavel que dias ativos.
    """
    log.info("Atribuindo ratings PCR (escala E-S + NEW v2.0, regra Castrin 27/04)...")
    result = df.copy()

    # Separa novatos ANTES do ranking PVS (regra Castrin 27/04/2026):
    # NEW = "limbo benigno" — jogador em janela de onboarding nao recebe regua
    # agressiva (nem corte de bonus, nem bonus alto). Sai do NEW quando passa
    # >=30 dias do cadastro OU faz >=3 depositos totais.
    # Antes (v1.3): days_active<7 AND num_deposits<2 (43% da base virava NEW,
    # inflando o bucket com casuais antigos low-deposit).
    eh_novato = (
        (result["days_since_registration"] < NEW_REGISTRATION_DAYS)
        & (result["num_deposits"] < NEW_DEPOSITS_THRESHOLD)
    )
    qtd_novatos = int(eh_novato.sum())
    qtd_maduros = int((~eh_novato).sum())
    log.info(
        f"  NEW (limbo benigno): {qtd_novatos:,} "
        f"({qtd_novatos/len(result)*100:.1f}% da base) "
        f"| days_since_registration < {NEW_REGISTRATION_DAYS} "
        f"AND num_deposits < {NEW_DEPOSITS_THRESHOLD}"
    )
    log.info(f"  Maduros (entram no ranking PVS): {qtd_maduros:,}")

    # Calcula percentis apenas nos maduros (pra nao distorcer a cauda)
    pvs_maduros = result.loc[~eh_novato, "pvs"]
    if qtd_maduros == 0:
        log.warning("  Nenhum jogador maduro — nao ha ranking PVS pra calcular.")
        result["rating"] = np.where(eh_novato, "NEW", "E")
        return result

    p25 = pvs_maduros.quantile(0.25)
    p50 = pvs_maduros.quantile(0.50)
    p75 = pvs_maduros.quantile(0.75)
    p92 = pvs_maduros.quantile(0.92)
    p99 = pvs_maduros.quantile(0.99)

    log.info(
        f"  Cortes PVS (so maduros): "
        f"E<{p25:.1f} | D<{p50:.1f} | C<{p75:.1f} | "
        f"B<{p92:.1f} | A<{p99:.1f} | S>={p99:.1f}"
    )

    conditions = [
        result["pvs"] >= p99,
        result["pvs"] >= p92,
        result["pvs"] >= p75,
        result["pvs"] >= p50,
        result["pvs"] >= p25,
    ]
    choices = ["S", "A", "B", "C", "D"]
    result["rating"] = np.select(conditions, choices, default="E")

    # Sobrescreve novatos com NEW (mesmo que tenham PVS alto — amostra insuficiente)
    result.loc[eh_novato, "rating"] = "NEW"

    return result


def calcular_metricas_derivadas(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula W/D ratio, net deposit e GGR por dia ativo."""
    result = df.copy()
    result["wd_ratio"] = np.where(
        result["total_deposits"] > 0,
        result["total_cashouts"] / result["total_deposits"],
        0,
    )
    result["net_deposit"] = result["total_deposits"] - result["total_cashouts"]
    result["ggr_por_dia"] = np.where(
        result["days_active"] > 0,
        result["ggr_total"] / result["days_active"],
        0,
    )
    return result


# ============================================================
# PERSISTENCIA — Super Nova DB
# ============================================================
def setup_table():
    """Cria schema, tabela, indices e views se nao existirem."""
    log.info("Verificando/criando estrutura no Super Nova DB...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    # v1.3: amplia VARCHAR(2) -> VARCHAR(10) em bases ja existentes pra aceitar 'NEW'.
    # Idempotente — roda sempre e e no-op se ja estiver VARCHAR(10).
    try:
        execute_supernova(DDL_ALTER_RATING_V13)
    except Exception as e:
        log.warning(f"  ALTER TABLE rating VARCHAR(10) falhou (provavelmente ja aplicado): {e}")
    for idx_sql in DDL_INDEXES:
        execute_supernova(idx_sql)
    execute_supernova(DDL_VIEW)
    execute_supernova(DDL_VIEW_RESUMO)
    log.info("  OK: tabela + indices + views prontos")


def gravar_no_banco(df: pd.DataFrame, snapshot_date: str):
    """
    Grava PCR no Super Nova DB.
    Estrategia: DELETE WHERE snapshot_date + INSERT (preserva historico, idempotente no dia).
    Alinhado com scripts/risk_matrix_pipeline.py (mesmo padrao).
    """
    log.info(f"Gravando {len(df):,} jogadores no Super Nova DB (snapshot {snapshot_date})...")

    cols_insert = [
        "snapshot_date", "player_id", "external_id", "rating", "pvs",
        "ggr_total", "ngr_total", "total_deposits", "total_cashouts",
        "num_deposits", "days_active", "recency_days", "product_type",
        "casino_rounds", "sport_bets", "bonus_issued", "bonus_ratio",
        "wd_ratio", "net_deposit", "margem_ggr", "ggr_por_dia",
        "affiliate_id", "c_category", "registration_date",
    ]

    insert_sql = f"""
        INSERT INTO multibet.pcr_ratings ({', '.join(cols_insert)})
        VALUES ({', '.join(['%s'] * len(cols_insert))})
    """

    # Preparar registros
    records = []
    for _, row in df.iterrows():
        records.append((
            snapshot_date,
            _safe_int(row.get("player_id")),
            _safe_int(row.get("external_id")),
            str(row.get("rating", "E")),
            round(float(row.get("pvs", 0)), 2),
            _safe_float(row.get("ggr_total")),
            _safe_float(row.get("ngr_total")),
            _safe_float(row.get("total_deposits")),
            _safe_float(row.get("total_cashouts")),
            _safe_int(row.get("num_deposits")),
            _safe_int(row.get("days_active")),
            _safe_int(row.get("recency_days")),
            str(row.get("product_type", "OUTRO")),
            _safe_int(row.get("casino_rounds")),
            _safe_int(row.get("sport_bets")),
            _safe_float(row.get("bonus_issued")),
            _safe_float(row.get("bonus_ratio"), decimals=4),
            _safe_float(row.get("wd_ratio"), decimals=4),
            _safe_float(row.get("net_deposit")),
            _safe_float(row.get("margem_ggr"), decimals=4),
            _safe_float(row.get("ggr_por_dia")),
            str(row.get("affiliate_id")) if pd.notna(row.get("affiliate_id")) else None,
            str(row.get("c_category")) if pd.notna(row.get("c_category")) else None,
            row.get("registration_date") if pd.notna(row.get("registration_date")) else None,
        ))

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # DELETE apenas do snapshot atual — preserva historico dos dias anteriores
            cur.execute(
                "DELETE FROM multibet.pcr_ratings WHERE snapshot_date = %s",
                (snapshot_date,),
            )
            log.info(f"  -> snapshot {snapshot_date} limpo (historico preservado)")

            # Insert em batch (paginas de 1000)
            psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()
        log.info(f"  -> {len(records):,} registros inseridos com sucesso")
    finally:
        conn.close()
        tunnel.stop()


# ============================================================
# HELPERS
# ============================================================
def _safe_int(val):
    """Converte para int ou None."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val, decimals=2):
    """Converte para float arredondado ou None."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        return round(float(val), decimals)
    except (ValueError, TypeError):
        return None


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="PCR Pipeline — Player Credit Rating")
    parser.add_argument("--dry-run", action="store_true", help="Apenas CSV, sem gravar no banco")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"PCR PIPELINE v1.0 — snapshot {SNAPSHOT_DATE}")
    log.info(f"  Modo: {'DRY-RUN (sem banco)' if args.dry_run else 'PRODUCAO (grava no banco)'}")
    log.info("=" * 60)

    # 1. Extrair dados do Athena
    df = extrair_metricas_jogadores()
    if df.empty:
        log.error("Nenhum dado retornado do Athena. Abortando.")
        return

    # 2. Calcular metricas derivadas
    df = calcular_metricas_derivadas(df)

    # 3. Calcular PVS
    df = calcular_pvs(df)

    # 4. Atribuir ratings
    df = atribuir_rating(df)

    # 5. Resumo no log
    resumo = df.groupby("rating").agg(
        jogadores=("player_id", "count"),
        ggr_total=("ggr_total", "sum"),
        pvs_medio=("pvs", "mean"),
    ).reset_index()
    log.info("Distribuicao de ratings:")
    for _, r in resumo.iterrows():
        log.info(f"  {r['rating']}: {r['jogadores']:,} jogadores | GGR R${r['ggr_total']:,.0f} | PVS medio {r['pvs_medio']:.1f}")

    # 6. Salvar CSV (sempre, independente de dry-run)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cols_csv = [
        "player_id", "external_id", "rating", "pvs",
        "ggr_total", "ngr_total", "total_deposits", "total_cashouts",
        "num_deposits", "days_active", "recency_days",
        "product_type", "casino_rounds", "sport_bets",
        "bonus_issued", "bonus_ratio", "wd_ratio", "net_deposit",
        "margem_ggr", "ggr_por_dia", "affiliate_id", "c_category",
        "registration_date",
    ]
    cols_existentes = [c for c in cols_csv if c in df.columns]
    csv_path = os.path.join(OUTPUT_DIR, f"pcr_ratings_{SNAPSHOT_DATE}_FINAL.csv")
    df[cols_existentes].sort_values("pvs", ascending=False).to_csv(csv_path, index=False)
    log.info(f"CSV salvo: {csv_path} ({len(df):,} jogadores)")

    # 7. Gravar no banco (se nao for dry-run)
    if not args.dry_run:
        setup_table()
        gravar_no_banco(df, SNAPSHOT_DATE)
        log.info("Gravacao no Super Nova DB concluida!")
    else:
        log.info("DRY-RUN: banco nao foi alterado.")

    log.info("=" * 60)
    log.info(f"PCR PIPELINE CONCLUIDO — {len(df):,} jogadores processados")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
