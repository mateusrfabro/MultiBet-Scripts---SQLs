"""
Pipeline: Segmentacao A+S Diaria — entrega para CRM operacionalizar
======================================================================
Demanda Castrin (27/04/2026): enviar diariamente base com jogadores Rating
A e S (do PCR) + cruzamento com Matriz de Risco + coluna 'tendencia'
(Estavel/Subindo/Caindo) para o time de CRM operacionalizar a segmentacao.

ENTRADAS (Super Nova DB — populadas por outros pipelines):
  - multibet.pcr_atual    (PCR rodado pelo orquestrador as 03:30 BRT)
  - multibet.matriz_risco (Matriz Risco v2 rodada as 02:30 BRT)

SAIDAS:
  - multibet.segmentacao_sa_diaria  (tabela incremental, idempotente no dia)
  - multibet.celula_monitor_diario  (monitora celulas Rating x Tier vs anomalias)
  - output/players_segmento_SA_<date>_FINAL.csv (BR: sep=";" decimal=",")
  - output/players_segmento_SA_<date>_FINAL_legenda.txt
  - E-mail enviado a 6 destinatarios (CRM)

REGRAS CRITICAS (do GUIA_REPRODUCAO_PCR.md):
  1. NAO filtrar c_category — vai como coluna informativa, CRM decide
  2. CSVs em formato BR: sep=";", decimal=",", encoding utf-8-sig
  3. Persistencia incremental (NUNCA TRUNCATE — soma snapshot novo)
  4. Janela 90d rolling (mesma do PCR upstream)

CRON EC2 ETL: 0 7 * * *  (07:00 UTC = 04:00 BRT, 30min apos PCR)

Uso:
    python pipelines/segmentacao_sa_diaria.py              # producao completa
    python pipelines/segmentacao_sa_diaria.py --no-email   # gera CSV mas nao envia
    python pipelines/segmentacao_sa_diaria.py --no-db      # so CSV, nao grava banco
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2.extras

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova, get_supernova_connection
from db.email_sender import enviar_email
from pipelines.segmentacao_sa_enriquecimento import (
    bloco_4_derivaveis,
    bloco_5_risk_tags_flags,
    bloco_5b_btr_bonus,
    bloco_6_kyc,
    bloco_1_2_metricas_30d,
    bloco_3_top_jogos_e_temporal,
    assert_all_v2_cols,
)
from pipelines.segmentacao_sa_smartico import publicar_smartico

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
BRT = timezone(timedelta(hours=-3))
SNAPSHOT_DATE = datetime.now(BRT).strftime("%Y-%m-%d")
OUTPUT_DIR = "output"

# Limiar de "quase mudar de tier" (proximidade percentual ao corte)
# Ex: A com PVS >= (limite_S - 2 pts) é "Subindo"; S com PVS <= (limite_A + 2 pts) é "Caindo"
TENDENCIA_BUFFER_PVS = 2.0
# Quantos dias atras comparar para tendencia temporal (quando ha snapshot)
TENDENCIA_LOOKBACK_DAYS = 7
# Delta de PVS pra considerar "Subindo"/"Caindo" no comparativo temporal
TENDENCIA_DELTA_TEMPORAL = 3.0

# Destinatarios pedidos pelo Castrin (27/04/2026)
# MODO TESTE: enviando soh pro caio.ferreira (head) validar antes de liberar geral.
# Lista completa preservada comentada — descomentar apos OK do head.
EMAIL_DESTINATARIOS = [
    "caio.ferreira@multi.bet.br",
]
# EMAIL_DESTINATARIOS_PROD = [
#     "victor.campello@multi.bet.br",
#     "liliane.carvalho@multi.bet.br",
#     "raphael.braga@multi.bet.br",
#     "ext.andreza.ribeiro@multi.bet.br",
#     "felipe.lio@multi.bet.br",
#     "gabriel.tameirao@multi.bet.br",
# ]

# ============================================================
# DDL
# ============================================================
DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_SEGMENTACAO = """
CREATE TABLE IF NOT EXISTS multibet.segmentacao_sa_diaria (
    snapshot_date       DATE         NOT NULL,
    player_id           BIGINT       NOT NULL,
    external_id         BIGINT,
    rating              VARCHAR(10)  NOT NULL,
    pvs                 NUMERIC(8,2) NOT NULL,
    tendencia           VARCHAR(15),  -- Estavel / Subindo / Caindo
    tendencia_motivo    VARCHAR(50),  -- "threshold" / "temporal+3.5" etc.
    classificacao_risco VARCHAR(30),  -- Muito Bom/Bom/Mediano/Ruim/Muito Ruim/Nao Identificado
    score_risco         NUMERIC(8,2),
    c_category          VARCHAR(50),
    affiliate_id        VARCHAR(300),
    registration_date   DATE,
    -- Metricas agregadas 90d (do PCR)
    ggr_total           NUMERIC(15,2),
    ngr_total           NUMERIC(15,2),
    total_deposits      NUMERIC(15,2),
    total_cashouts      NUMERIC(15,2),
    num_deposits        INTEGER,
    days_active         INTEGER,
    recency_days        INTEGER,
    product_type        VARCHAR(10),
    casino_rounds       BIGINT,
    sport_bets          BIGINT,
    bonus_issued        NUMERIC(15,2),
    bonus_ratio         NUMERIC(8,4),
    wd_ratio            NUMERIC(8,4),
    net_deposit         NUMERIC(15,2),
    margem_ggr          NUMERIC(8,4),
    ggr_por_dia         NUMERIC(15,2),
    -- ===== v2: enriquecimento (32 cols Castrin) =====
    -- Bloco 4: ciclo de vida e flags regulatorios
    lifecycle_status            VARCHAR(15),  -- NEW/ACTIVE/AT_RISK/CHURNED/DORMANT
    rg_status                   VARCHAR(15),  -- NORMAL/RG_CLOSED/RG_COOL_OFF
    account_restricted_flag     SMALLINT,
    self_excluded_flag          SMALLINT,
    primary_vertical            VARCHAR(10),  -- CASINO/SPORT/MISTO/OUTRO
    product_mix                 VARCHAR(15),  -- CASINO_PURO/SPORT_PURO/MISTO/INACTIVE
    -- Bloco 6: KYC + restricoes
    kyc_status                  VARCHAR(20),
    kyc_level                   VARCHAR(20),
    self_exclusion_status       VARCHAR(30),
    cool_off_status             VARCHAR(30),
    restricted_product          VARCHAR(50),
    -- Bloco 5: bonus abuse flag
    bonus_abuse_flag            SMALLINT,
    -- Bloco 1+2: metricas 30d (financeiras + aposta)
    ggr_30d                     NUMERIC(15,2),
    ngr_30d                     NUMERIC(15,2),
    deposit_amount_30d          NUMERIC(15,2),
    deposit_count_30d           INTEGER,
    withdrawal_amount_30d       NUMERIC(15,2),
    withdrawal_count_30d        INTEGER,
    avg_deposit_ticket_30d      NUMERIC(15,2),
    avg_deposit_ticket_lifetime NUMERIC(15,2),
    bet_amount_30d              NUMERIC(15,2),
    bet_count_30d               BIGINT,
    avg_bet_ticket_30d          NUMERIC(15,4),
    avg_deposit_ticket_tier     NUMERIC(15,2),
    avg_bet_ticket_tier         NUMERIC(15,4),
    -- Bloco 3: top jogos/providers/temporal por TIER
    top_provider_1              VARCHAR(60),
    top_provider_2              VARCHAR(60),
    top_game_1                  VARCHAR(120),
    top_game_2                  VARCHAR(120),
    top_game_1_tier_turnover    VARCHAR(120),
    top_game_2_tier_turnover    VARCHAR(120),
    top_game_3_tier_turnover    VARCHAR(120),
    top_game_1_tier_rounds      VARCHAR(120),
    top_game_2_tier_rounds      VARCHAR(120),
    top_game_3_tier_rounds      VARCHAR(120),
    dominant_weekday            VARCHAR(10),
    dominant_timebucket         VARCHAR(15),  -- MADRUGADA/MANHA/TARDE/NOITE
    last_product_played         VARCHAR(15),
    -- Bloco 5b: BTR + bonus extras
    bonus_issued_30d            NUMERIC(15,2),
    btr_30d                     NUMERIC(8,4),
    btr_casino_30d              NUMERIC(8,4),
    btr_sport_30d               NUMERIC(8,4),
    bonus_dependency_ratio_lifetime NUMERIC(8,4),
    ngr_per_bonus_real_30d      NUMERIC(15,4),
    last_bonus_date             DATE,
    last_bonus_type             VARCHAR(15),
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, player_id)
);
"""

DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_seg_sa_snapshot ON multibet.segmentacao_sa_diaria (snapshot_date DESC);",
    "CREATE INDEX IF NOT EXISTS idx_seg_sa_rating ON multibet.segmentacao_sa_diaria (snapshot_date, rating);",
    "CREATE INDEX IF NOT EXISTS idx_seg_sa_tendencia ON multibet.segmentacao_sa_diaria (snapshot_date, tendencia);",
]

DDL_CELULA_MONITOR = """
CREATE TABLE IF NOT EXISTS multibet.celula_monitor_diario (
    snapshot_date       DATE         NOT NULL,
    rating              VARCHAR(10)  NOT NULL,
    classificacao_risco VARCHAR(30)  NOT NULL,
    jogadores           INTEGER,
    bonus_emitido       NUMERIC(15,2),
    ngr_total           NUMERIC(15,2),
    pct_bonus_ngr       NUMERIC(10,2),
    sinal_negativo      BOOLEAN,     -- ngr < 0 nesta celula?
    rodadas_neg_consec  INTEGER,     -- contador consecutivo (sugestao Castrin)
    flag_investigar     BOOLEAN,     -- true se rodadas_neg_consec >= 3
    created_at          TIMESTAMPTZ  DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, rating, classificacao_risco)
);
"""

DDL_VIEW_ATUAL = """
CREATE OR REPLACE VIEW multibet.segmentacao_sa_atual AS
SELECT *
FROM multibet.segmentacao_sa_diaria
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM multibet.segmentacao_sa_diaria);
"""


# ============================================================
# EXTRACAO
# ============================================================
def carregar_pcr_atual() -> pd.DataFrame:
    """Carrega snapshot mais recente do PCR (do orquestrador)."""
    log.info("Carregando multibet.pcr_atual...")
    rows = execute_supernova(
        """
        SELECT player_id, external_id, rating, pvs,
               ggr_total, ngr_total, total_deposits, total_cashouts,
               num_deposits, days_active, recency_days, product_type,
               casino_rounds, sport_bets, bonus_issued, bonus_ratio,
               wd_ratio, net_deposit, margem_ggr, ggr_por_dia,
               affiliate_id, c_category, registration_date, snapshot_date
        FROM multibet.pcr_atual;
        """,
        fetch=True,
    )
    if not rows:
        raise RuntimeError("multibet.pcr_atual vazio — PCR upstream nao rodou?")

    df = pd.DataFrame(rows, columns=[
        "player_id", "external_id", "rating", "pvs",
        "ggr_total", "ngr_total", "total_deposits", "total_cashouts",
        "num_deposits", "days_active", "recency_days", "product_type",
        "casino_rounds", "sport_bets", "bonus_issued", "bonus_ratio",
        "wd_ratio", "net_deposit", "margem_ggr", "ggr_por_dia",
        "affiliate_id", "c_category", "registration_date", "pcr_snapshot_date",
    ])
    log.info(f"  -> {len(df):,} jogadores (snapshot PCR: {df['pcr_snapshot_date'].iloc[0]})")
    return df


def carregar_matriz_risco() -> pd.DataFrame:
    """
    Carrega Matriz de Risco v2.

    JOIN canonico (alinhado com 01_aplicar_modelo_v2.py do Castrin):
    PCR.external_id (Smartico) <-> matriz_risco.user_ext_id (Smartico).
    """
    log.info("Carregando multibet.matriz_risco...")
    rows = execute_supernova(
        "SELECT user_ext_id, classificacao, score_norm FROM multibet.matriz_risco;",
        fetch=True,
    )
    mr = pd.DataFrame(rows, columns=["user_ext_id", "classificacao_risco", "score_risco"])
    mr["user_ext_id"] = mr["user_ext_id"].astype(str)
    log.info(f"  -> {len(mr):,} jogadores na matriz")
    return mr


def carregar_pvs_lookback(dias: int) -> pd.DataFrame | None:
    """
    Tenta carregar snapshot de N dias atras pra calcular tendencia temporal.
    Retorna None se nao existe snapshot da data alvo.
    """
    data_alvo = (datetime.now(BRT) - timedelta(days=dias)).strftime("%Y-%m-%d")
    log.info(f"Procurando snapshot de {data_alvo} pra tendencia temporal...")
    rows = execute_supernova(
        "SELECT player_id, pvs, rating FROM multibet.pcr_ratings WHERE snapshot_date = %s;",
        params=(data_alvo,),
        fetch=True,
    )
    if not rows:
        log.info(f"  -> sem snapshot em {data_alvo}, tendencia sera so threshold-based")
        return None
    df = pd.DataFrame(rows, columns=["player_id", "pvs_anterior", "rating_anterior"])
    log.info(f"  -> {len(df):,} jogadores encontrados em {data_alvo}")
    return df


# ============================================================
# TRANSFORMACAO
# ============================================================
def filtrar_a_e_s(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra Rating A ou S (foco da entrega CRM)."""
    sa = df[df["rating"].isin(["A", "S"])].copy()
    log.info(f"Filtro Rating A+S: {len(sa):,} jogadores ({(df['rating']=='S').sum():,} S + {(df['rating']=='A').sum():,} A)")
    return sa


def calcular_tendencia(df: pd.DataFrame, df_lookback: pd.DataFrame | None) -> pd.DataFrame:
    """
    Hibrido:
      - Threshold (sempre): A com PVS perto do corte S = Subindo; S com PVS perto do corte A = Caindo
      - Temporal (se ha snapshot): delta de PVS vs N dias atras
    Combina os dois — temporal tem prioridade quando disponivel.
    """
    log.info("Calculando coluna 'tendencia' (hibrido threshold + temporal)...")
    df = df.copy()

    # Cortes PVS dentro do dataset (so Rating A e S aqui)
    pvs_a = df.loc[df["rating"] == "A", "pvs"].astype(float)
    pvs_s = df.loc[df["rating"] == "S", "pvs"].astype(float)
    if pvs_a.empty or pvs_s.empty:
        log.warning("Sem A ou sem S no dataset — tendencia sera 'Estavel' pra todos.")
        df["tendencia"] = "Estavel"
        df["tendencia_motivo"] = "sem_corte"
        return df

    # Limite A->S = max(PVS dos A) ~= min(PVS dos S)
    limite_as = float(pvs_s.min())
    log.info(f"  Limite A<->S no PVS: {limite_as:.2f} (corte natural entre tiers)")

    # Threshold-based default
    df["pvs"] = df["pvs"].astype(float)
    df["tendencia"] = "Estavel"
    df["tendencia_motivo"] = "threshold"

    eh_subindo = (df["rating"] == "A") & (df["pvs"] >= limite_as - TENDENCIA_BUFFER_PVS)
    eh_caindo  = (df["rating"] == "S") & (df["pvs"] <= limite_as + TENDENCIA_BUFFER_PVS)
    df.loc[eh_subindo, "tendencia"] = "Subindo"
    df.loc[eh_caindo,  "tendencia"] = "Caindo"

    qtd_sub = int(eh_subindo.sum())
    qtd_cai = int(eh_caindo.sum())
    log.info(f"  Threshold (buffer +-{TENDENCIA_BUFFER_PVS}): {qtd_sub} subindo, {qtd_cai} caindo")

    # Temporal (sobrescreve quando disponivel e delta significativo)
    if df_lookback is not None and not df_lookback.empty:
        df_lookback["player_id"] = df_lookback["player_id"].astype(int)
        df["player_id"] = df["player_id"].astype(int)
        df = df.merge(df_lookback, on="player_id", how="left")
        df["delta_pvs"] = df["pvs"] - df["pvs_anterior"].astype(float)

        sub_temp = df["delta_pvs"] >= TENDENCIA_DELTA_TEMPORAL
        cai_temp = df["delta_pvs"] <= -TENDENCIA_DELTA_TEMPORAL

        # Sobrescreve quando o sinal temporal e claro
        df.loc[sub_temp, "tendencia"] = "Subindo"
        df.loc[sub_temp, "tendencia_motivo"] = "temporal_+" + df.loc[sub_temp, "delta_pvs"].round(1).astype(str)
        df.loc[cai_temp, "tendencia"] = "Caindo"
        df.loc[cai_temp, "tendencia_motivo"] = "temporal_" + df.loc[cai_temp, "delta_pvs"].round(1).astype(str)

        qtd_sub_t = int(sub_temp.sum())
        qtd_cai_t = int(cai_temp.sum())
        log.info(f"  Temporal (delta>={TENDENCIA_DELTA_TEMPORAL}): {qtd_sub_t} subindo, {qtd_cai_t} caindo")

        # Drop colunas auxiliares
        df = df.drop(columns=["pvs_anterior", "rating_anterior", "delta_pvs"], errors="ignore")

    # Resumo final
    log.info("Distribuicao final de tendencia:")
    for v, q in df["tendencia"].value_counts().items():
        log.info(f"  {v}: {q:,}")

    return df


def juntar_matriz(df: pd.DataFrame, mr: pd.DataFrame) -> pd.DataFrame:
    """
    Joina segmentacao A+S com Matriz de Risco.
    JOIN canonico Castrin: external_id (PCR/Smartico) <-> user_ext_id (Matriz/Smartico).
    """
    log.info("Cruzando com Matriz de Risco (external_id <-> user_ext_id)...")
    df["external_id_str"] = df["external_id"].astype(str)
    df = df.merge(mr, left_on="external_id_str", right_on="user_ext_id", how="left")
    df = df.drop(columns=["external_id_str", "user_ext_id"], errors="ignore")
    df["classificacao_risco"] = df["classificacao_risco"].fillna("Nao Identificado")
    cobertura = (df["classificacao_risco"] != "Nao Identificado").sum() / len(df) * 100
    log.info(f"  Cobertura matriz: {cobertura:.1f}%")
    return df


# ============================================================
# CELULA MONITOR (sugestao Castrin: flag se 3 rodadas negativas)
# ============================================================
def atualizar_celula_monitor(df_full_pcr: pd.DataFrame, mr: pd.DataFrame, snapshot_date: str):
    """
    Calcula crosstab Rating x Classificacao Risco e mantem contador de
    rodadas consecutivas negativas. Flag dispara se >= 3 rodadas.
    """
    log.info("Atualizando celula_monitor_diario...")

    # Cruzamento full (todos ratings, nao so A+S)
    # JOIN canonico Castrin: external_id <-> user_ext_id
    full = df_full_pcr.copy()
    full["external_id_str"] = full["external_id"].astype(str)
    full = full.merge(mr, left_on="external_id_str", right_on="user_ext_id", how="left")
    full["classificacao_risco"] = full["classificacao_risco"].fillna("Nao Identificado")

    grp = full.groupby(["rating", "classificacao_risco"]).agg(
        jogadores=("player_id", "count"),
        bonus_emitido=("bonus_issued", "sum"),
        ngr_total=("ngr_total", "sum"),
    ).reset_index()
    grp["pct_bonus_ngr"] = np.where(
        grp["ngr_total"] != 0, grp["bonus_emitido"] / grp["ngr_total"] * 100, np.nan
    )
    grp["sinal_negativo"] = grp["ngr_total"] < 0
    log.info(f"  -> {len(grp)} celulas calculadas, {grp['sinal_negativo'].sum()} com NGR negativo")

    # Pega rodadas_neg_consec do snapshot anterior pra cada celula
    rows_prev = execute_supernova(
        """
        SELECT rating, classificacao_risco, rodadas_neg_consec
        FROM multibet.celula_monitor_diario
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date) FROM multibet.celula_monitor_diario
            WHERE snapshot_date < %s
        );
        """,
        params=(snapshot_date,),
        fetch=True,
    )
    prev = pd.DataFrame(rows_prev or [], columns=["rating", "classificacao_risco", "rodadas_neg_anterior"])

    grp = grp.merge(prev, on=["rating", "classificacao_risco"], how="left")
    grp["rodadas_neg_anterior"] = grp["rodadas_neg_anterior"].fillna(0).astype(int)
    grp["rodadas_neg_consec"] = np.where(
        grp["sinal_negativo"], grp["rodadas_neg_anterior"] + 1, 0
    )
    grp["flag_investigar"] = grp["rodadas_neg_consec"] >= 3
    flag_count = int(grp["flag_investigar"].sum())
    if flag_count > 0:
        log.warning(f"  ⚠ {flag_count} celulas com >=3 rodadas negativas consecutivas (flag_investigar=true)")

    # Persiste idempotente: DELETE WHERE snapshot_date = today + INSERT
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM multibet.celula_monitor_diario WHERE snapshot_date = %s;",
                (snapshot_date,),
            )
            registros = [
                (
                    snapshot_date, str(r["rating"]), str(r["classificacao_risco"]),
                    int(r["jogadores"]),
                    float(r["bonus_emitido"]) if pd.notna(r["bonus_emitido"]) else None,
                    float(r["ngr_total"]) if pd.notna(r["ngr_total"]) else None,
                    float(r["pct_bonus_ngr"]) if pd.notna(r["pct_bonus_ngr"]) else None,
                    bool(r["sinal_negativo"]),
                    int(r["rodadas_neg_consec"]),
                    bool(r["flag_investigar"]),
                )
                for _, r in grp.iterrows()
            ]
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO multibet.celula_monitor_diario
                  (snapshot_date, rating, classificacao_risco, jogadores,
                   bonus_emitido, ngr_total, pct_bonus_ngr, sinal_negativo,
                   rodadas_neg_consec, flag_investigar)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                registros,
                page_size=200,
            )
        conn.commit()
        log.info(f"  -> {len(registros)} celulas inseridas")
    finally:
        conn.close()
        tunnel.stop()


# ============================================================
# PERSISTENCIA
# ============================================================
def setup_tabelas():
    log.info("Garantindo schema/tabelas/views...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_SEGMENTACAO)
    for sql in DDL_INDEXES:
        execute_supernova(sql)
    execute_supernova(DDL_CELULA_MONITOR)
    execute_supernova(DDL_VIEW_ATUAL)
    log.info("  OK")


def gravar_segmentacao(df: pd.DataFrame, snapshot_date: str):
    """
    Persistencia idempotente: DELETE WHERE snapshot_date + INSERT.
    NAO faz TRUNCATE — mantem historico de todos os snapshots.
    """
    log.info(f"Gravando {len(df):,} jogadores em multibet.segmentacao_sa_diaria...")
    cols = [
        "snapshot_date", "player_id", "external_id", "rating", "pvs",
        "tendencia", "tendencia_motivo", "classificacao_risco", "score_risco",
        "c_category", "affiliate_id", "registration_date",
        "ggr_total", "ngr_total", "total_deposits", "total_cashouts",
        "num_deposits", "days_active", "recency_days", "product_type",
        "casino_rounds", "sport_bets", "bonus_issued", "bonus_ratio",
        "wd_ratio", "net_deposit", "margem_ggr", "ggr_por_dia",
        # ===== v2: enriquecimento =====
        # Bloco 4
        "lifecycle_status", "rg_status", "account_restricted_flag",
        "self_excluded_flag", "primary_vertical", "product_mix",
        # Bloco 6
        "kyc_status", "kyc_level", "self_exclusion_status",
        "cool_off_status", "restricted_product",
        # Bloco 5
        "bonus_abuse_flag",
        # Bloco 1+2
        "ggr_30d", "ngr_30d", "deposit_amount_30d", "deposit_count_30d",
        "withdrawal_amount_30d", "withdrawal_count_30d",
        "avg_deposit_ticket_30d", "avg_deposit_ticket_lifetime",
        "bet_amount_30d", "bet_count_30d", "avg_bet_ticket_30d",
        "avg_deposit_ticket_tier", "avg_bet_ticket_tier",
        # Bloco 3
        "top_provider_1", "top_provider_2", "top_game_1", "top_game_2",
        "top_game_1_tier_turnover", "top_game_2_tier_turnover", "top_game_3_tier_turnover",
        "top_game_1_tier_rounds", "top_game_2_tier_rounds", "top_game_3_tier_rounds",
        "dominant_weekday", "dominant_timebucket", "last_product_played",
        # Bloco 5b
        "bonus_issued_30d", "btr_30d", "btr_casino_30d", "btr_sport_30d",
        "bonus_dependency_ratio_lifetime", "ngr_per_bonus_real_30d",
        "last_bonus_date", "last_bonus_type",
    ]
    insert_sql = f"""
        INSERT INTO multibet.segmentacao_sa_diaria ({', '.join(cols)})
        VALUES ({', '.join(['%s'] * len(cols))});
    """

    def _safe_str(v, max_len: int | None = None):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        s = str(v)
        return s[:max_len] if max_len else s

    registros = []
    for _, r in df.iterrows():
        registros.append((
            snapshot_date,
            _safe_int(r.get("player_id")),
            _safe_int(r.get("external_id")),
            str(r.get("rating")),
            float(r.get("pvs")),
            str(r.get("tendencia")),
            str(r.get("tendencia_motivo")),
            str(r.get("classificacao_risco")),
            _safe_float(r.get("score_risco")),
            str(r.get("c_category")) if pd.notna(r.get("c_category")) else None,
            str(r.get("affiliate_id")) if pd.notna(r.get("affiliate_id")) else None,
            r.get("registration_date") if pd.notna(r.get("registration_date")) else None,
            _safe_float(r.get("ggr_total")),
            _safe_float(r.get("ngr_total")),
            _safe_float(r.get("total_deposits")),
            _safe_float(r.get("total_cashouts")),
            _safe_int(r.get("num_deposits")),
            _safe_int(r.get("days_active")),
            _safe_int(r.get("recency_days")),
            str(r.get("product_type")) if pd.notna(r.get("product_type")) else None,
            _safe_int(r.get("casino_rounds")),
            _safe_int(r.get("sport_bets")),
            _safe_float(r.get("bonus_issued")),
            _safe_float(r.get("bonus_ratio"), 4),
            _safe_float(r.get("wd_ratio"), 4),
            _safe_float(r.get("net_deposit")),
            _safe_float(r.get("margem_ggr"), 4),
            _safe_float(r.get("ggr_por_dia")),
            # ===== v2 =====
            _safe_str(r.get("LIFECYCLE_STATUS"), 15),
            _safe_str(r.get("RG_STATUS"), 15),
            _safe_int(r.get("ACCOUNT_RESTRICTED_FLAG")),
            _safe_int(r.get("SELF_EXCLUDED_FLAG")),
            _safe_str(r.get("PRIMARY_VERTICAL"), 10),
            _safe_str(r.get("PRODUCT_MIX"), 15),
            _safe_str(r.get("KYC_STATUS"), 20),
            _safe_str(r.get("kyc_level"), 20),
            _safe_str(r.get("self_exclusion_status"), 30),
            _safe_str(r.get("cool_off_status"), 30),
            _safe_str(r.get("restricted_product"), 50),
            _safe_int(r.get("BONUS_ABUSE_FLAG")),
            _safe_float(r.get("GGR_30D")),
            _safe_float(r.get("NGR_30D")),
            _safe_float(r.get("DEPOSIT_AMOUNT_30D")),
            _safe_int(r.get("DEPOSIT_COUNT_30D")),
            _safe_float(r.get("WITHDRAWAL_AMOUNT_30D")),
            _safe_int(r.get("WITHDRAWAL_COUNT_30D")),
            _safe_float(r.get("AVG_DEPOSIT_TICKET_30D")),
            _safe_float(r.get("AVG_DEPOSIT_TICKET_LIFETIME")),
            _safe_float(r.get("BET_AMOUNT_30D")),
            _safe_int(r.get("BET_COUNT_30D")),
            _safe_float(r.get("AVG_BET_TICKET_30D"), 4),
            _safe_float(r.get("AVG_DEPOSIT_TICKET_TIER")),
            _safe_float(r.get("AVG_BET_TICKET_TIER"), 4),
            _safe_str(r.get("TOP_PROVIDER_1"), 60),
            _safe_str(r.get("TOP_PROVIDER_2"), 60),
            _safe_str(r.get("TOP_GAME_1"), 120),
            _safe_str(r.get("TOP_GAME_2"), 120),
            _safe_str(r.get("TOP_GAME_1_TIER_TURNOVER"), 120),
            _safe_str(r.get("TOP_GAME_2_TIER_TURNOVER"), 120),
            _safe_str(r.get("TOP_GAME_3_TIER_TURNOVER"), 120),
            _safe_str(r.get("TOP_GAME_1_TIER_ROUNDS"), 120),
            _safe_str(r.get("TOP_GAME_2_TIER_ROUNDS"), 120),
            _safe_str(r.get("TOP_GAME_3_TIER_ROUNDS"), 120),
            _safe_str(r.get("DOMINANT_WEEKDAY"), 10),
            _safe_str(r.get("DOMINANT_TIMEBUCKET"), 15),
            _safe_str(r.get("LAST_PRODUCT_PLAYED"), 15),
            _safe_float(r.get("BONUS_ISSUED_30D")),
            _safe_float(r.get("BTR_30D"), 4),
            _safe_float(r.get("BTR_CASINO_30D"), 4),
            _safe_float(r.get("BTR_SPORT_30D"), 4),
            _safe_float(r.get("BONUS_DEPENDENCY_RATIO_LIFETIME"), 4),
            _safe_float(r.get("NGR_PER_BONUS_REAL_30D"), 4),
            r.get("LAST_BONUS_DATE") if pd.notna(r.get("LAST_BONUS_DATE")) else None,
            _safe_str(r.get("LAST_BONUS_TYPE"), 15),
        ))

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM multibet.segmentacao_sa_diaria WHERE snapshot_date = %s;",
                (snapshot_date,),
            )
            log.info(f"  -> snapshot {snapshot_date} limpo (historico preservado)")
            psycopg2.extras.execute_batch(cur, insert_sql, registros, page_size=500)
        conn.commit()
        log.info(f"  -> {len(registros):,} registros inseridos")
    finally:
        conn.close()
        tunnel.stop()


# ============================================================
# CSV + LEGENDA + EMAIL
# ============================================================
def gerar_csv(df: pd.DataFrame, snapshot_date: str) -> tuple[Path, Path]:
    """Gera CSV BR (sep=';' decimal=',') + legenda."""
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    csv_path = Path(OUTPUT_DIR) / f"players_segmento_SA_{snapshot_date}_FINAL.csv"
    legenda_path = Path(OUTPUT_DIR) / f"players_segmento_SA_{snapshot_date}_FINAL_legenda.txt"

    # Ordena por PVS desc, escreve formato BR
    # Ordem alinhada com CSV de referencia do Castrin (57 col)
    cols_csv = [
        # ----- Identificacao + classificacao -----
        "player_id", "external_id", "registration_date", "affiliate_id",
        "rating", "pvs", "tendencia", "tendencia_motivo",
        "classificacao_risco", "score_risco",
        "LIFECYCLE_STATUS", "recency_days",
        # ----- Metricas 30d -----
        "GGR_30D", "NGR_30D", "ngr_total",  # NGR_90D = ngr_total
        "DEPOSIT_AMOUNT_30D", "DEPOSIT_COUNT_30D",
        "AVG_DEPOSIT_TICKET_30D", "AVG_DEPOSIT_TICKET_TIER",
        "BET_AMOUNT_30D", "BET_COUNT_30D",
        "AVG_BET_TICKET_30D", "AVG_BET_TICKET_TIER",
        "WITHDRAWAL_AMOUNT_30D", "WITHDRAWAL_COUNT_30D",
        # ----- Comportamento e produto -----
        "PRODUCT_MIX", "PRIMARY_VERTICAL",
        "TOP_PROVIDER_1", "TOP_PROVIDER_2",
        "TOP_GAME_1", "TOP_GAME_2",
        "TOP_GAME_1_TIER_TURNOVER", "TOP_GAME_2_TIER_TURNOVER", "TOP_GAME_3_TIER_TURNOVER",
        "TOP_GAME_1_TIER_ROUNDS",   "TOP_GAME_2_TIER_ROUNDS",   "TOP_GAME_3_TIER_ROUNDS",
        "DOMINANT_WEEKDAY", "DOMINANT_TIMEBUCKET", "LAST_PRODUCT_PLAYED",
        # ----- Bonus -----
        "BONUS_ISSUED_30D",
        "BTR_30D", "BTR_CASINO_30D", "BTR_SPORT_30D",
        "LAST_BONUS_DATE", "LAST_BONUS_TYPE",
        "bonus_ratio",  # = BONUS_DEPENDENCY_RATIO_30D (proxy 90d aceito enquanto)
        "BONUS_DEPENDENCY_RATIO_LIFETIME", "NGR_PER_BONUS_REAL_30D",
        # ----- Risco / regulatorio / KYC -----
        "classificacao_risco",  # = RISK_MATRIX_TIER
        "RG_STATUS", "ACCOUNT_RESTRICTED_FLAG", "SELF_EXCLUDED_FLAG",
        "BONUS_ABUSE_FLAG",
        "KYC_STATUS", "AVG_DEPOSIT_TICKET_LIFETIME",
        "kyc_level", "self_exclusion_status", "cool_off_status",
        "restricted_product",
        "c_category",
    ]
    cols_existentes = [c for c in cols_csv if c in df.columns]
    df[cols_existentes].sort_values("pvs", ascending=False).to_csv(
        csv_path, index=False, sep=";", decimal=",", encoding="utf-8-sig"
    )
    log.info(f"CSV salvo: {csv_path} ({len(df):,} linhas)")

    # Legenda — versao expandida v2 (57 colunas)
    legenda = f"""LEGENDA — players_segmento_SA_{snapshot_date}
==============================================================

Snapshot: {snapshot_date} | Janelas: 90d (rating/PCR) + 30d (gatilhos operacionais)
Fonte: multibet.segmentacao_sa_atual (Super Nova DB)

REGRA GERAL: a base diaria inclui TODOS os c_category com Rating A ou S
para visibilidade total. CRM filtra conforme o uso operacional.
Janelas 30d e 90d terminam em D-1 (excluem dia parcial).

==============================================================
BLOCO 1 — IDENTIFICACAO E CLASSIFICACAO
==============================================================

  player_id         ID interno do jogador (ecr_id, 18 digitos).
  external_id       ID Smartico (CRM).
  rating            Tier do PCR. Apenas 'A' (VIP, top 7-10%) ou 'S' (Whale, top 1%).
  pvs               Player Value Score (0-100). Ranking percentil baseado em
                    9 componentes (GGR, deposito, recencia, margem, frequencia,
                    dias ativos, mix produto, taxa atividade, penalidade bonus).
                    Cortes: A 92-99% | S top 1%.

  tendencia         Estavel | Subindo | Caindo
                    - Subindo: A com PVS perto do corte S (em rota pra virar S)
                    - Caindo:  S com PVS perto do corte A (em rota pra virar A)
                    - Estavel: nenhum dos casos acima
  tendencia_motivo  'threshold' (proximidade ao corte) ou 'temporal_+X.X' (delta
                    de PVS vs snapshot de 7 dias atras).

  classificacao_risco  Tier comportamental (Matriz de Risco v2):
                       Muito Bom | Bom | Mediano | Ruim | Muito Ruim | Nao Identificado
                       Mede COMO o jogador joga (21 tags comportamentais).
                       (= RISK_MATRIX_TIER no CSV de referencia)
  score_risco       Score normalizado 0-100 da Matriz.

  c_category        Status atual da conta:
                    real_user / rg_closed / rg_cool_off / closed / fraud / play_user
  affiliate_id      ID do afiliado de aquisicao.
  registration_date Data de cadastro.

  LIFECYCLE_STATUS  Ciclo de vida do jogador:
                    NEW      - tenure < 30d AND num_deposits < 3 (onboarding)
                    ACTIVE   - recency_days <= 7 (ativo recente)
                    AT_RISK  - 8 <= recency <= 30 (gatilho de retencao)
                    CHURNED  - 31 <= recency <= 90 (reengajamento)
                    DORMANT  - recency > 90 (dormente)
  recency_days      Dias desde a ultima atividade.

==============================================================
BLOCO 2 — METRICAS DE VALOR (90d e 30d)
==============================================================

  ggr_total / ngr_total        GGR/NGR 90d (BRL).
  total_deposits / cashouts    Volumes 90d (BRL).
  num_deposits / days_active   Contagens 90d.
  product_type                 CASINO | SPORT | MISTO | OUTRO (90d).
  casino_rounds / sport_bets   Rodadas e apostas 90d.
  bonus_issued / bonus_ratio   Bonus emitido e dependencia (90d).
  wd_ratio / net_deposit       Saque/deposito e net_deposit (90d).
  margem_ggr / ggr_por_dia     Margem e GGR diario.

  GGR_30D / NGR_30D            GGR/NGR ultimos 30 dias.
  DEPOSIT_AMOUNT_30D           Volume depositado 30d.
  DEPOSIT_COUNT_30D            Numero de depositos 30d.
  WITHDRAWAL_AMOUNT_30D        Volume sacado 30d.
  WITHDRAWAL_COUNT_30D         Numero de saques 30d.
  AVG_DEPOSIT_TICKET_30D       Ticket medio do jogador 30d.
  AVG_DEPOSIT_TICKET_LIFETIME  Ticket medio lifetime.
  AVG_DEPOSIT_TICKET_TIER      Ticket medio do TIER (rating x matriz) — ref.
  BET_AMOUNT_30D               Turnover (apostado) 30d.
  BET_COUNT_30D                Total de apostas 30d (casino + sport).
  AVG_BET_TICKET_30D           Ticket medio aposta 30d.
  AVG_BET_TICKET_TIER          Ticket medio aposta do TIER — referencia.

==============================================================
BLOCO 3 — COMPORTAMENTO E PRODUTO
==============================================================

  PRODUCT_MIX                  CASINO_PURO | SPORT_PURO | MISTO | INACTIVE.
  PRIMARY_VERTICAL             Vertical principal (CASINO/SPORT/MISTO).
  TOP_PROVIDER_1/2             Top providers do TIER por NGR (90d).
  TOP_GAME_1/2                 Top jogos do TIER por NGR (90d).
  TOP_GAME_1/2/3_TIER_TURNOVER Top 3 jogos do TIER por turnover (apostado).
  TOP_GAME_1/2/3_TIER_ROUNDS   Top 3 jogos do TIER por numero de rodadas.
  DOMINANT_WEEKDAY             Dia da semana dominante do TIER (90d).
  DOMINANT_TIMEBUCKET          Bucket de horario dominante: MADRUGADA (0-5h),
                               MANHA (6-11h), TARDE (12-17h), NOITE (18-23h).
  LAST_PRODUCT_PLAYED          Ultimo produto jogado pelo player.

==============================================================
BLOCO 4 — BONUS E BTR
==============================================================

  BONUS_ISSUED_30D             Bonus emitido 30d (BRL).
  BTR_30D                      Bonus Turnover Ratio 30d
                               (bonus_turnedreal / bonus_issued).
  BTR_CASINO_30D / SPORT_30D   BTR splitado por vertical (proxy via realbet).
  LAST_BONUS_DATE              Ultima data com bonus_issued > 0 (180d window).
  LAST_BONUS_TYPE              Tipo do ultimo bonus (proxy = CASH).
  BONUS_DEPENDENCY_RATIO_LIFETIME  Dependencia de bonus lifetime.
  NGR_PER_BONUS_REAL_30D       NGR / bonus_issued 30d (eficiencia bonus).

==============================================================
BLOCO 5 — RISCO REGULATORIO E KYC
==============================================================

  RG_STATUS                    NORMAL | RG_CLOSED | RG_COOL_OFF.
  ACCOUNT_RESTRICTED_FLAG      1 se conta tem alguma restricao operacional.
  SELF_EXCLUDED_FLAG           1 se jogador em auto-exclusao permanente.
  BONUS_ABUSE_FLAG             1 se Matriz de Risco aponta abuso de bonus
                               (potencial_abuser != 0 OU promo_chainer != 0).
  KYC_STATUS / kyc_level       Nivel de KYC: KYC_0 | KYC_1 | KYC_2 | KYC_3.
  self_exclusion_status        Detalhe de auto-exclusao (None / SELF_EXCLUDED_PERM).
  cool_off_status              Detalhe de pausa cool-off (None / COOL_OFF_ACTIVE).
  restricted_product           Produtos restritos (ex: 'casino, sports_book').

==============================================================

Geracao: pipeline EC2 ETL diario as 04:00 BRT (30min apos PCR upstream).
"""
    legenda_path.write_text(legenda, encoding="utf-8")
    log.info(f"Legenda salva: {legenda_path}")
    return csv_path, legenda_path


def montar_corpo_email(df: pd.DataFrame, snapshot_date: str) -> str:
    """HTML enxuto para o corpo do e-mail — resumo + leitura rapida."""
    n_total = len(df)
    n_s = (df["rating"] == "S").sum()
    n_a = (df["rating"] == "A").sum()
    n_subindo = (df["tendencia"] == "Subindo").sum()
    n_caindo  = (df["tendencia"] == "Caindo").sum()
    n_estavel = (df["tendencia"] == "Estavel").sum()
    ggr_total = df["ggr_total"].astype(float).sum()

    return f"""
    <html><body style="font-family: Arial, sans-serif; color: #2c3e50;">
      <h2 style="color: #0f3460;">Segmentacao A+S diaria — {snapshot_date}</h2>
      <p>Em anexo a base do dia com os jogadores de Rating <strong>A</strong> e <strong>S</strong> do PCR.</p>

      <h3>Resumo</h3>
      <ul>
        <li>Total: <strong>{n_total:,}</strong> jogadores</li>
        <li>S (Whale, top 1%): <strong>{n_s:,}</strong></li>
        <li>A (VIP, 92-99%): <strong>{n_a:,}</strong></li>
        <li>GGR 90d desta base: <strong>R$ {ggr_total:,.0f}</strong></li>
      </ul>

      <h3>Tendencia (movimento entre tiers)</h3>
      <ul>
        <li>Subindo (A perto de virar S): <strong>{n_subindo:,}</strong></li>
        <li>Caindo (S perto de virar A): <strong>{n_caindo:,}</strong></li>
        <li>Estavel: <strong>{n_estavel:,}</strong></li>
      </ul>

      <p style="color: #7f8c8d; font-size: 0.9em;">
        Detalhes das colunas no arquivo <em>_legenda.txt</em> em anexo.<br>
        Pipeline automatizado — Super Nova Analytics.
      </p>
    </body></html>
    """.replace("R$ ", "R$ ")  # noop, mantem espaco


# ============================================================
# HELPERS
# ============================================================
def _safe_int(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _safe_float(v, decimals=2):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    try:
        return round(float(v), decimals)
    except (ValueError, TypeError):
        return None


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Segmentacao A+S Diaria")
    parser.add_argument("--no-email", action="store_true", help="Nao envia email")
    parser.add_argument("--no-db", action="store_true", help="Nao grava no banco")
    parser.add_argument("--push-smartico", action="store_true",
                         help="Publica tags SEG_* no Smartico (default: skip)")
    parser.add_argument("--smartico-canary", action="store_true",
                         help="So 1 jogador canary (use com --push-smartico)")
    parser.add_argument("--smartico-confirm", action="store_true",
                         help="OBRIGATORIO para envio real (sem dry-run)")
    parser.add_argument("--smartico-dry-run", action="store_true",
                         help="Forca dry-run (so JSON, sem enviar)")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info(f"SEGMENTACAO A+S DIARIA — snapshot {SNAPSHOT_DATE}")
    log.info(f"Modo: {'DRY (sem banco/email)' if (args.no_db and args.no_email) else 'PROD'}")
    log.info("=" * 70)

    # 1. Setup tabelas (idempotente)
    if not args.no_db:
        setup_tabelas()

    # 2. Carregar PCR atual + Matriz Risco
    df_pcr_full = carregar_pcr_atual()
    mr = carregar_matriz_risco()

    # 3. Atualizar celula_monitor (sugestao Castrin: flag 3 rodadas negativas)
    if not args.no_db:
        atualizar_celula_monitor(df_pcr_full, mr, SNAPSHOT_DATE)

    # 4. ENRIQUECIMENTO LEVE — toda a base PCR (~136k) pro Smartico
    #    (tendencia ainda nao — calculada apos filtro A+S, pois usa cortes A/S)
    log.info("=" * 70)
    log.info("ENRIQUECIMENTO LEVE — toda a base PCR (Bloco 4 + 5) — pro Smartico")
    log.info("=" * 70)
    df_full = juntar_matriz(df_pcr_full, mr)
    df_full = bloco_4_derivaveis(df_full, snapshot_date=SNAPSHOT_DATE)
    df_full = bloco_5_risk_tags_flags(df_full, snapshot_date=SNAPSHOT_DATE)
    # tendencia placeholder (sera preenchida apenas pra A+S no proximo passo)
    if "tendencia" not in df_full.columns:
        df_full["tendencia"] = "Estavel"
        df_full["tendencia_motivo"] = "default_nao_AS"

    # 5. Filtrar A+S
    sa = filtrar_a_e_s(df_full)
    if sa.empty:
        log.error("Nenhum jogador A ou S encontrado — abortando.")
        return

    # 6. Calcular tendencia (hibrido) — apenas A+S
    df_lookback = carregar_pvs_lookback(TENDENCIA_LOOKBACK_DAYS)
    sa = calcular_tendencia(sa, df_lookback)
    # Atualiza df_full com tendencia calculada dos A+S (pro Smartico)
    df_full = df_full.merge(
        sa[["player_id", "tendencia", "tendencia_motivo"]],
        on="player_id", how="left", suffixes=("_old", "")
    )
    df_full["tendencia"] = df_full["tendencia"].fillna(df_full["tendencia_old"])
    df_full["tendencia_motivo"] = df_full["tendencia_motivo"].fillna(df_full["tendencia_motivo_old"])
    df_full = df_full.drop(columns=["tendencia_old", "tendencia_motivo_old"], errors="ignore")

    # 7. Enriquecimento PESADO — apenas A+S (Castrin pediu 57 col no CSV)
    log.info("=" * 70)
    log.info("ENRIQUECIMENTO PESADO A+S — adicionando 32 colunas (KYC, 30d, top jogos, BTR)")
    log.info("=" * 70)
    sa = bloco_6_kyc(sa, snapshot_date=SNAPSHOT_DATE)
    sa = bloco_1_2_metricas_30d(sa, snapshot_date=SNAPSHOT_DATE)
    sa = bloco_3_top_jogos_e_temporal(sa, snapshot_date=SNAPSHOT_DATE)
    sa = bloco_5b_btr_bonus(sa, snapshot_date=SNAPSHOT_DATE)
    log.info(f"Enriquecimento OK — A+S final: {len(sa):,} linhas x {len(sa.columns)} cols")
    log.info(f"  Base full pro Smartico: {len(df_full):,} linhas x {len(df_full.columns)} cols")

    # FIX A1 (best-practices 28/04): valida que TODAS as 32 colunas v2 existem
    # antes de gravar/exportar — evita NULL silencioso por mudanca de naming.
    assert_all_v2_cols(sa, where="apos enriquecimento A+S")

    # 7. Persistir incremental
    if not args.no_db:
        gravar_segmentacao(sa, SNAPSHOT_DATE)

    # 8. Gerar CSV + legenda
    csv_path, legenda_path = gerar_csv(sa, SNAPSHOT_DATE)

    # 9. Enviar email
    if not args.no_email:
        corpo = montar_corpo_email(sa, SNAPSHOT_DATE)
        ok = enviar_email(
            destinatarios=EMAIL_DESTINATARIOS,
            assunto=f"Segmentacao A+S diaria — {SNAPSHOT_DATE}",
            corpo_html=corpo,
            anexos=[str(csv_path), str(legenda_path)],
        )
        if not ok:
            log.warning("Email NAO enviado (verifique config SMTP no .env). CSV gerado em: %s", csv_path)
    else:
        log.info("--no-email: pulando envio. CSV em: %s", csv_path)

    # 10. Publicar tags SEG_* no Smartico (opcional, default: skip)
    # ATENCAO: Smartico recebe a base COMPLETA (~136k), nao so A+S.
    # Tags operacionais devem cobrir base inteira pra que jornadas/automation
    # do CRM possam disparar pra qualquer player. CSV via e-mail = so A+S.
    if args.push_smartico:
        log.info("=" * 70)
        log.info("PUBLICACAO SMARTICO — core_external_markers (SEG_*) — BASE COMPLETA")
        log.info("=" * 70)
        smt_result = publicar_smartico(
            df=df_full,  # toda a base PCR, nao so A+S
            snapshot_date=SNAPSHOT_DATE,
            dry_run=args.smartico_dry_run or not args.smartico_confirm,
            canary=args.smartico_canary,
            skip_cjm=True,  # SEMPRE True (popula sem disparar Automation)
            confirm=args.smartico_confirm,
        )
        log.info(f"Smartico: ADD={smt_result.get('total_eventos_add', 0)} | "
                 f"REMOVE={smt_result.get('total_eventos_remove', 0)} | "
                 f"sent={smt_result.get('sent', 0)} | "
                 f"failed={smt_result.get('failed', 0)}")
    else:
        log.info("--push-smartico nao usado: pulando publicacao Smartico.")

    log.info("=" * 70)
    log.info(f"PIPELINE CONCLUIDO — {len(sa):,} jogadores A+S processados")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
