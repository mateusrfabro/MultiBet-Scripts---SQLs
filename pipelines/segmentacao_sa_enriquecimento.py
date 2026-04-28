"""
Modulo de enriquecimento da segmentacao SA — adiciona ate 32 colunas
faltantes pedidas pelo Castrin (CSV de referencia: 57 colunas).

Cada bloco e uma funcao independente que recebe o DataFrame base
(saida do PCR + matriz_risco) e devolve as colunas extras.

BLOCOS:
  - bloco_4_derivaveis(df)        : LIFECYCLE_STATUS, RG_STATUS, FLAGs (5 col)
                                    100% pandas — sem query externa.
  - bloco_6_kyc(df, snapshot)     : KYC_STATUS, kyc_level, self_exclusion_status,
                                    cool_off_status, restricted_product (5 col).
                                    Fonte: ecr_ec2.tbl_ecr_kyc_level + regulatory_ec2.
  - bloco_5_risk_tags_flags(df)   : BONUS_ABUSE_FLAG (1 col, mas + uteis).
                                    Fonte: multibet.risk_tags (Super Nova DB).
  - bloco_1_metricas_30d(df, snap): GGR_30D, NGR_30D, DEPOSIT/WITHDRAWAL 30d (8 col).
                                    Fonte: ps_bi.fct_player_activity_daily.

Premissa: DataFrame de entrada (df) tem as colunas do PCR atual
(player_id, external_id, c_category, recency_days, registration_date,
num_deposits, product_type, casino_rounds, sport_bets).

Janela 30d: rolling, terminando em D-1 (exclusivo) — alinhado com regra
do PCR upstream que usa janela 90d rolling tambem terminando em D-1.

Uso:
    from pipelines.segmentacao_sa_enriquecimento import (
        bloco_4_derivaveis, bloco_6_kyc,
    )
    df = bloco_4_derivaveis(df)
    df = bloco_6_kyc(df, snapshot_date='2026-04-27')
"""
import logging
from datetime import datetime, timedelta, date
from typing import Optional, List

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ============================================================
# CONTRATO DE COLUNAS V2 — gerado pelos blocos, consumido por gravar/csv
# ============================================================
# Defesa contra bug silencioso: se um bloco renomear coluna sem atualizar
# este contrato, a asserção dispara antes de gravar NULL no banco.
COLS_BLOCO_4 = [
    "LIFECYCLE_STATUS", "RG_STATUS", "ACCOUNT_RESTRICTED_FLAG",
    "SELF_EXCLUDED_FLAG", "PRIMARY_VERTICAL",
]
COLS_BLOCO_5 = ["BONUS_ABUSE_FLAG"]
COLS_BLOCO_6 = [
    "KYC_STATUS", "kyc_level", "self_exclusion_status",
    "cool_off_status", "restricted_product",
]
COLS_BLOCO_1_2 = [
    "GGR_30D", "NGR_30D", "DEPOSIT_AMOUNT_30D", "DEPOSIT_COUNT_30D",
    "WITHDRAWAL_AMOUNT_30D", "WITHDRAWAL_COUNT_30D",
    "AVG_DEPOSIT_TICKET_30D", "AVG_DEPOSIT_TICKET_LIFETIME",
    "BET_AMOUNT_30D", "BET_COUNT_30D", "AVG_BET_TICKET_30D",
    "AVG_DEPOSIT_TICKET_TIER", "AVG_BET_TICKET_TIER",
]
COLS_BLOCO_3 = [
    "PRODUCT_MIX", "TOP_PROVIDER_1", "TOP_PROVIDER_2",
    "TOP_GAME_1", "TOP_GAME_2",
    "TOP_GAME_1_TIER_TURNOVER", "TOP_GAME_2_TIER_TURNOVER", "TOP_GAME_3_TIER_TURNOVER",
    "TOP_GAME_1_TIER_ROUNDS",   "TOP_GAME_2_TIER_ROUNDS",   "TOP_GAME_3_TIER_ROUNDS",
    "DOMINANT_WEEKDAY", "DOMINANT_TIMEBUCKET", "LAST_PRODUCT_PLAYED",
]
COLS_BLOCO_5B = [
    "BONUS_ISSUED_30D", "BTR_30D", "BTR_CASINO_30D", "BTR_SPORT_30D",
    "BONUS_DEPENDENCY_RATIO_LIFETIME", "NGR_PER_BONUS_REAL_30D",
    "LAST_BONUS_DATE", "LAST_BONUS_TYPE",
]
ALL_V2_COLS = (COLS_BLOCO_4 + COLS_BLOCO_5 + COLS_BLOCO_6
               + COLS_BLOCO_1_2 + COLS_BLOCO_3 + COLS_BLOCO_5B)


def assert_all_v2_cols(df: pd.DataFrame, where: str = "") -> None:
    """
    Garante que todas as colunas v2 esperadas existem no DataFrame antes de
    persistir/exportar. Falha rapido em vez de gravar NULL silencioso no banco.
    """
    missing = [c for c in ALL_V2_COLS if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"[assert_all_v2_cols/{where}] {len(missing)} colunas v2 ausentes: {missing}"
        )


# ============================================================
# HELPER — batching de queries Athena com IN clause grande
# ============================================================
def _run_athena_in_batches(sql_template: str, player_ids: list, database: str,
                            batch_size: int = 4000) -> pd.DataFrame:
    """
    Athena tem limite de ~256KB por query. Para listas grandes de IDs (10k+),
    quebra em batches e concatena os resultados.

    sql_template: SQL com placeholder '{ids_str}' onde vai a lista de IDs.
    """
    from db.athena import query_athena
    if len(player_ids) <= batch_size:
        ids_str = ", ".join(str(p) for p in player_ids)
        return query_athena(sql_template.format(ids_str=ids_str), database=database)

    log.info(f"  IN clause com {len(player_ids):,} ids — quebrando em batches de {batch_size}")
    parts = []
    n_batches = (len(player_ids) + batch_size - 1) // batch_size
    for i in range(n_batches):
        chunk = player_ids[i * batch_size:(i + 1) * batch_size]
        ids_str = ", ".join(str(p) for p in chunk)
        log.info(f"  Batch {i + 1}/{n_batches} ({len(chunk)} ids)...")
        df = query_athena(sql_template.format(ids_str=ids_str), database=database)
        parts.append(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


# ============================================================
# BLOCO 4 — DERIVAVEIS (sem query)
# ============================================================
def bloco_4_derivaveis(df: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """
    Adiciona colunas DERIVAVEIS — calculadas a partir do que ja temos.

    Colunas geradas:
      - LIFECYCLE_STATUS    : NEW | ACTIVE | AT_RISK | CHURNED | DORMANT
      - RG_STATUS           : RG_CLOSED | RG_COOL_OFF | NORMAL
      - ACCOUNT_RESTRICTED_FLAG : 1 se RG_CLOSED/RG_COOL_OFF/closed/fraud
      - SELF_EXCLUDED_FLAG  : 1 se rg_closed
      - PRIMARY_VERTICAL    : CASINO | SPORT | MISTO | OUTRO

    Regras LIFECYCLE_STATUS (alinhadas com pratica iGaming):
      - NEW    : tenure < 30d AND num_deposits < 3
      - ACTIVE : recency_days <= 7
      - AT_RISK: 8 <= recency_days <= 30
      - CHURNED: 31 <= recency_days <= 90
      - DORMANT: recency_days > 90

    A regra NEW tem prioridade (sobrescreve as outras).
    Castrin usa AT_RISK como gatilho operacional — ver player com
    recency=16 dias = AT_RISK no CSV de referencia.
    """
    log.info("[Bloco 4] Derivando colunas (sem query)...")
    df = df.copy()

    # Snapshot date — usa hoje (BRT) se nao fornecido
    if snapshot_date is None:
        snapshot_date = date.today().isoformat()
    snap = pd.to_datetime(snapshot_date).date()

    # ----- LIFECYCLE_STATUS -----
    # Tenure em dias (snap - registration_date)
    reg = pd.to_datetime(df["registration_date"], errors="coerce").dt.date
    tenure_days = pd.Series(
        [(snap - r).days if pd.notna(r) else 9999 for r in reg],
        index=df.index,
    )
    recency = pd.to_numeric(df["recency_days"], errors="coerce").fillna(9999)
    num_dep = pd.to_numeric(df["num_deposits"], errors="coerce").fillna(0)

    df["LIFECYCLE_STATUS"] = "DORMANT"
    df.loc[recency <= 90, "LIFECYCLE_STATUS"] = "CHURNED"
    df.loc[recency <= 30, "LIFECYCLE_STATUS"] = "AT_RISK"
    df.loc[recency <= 7,  "LIFECYCLE_STATUS"] = "ACTIVE"
    # NEW sobrescreve (tem prioridade)
    eh_new = (tenure_days < 30) & (num_dep < 3)
    df.loc[eh_new, "LIFECYCLE_STATUS"] = "NEW"

    log.info(f"  LIFECYCLE_STATUS: {df['LIFECYCLE_STATUS'].value_counts().to_dict()}")

    # ----- RG_STATUS -----
    cc = df["c_category"].fillna("").str.lower()
    df["RG_STATUS"] = "NORMAL"
    df.loc[cc == "rg_closed",   "RG_STATUS"] = "RG_CLOSED"
    df.loc[cc == "rg_cool_off", "RG_STATUS"] = "RG_COOL_OFF"

    # ----- ACCOUNT_RESTRICTED_FLAG -----
    # Qualquer status que limita operacao normal
    restritos = {"rg_closed", "rg_cool_off", "closed", "fraud"}
    df["ACCOUNT_RESTRICTED_FLAG"] = cc.isin(restritos).astype(int)

    # ----- SELF_EXCLUDED_FLAG -----
    df["SELF_EXCLUDED_FLAG"] = (cc == "rg_closed").astype(int)

    # ----- PRIMARY_VERTICAL (mapping de product_type) -----
    # product_type atual: CASINO | SPORT | MISTO | OUTRO
    pt = df["product_type"].fillna("OUTRO").astype(str).str.upper()
    mapa = {"CASINO": "CASINO", "SPORT": "SPORT", "MISTO": "MISTO"}
    df["PRIMARY_VERTICAL"] = pt.map(mapa).fillna("OUTRO")

    log.info(f"  RG_STATUS: {df['RG_STATUS'].value_counts().to_dict()}")
    log.info(f"  Restricted={df['ACCOUNT_RESTRICTED_FLAG'].sum()} | SelfExcl={df['SELF_EXCLUDED_FLAG'].sum()}")
    log.info(f"  PRIMARY_VERTICAL: {df['PRIMARY_VERTICAL'].value_counts().to_dict()}")
    return df


# ============================================================
# BLOCO 6 — KYC + RESTRICOES (1 query Athena)
# ============================================================
def bloco_6_kyc(df: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """
    Adiciona colunas KYC e restricoes regulatorias.

    Colunas geradas:
      - KYC_STATUS       : KYC_0 | KYC_1 | KYC_2 | KYC_3 (snake case do c_level)
      - kyc_level        : duplicata em lowercase (Castrin pediu ambas no CSV)
      - self_exclusion_status : flag/desc de self_exclusion (None = sem)
      - cool_off_status  : flag/desc de cool_off (None = sem)
      - restricted_product : produtos restritos (csv: "casino, sports_book")

    Fonte:
      - ecr_ec2.tbl_ecr_kyc_level (c_ecr_id <-> player_id)
      - Restricoes: derivadas de c_category por enquanto.
        TODO: integrar regulatory_ec2 quando schema for confirmado.

    Atencao: c_ecr_id e o player_id. JOIN direto.
    """
    from db.athena import query_athena

    log.info("[Bloco 6] Enriquecendo KYC...")
    player_ids = df["player_id"].dropna().astype("int64").unique().tolist()
    if not player_ids:
        log.warning("  Sem player_ids — pulando.")
        df["KYC_STATUS"] = None
        df["kyc_level"] = None
        df["self_exclusion_status"] = None
        df["cool_off_status"] = None
        df["restricted_product"] = None
        return df

    # OTIMIZACAO: max() + group by (sem window function) — mais barato em Iceberg.
    # FIX Bug M1 (auditoria 28/04): SEM filtro temporal — whales/VIPs com KYC
    # concluido em 2020-2022 podem nunca ter atualizado. Filtro c_ecr_id IN(...)
    # ja restringe a ~10.9k IDs, escaneamento e barato.
    sql_template = f"""
    WITH max_dates AS (
        SELECT c_ecr_id, MAX(c_updated_time) AS max_t
        FROM ecr_ec2.tbl_ecr_kyc_level
        WHERE c_ecr_id IN ({{ids_str}})
        GROUP BY c_ecr_id
    )
    SELECT k.c_ecr_id AS player_id, k.c_level, k.c_desc
    FROM ecr_ec2.tbl_ecr_kyc_level k
    INNER JOIN max_dates m ON m.c_ecr_id = k.c_ecr_id
                          AND m.max_t   = k.c_updated_time
    """
    log.info(f"  Athena: KYC level para {len(player_ids):,} players...")
    kyc = _run_athena_in_batches(sql_template, player_ids, database="ecr_ec2", batch_size=4000)
    log.info(f"  -> {len(kyc):,} linhas KYC")

    # Normaliza KYC_STATUS — c_level pode vir como "KYC_2" (ja prefixado) ou "2"
    kyc["player_id"] = pd.to_numeric(kyc["player_id"], errors="coerce").astype("Int64")
    lvl = kyc["c_level"].astype(str).str.strip()
    kyc["KYC_STATUS"] = lvl.where(lvl.str.startswith("KYC_"), "KYC_" + lvl)
    kyc["kyc_level"] = kyc["KYC_STATUS"]  # duplicata por compat com CSV Castrin
    kyc = kyc[["player_id", "KYC_STATUS", "kyc_level"]]

    # Merge
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df = df.merge(kyc, on="player_id", how="left")
    cobertura = df["KYC_STATUS"].notna().sum() / len(df) * 100
    log.info(f"  Cobertura KYC: {cobertura:.1f}%")

    # ----- Derivadas restritas (a partir de c_category enquanto regulatory_ec2 nao for integrado) -----
    cc = df["c_category"].fillna("").str.lower()
    df["self_exclusion_status"] = None
    df.loc[cc == "rg_closed", "self_exclusion_status"] = "SELF_EXCLUDED_PERM"

    df["cool_off_status"] = None
    df.loc[cc == "rg_cool_off", "cool_off_status"] = "COOL_OFF_ACTIVE"

    # restricted_product: Castrin colocou "casino, sports_book" para players
    # rg_closed/cool_off (todos os produtos bloqueados). Aplicamos mesma logica.
    df["restricted_product"] = None
    df.loc[df["ACCOUNT_RESTRICTED_FLAG"] == 1, "restricted_product"] = "casino, sports_book"

    return df


# ============================================================
# BLOCO 5 — BONUS_ABUSE_FLAG via risk_tags (1 query Super Nova DB)
# ============================================================
def bloco_5_risk_tags_flags(df: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """
    Adiciona BONUS_ABUSE_FLAG a partir das tags de risco populadas
    pelo Mauro/Gusta em multibet.risk_tags.

    Regra:
      BONUS_ABUSE_FLAG = 1 se (potencial_abuser != 0 OR promo_chainer != 0)

    Fonte: multibet.risk_tags (Super Nova DB) — populada diariamente.
    JOIN: external_id (Smartico) <-> user_ext_id.
    """
    from db.supernova import execute_supernova

    log.info("[Bloco 5] BONUS_ABUSE_FLAG via risk_tags...")

    if snapshot_date is None:
        snapshot_date = date.today().isoformat()

    # Pega ultimo snapshot disponivel <= snapshot_date
    rows = execute_supernova(
        """
        SELECT user_ext_id, potencial_abuser, promo_chainer
        FROM multibet.risk_tags
        WHERE snapshot_date = (
            SELECT MAX(snapshot_date) FROM multibet.risk_tags
            WHERE snapshot_date <= %s
        );
        """,
        params=(snapshot_date,),
        fetch=True,
    )
    if not rows:
        log.warning("  risk_tags sem dados — BONUS_ABUSE_FLAG = 0 default")
        df["BONUS_ABUSE_FLAG"] = 0
        return df

    rt = pd.DataFrame(rows, columns=["user_ext_id", "potencial_abuser", "promo_chainer"])
    rt["user_ext_id"] = rt["user_ext_id"].astype(str)
    rt["BONUS_ABUSE_FLAG"] = (
        (rt["potencial_abuser"].fillna(0).astype(int) != 0)
        | (rt["promo_chainer"].fillna(0).astype(int) != 0)
    ).astype(int)
    rt = rt[["user_ext_id", "BONUS_ABUSE_FLAG"]]

    df = df.copy()
    df["external_id_str"] = df["external_id"].astype(str)
    df = df.merge(rt, left_on="external_id_str", right_on="user_ext_id", how="left")
    df["BONUS_ABUSE_FLAG"] = df["BONUS_ABUSE_FLAG"].fillna(0).astype(int)
    df = df.drop(columns=["external_id_str", "user_ext_id"], errors="ignore")
    log.info(f"  BONUS_ABUSE_FLAG=1 em {df['BONUS_ABUSE_FLAG'].sum():,} players")
    return df


# ============================================================
# BLOCOS 1+2 — METRICAS 30D (financeiras + aposta) em 1 query unificada
# ============================================================
def bloco_1_2_metricas_30d(df: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """
    Adiciona 13 colunas de metricas 30d (financeiras + aposta) em 1 query Athena.

    Bloco 1 — Financeiras 30d (8 col):
      - GGR_30D, NGR_30D
      - DEPOSIT_AMOUNT_30D, DEPOSIT_COUNT_30D
      - WITHDRAWAL_AMOUNT_30D, WITHDRAWAL_COUNT_30D
      - AVG_DEPOSIT_TICKET_30D
      - AVG_DEPOSIT_TICKET_LIFETIME (sem filtro de data)

    Bloco 2 — Aposta 30d (5 col):
      - BET_AMOUNT_30D (turnover = casino + sport)
      - BET_COUNT_30D
      - AVG_BET_TICKET_30D
      - AVG_BET_TICKET_TIER       (media do (rating x matriz_risco) do player)
      - AVG_DEPOSIT_TICKET_TIER   (media do (rating x matriz_risco) do player)

    Janela: 30d rolling, terminando em D-1 (exclui parcial). Alinhada com
    pratica do PCR upstream (90d) — auditavel/replicavel.

    Fonte: ps_bi.fct_player_activity_daily (view dbt, BRL pre-agregado).
    Performance: 1 query, ~12k player_ids, ~5-15s no Athena.
    """
    from db.athena import query_athena

    log.info("[Bloco 1+2] Metricas 30d (financeiras + aposta)...")
    if snapshot_date is None:
        snapshot_date = date.today().isoformat()
    snap = pd.to_datetime(snapshot_date).date()
    janela_ini = snap - timedelta(days=30)

    player_ids = df["player_id"].dropna().astype("int64").unique().tolist()
    if not player_ids:
        log.warning("  Sem player_ids — pulando.")
        return df

    sql_template = f"""
    WITH metrics_30d AS (
        SELECT
            f.player_id,
            COALESCE(SUM(f.ggr_base), 0)                  AS ggr_30d,
            COALESCE(SUM(f.ngr_base), 0)                  AS ngr_30d,
            COALESCE(SUM(f.deposit_success_count), 0)     AS deposit_count_30d,
            COALESCE(SUM(f.deposit_success_base), 0)      AS deposit_amount_30d,
            COALESCE(SUM(f.cashout_success_count), 0)     AS withdrawal_count_30d,
            COALESCE(SUM(f.cashout_success_base), 0)      AS withdrawal_amount_30d,
            COALESCE(SUM(f.casino_realbet_base), 0)
              + COALESCE(SUM(f.sb_realbet_base), 0)       AS bet_amount_30d,
            COALESCE(SUM(f.casino_realbet_count), 0)
              + COALESCE(SUM(f.sb_realbet_count), 0)      AS bet_count_30d
        FROM ps_bi.fct_player_activity_daily f
        WHERE f.activity_date >= DATE '{janela_ini}'
          AND f.activity_date <  DATE '{snap}'
          AND f.player_id IN ({{ids_str}})
        GROUP BY f.player_id
    ),
    metrics_lifetime AS (
        SELECT
            f.player_id,
            COALESCE(SUM(f.deposit_success_count), 0) AS deposit_count_lifetime,
            COALESCE(SUM(f.deposit_success_base), 0)  AS deposit_amount_lifetime
        FROM ps_bi.fct_player_activity_daily f
        WHERE f.player_id IN ({{ids_str}})
        GROUP BY f.player_id
    )
    SELECT
        m.player_id,
        m.ggr_30d, m.ngr_30d,
        m.deposit_count_30d, m.deposit_amount_30d,
        m.withdrawal_count_30d, m.withdrawal_amount_30d,
        m.bet_amount_30d, m.bet_count_30d,
        CASE WHEN m.deposit_count_30d > 0
             THEN m.deposit_amount_30d * 1.0 / m.deposit_count_30d
             ELSE NULL END AS avg_deposit_ticket_30d,
        CASE WHEN m.bet_count_30d > 0
             THEN m.bet_amount_30d * 1.0 / m.bet_count_30d
             ELSE NULL END AS avg_bet_ticket_30d,
        CASE WHEN l.deposit_count_lifetime > 0
             THEN l.deposit_amount_lifetime * 1.0 / l.deposit_count_lifetime
             ELSE NULL END AS avg_deposit_ticket_lifetime
    FROM metrics_30d m
    LEFT JOIN metrics_lifetime l ON m.player_id = l.player_id
    """
    log.info(f"  Athena: {len(player_ids):,} players (janela {janela_ini} a {snap}, excl D-0)...")
    metrics = _run_athena_in_batches(sql_template, player_ids, database="ps_bi", batch_size=4000)
    log.info(f"  -> {len(metrics):,} linhas")

    metrics["player_id"] = pd.to_numeric(metrics["player_id"], errors="coerce").astype("Int64")

    # Renomeia para padrao Castrin (UPPER)
    rename = {
        "ggr_30d": "GGR_30D",
        "ngr_30d": "NGR_30D",
        "deposit_count_30d": "DEPOSIT_COUNT_30D",
        "deposit_amount_30d": "DEPOSIT_AMOUNT_30D",
        "withdrawal_count_30d": "WITHDRAWAL_COUNT_30D",
        "withdrawal_amount_30d": "WITHDRAWAL_AMOUNT_30D",
        "bet_amount_30d": "BET_AMOUNT_30D",
        "bet_count_30d": "BET_COUNT_30D",
        "avg_deposit_ticket_30d": "AVG_DEPOSIT_TICKET_30D",
        "avg_bet_ticket_30d": "AVG_BET_TICKET_30D",
        "avg_deposit_ticket_lifetime": "AVG_DEPOSIT_TICKET_LIFETIME",
    }
    metrics = metrics.rename(columns=rename)

    df = df.copy()
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df = df.merge(metrics, on="player_id", how="left")

    # Players sem atividade 30d ficam com 0 (nao NaN) nas metricas absolutas
    cols_zero = ["GGR_30D", "NGR_30D", "DEPOSIT_AMOUNT_30D", "DEPOSIT_COUNT_30D",
                 "WITHDRAWAL_AMOUNT_30D", "WITHDRAWAL_COUNT_30D",
                 "BET_AMOUNT_30D", "BET_COUNT_30D"]
    for c in cols_zero:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    cobertura_30d = (df["DEPOSIT_COUNT_30D"] > 0).sum() / len(df) * 100
    log.info(f"  Cobertura atividade 30d (deposit > 0): {cobertura_30d:.1f}%")

    # ----- TIER averages (depende de rating + classificacao_risco no df) -----
    if "rating" in df.columns and "classificacao_risco" in df.columns:
        log.info("  Computando *_TIER (media por rating x matriz)...")
        # AVG_DEPOSIT_TICKET_TIER
        tier_dep = df.groupby(["rating", "classificacao_risco"])["AVG_DEPOSIT_TICKET_30D"].mean().reset_index()
        tier_dep = tier_dep.rename(columns={"AVG_DEPOSIT_TICKET_30D": "AVG_DEPOSIT_TICKET_TIER"})
        df = df.merge(tier_dep, on=["rating", "classificacao_risco"], how="left")

        tier_bet = df.groupby(["rating", "classificacao_risco"])["AVG_BET_TICKET_30D"].mean().reset_index()
        tier_bet = tier_bet.rename(columns={"AVG_BET_TICKET_30D": "AVG_BET_TICKET_TIER"})
        df = df.merge(tier_bet, on=["rating", "classificacao_risco"], how="left")
    else:
        log.warning("  Sem rating/classificacao_risco no df — *_TIER ficam nulos")
        df["AVG_DEPOSIT_TICKET_TIER"] = None
        df["AVG_BET_TICKET_TIER"] = None

    return df


# ============================================================
# BLOCO 3 — TOP JOGOS / PROVIDERS / HORARIO POR TIER (3 queries Athena)
# ============================================================
def bloco_3_top_jogos_e_temporal(df: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """
    Adiciona ate 14 colunas relacionadas a preferencias do TIER (rating x matriz_risco).

    Colunas geradas:
      - PRODUCT_MIX          : CASINO_PURO | SPORT_PURO | MISTO | INACTIVE (player-level)
      - TOP_PROVIDER_1, TOP_PROVIDER_2 : top providers por NGR no tier
      - TOP_GAME_1, TOP_GAME_2         : top jogos por NGR no tier
      - TOP_GAME_1/2/3_TIER_TURNOVER   : top 3 por turnover (real_bet) no tier
      - TOP_GAME_1/2/3_TIER_ROUNDS     : top 3 por rounds (bet_count) no tier
      - DOMINANT_WEEKDAY     : dia da semana dominante no tier (Dom/Seg/...)
      - DOMINANT_TIMEBUCKET  : bucket horario dominante no tier (MADRUGADA/MANHA/TARDE/NOITE)
      - LAST_PRODUCT_PLAYED  : ultimo produto jogado pelo player (CASINO|SPORT|OUTRO)

    Fonte:
      - ps_bi.fct_casino_activity_daily (top jogos casino)
      - ps_bi.fct_casino_activity_hourly (horario/dia)
      - ps_bi.dim_game (lookup nome + vendor)

    Janela: 90d rolling ate D-1 (mesma do PCR — para coerencia com tier).

    Estrategia: top jogos sao calculados POR TIER (rating x matriz_risco), nao
    por player. Faz sentido — Castrin replica os mesmos top jogos pra todos
    do mesmo tier (operador usa pra escolher temas de campanha).
    """
    from db.athena import query_athena

    log.info("[Bloco 3] Top jogos/providers/temporal por TIER...")
    if snapshot_date is None:
        snapshot_date = date.today().isoformat()
    snap = pd.to_datetime(snapshot_date).date()
    janela_ini = snap - timedelta(days=90)

    df = df.copy()

    # ----- PRODUCT_MIX (derivado, sem query) -----
    casino = pd.to_numeric(df.get("casino_rounds"), errors="coerce").fillna(0)
    sport = pd.to_numeric(df.get("sport_bets"), errors="coerce").fillna(0)
    df["PRODUCT_MIX"] = "INACTIVE"
    df.loc[(casino > 0) & (sport == 0), "PRODUCT_MIX"] = "CASINO_PURO"
    df.loc[(casino == 0) & (sport > 0), "PRODUCT_MIX"] = "SPORT_PURO"
    df.loc[(casino > 0) & (sport > 0),  "PRODUCT_MIX"] = "MISTO"
    log.info(f"  PRODUCT_MIX: {df['PRODUCT_MIX'].value_counts().to_dict()}")

    player_ids = df["player_id"].dropna().astype("int64").unique().tolist()
    if not player_ids:
        log.warning("  Sem player_ids — pulando queries.")
        for c in ("TOP_PROVIDER_1", "TOP_PROVIDER_2",
                  "TOP_GAME_1", "TOP_GAME_2",
                  "TOP_GAME_1_TIER_TURNOVER", "TOP_GAME_2_TIER_TURNOVER", "TOP_GAME_3_TIER_TURNOVER",
                  "TOP_GAME_1_TIER_ROUNDS",   "TOP_GAME_2_TIER_ROUNDS",   "TOP_GAME_3_TIER_ROUNDS",
                  "DOMINANT_WEEKDAY", "DOMINANT_TIMEBUCKET", "LAST_PRODUCT_PLAYED"):
            df[c] = None
        return df

    # ===== Q1: top jogos por player (casino) — 90d =====
    sql_jogos_template = f"""
    SELECT
        f.player_id,
        f.game_id,
        SUM(f.real_bet_amount_base)                                        AS turnover,
        SUM(f.real_bet_amount_base) - SUM(f.real_win_amount_base)          AS ngr_game,
        SUM(f.bet_count)                                                   AS rounds
    FROM ps_bi.fct_casino_activity_daily f
    WHERE f.activity_date >= DATE '{janela_ini}'
      AND f.activity_date <  DATE '{snap}'
      AND f.player_id IN ({{ids_str}})
      AND f.bet_count > 0
      AND f.game_id IS NOT NULL
      AND f.game_id NOT IN ('0', 'dummy', 'dummy_game')
    GROUP BY f.player_id, f.game_id
    """
    log.info(f"  Q1: jogos casino para {len(player_ids):,} players...")
    jogos = _run_athena_in_batches(sql_jogos_template, player_ids, database="ps_bi", batch_size=4000)
    log.info(f"  Q1: {len(jogos):,} linhas (player x game)")

    # ===== Q2: lookup catalogo de jogos =====
    # FONTE PRIMARIA: multibet.game_image_mapping (Super Nova DB)
    # - 2.721 jogos com game_name legivel ("Fortune Ox") + provider_display_name
    #   ("Pragmatic Play"), atualizado diariamente.
    # - JOIN: Athena game_id pode vir como "{provider_game_id}" (slot) ou
    #   "{provider_game_id}_{room_id}" (live). Tentamos match direto e via
    #   prefixo (split do "_" em jogos live).
    # FONTES FALLBACK: bireports_ec2.tbl_vendor_games_mapping_data, ps_bi.dim_game
    from db.supernova import execute_supernova as _exec_sn
    log.info("  Q2: catalogo de jogos (multibet.game_image_mapping no Super Nova DB)...")
    try:
        rows_dim = _exec_sn(
            """
            SELECT provider_game_id,
                   COALESCE(NULLIF(game_name, ''), provider_game_id) AS game_desc,
                   COALESCE(NULLIF(provider_display_name, ''), vendor_id) AS vendor_id
            FROM multibet.game_image_mapping
            WHERE is_active IS TRUE OR is_active IS NULL;
            """,
            fetch=True,
        )
        dim_local = pd.DataFrame(rows_dim, columns=["provider_game_id", "game_desc", "vendor_id"])
        dim_local["provider_game_id"] = dim_local["provider_game_id"].astype(str)
        dim_local = dim_local.drop_duplicates("provider_game_id")
        log.info(f"  Q2: {len(dim_local):,} jogos no game_image_mapping")

        # Para casar com Athena game_id (que pode ter sufixo _roomId), criamos
        # um lookup duplicado: game_id direto = provider_game_id, e tambem
        # split por "_" pra casar jogos live.
        dim = dim_local.rename(columns={"provider_game_id": "game_id"})
    except Exception as e:
        log.warning(f"  Q2: falha em game_image_mapping ({e}) — fallback bireports_ec2")
        try:
            dim = query_athena(
                """
                SELECT c_game_id AS game_id, c_game_desc AS game_desc, c_vendor_id AS vendor_id
                FROM bireports_ec2.tbl_vendor_games_mapping_data
                """,
                database="bireports_ec2",
            ).drop_duplicates("game_id")
            log.info(f"  Q2: {len(dim):,} jogos (fallback bireports_ec2)")
        except Exception as e2:
            log.warning(f"  Q2: fallback 2 falhou ({e2}) — usando ps_bi.dim_game")
            dim = query_athena(
                "SELECT game_id, game_desc, vendor_id FROM ps_bi.dim_game",
                database="ps_bi",
            ).drop_duplicates("game_id")
            log.info(f"  Q2: {len(dim):,} jogos (fallback dim_game)")

    # ===== Q3: hour/day patterns =====
    sql_hour_template = f"""
    SELECT
        f.player_id,
        f.activity_date,
        f.activity_hour,
        SUM(f.bet_count) AS bets
    FROM ps_bi.fct_casino_activity_hourly f
    WHERE f.activity_date >= DATE '{janela_ini}'
      AND f.activity_date <  DATE '{snap}'
      AND f.player_id IN ({{ids_str}})
      AND f.bet_count > 0
    GROUP BY f.player_id, f.activity_date, f.activity_hour
    """
    log.info("  Q3: hour/day patterns...")
    hours = _run_athena_in_batches(sql_hour_template, player_ids, database="ps_bi", batch_size=4000)
    log.info(f"  Q3: {len(hours):,} linhas")

    # =====================================================
    # AGREGACOES PANDAS — por TIER (rating × classificacao_risco)
    # =====================================================
    # Anexa rating + matriz ao DataFrame de jogos
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    jogos["player_id"] = pd.to_numeric(jogos["player_id"], errors="coerce").astype("Int64")
    hours["player_id"] = pd.to_numeric(hours["player_id"], errors="coerce").astype("Int64")

    tier_lookup = df[["player_id", "rating", "classificacao_risco"]].drop_duplicates("player_id")

    jogos = jogos.merge(tier_lookup, on="player_id", how="left")

    # Match em 2 passos: direto pelo game_id; e fallback via prefixo (split "_")
    # para jogos live cujo game_id Athena vem como "{provider_game_id}_{room_id}".
    jogos["game_id_str"] = jogos["game_id"].astype(str)
    dim["game_id"] = dim["game_id"].astype(str)
    jogos = jogos.merge(dim[["game_id", "game_desc", "vendor_id"]], on="game_id", how="left")

    # Para os que ficaram sem nome, tenta match pelo prefixo antes do "_"
    sem_nome = jogos["game_desc"].isna()
    if sem_nome.any():
        jogos.loc[sem_nome, "prefix"] = jogos.loc[sem_nome, "game_id_str"].str.split("_").str[0]
        dim_pref = dim.rename(columns={"game_id": "prefix",
                                          "game_desc": "game_desc_pref",
                                          "vendor_id": "vendor_id_pref"})
        jogos = jogos.merge(dim_pref, on="prefix", how="left")
        jogos["game_desc"] = jogos["game_desc"].fillna(jogos.get("game_desc_pref"))
        jogos["vendor_id"] = jogos["vendor_id"].fillna(jogos.get("vendor_id_pref"))
        jogos = jogos.drop(columns=["game_desc_pref", "vendor_id_pref", "prefix"], errors="ignore")

    # Ainda sem nome → usa game_id como fallback final
    jogos["game_desc"] = jogos["game_desc"].fillna(jogos["game_id_str"])

    # Renomeia agregados sportsbook (altenar-games etc) pra 'Sportsbook' no display
    is_sportsbook = (jogos["game_id_str"].str.lower().str.startswith("altenar")
                     | (jogos["vendor_id"].astype(str).str.lower() == "altenar"))
    jogos.loc[is_sportsbook, "game_desc"] = "Sportsbook"
    jogos.loc[is_sportsbook, "vendor_id"] = "Sportsbook"

    jogos = jogos.drop(columns=["game_id_str"], errors="ignore")

    cobertura_nome = (jogos["game_desc"] != jogos["game_id"].astype(str)).sum() / max(len(jogos), 1) * 100
    log.info(f"  Cobertura nome de jogo: {cobertura_nome:.1f}% (incl. Sportsbook agregado)")

    hours = hours.merge(tier_lookup, on="player_id", how="left")

    # ----- Top jogos por TIER (3 ordenacoes diferentes) -----
    def top_n_per_tier(j_df: pd.DataFrame, metric: str, n: int = 3) -> pd.DataFrame:
        """Para cada (rating, matriz), retorna top n game_desc pela metrica."""
        agg = j_df.groupby(["rating", "classificacao_risco", "game_desc"])[metric].sum().reset_index()
        agg = agg.sort_values(["rating", "classificacao_risco", metric], ascending=[True, True, False])
        agg["rank"] = agg.groupby(["rating", "classificacao_risco"]).cumcount() + 1
        agg = agg[agg["rank"] <= n]
        # Pivota: rank 1, 2, 3 → colunas
        out = agg.pivot_table(index=["rating", "classificacao_risco"], columns="rank",
                               values="game_desc", aggfunc="first").reset_index()
        out.columns.name = None
        return out

    log.info("  Pandas: top jogos por TIER (turnover, rounds, ngr)...")
    top_turn = top_n_per_tier(jogos, "turnover", 3)
    top_turn.columns = ["rating", "classificacao_risco",
                          "TOP_GAME_1_TIER_TURNOVER", "TOP_GAME_2_TIER_TURNOVER", "TOP_GAME_3_TIER_TURNOVER"]

    top_rnd = top_n_per_tier(jogos, "rounds", 3)
    top_rnd.columns = ["rating", "classificacao_risco",
                         "TOP_GAME_1_TIER_ROUNDS", "TOP_GAME_2_TIER_ROUNDS", "TOP_GAME_3_TIER_ROUNDS"]

    top_ngr = top_n_per_tier(jogos, "ngr_game", 2)
    top_ngr.columns = ["rating", "classificacao_risco", "TOP_GAME_1", "TOP_GAME_2"]

    # ----- Top providers por TIER (top 2 por NGR) -----
    log.info("  Pandas: top providers por TIER...")
    prov = jogos.dropna(subset=["vendor_id"]).copy()
    prov["vendor_id"] = prov["vendor_id"].astype(str)
    prov_agg = prov.groupby(["rating", "classificacao_risco", "vendor_id"])["ngr_game"].sum().reset_index()
    prov_agg = prov_agg.sort_values(["rating", "classificacao_risco", "ngr_game"],
                                       ascending=[True, True, False])
    prov_agg["rank"] = prov_agg.groupby(["rating", "classificacao_risco"]).cumcount() + 1
    prov_agg = prov_agg[prov_agg["rank"] <= 2]
    top_prov = prov_agg.pivot_table(index=["rating", "classificacao_risco"], columns="rank",
                                       values="vendor_id", aggfunc="first").reset_index()
    top_prov.columns.name = None
    top_prov.columns = ["rating", "classificacao_risco", "TOP_PROVIDER_1", "TOP_PROVIDER_2"]

    # ----- Dia/hora dominante por TIER -----
    log.info("  Pandas: DOMINANT_WEEKDAY e DOMINANT_TIMEBUCKET por TIER...")
    hours["activity_date_dt"] = pd.to_datetime(hours["activity_date"], errors="coerce")
    weekday_pt = {0: "Seg", 1: "Ter", 2: "Qua", 3: "Qui", 4: "Sex", 5: "Sab", 6: "Dom"}
    hours["weekday"] = hours["activity_date_dt"].dt.weekday.map(weekday_pt)

    def hour_to_bucket(h):
        h = int(h) if pd.notna(h) else -1
        if 0 <= h <= 5:   return "MADRUGADA"
        if 6 <= h <= 11:  return "MANHA"
        if 12 <= h <= 17: return "TARDE"
        if 18 <= h <= 23: return "NOITE"
        return None
    hours["timebucket"] = hours["activity_hour"].apply(hour_to_bucket)

    # Para cada (rating, matriz), modal weekday e timebucket (ponderado por bets)
    def modal_per_tier(h_df: pd.DataFrame, col: str) -> pd.DataFrame:
        agg = h_df.groupby(["rating", "classificacao_risco", col])["bets"].sum().reset_index()
        idx = agg.groupby(["rating", "classificacao_risco"])["bets"].idxmax()
        out = agg.loc[idx].reset_index(drop=True)
        return out[["rating", "classificacao_risco", col]]

    dom_wd = modal_per_tier(hours.dropna(subset=["weekday"]), "weekday")
    dom_wd.columns = ["rating", "classificacao_risco", "DOMINANT_WEEKDAY"]
    dom_tb = modal_per_tier(hours.dropna(subset=["timebucket"]), "timebucket")
    dom_tb.columns = ["rating", "classificacao_risco", "DOMINANT_TIMEBUCKET"]

    # ----- LAST_PRODUCT_PLAYED por player (proxy via product_type do PCR) -----
    pt = df["product_type"].fillna("OUTRO").astype(str).str.upper()
    df["LAST_PRODUCT_PLAYED"] = pt.where(pt.isin(["CASINO", "SPORT", "MISTO"]), "OUTRO")

    # ----- Merge tudo no df principal por (rating, classificacao_risco) -----
    log.info("  Merging tier-level cols no df principal...")
    for d in (top_turn, top_rnd, top_ngr, top_prov, dom_wd, dom_tb):
        df = df.merge(d, on=["rating", "classificacao_risco"], how="left")

    # Stats
    cols_added = ["PRODUCT_MIX", "TOP_PROVIDER_1", "TOP_PROVIDER_2",
                   "TOP_GAME_1", "TOP_GAME_2",
                   "TOP_GAME_1_TIER_TURNOVER", "TOP_GAME_2_TIER_TURNOVER", "TOP_GAME_3_TIER_TURNOVER",
                   "TOP_GAME_1_TIER_ROUNDS",   "TOP_GAME_2_TIER_ROUNDS",   "TOP_GAME_3_TIER_ROUNDS",
                   "DOMINANT_WEEKDAY", "DOMINANT_TIMEBUCKET", "LAST_PRODUCT_PLAYED"]
    log.info(f"  Bloco 3 OK — {len(cols_added)} colunas adicionadas")
    return df


# ============================================================
# BLOCO 5b — BTR + bonus extras (1 query Athena unificada)
# ============================================================
def bloco_5b_btr_bonus(df: pd.DataFrame, snapshot_date: Optional[str] = None) -> pd.DataFrame:
    """
    Adiciona 7 colunas de BTR e bonus avancado.

    Colunas geradas:
      - BONUS_ISSUED_30D                : valor de bonus emitido em 30d (BRL)
      - BTR_30D                         : bonus_turned_real / bonus_issued (30d)
      - BTR_CASINO_30D                  : split por produto (proxy se nao tiver split nativo)
      - BTR_SPORT_30D                   : split por produto
      - BONUS_DEPENDENCY_RATIO_LIFETIME : bonus_issued_lt / total_deposits_lt
      - NGR_PER_BONUS_REAL_30D          : ngr_30d / bonus_issued_30d (None se sem bonus)
      - LAST_BONUS_DATE                 : ultima data com bonus_issued > 0 (90d window)
      - LAST_BONUS_TYPE                 : 'CASH' / 'FREESPIN' / 'OUTRO' — proxy = 'CASH'
                                          (bonus_ec2 nao acessivel ainda)

    Fonte: ps_bi.fct_player_activity_daily (campo bonus_issued_base / bonus_turnedreal_base).

    Janela: 30d rolling ate D-1 (consistente com Bloco 1+2).
    """
    from db.athena import query_athena

    log.info("[Bloco 5b] BTR + bonus extras (30d + lifetime)...")
    if snapshot_date is None:
        snapshot_date = date.today().isoformat()
    snap = pd.to_datetime(snapshot_date).date()
    janela_ini = snap - timedelta(days=30)

    df = df.copy()
    player_ids = df["player_id"].dropna().astype("int64").unique().tolist()
    if not player_ids:
        log.warning("  Sem player_ids — pulando.")
        for c in ("BONUS_ISSUED_30D", "BTR_30D", "BTR_CASINO_30D", "BTR_SPORT_30D",
                  "BONUS_DEPENDENCY_RATIO_LIFETIME", "NGR_PER_BONUS_REAL_30D",
                  "LAST_BONUS_DATE", "LAST_BONUS_TYPE"):
            df[c] = None
        return df

    sql_template = f"""
    WITH b30 AS (
        SELECT
            f.player_id,
            COALESCE(SUM(f.bonus_issued_base), 0)      AS bonus_issued_30d,
            COALESCE(SUM(f.bonus_turnedreal_base), 0)  AS bonus_turnedreal_30d,
            COALESCE(SUM(f.ngr_base), 0)               AS ngr_30d,
            COALESCE(SUM(f.casino_realbet_base), 0)    AS casino_realbet_30d,
            COALESCE(SUM(f.sb_realbet_base), 0)        AS sb_realbet_30d,
            COALESCE(SUM(f.casino_realbet_base), 0)
              + COALESCE(SUM(f.sb_realbet_base), 0)    AS realbet_total_30d
        FROM ps_bi.fct_player_activity_daily f
        WHERE f.activity_date >= DATE '{janela_ini}'
          AND f.activity_date <  DATE '{snap}'
          AND f.player_id IN ({{ids_str}})
        GROUP BY f.player_id
    ),
    blife AS (
        SELECT
            f.player_id,
            COALESCE(SUM(f.bonus_issued_base), 0)     AS bonus_issued_lifetime,
            COALESCE(SUM(f.deposit_success_base), 0)  AS deposit_lifetime
        FROM ps_bi.fct_player_activity_daily f
        WHERE f.player_id IN ({{ids_str}})
        GROUP BY f.player_id
    ),
    last_bn AS (
        SELECT
            f.player_id,
            MAX(f.activity_date) AS last_bonus_date
        FROM ps_bi.fct_player_activity_daily f
        WHERE f.player_id IN ({{ids_str}})
          AND f.bonus_issued_base > 0
          AND f.activity_date >= DATE '{snap - timedelta(days=180)}'
        GROUP BY f.player_id
    )
    SELECT
        b30.player_id,
        b30.bonus_issued_30d,
        b30.bonus_turnedreal_30d,
        b30.ngr_30d AS ngr_30d_for_btr,
        b30.casino_realbet_30d,
        b30.sb_realbet_30d,
        b30.realbet_total_30d,
        bl.bonus_issued_lifetime,
        bl.deposit_lifetime,
        lb.last_bonus_date
    FROM b30
    LEFT JOIN blife bl ON b30.player_id = bl.player_id
    LEFT JOIN last_bn lb ON b30.player_id = lb.player_id
    """
    log.info(f"  Athena: BTR/bonus para {len(player_ids):,} players...")
    raw = _run_athena_in_batches(sql_template, player_ids, database="ps_bi", batch_size=3000)
    log.info(f"  -> {len(raw):,} linhas")

    raw["player_id"] = pd.to_numeric(raw["player_id"], errors="coerce").astype("Int64")

    # FIX M5/M6 (auditoria 28/04): vetorizado com np.where (~5x mais rapido que apply)
    issued = raw["bonus_issued_30d"].fillna(0).astype(float)
    turned = raw["bonus_turnedreal_30d"].fillna(0).astype(float)
    realbet = raw["realbet_total_30d"].fillna(0).astype(float)
    casino_rb = raw["casino_realbet_30d"].fillna(0).astype(float)
    sport_rb = raw["sb_realbet_30d"].fillna(0).astype(float)
    ngr_30d = raw["ngr_30d_for_btr"].fillna(0).astype(float)
    deposit_lt = raw["deposit_lifetime"].fillna(0).astype(float)
    bonus_lt = raw["bonus_issued_lifetime"].fillna(0).astype(float)

    raw["BONUS_ISSUED_30D"] = issued

    # BTR_30D = bonus_turnedreal / bonus_issued (None se sem bonus)
    issued_safe = issued.replace(0, np.nan)
    raw["BTR_30D"] = turned / issued_safe  # NaN onde issued=0

    # BTR split por produto (proxy via proporcao realbet)
    realbet_safe = realbet.replace(0, np.nan)
    prop_casino = (casino_rb / realbet_safe).fillna(0)
    prop_sport  = (sport_rb / realbet_safe).fillna(0)
    raw["BTR_CASINO_30D"] = (turned * prop_casino) / issued_safe
    raw["BTR_SPORT_30D"]  = (turned * prop_sport)  / issued_safe

    # BONUS_DEPENDENCY_RATIO_LIFETIME
    deposit_lt_safe = deposit_lt.replace(0, np.nan)
    raw["BONUS_DEPENDENCY_RATIO_LIFETIME"] = bonus_lt / deposit_lt_safe

    # NGR_PER_BONUS_REAL_30D
    raw["NGR_PER_BONUS_REAL_30D"] = ngr_30d / issued_safe

    # LAST_BONUS_DATE / LAST_BONUS_TYPE
    raw["LAST_BONUS_DATE"] = pd.to_datetime(raw["last_bonus_date"], errors="coerce").dt.date
    # Sem acesso a bonus_ec2 com tipo, deixamos 'CASH' como proxy para quem teve bonus emitido
    raw["LAST_BONUS_TYPE"] = raw["LAST_BONUS_DATE"].apply(lambda d: "CASH" if pd.notna(d) else None)

    # Seleciona somente as colunas finais para merge
    cols_final = ["player_id", "BONUS_ISSUED_30D", "BTR_30D", "BTR_CASINO_30D", "BTR_SPORT_30D",
                  "BONUS_DEPENDENCY_RATIO_LIFETIME", "NGR_PER_BONUS_REAL_30D",
                  "LAST_BONUS_DATE", "LAST_BONUS_TYPE"]
    raw_final = raw[cols_final]

    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df = df.merge(raw_final, on="player_id", how="left")

    # Players sem atividade → BONUS_ISSUED_30D = 0 (mas BTRs ficam None)
    df["BONUS_ISSUED_30D"] = df["BONUS_ISSUED_30D"].fillna(0)

    log.info(f"  BTR_30D nao-nulo em {df['BTR_30D'].notna().sum():,} players")
    log.info(f"  LAST_BONUS_DATE nao-nulo em {df['LAST_BONUS_DATE'].notna().sum():,} players")
    return df
