"""
Análise Consolidada — Campanhas RETEM (06/03 a 10/03/2026)
===========================================================
Consolida TODAS as campanhas [RETEM] do período (WhatsApp, Push, In-App/Popup)
por segmento: Ativação, Recuperação, Retenção, Monetização, Pagamento, Urgência, Gire e Ganhe.

Abas do Excel:
  1. Resumo por Segmento — métricas consolidadas (users deduplicados)
  2. Conversão por Canal  — WhatsApp × Push × In-App por segmento
  3. Breakdown por Jogo   — turnover, apostas e receita por jogo
  4. Detalhe Diário       — segmento × dia
  5. Nota Metodológica    — regras aplicadas

Regras de negócio:
  • Janela Opção B: só atividade financeira APÓS first_contact_date individual
  • Multi-campanha: user em >1 segmento conta em todos
  • Dentro do mesmo segmento: user deduplicado (first_contact_date = menor data)
  • FTD: apenas Ativação (Rafael CRM)
  • Centavos Redshift: ÷100
  • Bridge ecr.tbl_ecr para NGR (c_ecr_id 18dig → c_external_id 15dig)
"""

import sys
import os
import logging
import pandas as pd
import numpy as np

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.bigquery import query_bigquery
from db.redshift import query_redshift

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Configurações ────────────────────────────────────────────────────────────
DATA_INICIO = "2026-03-06"
DATA_FIM    = "2026-03-10"
LABEL_ID    = 24105

OUTPUT_DIR = "c:/Users/NITRO/OneDrive - PGX/MultiBet/analysis/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Segmentos com FTD (confirmado por Rafael CRM)
SEGMENTOS_COM_FTD = {"Ativação"}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def to_date(series: pd.Series) -> pd.Series:
    """Normaliza coluna para datetime.date (sem fuso)."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None).dt.date


def bq_to_ecr(df: pd.DataFrame, col: str = "user_ext_id") -> pd.DataFrame:
    """Converte user_ext_id (string BQ) → ecr_id (int) e descarta nulos."""
    df = df.copy()
    df["ecr_id"] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["ecr_id"]).assign(ecr_id=lambda d: d["ecr_id"].astype(int))


def classify_segment(name: str) -> str:
    """Classifica resource_name em segmento."""
    n = name.upper()
    if "ATIVA" in n:
        return "Ativação"
    if "RECUPERA" in n:
        return "Recuperação"
    if "RETEN" in n:
        return "Retenção"
    if "MONETIZA" in n:
        return "Monetização"
    if "PAGAMENTO" in n:
        return "Pagamento"
    if "URGENCIA" in n or "URGÊNCIA" in n:
        return "Urgência"
    if "GIRE" in n or "CONFIRMA" in n:
        return "Gire e Ganhe"
    return "Outro"


def classify_canal(provider_id) -> str:
    """Classifica label_provider_id em canal."""
    if pd.isna(provider_id):
        return "In-App"
    pid = int(provider_id)
    if pid == 1536:
        return "WhatsApp"
    if pid == 611:
        return "Push"
    return f"Outro({pid})"


# ─── STEP 1: BigQuery — Todos os registros de comunicação RETEM ──────────────
log.info("STEP 1: Buscando TODOS os registros de comunicação RETEM no BigQuery...")

df_comm = query_bigquery(f"""
SELECT
    jc.resource_id,
    r.resource_name,
    jc.label_provider_id,
    jc.fact_type_id,
    jc.user_ext_id,
    MIN(jc.fact_date) AS first_contact_date
FROM `smartico-bq6.dwh_ext_24105.j_communication` jc
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_resource` r
    ON jc.resource_id = r.resource_id AND r.label_id = {LABEL_ID}
WHERE jc.label_id = {LABEL_ID}
  AND DATE(jc.fact_date) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND UPPER(COALESCE(r.resource_name, '')) LIKE '%RETEM%'
  AND jc.user_ext_id IS NOT NULL
GROUP BY jc.resource_id, r.resource_name, jc.label_provider_id,
         jc.fact_type_id, jc.user_ext_id
""")

log.info(f"{len(df_comm)} registros de comunicação carregados.")

# Classificar segmento e canal
df_comm["segmento"] = df_comm["resource_name"].fillna("").apply(classify_segment)
df_comm["canal"] = df_comm["label_provider_id"].apply(classify_canal)

FACT_TYPE = {1: "enviado", 2: "entregue", 4: "falha", 5: "clique", 6: "bloqueado"}
df_comm["status"] = df_comm["fact_type_id"].map(FACT_TYPE).fillna("outro")

# Entregues (fact_type_id = 2) com first_contact_date
df_entregues_raw = (
    df_comm[df_comm["fact_type_id"] == 2]
    [["resource_id", "resource_name", "segmento", "canal",
      "user_ext_id", "first_contact_date"]]
    .pipe(bq_to_ecr)
)
df_entregues_raw["first_contact_date"] = to_date(df_entregues_raw["first_contact_date"])

# Extrair data da campanha do resource_name (DD/MM/YYYY) para detalhe diário
df_entregues_raw["data_campanha"] = pd.to_datetime(
    df_entregues_raw["resource_name"].str.extract(r"(\d{2}/\d{2}/\d{4})")[0],
    format="%d/%m/%Y", errors="coerce"
).dt.date

log.info(f"Entregues raw: {len(df_entregues_raw)} registros | "
         f"{df_entregues_raw['ecr_id'].nunique()} users únicos")

# ─── Deduplicar por segmento: 1 user = 1 entrada por segmento ───────────────
# Dentro do mesmo segmento, user pode ter recebido WhatsApp + Push → pegar menor date
df_entregues = (
    df_entregues_raw
    .sort_values("first_contact_date")
    .drop_duplicates(subset=["segmento", "ecr_id"], keep="first")
    [["segmento", "ecr_id", "first_contact_date"]]
)
log.info(f"Entregues deduplicados por segmento: {len(df_entregues)}")

# Canal por segmento × user (para aba Conversão por Canal)
# Um user pode ter recebido por múltiplos canais
df_user_canal = (
    df_entregues_raw
    .drop_duplicates(subset=["segmento", "canal", "ecr_id"])
    [["segmento", "canal", "ecr_id"]]
)

# ─── STEP 2: BigQuery — Depósitos diários ────────────────────────────────────
log.info("STEP 2: Buscando depósitos no BigQuery...")

df_dep_daily = query_bigquery(f"""
SELECT
    CAST(u.user_ext_id AS INT64)   AS ecr_id,
    DATE(d.event_time)              AS event_date,
    COUNT(*)                        AS qtd_depositos,
    SUM(d.acc_last_deposit_amount)  AS valor_total_dep
FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` d
JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON d.user_id = u.user_id
WHERE d.label_id = {LABEL_ID}
  AND DATE(d.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND (d.acc_is_rollback IS NULL OR d.acc_is_rollback = FALSE)
GROUP BY u.user_ext_id, DATE(d.event_time)
""")
df_dep_daily["event_date"] = pd.to_datetime(df_dep_daily["event_date"]).dt.date
log.info(f"{df_dep_daily['ecr_id'].nunique()} jogadores com depósito no período.")

# ─── STEP 3: BigQuery — Casino bets diários ──────────────────────────────────
log.info("STEP 3: Buscando apostas de casino no BigQuery...")

df_casino_bets_daily = query_bigquery(f"""
SELECT
    u.user_ext_id                              AS user_ext_id,
    DATE(b.event_time)                          AS event_date,
    COALESCE(g.game_name,     'Desconhecido')  AS game_name,
    COALESCE(p.provider_name, 'Desconhecido')  AS provider_name,
    SUM(b.casino_last_bet_amount_real)          AS real_bets,
    COUNT(*)                                    AS qtd_apostas
FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON b.user_id = u.user_id
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_casino_game_name` g
    ON CAST(b.casino_last_bet_game_name AS INT64) = g.smr_game_id AND g.label_id = {LABEL_ID}
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_casino_provider_name` p
    ON CAST(b.casino_last_bet_game_provider AS INT64) = p.smr_provider_id AND p.label_id = {LABEL_ID}
WHERE b.label_id = {LABEL_ID}
  AND DATE(b.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND (b.casino_is_rollback IS NULL OR b.casino_is_rollback = FALSE)
  AND (b.casino_is_free_bet  IS NULL OR b.casino_is_free_bet  = FALSE)
GROUP BY u.user_ext_id, DATE(b.event_time), g.game_name, p.provider_name
""")
df_casino_bets_daily = bq_to_ecr(df_casino_bets_daily)
df_casino_bets_daily["event_date"] = pd.to_datetime(df_casino_bets_daily["event_date"]).dt.date
log.info(f"{len(df_casino_bets_daily)} linhas de casino bets.")

# ─── STEP 4: BigQuery — Casino wins diários ──────────────────────────────────
log.info("STEP 4: Buscando ganhos de casino no BigQuery...")

df_casino_wins_daily = query_bigquery(f"""
SELECT
    u.user_ext_id                        AS user_ext_id,
    DATE(w.event_time)                   AS event_date,
    SUM(w.casino_last_win_amount_real)   AS real_wins
FROM `smartico-bq6.dwh_ext_24105.tr_casino_win` w
JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON w.user_id = u.user_id
WHERE w.label_id = {LABEL_ID}
  AND DATE(w.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND (w.casino_is_rollback IS NULL OR w.casino_is_rollback = FALSE)
GROUP BY u.user_ext_id, DATE(w.event_time)
""")
df_casino_wins_daily = bq_to_ecr(df_casino_wins_daily)
df_casino_wins_daily["event_date"] = pd.to_datetime(df_casino_wins_daily["event_date"]).dt.date
log.info(f"{df_casino_wins_daily['ecr_id'].nunique()} jogadores com wins de casino.")

# ─── STEP 5: BigQuery — Sport bets settled diários ───────────────────────────
log.info("STEP 5: Buscando apostas esportivas no BigQuery...")

df_sport_daily = query_bigquery(f"""
SELECT
    u.user_ext_id                             AS user_ext_id,
    DATE(s.event_time)                        AS event_date,
    SUM(s.sport_last_bet_amount_real)         AS real_bets_sport,
    SUM(s.sport_last_bet_win_amount_real)     AS real_wins_sport,
    COUNT(*)                                  AS qtd_apostas_sport
FROM `smartico-bq6.dwh_ext_24105.tr_sport_bet_settled` s
JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON s.user_id = u.user_id
WHERE s.label_id = {LABEL_ID}
  AND DATE(s.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND (s.sport_is_rollback IS NULL OR s.sport_is_rollback = FALSE)
GROUP BY u.user_ext_id, DATE(s.event_time)
""")
df_sport_daily = bq_to_ecr(df_sport_daily)
df_sport_daily["event_date"] = pd.to_datetime(df_sport_daily["event_date"]).dt.date
log.info(f"{df_sport_daily['ecr_id'].nunique()} jogadores com apostas esportivas.")

# ─── STEP 6: FTD via lógica de campanha ──────────────────────────────────────
log.info("STEP 6: FTD calculado via lógica de campanha (Ativação = depositantes).")

# ─── STEP 7: Redshift — Bonus turned real (÷100 centavos→reais) ──────────────
log.info("STEP 7: Buscando bonus turned real no Redshift...")

df_bonus_daily = query_redshift(f"""
SELECT
    s.c_ecr_id                                          AS ecr_id,
    s.c_created_date                                    AS event_date,
    SUM(s.c_txn_crp_amount_ecr_crncy
      + s.c_txn_wrp_amount_ecr_crncy
      + s.c_txn_rrp_amount_ecr_crncy) / 100.0          AS bonus_turned_real
FROM bireports.tbl_ecr_txn_type_wise_daily_summary s
JOIN fund.tbl_real_fund_txn_type_mst f ON s.c_txn_type = f.c_txn_type
WHERE s.c_created_date >= '{DATA_INICIO}'
  AND s.c_created_date <= '{DATA_FIM}'
  AND f.c_txn_identifier_key IN ('ISSUE_BONUS', 'PARTIAL_ISSUE_BONUS')
GROUP BY s.c_ecr_id, s.c_created_date
""")
df_bonus_daily["ecr_id"] = df_bonus_daily["ecr_id"].astype(int)
df_bonus_daily["bonus_turned_real"] = df_bonus_daily["bonus_turned_real"].astype(float)
df_bonus_daily["event_date"] = pd.to_datetime(df_bonus_daily["event_date"]).dt.date
log.info(f"{len(df_bonus_daily)} linhas de bonus turned real.")

# ─── STEP 8: Redshift — Ajustes manuais (÷100) ──────────────────────────────
log.info("STEP 8: Buscando ajustes manuais no Redshift...")

df_adj_daily = query_redshift(f"""
SELECT
    s.c_ecr_id                                         AS ecr_id,
    s.c_created_date                                   AS event_date,
    SUM(
        CASE
            WHEN f.c_txn_identifier_key IN (
                'REAL_CASH_ADDITION_BY_CS', 'CASINO_MANUAL_CREDIT', 'SB_MANUAL_CREDIT'
            ) THEN  s.c_txn_real_cash_amount_ecr_crncy
            WHEN f.c_txn_identifier_key IN (
                'REAL_CASH_REMOVAL_BY_CS',  'CASINO_MANUAL_DEBIT',  'SB_MANUAL_DEBIT'
            ) THEN -s.c_txn_real_cash_amount_ecr_crncy
            ELSE 0
        END
    ) / 100.0                                          AS real_cash_adjustments
FROM bireports.tbl_ecr_txn_type_wise_daily_summary s
JOIN fund.tbl_real_fund_txn_type_mst f ON s.c_txn_type = f.c_txn_type
WHERE s.c_created_date >= '{DATA_INICIO}'
  AND s.c_created_date <= '{DATA_FIM}'
  AND f.c_txn_identifier_key IN (
        'REAL_CASH_ADDITION_BY_CS', 'REAL_CASH_REMOVAL_BY_CS',
        'CASINO_MANUAL_CREDIT',     'CASINO_MANUAL_DEBIT',
        'SB_MANUAL_CREDIT',         'SB_MANUAL_DEBIT'
  )
GROUP BY s.c_ecr_id, s.c_created_date
""")
df_adj_daily["ecr_id"] = df_adj_daily["ecr_id"].astype(int)
df_adj_daily["real_cash_adjustments"] = df_adj_daily["real_cash_adjustments"].astype(float)
df_adj_daily["event_date"] = pd.to_datetime(df_adj_daily["event_date"]).dt.date
log.info(f"{len(df_adj_daily)} linhas de ajustes.")

# ─── STEP 9: Bridge ecr.tbl_ecr (18dig → 15dig) ─────────────────────────────
log.info("STEP 9: Carregando bridge (ecr.tbl_ecr)...")

rs_ecr_ids = set(df_bonus_daily["ecr_id"]).union(set(df_adj_daily["ecr_id"]))
if rs_ecr_ids:
    ids_list = ",".join(str(i) for i in rs_ecr_ids)
    df_bridge = query_redshift(f"""
    SELECT c_ecr_id, c_external_id
    FROM ecr.tbl_ecr
    WHERE c_ecr_id IN ({ids_list})
    """)
    df_bridge["c_ecr_id"]      = df_bridge["c_ecr_id"].astype(int)
    df_bridge["c_external_id"] = df_bridge["c_external_id"].astype(int)
    log.info(f"Bridge: {len(df_bridge)} mapeamentos carregados.")

    def remap_ecr(df: pd.DataFrame) -> pd.DataFrame:
        """Troca ecr_id (18 dígitos Redshift) por c_external_id (15 dígitos BQ)."""
        return (
            df.merge(df_bridge, left_on="ecr_id", right_on="c_ecr_id", how="left")
            .assign(ecr_id=lambda d: d["c_external_id"])
            .drop(columns=["c_ecr_id", "c_external_id"])
            .dropna(subset=["ecr_id"])
            .astype({"ecr_id": int})
        )

    df_bonus_daily = remap_ecr(df_bonus_daily)
    df_adj_daily   = remap_ecr(df_adj_daily)
    log.info(
        f"Após bridge: {df_bonus_daily['ecr_id'].nunique()} users bonus | "
        f"{df_adj_daily['ecr_id'].nunique()} users ajustes"
    )
else:
    log.warning("Nenhum dado de bonus/ajustes para bridge.")

# ─── Nota: multi-segmento ────────────────────────────────────────────────────
ecr_por_seg = (
    df_entregues.groupby("ecr_id")["segmento"]
    .nunique().reset_index(name="n_segmentos")
)
multi_seg = ecr_por_seg[ecr_por_seg["n_segmentos"] > 1]
log.info(f"{len(multi_seg)} usuários em mais de um segmento.")


# ─── Função de cálculo por grupo (segmento ou segmento+dia) ──────────────────

def calcular_metricas(users_fc: pd.DataFrame, label: dict) -> dict:
    """
    Calcula métricas para um grupo de users com first_contact_date.
    users_fc: DataFrame com [ecr_id, first_contact_date]
    label: dict com metadados (Segmento, etc.)
    Retorna dict com métricas + breakdown list.
    """
    n_impactados = len(users_fc)

    def apply_window(df_daily, date_col="event_date"):
        return (
            df_daily.merge(users_fc, on="ecr_id", how="inner")
            .pipe(lambda d: d[d[date_col] >= d["first_contact_date"]])
            .drop(columns="first_contact_date")
        )

    # Depósitos
    dep_agg = (
        apply_window(df_dep_daily)
        .groupby("ecr_id")
        .agg(qtd_depositos=("qtd_depositos", "sum"),
             valor_total_dep=("valor_total_dep", "sum"))
        .reset_index()
    )

    # Casino bets
    casino_bets_filtered = apply_window(df_casino_bets_daily)
    casino_agg = (
        casino_bets_filtered
        .groupby("ecr_id")
        .agg(real_bets_casino=("real_bets", "sum"),
             qtd_apostas_casino=("qtd_apostas", "sum"))
        .reset_index()
    )

    # Casino wins
    casino_wins_agg = (
        apply_window(df_casino_wins_daily)
        .groupby("ecr_id")["real_wins"].sum()
        .reset_index().rename(columns={"real_wins": "real_wins_casino"})
    )

    # Sport
    sport_agg = (
        apply_window(df_sport_daily)
        .groupby("ecr_id")
        .agg(real_bets_sport=("real_bets_sport", "sum"),
             real_wins_sport=("real_wins_sport", "sum"),
             qtd_apostas_sport=("qtd_apostas_sport", "sum"))
        .reset_index()
    )

    # Bonus + Ajustes
    bonus_agg = (
        apply_window(df_bonus_daily)
        .groupby("ecr_id")["bonus_turned_real"].sum()
        .reset_index()
    )
    adj_agg = (
        apply_window(df_adj_daily)
        .groupby("ecr_id")["real_cash_adjustments"].sum()
        .reset_index()
    )

    # Merge
    df_camp = (
        users_fc[["ecr_id"]]
        .merge(dep_agg,          on="ecr_id", how="left")
        .merge(casino_agg,       on="ecr_id", how="left")
        .merge(casino_wins_agg,  on="ecr_id", how="left")
        .merge(sport_agg,        on="ecr_id", how="left")
        .merge(bonus_agg,        on="ecr_id", how="left")
        .merge(adj_agg,          on="ecr_id", how="left")
        .fillna(0)
    )
    for col in df_camp.columns:
        if col != "ecr_id":
            df_camp[col] = df_camp[col].astype(float)

    # Totais
    df_camp["real_bets"]   = df_camp["real_bets_casino"]   + df_camp["real_bets_sport"]
    df_camp["real_wins"]   = df_camp["real_wins_casino"]   + df_camp["real_wins_sport"]
    df_camp["qtd_apostas"] = df_camp["qtd_apostas_casino"] + df_camp["qtd_apostas_sport"]
    df_camp["ggr"]         = df_camp["real_bets"] - df_camp["real_wins"]
    df_camp["ngr"]         = df_camp["real_bets"] - (
        df_camp["real_wins"] + df_camp["bonus_turned_real"] + df_camp["real_cash_adjustments"]
    )

    depositantes = df_camp[df_camp["qtd_depositos"] > 0]
    n_dep        = len(depositantes)
    total_dep    = float(depositantes["valor_total_dep"].sum())
    ticket_medio = total_dep / n_dep if n_dep > 0 else 0

    segmento = label.get("Segmento", "")
    n_ftd = n_dep if segmento in SEGMENTOS_COM_FTD else 0

    metricas = {
        **label,
        "Usuários Impactados":   n_impactados,
        "FTD":                   n_ftd,
        "Depositantes":          n_dep,
        "Conv. Depósito (%)":    round(n_dep / n_impactados * 100, 2) if n_impactados else 0,
        "Total Depósitos (R$)":  round(total_dep, 2),
        "Ticket Médio (R$)":     round(ticket_medio, 2),
        "Apostadores":           int((df_camp["real_bets"] > 0).sum()),
        "Qtd Apostas":           int(df_camp["qtd_apostas"].sum()),
        "Turnover (R$)":         round(float(df_camp["real_bets"].sum()), 2),
        "GGR (R$)":              round(float(df_camp["ggr"].sum()), 2),
        "NGR (R$)":              round(float(df_camp["ngr"].sum()), 2),
    }

    # Breakdown por jogo
    breakdown = (
        casino_bets_filtered
        .groupby(["game_name", "provider_name"])
        .agg(turnover=("real_bets", "sum"), qtd_apostas=("qtd_apostas", "sum"))
        .reset_index()
        .sort_values("turnover", ascending=False)
    )
    # Cast Decimal → float para evitar TypeError
    breakdown["turnover"] = breakdown["turnover"].astype(float)
    breakdown["qtd_apostas"] = breakdown["qtd_apostas"].astype(float)

    # Receita por jogo: approx via proporção (wins são agregados, não por jogo)
    turnover_total = breakdown["turnover"].sum()
    wins_total = float(df_camp["real_wins_casino"].sum())
    if turnover_total > 0:
        breakdown["receita_jogo"] = breakdown["turnover"] - (
            wins_total * breakdown["turnover"] / turnover_total
        )
    else:
        breakdown["receita_jogo"] = 0.0

    return metricas, breakdown


# ─── STEP 10: Cálculo por SEGMENTO (consolidado) ─────────────────────────────
log.info("STEP 10: Calculando métricas por segmento consolidado...")

segmentos = sorted(df_entregues["segmento"].unique())
resultados_seg = []
breakdowns_seg = []

for seg in segmentos:
    users_fc = (
        df_entregues[df_entregues["segmento"] == seg]
        [["ecr_id", "first_contact_date"]]
        .drop_duplicates("ecr_id")
    )
    log.info(f"  → {seg}: {len(users_fc)} users")
    metricas, breakdown = calcular_metricas(users_fc, {"Segmento": seg})
    resultados_seg.append(metricas)
    breakdowns_seg.append(breakdown.assign(Segmento=seg))

df_resumo = pd.DataFrame(resultados_seg)

df_breakdown = (
    pd.concat(breakdowns_seg, ignore_index=True)
    [["Segmento", "game_name", "provider_name", "turnover", "qtd_apostas", "receita_jogo"]]
    .rename(columns={
        "game_name":     "Jogo",
        "provider_name": "Provedor",
        "turnover":      "Turnover (R$)",
        "qtd_apostas":   "Qtd Apostas",
        "receita_jogo":  "Receita Estimada (R$)",
    })
)

# ─── STEP 11: Conversão por Canal ────────────────────────────────────────────
log.info("STEP 11: Calculando conversão por canal...")

# Users que depositaram no período (com window)
depositantes_set = {}
for seg in segmentos:
    users_fc = (
        df_entregues[df_entregues["segmento"] == seg]
        [["ecr_id", "first_contact_date"]]
        .drop_duplicates("ecr_id")
    )
    dep_window = (
        df_dep_daily.merge(users_fc, on="ecr_id", how="inner")
        .pipe(lambda d: d[d["event_date"] >= d["first_contact_date"]])
    )
    depositantes_set[seg] = set(dep_window["ecr_id"].unique())

canal_rows = []
for seg in segmentos:
    seg_canais = df_user_canal[df_user_canal["segmento"] == seg]
    for canal in sorted(seg_canais["canal"].unique()):
        users_canal = set(seg_canais[seg_canais["canal"] == canal]["ecr_id"])
        n_users = len(users_canal)
        # Depositantes que receberam por este canal
        n_dep_canal = len(users_canal & depositantes_set.get(seg, set()))
        conv = round(n_dep_canal / n_users * 100, 2) if n_users else 0
        canal_rows.append({
            "Segmento":           seg,
            "Canal":              canal,
            "Usuários Alcançados": n_users,
            "Depositantes":       n_dep_canal,
            "Conv. Depósito (%)": conv,
        })

df_canal = pd.DataFrame(canal_rows)

# ─── STEP 12: Detalhe Diário (segmento × dia) ────────────────────────────────
log.info("STEP 12: Calculando detalhe diário...")

# Agrupar entregues por segmento + data_campanha
df_ent_diario = (
    df_entregues_raw
    .dropna(subset=["data_campanha"])
    .sort_values("first_contact_date")
    .drop_duplicates(subset=["segmento", "data_campanha", "ecr_id"], keep="first")
)

resultados_diario = []
dias_seg = df_ent_diario.groupby(["segmento", "data_campanha"]).size().reset_index(name="n")

for _, row in dias_seg.iterrows():
    seg = row["segmento"]
    dia = row["data_campanha"]
    users_fc = (
        df_ent_diario[
            (df_ent_diario["segmento"] == seg) &
            (df_ent_diario["data_campanha"] == dia)
        ][["ecr_id", "first_contact_date"]]
        .drop_duplicates("ecr_id")
    )
    log.info(f"  → {seg} | {dia}: {len(users_fc)} users")
    metricas, _ = calcular_metricas(users_fc, {"Segmento": seg, "Data": str(dia)})
    resultados_diario.append(metricas)

df_diario = pd.DataFrame(resultados_diario)

# ─── Nota Metodológica ───────────────────────────────────────────────────────
df_nota = pd.DataFrame([
    {
        "Tópico": "Período Analisado",
        "Descrição": (
            f"Campanhas disparadas de {DATA_INICIO} a 2026-03-09. "
            f"Atividade financeira medida de {DATA_INICIO} a {DATA_FIM}."
        ),
    },
    {
        "Tópico": "Janela de Atribuição (Opção B)",
        "Descrição": (
            "Só é contabilizada atividade financeira ocorrida APÓS "
            "o primeiro recebimento (entrega) da mensagem por cada usuário "
            "individualmente. Atividade anterior à entrega é ignorada."
        ),
    },
    {
        "Tópico": "Deduplicação por Segmento",
        "Descrição": (
            "Dentro do mesmo segmento, se o usuário recebeu por WhatsApp + Push, "
            "ele conta UMA vez (first_contact_date = menor data entre os canais). "
            "Na aba 'Conversão por Canal', o mesmo user pode aparecer em >1 canal."
        ),
    },
    {
        "Tópico": "Usuários em múltiplos segmentos",
        "Descrição": (
            f"{len(multi_seg)} usuários aparecem em mais de um segmento. "
            "A atividade deles é contabilizada em TODOS os segmentos em que aparecem. "
            "Isso pode causar dupla contagem nos totais consolidados."
        ),
    },
    {
        "Tópico": "FTD",
        "Descrição": (
            "FTD calculado apenas para segmento Ativação (Rafael CRM). "
            "Ativação segmenta usuários que nunca depositaram → "
            "qualquer depósito no período = FTD. Demais segmentos: FTD = 0."
        ),
    },
    {
        "Tópico": "NGR",
        "Descrição": (
            "NGR = Turnover - (Wins + Bonus Turned Real + Ajustes). "
            "Bonus e ajustes vêm do Redshift via bridge ecr.tbl_ecr "
            "(c_ecr_id 18dig → c_external_id 15dig = user_ext_id BQ). "
            "Valores Redshift em centavos → ÷100."
        ),
    },
    {
        "Tópico": "Receita por Jogo (estimada)",
        "Descrição": (
            "Wins do BigQuery são por user, não por jogo. "
            "A receita por jogo é estimada proporcionalmente: "
            "receita_jogo = turnover_jogo × (GGR_total / turnover_total)."
        ),
    },
    {
        "Tópico": "Canais",
        "Descrição": (
            "WhatsApp = label_provider_id 1536 (DisparoPro). "
            "Push = 611 (Smartico realtime, trigger: user went online). "
            "In-App = sem provider (popups, Gire e Ganhe, confirmações)."
        ),
    },
    {
        "Tópico": "Segmentos",
        "Descrição": (
            "Ativação (inclui 'sem FTD', 'sem KYC'), Recuperação, Retenção, "
            "Monetização, Pagamento (só 06/03), Urgência (só 08/03), "
            "Gire e Ganhe (07 e 09/03, inclui Confirmação)."
        ),
    },
    {
        "Tópico": "Fontes de dados",
        "Descrição": (
            "Comunicação, depósitos, bets, wins: BigQuery Smartico (dwh_ext_24105). "
            "Bonus turned real, ajustes: Redshift Pragmatic via bridge ecr.tbl_ecr."
        ),
    },
])

# ─── STEP 13: Salvar Excel ──────────────────────────────────────────────────
log.info("STEP 13: Salvando Excel...")

output_path = f"{OUTPUT_DIR}/retem_consolidado_06a10mar.xlsx"
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_resumo.to_excel(writer,     sheet_name="Resumo por Segmento",    index=False)
    df_canal.to_excel(writer,      sheet_name="Conversão por Canal",    index=False)
    df_breakdown.to_excel(writer,  sheet_name="Breakdown por Jogo",     index=False)
    df_diario.to_excel(writer,     sheet_name="Detalhe Diário",         index=False)
    df_nota.to_excel(writer,       sheet_name="Nota Metodológica",      index=False)

log.info(f"Arquivo salvo em: {output_path}")

print("\n" + "=" * 80)
print("RESUMO POR SEGMENTO (CONSOLIDADO)")
print("=" * 80)
print(df_resumo.to_string(index=False))
print("\n" + "=" * 80)
print("CONVERSÃO POR CANAL")
print("=" * 80)
print(df_canal.to_string(index=False))
print(f"\nArquivo Excel: {output_path}")
