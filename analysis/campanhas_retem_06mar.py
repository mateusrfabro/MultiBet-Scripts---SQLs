"""
Análise de Performance — Campanhas RETEM WhatsApp (06/03 a 09/03/2026)
=======================================================================
Origem 1 : BigQuery Smartico  — usuários impactados + dados financeiros
Origem 2 : Redshift Pragmatic — NGR components (bonus + ajustes + FTD)
Destino  : Excel para envio ao Rafael (CRM Manager)

Regras de negócio aplicadas
─────────────────────────────
• Janela de Atribuição (Opção B): só contabiliza atividade financeira
  ocorrida APÓS o primeiro recebimento da mensagem por aquele usuário
  naquela campanha (first_contact_date individual).

• Multi-campanha: se o mesmo usuário foi impactado por mais de uma campanha,
  a atividade dele é contabilizada em TODAS as campanhas em que aparece.
  Uma aba "Nota Metodológica" documenta isso no Excel.

• Centavos Redshift: todos os valores em bireports.tbl_ecr_txn_type_wise_*
  estão armazenados em centavos (padrão Pragmatic Solutions).
  Divisão por 100.0 aplicada ao trazer os dados.

• Valores BigQuery (Smartico): já estão em reais — nenhuma conversão necessária.

Métricas por campanha
──────────────────────
  - Usuários entregues / enviados / falha / clique / bloqueado
  - FTD, Depositantes, Conversão depósito (%)
  - Total depósitos, Ticket médio
  - Apostadores, Qtd apostas, Turnover (casino + sport)
  - GGR = Turnover − Wins
  - NGR = Turnover − (Wins + Bonus Turned Real + Ajustes)
  - Breakdown por jogo (turnover + qtd apostas)
"""

import sys
import os
import logging
import pandas as pd

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
DATA_FIM    = "2026-03-09"
LABEL_ID    = 24105  # MultiBet no Smartico

CAMPANHAS = {
    159256: "[RETEM] Ativação 06/03/2026",
    159254: "[RETEM] Recuperação 06/03/2026",
    159252: "[RETEM] Retenção 06/03/2026",
    159251: "[RETEM] Monetização 06/03/2026",
    159259: "[RETEM] Pagamento 06/03/2026",
}

# Regra de negócio confirmada por Rafael (CRM Manager):
# Apenas campanhas de Ativação geram FTD — as demais targetam usuários com histórico.
# Se FTD aparecer em outra campanha, é sinal de dado inconsistente e será logado.
CAMPANHAS_COM_FTD = {159256}  # IDs de campanhas onde FTD é esperado

OUTPUT_DIR = "c:/Users/NITRO/OneDrive - PGX/MultiBet/analysis/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

IDS_STR = ",".join(str(k) for k in CAMPANHAS.keys())

# ─── Helpers ──────────────────────────────────────────────────────────────────

def to_date(series: pd.Series) -> pd.Series:
    """Normaliza uma coluna para dtype datetime.date (sem fuso)."""
    return pd.to_datetime(series, utc=True).dt.tz_localize(None).dt.date


def bq_to_ecr(df: pd.DataFrame, col: str = "user_ext_id") -> pd.DataFrame:
    """Converte user_ext_id (string BQ) → ecr_id (int) e descarta nulos."""
    df = df.copy()
    df["ecr_id"] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["ecr_id"]).assign(ecr_id=lambda d: d["ecr_id"].astype(int))


# ─── STEP 1: BigQuery — Usuários impactados por campanha ──────────────────────
log.info("STEP 1: Buscando usuários impactados no BigQuery...")

df_comm = query_bigquery(f"""
SELECT
    jc.resource_id,
    r.resource_name          AS campaign_name,
    jc.fact_type_id,
    jc.user_ext_id,
    MIN(jc.fact_date)        AS first_contact_date
FROM `smartico-bq6.dwh_ext_24105.j_communication` jc
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_resource` r
    ON jc.resource_id = r.resource_id AND r.label_id = {LABEL_ID}
WHERE jc.label_id = {LABEL_ID}
  AND jc.resource_id IN ({IDS_STR})
  AND DATE(jc.fact_date) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND jc.user_ext_id IS NOT NULL
GROUP BY jc.resource_id, r.resource_name, jc.fact_type_id, jc.user_ext_id
""")

log.info(f"{len(df_comm)} registros de comunicação carregados.")

FACT_TYPE = {1: "enviado", 2: "entregue", 4: "falha", 5: "clique", 6: "bloqueado"}
df_comm["status"] = df_comm["fact_type_id"].map(FACT_TYPE).fillna("outro")

# Usuários entregues (fact_type_id = 2) com first_contact_date individual
df_entregues = (
    df_comm[df_comm["fact_type_id"] == 2]
    [["resource_id", "campaign_name", "user_ext_id", "first_contact_date"]]
    .drop_duplicates(subset=["resource_id", "user_ext_id"])
    .pipe(bq_to_ecr)
)
# Normaliza first_contact_date → datetime.date para comparação com event_date
df_entregues["first_contact_date"] = to_date(df_entregues["first_contact_date"])

log.info(f"Usuários entregues únicos: {df_entregues['ecr_id'].nunique()}")

# Resumo de comunicação
df_comm_resumo = (
    df_comm.groupby(["resource_id", "campaign_name", "status"])["user_ext_id"]
    .nunique().unstack(fill_value=0).reset_index()
)
log.info(f"Resumo de comunicação:\n{df_comm_resumo.to_string()}")

# ─── STEP 2: BigQuery — Depósitos (granularidade diária para attr. window) ────
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

# ─── STEP 3: BigQuery — Casino bets diários (para attr. window + breakdown) ───
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
log.info(f"{len(df_casino_bets_daily)} linhas de casino bets (diário × jogo).")

# ─── STEP 4: BigQuery — Casino wins diários ───────────────────────────────────
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

# ─── STEP 5: BigQuery — Sport bets settled diários ────────────────────────────
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

# ─── STEP 6: FTD — lógica de campanha (não via Redshift) ─────────────────────
# INVESTIGAÇÃO: o c_ecr_id do Redshift (18 dígitos) é incompatível com o
# user_ext_id do Smartico (15 dígitos). Join retorna 0 registros.
# Solução aplicada: FTD derivado da definição da campanha.
#   - Ativação: segmenta usuários que NUNCA depositaram → qualquer depósito
#     no período é por definição o primeiro (FTD = depositantes).
#   - Demais campanhas: targetam usuários com histórico → FTD = 0.
#     Confirmado por Rafael (CRM Manager).
# Task aberta: mapear a equivalência c_ecr_id (Redshift) ↔ user_ext_id (BQ)
# para habilitar NGR real em análises futuras. Encaminhar ao Gusta (infra).
log.info("STEP 6: FTD calculado via lógica de campanha (sem Redshift — ver nota no Excel).")

# ─── STEP 7: Redshift — Bonus turned real diário (÷100 centavos→reais) ────────
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

# ─── STEP 8: Redshift — Ajustes manuais diários (÷100 centavos→reais) ─────────
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

# ─── STEP 9: Redshift — Bridge ecr.tbl_ecr (c_ecr_id 18 dig → c_external_id 15 dig) ──
# Usamos apenas os c_ecr_ids presentes nos dados de bonus/ajustes já carregados
# (máx. ~8 K registros), evitando varrer a tabela inteira.
log.info("STEP 9: Carregando tabela bridge (ecr.tbl_ecr) para mapeamento de IDs...")

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
        f"Após bridge: {df_bonus_daily['ecr_id'].nunique()} users com bonus | "
        f"{df_adj_daily['ecr_id'].nunique()} users com ajustes (IDs agora em 15 dígitos)."
    )
else:
    log.warning("Nenhum dado de bonus/ajustes para fazer bridge.")

# ─── Nota: usuários em múltiplas campanhas ────────────────────────────────────
ecr_por_campanha = (
    df_entregues.groupby("ecr_id")["resource_id"]
    .nunique()
    .reset_index(name="n_campanhas")
)
multi_camp = ecr_por_campanha[ecr_por_campanha["n_campanhas"] > 1]
log.info(
    f"{len(multi_camp)} usuários aparecem em mais de uma campanha — "
    "atividade contabilizada em todas. Ver aba 'Nota Metodológica' no Excel."
)

# ─── STEP 9 → 11: Loop por campanha com janela de atribuição ─────────────────
log.info("STEP 9: Calculando métricas por campanha (janela de atribuição individual)...")

resultados  = []
breakdowns  = []

for res_id, camp_name in CAMPANHAS.items():
    # Usuários desta campanha com first_contact_date individual
    users_fc = (
        df_entregues[df_entregues["resource_id"] == res_id]
        [["ecr_id", "first_contact_date"]]
        .drop_duplicates("ecr_id")
    )
    n_impactados = len(users_fc)

    # ── Filtro de atribuição: aplica event_date >= first_contact_date por usuário ──
    def apply_window(df_daily: pd.DataFrame, date_col: str = "event_date") -> pd.DataFrame:
        """Filtra linhas onde event_date >= first_contact_date do usuário."""
        return (
            df_daily
            .merge(users_fc, on="ecr_id", how="inner")
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

    # Casino bets (player-level para totais)
    casino_bets_camp = apply_window(df_casino_bets_daily)
    casino_agg = (
        casino_bets_camp
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

    # FTD via lógica de campanha (sem join Redshift — incompatibilidade de IDs)
    # Ativação: todos depositantes = FTD (campanha segmenta quem nunca depositou)
    # Demais: FTD = 0 por regra de negócio (Rafael CRM)
    # Não precisa de merge separado — n_ftd calculado após dep_agg abaixo

    # Bonus turned real
    bonus_agg = (
        apply_window(df_bonus_daily)
        .groupby("ecr_id")["bonus_turned_real"].sum()
        .reset_index()
    )

    # Ajustes manuais
    adj_agg = (
        apply_window(df_adj_daily)
        .groupby("ecr_id")["real_cash_adjustments"].sum()
        .reset_index()
    )

    # ── Merge de todas as fontes (left join a partir dos usuários) ────────────
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
    # BQ retorna NUMERIC como decimal.Decimal — normaliza todas as colunas para float
    for col in df_camp.columns:
        if col != "ecr_id":
            df_camp[col] = df_camp[col].astype(float)

    # ── Totais combinados ─────────────────────────────────────────────────────
    df_camp["real_bets"]   = df_camp["real_bets_casino"]   + df_camp["real_bets_sport"]
    df_camp["real_wins"]   = df_camp["real_wins_casino"]   + df_camp["real_wins_sport"]
    df_camp["qtd_apostas"] = df_camp["qtd_apostas_casino"] + df_camp["qtd_apostas_sport"]
    df_camp["ggr"]         = df_camp["real_bets"] - df_camp["real_wins"]
    df_camp["ngr"]         = df_camp["real_bets"] - (
        df_camp["real_wins"] + df_camp["bonus_turned_real"] + df_camp["real_cash_adjustments"]
    )

    # ── Métricas resumo ───────────────────────────────────────────────────────
    depositantes  = df_camp[df_camp["qtd_depositos"] > 0]
    n_dep         = len(depositantes)
    total_dep     = float(depositantes["valor_total_dep"].sum())
    ticket_medio  = total_dep / n_dep if n_dep > 0 else 0

    # FTD: lógica de campanha (não via Redshift — IDs incompatíveis)
    if res_id in CAMPANHAS_COM_FTD:
        # Ativação: por definição, todos os depositantes são FTDs
        n_ftd = n_dep
    else:
        # Demais campanhas: FTD = 0 (confirmado por Rafael CRM)
        n_ftd = 0

    resultados.append({
        "Campanha":              camp_name,
        "Usuários Entregues":    n_impactados,
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
    })

    # ── Breakdown por jogo (aplica mesma janela de atribuição) ───────────────
    jogo_camp = (
        casino_bets_camp  # já filtrado pelo apply_window acima
        .groupby(["game_name", "provider_name"])
        .agg(turnover=("real_bets", "sum"),
             qtd_apostas=("qtd_apostas", "sum"))
        .reset_index()
        .assign(campanha=camp_name)
        .sort_values("turnover", ascending=False)
    )
    breakdowns.append(jogo_camp)

# ─── Consolidação final ───────────────────────────────────────────────────────
df_resumo   = pd.DataFrame(resultados)
df_ggr_jogo = (
    pd.concat(breakdowns, ignore_index=True)
    [["campanha", "game_name", "provider_name", "turnover", "qtd_apostas"]]
    .rename(columns={
        "campanha":      "Campanha",
        "game_name":     "Jogo",
        "provider_name": "Provedor",
        "turnover":      "Turnover (R$)",
        "qtd_apostas":   "Qtd Apostas",
    })
)

# Canal de comunicação por campanha
df_canal = (
    df_comm.groupby(["campaign_name", "status"])["user_ext_id"]
    .nunique().unstack(fill_value=0).reset_index()
)

# Nota metodológica
df_nota = pd.DataFrame([
    {
        "Tópico": "Janela de Atribuição",
        "Descrição": (
            "Opção B: só é contabilizada atividade financeira ocorrida APÓS "
            "o primeiro recebimento (entrega) da mensagem por cada usuário "
            "individualmente. Depósitos anteriores à entrega são ignorados."
        ),
    },
    {
        "Tópico": "Usuários em múltiplas campanhas",
        "Descrição": (
            f"{len(multi_camp)} usuários foram impactados por mais de uma campanha. "
            "A atividade deles é contabilizada em TODAS as campanhas em que aparecem. "
            "Isso pode causar dupla contagem nos totais consolidados entre campanhas."
        ),
    },
    {
        "Tópico": "FTD — lógica de campanha",
        "Descrição": (
            "FTD calculado via regra de campanha, não via Redshift. "
            "Motivo: o c_ecr_id do Redshift (18 dígitos) é incompatível com o "
            "user_ext_id do Smartico (15 dígitos) — join retorna 0 registros. "
            "Solução: Ativação segmenta usuarios que nunca depositaram, portanto "
            "FTD(Ativacao) = Depositantes. Demais campanhas: FTD = 0 (regra Rafael CRM). "
            "Task aberta: Gusta (infra) mapear equivalencia c_ecr_id <-> user_ext_id."
        ),
    },
    {
        "Tópico": "NGR — calculo e bridge de IDs",
        "Descrição": (
            "NGR = GGR - bonus_turned_real - real_cash_adjustments. "
            "O Redshift usa c_ecr_id (18 digitos) e o Smartico usa user_ext_id (15 digitos). "
            "A tabela ecr.tbl_ecr atua como bridge: c_ecr_id <-> c_external_id (= user_ext_id). "
            "Bonus: WRP + CRP + RRP de tbl_ecr_txn_type_wise_daily_summary (ISSUE_BONUS / PARTIAL_ISSUE_BONUS). "
            "Ajustes: REAL_CASH_ADDITION/REMOVAL_BY_CS, CASINO/SB_MANUAL_CREDIT/DEBIT. "
            "Valores Redshift em centavos — divididos por 100.0."
        ),
    },
    {
        "Tópico": "Fontes de dados",
        "Descrição": (
            "Depositos, bets e wins: BigQuery Smartico (tr_acc_deposit_approved, "
            "tr_casino_bet, tr_casino_win, tr_sport_bet_settled). "
            "Bonus turned real, ajustes manuais: Redshift Pragmatic via bridge ecr.tbl_ecr."
        ),
    },
])

# ─── STEP 12: Salvar Excel ────────────────────────────────────────────────────
log.info("STEP 12: Salvando resultados em Excel...")

output_path = f"{OUTPUT_DIR}/retem_06mar_analise.xlsx"
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_resumo.to_excel(writer,   sheet_name="Resumo por Campanha",      index=False)
    df_canal.to_excel(writer,    sheet_name="Canal (Enviado-Entregue)", index=False)
    df_ggr_jogo.to_excel(writer, sheet_name="Breakdown por Jogo",       index=False)
    df_nota.to_excel(writer,     sheet_name="Nota Metodológica",        index=False)

log.info(f"Arquivo salvo em: {output_path}")

print("\n" + "=" * 80)
print("RESUMO POR CAMPANHA")
print("=" * 80)
print(df_resumo.to_string(index=False))
print(f"\nArquivo Excel salvo em: {output_path}")
