"""
Extracao v3: Cruzamento completo campanhas reais -> bonus -> financeiro.

STEP 1: BigQuery — Funil CRM + Bonus + Custo por automation_rule_id x dia
STEP 2: BigQuery — Coorte user_ext_ids por rule_id
STEP 3: Athena  — Financeiro da coorte (em batches)
STEP 4: Python  — Agregar financeiro por rule_id e merge

Resultado: campanhas_v3.csv (1 linha por rule_id x dia, com tudo)
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import logging
from db.bigquery import query_bigquery
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DS = "smartico-bq6.dwh_ext_24105"
DATE_FROM = "2026-03-01"
DATE_TO = "2026-03-30"
OUT = os.path.join(os.path.dirname(__file__), "..", "data", "crm_csvs")

CANAL_MAP = {50: "popup", 60: "SMS", 64: "WhatsApp", 30: "push", 40: "push", 31: "inbox"}

def classify_rule(name):
    if not name: return "outro"
    n = name.upper()
    if "RETENCAO" in n or "RETEM" in n or "DEP+APOST" in n: return "RETEM"
    if "DAILYFS" in n or "DAILY_FS" in n: return "DailyFS"
    if "CASHBACK" in n: return "Cashback"
    if "CHALLENGE" in n or "DESAFIO" in n or "PGS" in n: return "Challenge"
    if "LIFECYCLE" in n or "KLC" in n or "KYBORG" in n: return "Lifecycle"
    if "SPORTSBOOK" in n: return "CrossSell_Sports"
    if "LIMPAR" in n or "LIMPEZA" in n or "RESET" in n or "RETIRAR" in n or "UNMARK" in n: return "Sistema"
    return "outro"


# ================================================================
# STEP 1: Funil CRM + Bonus por rule_id x dia (BigQuery)
# ================================================================
log.info("STEP 1: Funil CRM + Bonus por campanha (BigQuery)...")
sql1 = f"""
WITH
rule_users AS (
    SELECT
        automation_rule_id AS rule_id,
        DATE(dt_executed, 'America/Sao_Paulo') AS report_date,
        user_ext_id
    FROM `{DS}.j_automation_rule_progress`
    WHERE DATE(dt_executed, 'America/Sao_Paulo') BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
      AND user_ext_id IS NOT NULL
    GROUP BY automation_rule_id, DATE(dt_executed, 'America/Sao_Paulo'), user_ext_id
),
bonus_por_user AS (
    SELECT
        user_ext_id,
        DATE(fact_date, 'America/Sao_Paulo') AS report_date,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 1 THEN bonus_id END) AS bonus_oferecidos,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 3 THEN bonus_id END) AS bonus_completados,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 4 THEN bonus_id END) AS bonus_expirados,
        SUM(CASE WHEN bonus_status_id = 3 THEN CAST(bonus_cost_value AS FLOAT64) ELSE 0 END) AS custo_bonus_brl
    FROM `{DS}.j_bonuses`
    WHERE DATE(fact_date, 'America/Sao_Paulo') BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
      AND user_ext_id IS NOT NULL
    GROUP BY user_ext_id, DATE(fact_date, 'America/Sao_Paulo')
)
SELECT
    ru.report_date,
    ru.rule_id,
    r.rule_name,
    r.is_active,
    CASE r.activity_type_id
        WHEN 50 THEN 'popup' WHEN 60 THEN 'SMS' WHEN 64 THEN 'WhatsApp'
        WHEN 30 THEN 'push' WHEN 40 THEN 'push' WHEN 31 THEN 'inbox'
        ELSE 'outro'
    END AS channel,
    COUNT(DISTINCT ru.user_ext_id) AS users_impactados,
    COALESCE(SUM(b.bonus_oferecidos), 0) AS bonus_oferecidos,
    COALESCE(SUM(b.bonus_completados), 0) AS bonus_completados,
    COALESCE(SUM(b.bonus_expirados), 0) AS bonus_expirados,
    ROUND(COALESCE(SUM(b.custo_bonus_brl), 0), 2) AS custo_bonus_brl
FROM rule_users ru
LEFT JOIN `{DS}.dm_automation_rule` r ON r.rule_id = ru.rule_id
LEFT JOIN bonus_por_user b ON b.user_ext_id = ru.user_ext_id AND b.report_date = ru.report_date
GROUP BY ru.report_date, ru.rule_id, r.rule_name, r.is_active, r.activity_type_id
ORDER BY ru.report_date, users_impactados DESC
"""
df_crm = query_bigquery(sql1)
log.info(f"  {len(df_crm)} registros (rule x dia)")
log.info(f"  Campanhas: {df_crm['rule_id'].nunique()}")

# Classificar e filtrar sistema
df_crm["campaign_type"] = df_crm["rule_name"].apply(classify_rule)
df_crm = df_crm[df_crm["campaign_type"] != "Sistema"]
log.info(f"  Campanhas reais (sem Sistema): {df_crm['rule_id'].nunique()}")


# ================================================================
# STEP 2: Coorte user_ext_ids por rule_id (BigQuery)
# ================================================================
log.info("STEP 2: Coorte users por campanha...")
sql2 = f"""
SELECT
    automation_rule_id AS rule_id,
    CAST(user_ext_id AS STRING) AS user_ext_id
FROM `{DS}.j_automation_rule_progress`
WHERE DATE(dt_executed, 'America/Sao_Paulo') BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
  AND user_ext_id IS NOT NULL
GROUP BY automation_rule_id, user_ext_id
"""
df_coorte = query_bigquery(sql2)
log.info(f"  {len(df_coorte)} registros (rule x user)")
all_users = df_coorte["user_ext_id"].unique().tolist()
log.info(f"  Users unicos: {len(all_users)}")


# ================================================================
# STEP 3: Financeiro da coorte (Athena - batches de 500)
# ================================================================
log.info("STEP 3: Financeiro Athena (batches)...")
user_strs = [str(u) for u in all_users]
all_fin = []
total_batches = (len(user_strs) - 1) // 500 + 1

for i in range(0, len(user_strs), 500):
    batch = user_strs[i:i + 500]
    ids = ",".join(batch)
    sql = f"""SELECT du.external_id AS user_ext_id,
        p.activity_date AS report_date,
        SUM(p.ggr_base) AS ggr_brl,
        SUM(p.ngr_base) AS ngr_brl,
        SUM(p.bet_amount_base) AS turnover_brl,
        SUM(p.deposit_success_base) AS depositos_brl,
        SUM(p.cashout_success_base) AS saques_brl,
        SUM(p.login_count) AS sessoes
    FROM ps_bi.dim_user du
    JOIN ps_bi.fct_player_activity_daily p ON p.player_id = du.ecr_id
    WHERE du.external_id IN ({ids})
      AND du.is_test = false
      AND p.activity_date BETWEEN DATE '{DATE_FROM}' AND DATE '{DATE_TO}'
    GROUP BY du.external_id, p.activity_date"""
    try:
        df_b = query_athena(sql, database="ps_bi")
        all_fin.append(df_b)
        bn = i // 500 + 1
        if bn % 20 == 0 or bn == total_batches:
            log.info(f"  Batch {bn}/{total_batches}: {len(df_b)}")
    except Exception as e:
        log.warning(f"  Batch {i // 500 + 1} falhou: {e}")

df_fin = pd.concat(all_fin, ignore_index=True) if all_fin else pd.DataFrame()
if not df_fin.empty:
    for c in ["ggr_brl", "ngr_brl", "turnover_brl", "depositos_brl", "saques_brl", "sessoes"]:
        df_fin[c] = pd.to_numeric(df_fin[c], errors="coerce").fillna(0)
    df_fin["user_ext_id"] = df_fin["user_ext_id"].astype(str)
    df_fin["net_deposit_brl"] = df_fin["depositos_brl"] - df_fin["saques_brl"]
log.info(f"  Financeiro: {len(df_fin)} registros")


# ================================================================
# STEP 4: Agregar financeiro por rule_id
# ================================================================
log.info("STEP 4: Cruzando campanha -> financeiro...")

# User financeiro agregado por mes (para evitar double counting diario)
user_fin = df_fin.groupby("user_ext_id")[["ggr_brl", "ngr_brl", "turnover_brl",
    "depositos_brl", "saques_brl", "net_deposit_brl", "sessoes"]].sum().reset_index()

# Cruzar coorte (rule -> users) com financeiro
df_coorte["user_ext_id"] = df_coorte["user_ext_id"].astype(str)
df_coorte["rule_id"] = df_coorte["rule_id"].astype(str)
merged = df_coorte.merge(user_fin, on="user_ext_id", how="left").fillna(0)

# Agregar por rule_id
rule_fin = merged.groupby("rule_id").agg(
    fin_users=("user_ext_id", "nunique"),
    ggr_brl=("ggr_brl", "sum"),
    ngr_brl=("ngr_brl", "sum"),
    turnover_brl=("turnover_brl", "sum"),
    depositos_brl=("depositos_brl", "sum"),
    saques_brl=("saques_brl", "sum"),
    net_deposit_brl=("net_deposit_brl", "sum"),
).reset_index()
log.info(f"  Financeiro por campanha: {len(rule_fin)}")


# ================================================================
# STEP 5: Resumo mensal (merge CRM + financeiro)
# ================================================================
log.info("STEP 5: Merge final...")

# Agregar CRM por rule
crm_agg = df_crm.groupby(["rule_id", "rule_name", "campaign_type", "is_active", "channel"]).agg(
    dias_ativa=("report_date", "nunique"),
    users_total=("users_impactados", "sum"),
    bonus_oferecidos=("bonus_oferecidos", "sum"),
    bonus_completados=("bonus_completados", "sum"),
    bonus_expirados=("bonus_expirados", "sum"),
    custo_bonus_brl=("custo_bonus_brl", "sum"),
).reset_index()
crm_agg["rule_id"] = crm_agg["rule_id"].astype(str)
crm_agg["status"] = crm_agg["is_active"].apply(lambda x: "ativa" if x else "inativa")

# Merge com financeiro
final = crm_agg.merge(rule_fin, on="rule_id", how="left").fillna(0)

# Calcular ROI e CPA
final["custo_crm_total"] = final["custo_bonus_brl"]  # + disparos se disponivel
final["roi"] = final.apply(
    lambda r: round(r["ggr_brl"] / r["custo_crm_total"], 1) if r["custo_crm_total"] > 0 else 0, axis=1)
final["cpa"] = final.apply(
    lambda r: round(r["custo_crm_total"] / r["fin_users"], 2) if r["fin_users"] > 0 else 0, axis=1)

# Salvar
final = final.sort_values("ggr_brl", ascending=False)
path = os.path.join(OUT, "campanhas_v3.csv")
final.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
log.info(f"Salvo: {path}")

# Salvar diario tambem
df_crm["rule_id"] = df_crm["rule_id"].astype(str)
path2 = os.path.join(OUT, "campanhas_v3_diario.csv")
df_crm.to_csv(path2, index=False, sep=";", encoding="utf-8-sig")
log.info(f"Salvo: {path2}")

# Mostrar top 10
log.info("\n=== TOP 10 CAMPANHAS v3 ===")
for _, r in final.head(10).iterrows():
    log.info(f"  [{r['campaign_type']:12s}] {str(r['rule_name'])[:45]:45s} | "
             f"Users: {int(r['users_total']):>10,} | "
             f"GGR: R$ {r['ggr_brl']:>12,.0f} | "
             f"Custo: R$ {r['custo_bonus_brl']:>10,.0f} | "
             f"ROI: {r['roi']}x | {r['status']}")

log.info("\nDONE")
