"""v3 filtrado: só campanhas < 100K users para cruzamento financeiro viável."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import logging
from db.bigquery import query_bigquery
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DS = "smartico-bq6.dwh_ext_24105"
DF = "2026-03-01"
DT = "2026-03-30"
OUT = os.path.join(os.path.dirname(__file__), "..", "data", "crm_csvs")

def classify(name):
    if not name: return "outro"
    n = name.upper()
    if "RETENCAO" in n or "DEP+APOST" in n: return "RETEM"
    if "DAILYFS" in n or "DAILY_FS" in n: return "DailyFS"
    if "CASHBACK" in n: return "Cashback"
    if "CHALLENGE" in n or "DESAFIO" in n or "PGS" in n: return "Challenge"
    if "LIFECYCLE" in n or "KLC" in n or "KYBORG" in n: return "Lifecycle"
    if "SPORTSBOOK" in n: return "CrossSell"
    if "LIMPAR" in n or "RESET" in n or "RETIRAR" in n or "UNMARK" in n: return "Sistema"
    return "outro"

# STEP 1: Campanhas + coorte FILTRADA (< 100K users, sem Sistema/DailyFS massivo)
log.info("STEP 1: Coorte filtrada (< 100K users)...")
sql = f"""
WITH rule_sizes AS (
    SELECT automation_rule_id, COUNT(DISTINCT user_ext_id) AS total_users
    FROM `{DS}.j_automation_rule_progress`
    WHERE DATE(dt_executed, 'America/Sao_Paulo') BETWEEN '{DF}' AND '{DT}'
    GROUP BY automation_rule_id
    HAVING COUNT(DISTINCT user_ext_id) < 100000
)
SELECT
    p.automation_rule_id AS rule_id,
    r.rule_name,
    r.is_active,
    CASE r.activity_type_id
        WHEN 50 THEN 'popup' WHEN 60 THEN 'SMS' WHEN 64 THEN 'WhatsApp'
        WHEN 30 THEN 'push' ELSE 'outro' END AS channel,
    CAST(p.user_ext_id AS STRING) AS user_ext_id,
    DATE(p.dt_executed, 'America/Sao_Paulo') AS report_date
FROM `{DS}.j_automation_rule_progress` p
JOIN rule_sizes rs ON rs.automation_rule_id = p.automation_rule_id
LEFT JOIN `{DS}.dm_automation_rule` r ON r.rule_id = p.automation_rule_id
WHERE DATE(p.dt_executed, 'America/Sao_Paulo') BETWEEN '{DF}' AND '{DT}'
  AND p.user_ext_id IS NOT NULL
"""
df_raw = query_bigquery(sql)
log.info(f"  {len(df_raw)} registros brutos")
df_raw["campaign_type"] = df_raw["rule_name"].apply(classify)
df_raw = df_raw[~df_raw["campaign_type"].isin(["Sistema"])]
log.info(f"  Campanhas: {df_raw['rule_id'].nunique()}")
all_users = df_raw["user_ext_id"].unique().tolist()
log.info(f"  Users unicos: {len(all_users)}")

# STEP 2: Bonus por user (BigQuery) — para os users da coorte filtrada
log.info("STEP 2: Bonus por user...")
# Quebrar em chunks de 50K users para BigQuery
user_strs = [str(u) for u in all_users]
all_bonus = []
chunk = 50000
for i in range(0, len(user_strs), chunk):
    batch = user_strs[i:i+chunk]
    ids = ",".join(f"'{u}'" for u in batch)
    sql_b = f"""
    SELECT user_ext_id,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 1 THEN bonus_id END) AS bonus_oferecidos,
        COUNT(DISTINCT CASE WHEN bonus_status_id = 3 THEN bonus_id END) AS bonus_completados,
        SUM(CASE WHEN bonus_status_id = 3 THEN CAST(bonus_cost_value AS FLOAT64) ELSE 0 END) AS custo_bonus_brl
    FROM `{DS}.j_bonuses`
    WHERE DATE(fact_date, 'America/Sao_Paulo') BETWEEN '{DF}' AND '{DT}'
      AND user_ext_id IN ({ids})
    GROUP BY user_ext_id
    """
    df_b = query_bigquery(sql_b)
    all_bonus.append(df_b)
    log.info(f"  Bonus batch {i//chunk+1}: {len(df_b)} users")

df_bonus = pd.concat(all_bonus, ignore_index=True) if all_bonus else pd.DataFrame()
if not df_bonus.empty:
    df_bonus["user_ext_id"] = df_bonus["user_ext_id"].astype(str)
    for c in ["bonus_oferecidos", "bonus_completados", "custo_bonus_brl"]:
        df_bonus[c] = pd.to_numeric(df_bonus[c], errors="coerce").fillna(0)
log.info(f"  Bonus: {len(df_bonus)} users com dados")

# STEP 3: Financeiro (Athena) — batches de 500
log.info("STEP 3: Financeiro Athena...")
all_fin = []
tb = (len(user_strs) - 1) // 500 + 1
for i in range(0, len(user_strs), 500):
    batch = user_strs[i:i+500]
    ids = ",".join(batch)
    sql_f = f"""SELECT du.external_id AS user_ext_id,
        SUM(p.ggr_base) AS ggr, SUM(p.ngr_base) AS ngr,
        SUM(p.bet_amount_base) AS turnover,
        SUM(p.deposit_success_base) AS dep, SUM(p.cashout_success_base) AS saq,
        SUM(p.login_count) AS sess
    FROM ps_bi.dim_user du
    JOIN ps_bi.fct_player_activity_daily p ON p.player_id = du.ecr_id
    WHERE du.external_id IN ({ids}) AND du.is_test = false
      AND p.activity_date BETWEEN DATE '{DF}' AND DATE '{DT}'
    GROUP BY du.external_id"""
    try:
        df_f = query_athena(sql_f, database="ps_bi")
        all_fin.append(df_f)
        bn = i // 500 + 1
        if bn % 10 == 0 or bn == tb:
            log.info(f"  Athena {bn}/{tb}: {len(df_f)}")
    except Exception as e:
        log.warning(f"  Athena {i//500+1} falhou: {e}")

df_fin = pd.concat(all_fin, ignore_index=True) if all_fin else pd.DataFrame()
if not df_fin.empty:
    df_fin["user_ext_id"] = df_fin["user_ext_id"].astype(str)
    for c in ["ggr", "ngr", "turnover", "dep", "saq", "sess"]:
        df_fin[c] = pd.to_numeric(df_fin[c], errors="coerce").fillna(0)
    df_fin["net_dep"] = df_fin["dep"] - df_fin["saq"]
log.info(f"  Financeiro: {len(df_fin)} users")

# STEP 4: Agregar por rule_id
log.info("STEP 4: Merge...")
df_raw["user_ext_id"] = df_raw["user_ext_id"].astype(str)
df_raw["rule_id"] = df_raw["rule_id"].astype(str)

# Agregar campanhas por rule
camp_agg = df_raw.groupby(["rule_id", "rule_name", "campaign_type", "is_active", "channel"]).agg(
    dias=("report_date", "nunique"),
    users_total=("user_ext_id", "nunique"),
).reset_index()
camp_agg["status"] = camp_agg["is_active"].apply(lambda x: "ativa" if x else "inativa")

# Coorte por rule
coorte = df_raw[["rule_id", "user_ext_id"]].drop_duplicates()

# Merge bonus
if not df_bonus.empty:
    coorte_b = coorte.merge(df_bonus, on="user_ext_id", how="left").fillna(0)
    bonus_agg = coorte_b.groupby("rule_id")[["bonus_oferecidos", "bonus_completados", "custo_bonus_brl"]].sum().reset_index()
else:
    bonus_agg = pd.DataFrame({"rule_id": camp_agg["rule_id"], "bonus_oferecidos": 0, "bonus_completados": 0, "custo_bonus_brl": 0})

# Merge financeiro
if not df_fin.empty:
    coorte_f = coorte.merge(df_fin, on="user_ext_id", how="left").fillna(0)
    fin_agg = coorte_f.groupby("rule_id")[["ggr", "ngr", "turnover", "dep", "saq", "net_dep", "sess"]].sum().reset_index()
    fin_agg.columns = ["rule_id", "ggr_brl", "ngr_brl", "turnover_brl", "depositos_brl", "saques_brl", "net_deposit_brl", "sessoes"]
else:
    fin_agg = pd.DataFrame({"rule_id": camp_agg["rule_id"]})

# Merge tudo
final = camp_agg.merge(bonus_agg, on="rule_id", how="left").merge(fin_agg, on="rule_id", how="left").fillna(0)
final["roi"] = final.apply(lambda r: round(r.get("ggr_brl",0) / r["custo_bonus_brl"], 1) if r["custo_bonus_brl"] > 0 else 0, axis=1)

# Salvar
final = final.sort_values("ggr_brl", ascending=False)
path = os.path.join(OUT, "campanhas_v3.csv")
final.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
log.info(f"Salvo: {path} ({len(final)} campanhas)")

log.info("\n=== TOP 10 ===")
for _, r in final.head(10).iterrows():
    log.info(f"  [{r['campaign_type']:10s}] {str(r['rule_name'])[:40]:40s} | "
             f"Users:{int(r['users_total']):>6,} | GGR:R${r.get('ggr_brl',0):>10,.0f} | "
             f"Custo:R${r['custo_bonus_brl']:>8,.0f} | ROI:{r['roi']}x | {r['status']}")
log.info("DONE")
