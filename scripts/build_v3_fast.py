"""
v3 FAST: Monta campanhas_v3.csv cruzando:
  - campanhas_v2.csv (dm_automation_rule, 48 campanhas) — JA EXTRAIDO
  - campanhas_diarias.csv (j_bonuses, bonus) — JA EXTRAIDO
  - financeiro_coorte.csv (Athena, 37K users) — JA EXTRAIDO
  - j_automation_rule_progress (BigQuery, coorte rule->user) — 1 QUERY RAPIDA

Sem ir ao Athena de novo. Tudo em < 2 min.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import logging
from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DS = "smartico-bq6.dwh_ext_24105"
OUT = os.path.join(os.path.dirname(__file__), "..", "data", "crm_csvs")

# 1. Carregar CSVs existentes
log.info("Carregando CSVs existentes...")
df_camps = pd.read_csv(os.path.join(OUT, "campanhas_resumo_v2.csv"), sep=";", encoding="utf-8-sig")
df_fin = pd.read_csv(os.path.join(OUT, "financeiro_coorte.csv"), sep=";", encoding="utf-8-sig")
log.info(f"  Campanhas v2: {len(df_camps)}")
log.info(f"  Financeiro: {len(df_fin)} registros, {df_fin['user_ext_id'].nunique()} users")

# 2. BigQuery: coorte rule_id -> user_ext_id (FILTRADA < 100K users)
log.info("BigQuery: coorte rule->user (filtrada)...")
sql = f"""
WITH sizes AS (
    SELECT automation_rule_id, COUNT(DISTINCT user_ext_id) AS n
    FROM `{DS}.j_automation_rule_progress`
    WHERE DATE(dt_executed, 'America/Sao_Paulo') BETWEEN '2026-03-01' AND '2026-03-30'
    GROUP BY automation_rule_id HAVING COUNT(DISTINCT user_ext_id) < 100000
)
SELECT p.automation_rule_id AS rule_id, CAST(p.user_ext_id AS STRING) AS user_ext_id
FROM `{DS}.j_automation_rule_progress` p
JOIN sizes s ON s.automation_rule_id = p.automation_rule_id
WHERE DATE(p.dt_executed, 'America/Sao_Paulo') BETWEEN '2026-03-01' AND '2026-03-30'
  AND p.user_ext_id IS NOT NULL
GROUP BY p.automation_rule_id, p.user_ext_id
"""
df_coorte = query_bigquery(sql)
df_coorte["rule_id"] = df_coorte["rule_id"].astype(str)
df_coorte["user_ext_id"] = df_coorte["user_ext_id"].astype(str)
log.info(f"  Coorte: {len(df_coorte)} (rule x user), {df_coorte['rule_id'].nunique()} campanhas")

# 3. Agregar financeiro por user (ja temos no CSV)
log.info("Agregando financeiro por user...")
for c in ["ggr_brl", "ngr_brl", "turnover_brl", "depositos_brl", "saques_brl", "net_deposit_brl", "sessoes"]:
    if c in df_fin.columns:
        df_fin[c] = pd.to_numeric(df_fin[c], errors="coerce").fillna(0)
df_fin["user_ext_id"] = df_fin["user_ext_id"].astype(str)

user_fin = df_fin.groupby("user_ext_id").agg(
    ggr_brl=("ggr_brl", "sum"), ngr_brl=("ngr_brl", "sum"),
    turnover_brl=("turnover_brl", "sum"), depositos_brl=("depositos_brl", "sum"),
    saques_brl=("saques_brl", "sum") if "saques_brl" in df_fin.columns else ("depositos_brl", lambda x: 0),
    sessoes=("sessoes", "sum"),
).reset_index()
if "net_deposit_brl" not in user_fin.columns:
    user_fin["net_deposit_brl"] = user_fin["depositos_brl"] - user_fin["saques_brl"]
log.info(f"  {len(user_fin)} users com financeiro")

# 4. Cruzar coorte com financeiro
log.info("Cruzando campanha -> financeiro...")
merged = df_coorte.merge(user_fin, on="user_ext_id", how="inner")
log.info(f"  Match: {len(merged)} (rule x user com financeiro)")

fin_by_rule = merged.groupby("rule_id").agg(
    fin_users=("user_ext_id", "nunique"),
    ggr_brl=("ggr_brl", "sum"), ngr_brl=("ngr_brl", "sum"),
    turnover_brl=("turnover_brl", "sum"), depositos_brl=("depositos_brl", "sum"),
    saques_brl=("saques_brl", "sum"), net_deposit_brl=("net_deposit_brl", "sum"),
    sessoes=("sessoes", "sum"),
).reset_index()

# 5. Merge com campanhas v2
log.info("Merge final...")
df_camps["campaign_id"] = df_camps["campaign_id"].astype(str)
final = df_camps.merge(fin_by_rule.rename(columns={"rule_id": "campaign_id"}),
                       on="campaign_id", how="left").fillna(0)

# ROI
final["roi"] = final.apply(
    lambda r: round(r["ggr_brl"] / max(r.get("custo_bonus_brl", 1), 1), 1) if r.get("ggr_brl", 0) > 0 else 0,
    axis=1)

final = final.sort_values("ggr_brl", ascending=False)
path = os.path.join(OUT, "campanhas_v3.csv")
final.to_csv(path, index=False, sep=";", encoding="utf-8-sig")
log.info(f"Salvo: {path} ({len(final)} campanhas)")

log.info("\n=== TOP 10 ===")
for _, r in final.head(10).iterrows():
    log.info(f"  {str(r.get('campaign_name',''))[:45]:45s} | "
             f"Users:{int(r.get('users_total',0)):>10,} | "
             f"Fin:{int(r.get('fin_users',0)):>6,} | "
             f"GGR:R${r.get('ggr_brl',0):>10,.0f} | "
             f"Dep:R${r.get('depositos_brl',0):>10,.0f} | "
             f"{r.get('campaign_type','')}")
log.info("DONE")
