"""Gera financeiro_por_campanha.csv cruzando coorte BigQuery com financeiro CSV."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from db.bigquery import query_bigquery

DS = "smartico-bq6.dwh_ext_24105"

print("Coorte BigQuery...")
sql = (
    f"SELECT DISTINCT CAST(entity_id AS STRING) AS campaign_id, user_ext_id "
    f"FROM `{DS}.j_bonuses` "
    f"WHERE bonus_status_id = 3 "
    f"AND DATE(fact_date, 'America/Sao_Paulo') BETWEEN '2026-03-01' AND '2026-03-30'"
)
df_coorte = query_bigquery(sql)
df_coorte["user_ext_id"] = df_coorte["user_ext_id"].astype(str)
print(f"  {len(df_coorte)} registros")

print("Financeiro CSV...")
df_fin = pd.read_csv("data/crm_csvs/financeiro_coorte.csv", sep=";", encoding="utf-8-sig")
for c in ["ggr_brl", "turnover_brl", "depositos_brl"]:
    df_fin[c] = pd.to_numeric(df_fin[c], errors="coerce").fillna(0)
df_fin["user_ext_id"] = df_fin["user_ext_id"].astype(str)

user_fin = df_fin.groupby("user_ext_id")[["ggr_brl", "turnover_brl", "depositos_brl"]].sum().reset_index()
print(f"  {len(user_fin)} users")

print("Cruzando...")
camp_fin = df_coorte.merge(user_fin, on="user_ext_id", how="left").fillna(0)
camp_agg = camp_fin.groupby("campaign_id").agg(
    users=("user_ext_id", "nunique"),
    ggr_brl=("ggr_brl", "sum"),
    turnover_brl=("turnover_brl", "sum"),
    depositos_brl=("depositos_brl", "sum"),
).reset_index()

out = "data/crm_csvs/financeiro_por_campanha.csv"
camp_agg.to_csv(out, index=False, sep=";", encoding="utf-8-sig")
print(f"Salvo: {out} ({len(camp_agg)} campanhas)")
top = camp_agg.sort_values("ggr_brl", ascending=False).head(5)
for _, r in top.iterrows():
    print(f"  {r['campaign_id']}: {int(r['users'])} users, GGR R$ {r['ggr_brl']:,.0f}")
print("DONE")
