"""
Extracao v2: Campanhas REAIS (dm_automation_rule) em vez de bonus individuais.

Fonte principal: j_automation_rule_progress + dm_automation_rule
Isso gera ~25-50 campanhas reais vs 1.580 bonus da v1.

CSVs gerados:
  1. campanhas_v2.csv — 1 linha por campanha x dia (automation_rule_id)
  2. campanhas_resumo_v2.csv — 1 linha por campanha (agregado mensal)

Periodo: 01/03/2026 a 30/03/2026 (BRT)
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
import logging
from db.bigquery import query_bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DS = "smartico-bq6.dwh_ext_24105"
DATE_FROM = "2026-03-01"
DATE_TO = "2026-03-30"
OUT = os.path.join(os.path.dirname(__file__), "..", "data", "crm_csvs")


def classify_rule(name):
    if not name:
        return "outro"
    n = name.upper()
    if "RETENCAO" in n or "RETEM" in n or "DEP+APOST" in n:
        return "RETEM"
    if "DAILYFS" in n or "DAILY_FS" in n:
        return "DailyFS"
    if "CASHBACK" in n:
        return "Cashback"
    if "CHALLENGE" in n or "DESAFIO" in n or "PGS" in n or "QUEST" in n:
        return "Challenge"
    if "LIFECYCLE" in n or "KLC" in n or "KYBORG" in n:
        return "Lifecycle"
    if "TORNEIO" in n or "TOURNAMENT" in n:
        return "Torneio"
    if "FREEBET" in n or "DEPOSITE" in n:
        return "Freebet"
    if "SPORTSBOOK" in n:
        return "CrossSell_Sports"
    if "TELEGRAM" in n:
        return "Telegram"
    if "LIMPAR" in n or "LIMPEZA" in n or "RESET" in n or "RETIRAR" in n or "UNMARK" in n:
        return "Sistema"
    return "outro"


# 1. Campanhas por dia (j_automation_rule_progress + dm_automation_rule)
log.info("Extraindo campanhas reais por dia...")
sql = f"""
SELECT
    DATE(p.dt_executed, 'America/Sao_Paulo') AS report_date,
    p.automation_rule_id AS campaign_id,
    r.rule_name AS campaign_name,
    r.is_active,
    COUNT(DISTINCT p.user_ext_id) AS users_impactados,
    COUNT(*) AS events
FROM `{DS}.j_automation_rule_progress` p
LEFT JOIN `{DS}.dm_automation_rule` r ON r.rule_id = p.automation_rule_id
WHERE DATE(p.dt_executed, 'America/Sao_Paulo') BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
GROUP BY report_date, p.automation_rule_id, r.rule_name, r.is_active
ORDER BY report_date, users_impactados DESC
"""
df = query_bigquery(sql)
log.info(f"  {len(df)} registros (campanha x dia)")
log.info(f"  Campanhas unicas: {df['campaign_id'].nunique()}")

# Classificar
df["campaign_type"] = df["campaign_name"].apply(classify_rule)
df["status"] = df["is_active"].apply(lambda x: "ativa" if x else "inativa")

# Filtrar campanhas de sistema (Limpar marcadores, Reset, etc.)
df_sistema = df[df["campaign_type"] == "Sistema"]
df_real = df[df["campaign_type"] != "Sistema"]
log.info(f"  Campanhas de sistema (excluidas): {df_sistema['campaign_id'].nunique()}")
log.info(f"  Campanhas reais: {df_real['campaign_id'].nunique()}")

# Salvar diario
path1 = os.path.join(OUT, "campanhas_v2.csv")
df_real.to_csv(path1, index=False, sep=";", encoding="utf-8-sig")
log.info(f"  Salvo: {path1}")

# 2. Resumo mensal por campanha
log.info("Gerando resumo mensal...")
resumo = df_real.groupby(["campaign_id", "campaign_name", "campaign_type", "status"]).agg(
    dias_ativa=("report_date", "nunique"),
    users_total=("users_impactados", "sum"),
    users_unicos_dia_medio=("users_impactados", "mean"),
    events_total=("events", "sum"),
).reset_index().sort_values("users_total", ascending=False)

path2 = os.path.join(OUT, "campanhas_resumo_v2.csv")
resumo.to_csv(path2, index=False, sep=";", encoding="utf-8-sig")
log.info(f"  Salvo: {path2}")

# Mostrar resumo
log.info("\n=== TOP CAMPANHAS REAIS MARCO/2026 ===")
for _, r in resumo.head(15).iterrows():
    log.info(f"  [{r['campaign_type']:12s}] {r['campaign_name'][:50]:50s} | {int(r['users_total']):>10,} users | {int(r['dias_ativa'])} dias | {r['status']}")

log.info(f"\nDistribuicao por tipo:")
tipo_agg = resumo.groupby("campaign_type").agg(
    campanhas=("campaign_id", "count"),
    users=("users_total", "sum"),
).sort_values("users", ascending=False)
for _, r in tipo_agg.iterrows():
    log.info(f"  {r.name:15s}: {int(r['campanhas'])} campanhas, {int(r['users']):>12,} users")

log.info("\nDONE")
