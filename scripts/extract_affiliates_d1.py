"""
Extração D-1 (24/03/2026) — 3 Afiliados consolidado
Modelo: Saques, REG, FTD, FTD Deposit, Dep Amount, GGR Cassino, GGR Sport, NGR
"""
import sys, warnings
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.athena import query_athena
from db.bigquery import query_bigquery
warnings.filterwarnings("ignore")

DATA = "2026-03-24"
IDS = "532570", "532571", "464673"
ids_sql = ",".join([f"'{x}'" for x in IDS])

# KPIs do dia (fct_player_activity_daily)
sql_kpis = f"""
WITH p AS (
    SELECT ecr_id FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ({ids_sql})
      AND (is_test = false OR is_test IS NULL)
)
SELECT
    COALESCE(SUM(a.cashout_success_base), 0) AS saques,
    COALESCE(SUM(a.deposit_success_base), 0) AS dep_amount,
    COALESCE(SUM(a.casino_realbet_base) - SUM(a.casino_real_win_base), 0) AS ggr_casino,
    COALESCE(SUM(a.sb_realbet_base) - SUM(a.sb_real_win_base), 0) AS ggr_sport,
    COALESCE(SUM(a.ngr_base), 0) AS ngr
FROM ps_bi.fct_player_activity_daily a
INNER JOIN p ON a.player_id = p.ecr_id
WHERE a.activity_date = DATE '{DATA}'
"""

# REG e FTD do dia (dim_user com conversao BRT)
sql_reg_ftd = f"""
SELECT
    COUNT(*) AS reg,
    COUNT_IF(CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}') AS ftd,
    SUM(CASE WHEN CAST(ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
        THEN ftd_amount_inhouse ELSE 0 END) AS ftd_deposit
FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) IN ({ids_sql})
  AND (is_test = false OR is_test IS NULL)
  AND CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
"""

print(f"Extraindo KPIs do dia {DATA}...")
df_kpis = query_athena(sql_kpis, database="ps_bi")
print(f"Extraindo REG/FTD do dia {DATA}...")
df_reg = query_athena(sql_reg_ftd, database="ps_bi")

k = df_kpis.iloc[0]
r = df_reg.iloc[0]

def fmt(v, is_brl=True):
    if v is None or str(v) in ("None", "nan", ""): return "R$ 0,00" if is_brl else "0"
    v = float(v)
    if is_brl:
        return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{int(v):,}".replace(",", ".")

print(f"\n{'='*50}")
print(f"Extracao Affiliates — {DATA}")
print(f"IDs: {', '.join(IDS)} (consolidado)")
print(f"{'  Metrica':<18}{'Valor':>18}")
print(f"  {'Saques':<16}{fmt(k['saques']):>18}")
print(f"  {'REG':<16}{fmt(r['reg'], False):>18}")
print(f"  {'FTD':<16}{fmt(r['ftd'], False):>18}")
print(f"  {'FTD Deposit':<16}{fmt(r['ftd_deposit']):>18}")
print(f"  {'Dep Amount':<16}{fmt(k['dep_amount']):>18}")
print(f"  {'GGR Cassino':<16}{fmt(k['ggr_casino']):>18}")
print(f"  {'GGR Sport':<16}{fmt(k['ggr_sport']):>18}")
print(f"  {'NGR':<16}{fmt(k['ngr']):>18}")
print(f"{'='*50}")
print(f"\nFonte: Athena (ps_bi) | Test users excluidos | Timestamps BRT")
print(f"*NGR proxy: GGR - bonus_issued. Formula canonica seria GGR - BTR - RCA.")

# Validacao rapida BigQuery — contagem REG do dia
print(f"\n--- Validacao Cruzada (REG {DATA}) ---")
bq_ids = ",".join(IDS)
df_bq = query_bigquery(f"""
SELECT COUNT(DISTINCT user_ext_id) AS reg_bq
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE core_affiliate_id IN ({bq_ids})
  AND DATE(core_registration_date, "America/Sao_Paulo") = '{DATA}'
""")
bq_reg = int(df_bq["reg_bq"].iloc[0])
a_reg = int(r["reg"])
delta = a_reg - bq_reg
div = abs(delta) / a_reg * 100 if a_reg else 0
print(f"REG Athena={a_reg} | BigQuery={bq_reg} | Delta={delta} ({div:.1f}%) {'OK' if div < 5 else 'ALERTA'}")
