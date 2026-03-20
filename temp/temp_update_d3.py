import sys
sys.path.insert(0, ".")
from db.bigquery import query_bigquery
from db.redshift import query_redshift

print("=" * 60)
print("CAMPANHA MULTIVERSO — ATUALIZAÇÃO D3 (16/03/2026)")
print("=" * 60)

# ========== BIGQUERY ==========

# 1. Funil
sql_funil = """
SELECT
    fact_type_id,
    CASE fact_type_id
        WHEN 1 THEN 'Enviado'
        WHEN 2 THEN 'Entregue'
        WHEN 3 THEN 'Visualizado'
        WHEN 4 THEN 'Clicou'
        WHEN 5 THEN 'Converteu'
    END AS etapa,
    COUNT(DISTINCT user_id) AS usuarios
FROM `smartico-bq6.dwh_ext_24105.j_communication`
WHERE resource_id = 164110
  AND fact_date >= '2026-03-13'
GROUP BY 1, 2
ORDER BY 1
"""
print("\n--- FUNIL (BigQuery) ---")
df_funil = query_bigquery(sql_funil)
print(df_funil.to_string(index=False))

# 2. Participantes
sql_part = """
SELECT COUNT(DISTINCT user_id) AS participantes
FROM `smartico-bq6.dwh_ext_24105.j_automation_rule_progress`
WHERE automation_rule_id IN (
    11547,11548,11549,11550,11551,11552,
    11555,11554,11553,11561,11557,11558,
    11562,11563,11564,11556,11559,11560
)
AND dt_executed >= TIMESTAMP('2026-03-13 20:00:00')
"""
print("\n--- PARTICIPANTES (BigQuery) ---")
df_part = query_bigquery(sql_part)
print(f"  Total: {df_part['participantes'].iloc[0]}")

# 3. Completadores por animal/quest
sql_comp = """
SELECT
    CASE
        WHEN label_bonus_template_id IN (30614,30615,30765) THEN 'Tiger'
        WHEN label_bonus_template_id IN (30363,30364,30083) THEN 'Rabbit'
        WHEN label_bonus_template_id IN (30511,30512,30777) THEN 'Ox'
        WHEN label_bonus_template_id IN (30783,30784,30780) THEN 'Snake'
        WHEN label_bonus_template_id IN (30781,30785,30771) THEN 'Dragon'
        WHEN label_bonus_template_id IN (30787,30786,30774) THEN 'Mouse'
    END AS animal,
    CASE
        WHEN label_bonus_template_id IN (30614,30363,30511,30783,30781,30787) THEN 'Q1 (5 FS)'
        WHEN label_bonus_template_id IN (30615,30364,30512,30784,30785,30786) THEN 'Q2 (15 FS)'
        WHEN label_bonus_template_id IN (30765,30083,30777,30780,30771,30774) THEN 'Q3 (25 FS)'
    END AS quest,
    COUNT(DISTINCT user_id) AS completers,
    COUNT(*) AS entregas
FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
WHERE label_bonus_template_id IN (
    30614,30615,30765,30363,30364,30083,
    30511,30512,30777,30783,30784,30780,
    30781,30785,30771,30787,30786,30774
)
AND redeem_date IS NOT NULL
AND fact_date >= '2026-03-13'
GROUP BY 1, 2
ORDER BY 1, 2
"""
print("\n--- COMPLETADORES POR QUEST (BigQuery) ---")
df_comp = query_bigquery(sql_comp)
print(df_comp.to_string(index=False))

# Totais
sql_comp_total = """
SELECT
    COUNT(DISTINCT user_id) AS completers_unicos,
    COUNT(*) AS total_entregas
FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
WHERE label_bonus_template_id IN (
    30614,30615,30765,30363,30364,30083,
    30511,30512,30777,30783,30784,30780,
    30781,30785,30771,30787,30786,30774
)
AND redeem_date IS NOT NULL
AND fact_date >= '2026-03-13'
"""
df_ct = query_bigquery(sql_comp_total)
print(f"\n  Completers únicos: {df_ct['completers_unicos'].iloc[0]}")
print(f"  Entregas totais:   {df_ct['total_entregas'].iloc[0]}")

# Calcular FS total
sql_fs = """
SELECT
    SUM(CASE
        WHEN label_bonus_template_id IN (30614,30363,30511,30783,30781,30787) THEN 5
        WHEN label_bonus_template_id IN (30615,30364,30512,30784,30785,30786) THEN 15
        WHEN label_bonus_template_id IN (30765,30083,30777,30780,30771,30774) THEN 25
    END) AS total_fs
FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
WHERE label_bonus_template_id IN (
    30614,30615,30765,30363,30364,30083,
    30511,30512,30777,30783,30784,30780,
    30781,30785,30771,30787,30786,30774
)
AND redeem_date IS NOT NULL
AND fact_date >= '2026-03-13'
"""
df_fs = query_bigquery(sql_fs)
print(f"  Free Spins total:  {df_fs['total_fs'].iloc[0]}")

# 4. Bônus duplicados
sql_dup = """
SELECT
    user_id,
    user_ext_id,
    label_bonus_template_id,
    entity_name,
    COUNT(*) AS vezes
FROM `smartico-bq6.dwh_ext_24105.j_bonuses`
WHERE label_bonus_template_id IN (
    30614,30615,30765,30363,30364,30083,
    30511,30512,30777,30783,30784,30780,
    30781,30785,30771,30787,30786,30774
)
AND redeem_date IS NOT NULL
AND fact_date >= '2026-03-13'
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
ORDER BY vezes DESC
"""
print("\n--- BÔNUS DUPLICADOS ---")
df_dup = query_bigquery(sql_dup)
if len(df_dup) > 0:
    print(df_dup.to_string(index=False))
else:
    print("  Nenhum bônus duplicado encontrado")

# ========== REDSHIFT ==========
# Pegar ext_ids dos participantes
sql_ext = """
SELECT DISTINCT user_ext_id
FROM `smartico-bq6.dwh_ext_24105.j_automation_rule_progress`
WHERE automation_rule_id IN (
    11547,11548,11549,11550,11551,11552,
    11555,11554,11553,11561,11557,11558,
    11562,11563,11564,11556,11559,11560
)
AND dt_executed >= TIMESTAMP('2026-03-13 20:00:00')
AND user_ext_id IS NOT NULL
"""
df_ext = query_bigquery(sql_ext)
ext_ids = ','.join(str(x) for x in df_ext['user_ext_id'].tolist())
n_ext = len(df_ext)
print(f"\n--- REDSHIFT: {n_ext} ext_ids para financeiro ---")

# Financeiro Redshift
sql_rs = f"""
WITH params AS (
    SELECT
        '2026-03-13 20:00:00'::TIMESTAMP AS start_utc,
        GETDATE() AS end_utc,
        100.0 AS divisor
),
participantes AS (
    SELECT DISTINCT e.c_ecr_id, e.c_external_id
    FROM ecr.tbl_ecr e
    WHERE e.c_external_id IN ({ext_ids})
),
user_metrics AS (
    SELECT
        f.c_ecr_id,
        SUM(f.c_amount_in_ecr_ccy) FILTER (WHERE f.c_txn_type = 27) AS bet_cents,
        SUM(f.c_amount_in_ecr_ccy) FILTER (WHERE f.c_txn_type = 45) AS win_cents,
        COUNT(*) FILTER (WHERE f.c_txn_type = 27) AS bets_qty
    FROM fund.tbl_real_fund_txn f
    INNER JOIN participantes p ON f.c_ecr_id = p.c_ecr_id
    CROSS JOIN params pr
    WHERE f.c_txn_status = 'SUCCESS'
      AND f.c_game_id IN ('4776','13097','8842','833','2603','18949')
      AND f.c_start_time BETWEEN pr.start_utc AND pr.end_utc
    GROUP BY 1
),
user_bonus AS (
    SELECT bs.c_ecr_id, SUM(bs.c_freespin_win) AS btr_cents
    FROM bonus.tbl_bonus_summary_details bs
    INNER JOIN participantes p ON bs.c_ecr_id = p.c_ecr_id
    CROSS JOIN params pr
    WHERE bs.c_issue_date BETWEEN pr.start_utc AND pr.end_utc
      AND bs.c_freespin_win > 0
    GROUP BY 1
),
user_cashier AS (
    SELECT
        d.c_ecr_id,
        SUM(d.c_credited_amount_in_ecr_ccy) AS dep_cents,
        MAX(CASE WHEN d.c_created_time::DATE = '2026-03-13' THEN 1 ELSE 0 END) AS is_d0,
        MAX(CASE WHEN d.c_created_time::DATE = '2026-03-14' THEN 1 ELSE 0 END) AS is_d1,
        MAX(CASE WHEN d.c_created_time::DATE = '2026-03-15' THEN 1 ELSE 0 END) AS is_d2,
        MAX(CASE WHEN d.c_created_time::DATE = '2026-03-16' THEN 1 ELSE 0 END) AS is_d3
    FROM cashier.tbl_cashier_deposit d
    INNER JOIN participantes p ON d.c_ecr_id = p.c_ecr_id
    CROSS JOIN params pr
    WHERE d.c_txn_status = 'txn_confirmed_success'
      AND d.c_created_time BETWEEN pr.start_utc AND pr.end_utc
    GROUP BY 1
)
SELECT
    COUNT(p.c_ecr_id) AS total_participantes_rs,
    COUNT(m.c_ecr_id) AS com_apostas_rs,
    REPLACE(TO_CHAR(SUM(COALESCE(m.bet_cents,0))/100.0,'FM999G999G990D00'),'.', ',') AS turnover_brl,
    REPLACE(TO_CHAR(SUM(COALESCE(m.win_cents,0))/100.0,'FM999G999G990D00'),'.', ',') AS ganho_brl,
    REPLACE(TO_CHAR((SUM(COALESCE(m.bet_cents,0))-SUM(COALESCE(m.win_cents,0)))/100.0,'FM999G999G990D00'),'.', ',') AS ggr_brl,
    REPLACE(TO_CHAR(SUM(COALESCE(b.btr_cents,0))/100.0,'FM999G999G990D00'),'.', ',') AS btr_brl,
    REPLACE(TO_CHAR((SUM(COALESCE(m.bet_cents,0))-SUM(COALESCE(m.win_cents,0))-SUM(COALESCE(b.btr_cents,0)))/100.0,'FM999G999G990D00'),'.', ',') AS ngr_brl,
    REPLACE(TO_CHAR(SUM(COALESCE(c.dep_cents,0))/100.0,'FM999G999G990D00'),'.', ',') AS cashin_brl,
    SUM(COALESCE(c.is_d0,0)) AS dep_d0,
    SUM(COALESCE(c.is_d1,0)) AS dep_d1,
    SUM(COALESCE(c.is_d2,0)) AS dep_d2,
    SUM(COALESCE(c.is_d3,0)) AS dep_d3,
    SUM(CASE WHEN c.is_d0=1 AND c.is_d1=1 THEN 1 ELSE 0 END) AS ret_d0d1,
    SUM(CASE WHEN c.is_d0=1 AND c.is_d2=1 THEN 1 ELSE 0 END) AS ret_d0d2,
    SUM(CASE WHEN c.is_d0=1 AND c.is_d3=1 THEN 1 ELSE 0 END) AS ret_d0d3,
    ROUND((SUM(COALESCE(m.bet_cents,0))-SUM(COALESCE(m.win_cents,0)))*1.0/NULLIF(SUM(COALESCE(m.bet_cents,0)),0)*100,2) AS hold_rate_pct
FROM participantes p
LEFT JOIN user_metrics m ON p.c_ecr_id = m.c_ecr_id
LEFT JOIN user_bonus b ON p.c_ecr_id = b.c_ecr_id
LEFT JOIN user_cashier c ON p.c_ecr_id = c.c_ecr_id
"""
print("\n--- FINANCEIRO REDSHIFT ---")
df_rs = query_redshift(sql_rs)
for col in df_rs.columns:
    print(f"  {col}: {df_rs[col].iloc[0]}")

print("\n" + "=" * 60)
print("ATUALIZAÇÃO COMPLETA — " + str(df_part['participantes'].iloc[0]) + " participantes")
print("=" * 60)