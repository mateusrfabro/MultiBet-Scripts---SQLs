"""
Validação cruzada: totais da plataforma vs totais das campanhas RETEM.
Compara depósitos, bets e wins da plataforma inteira com os números
atribuídos às campanhas no período 06-10/03/2026.
"""
import sys
import pandas as pd

sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.bigquery import query_bigquery

LABEL_ID = 24105
DATA_INICIO = "2026-03-06"
DATA_FIM = "2026-03-10"

print("=" * 70)
print("VALIDAÇÃO CRUZADA — Plataforma vs Campanhas RETEM")
print(f"Período: {DATA_INICIO} a {DATA_FIM}")
print("=" * 70)

# ─── 1. Total de depósitos da PLATAFORMA inteira ─────────────────────────────
print("\n1. DEPÓSITOS — Plataforma inteira...")
df_plat_dep = query_bigquery(f"""
SELECT
    COUNT(DISTINCT u.user_ext_id) AS total_depositantes,
    COUNT(*) AS total_transacoes,
    SUM(d.acc_last_deposit_amount) AS total_valor
FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` d
JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON d.user_id = u.user_id
WHERE d.label_id = {LABEL_ID}
  AND DATE(d.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND (d.acc_is_rollback IS NULL OR d.acc_is_rollback = FALSE)
""")
print(f"   Depositantes: {df_plat_dep['total_depositantes'].iloc[0]:,}")
print(f"   Transações:   {df_plat_dep['total_transacoes'].iloc[0]:,}")
print(f"   Valor total:  R$ {float(df_plat_dep['total_valor'].iloc[0]):,.2f}")

# ─── 2. Total de casino bets da PLATAFORMA inteira ───────────────────────────
print("\n2. CASINO BETS — Plataforma inteira...")
df_plat_bets = query_bigquery(f"""
SELECT
    COUNT(DISTINCT u.user_ext_id) AS total_apostadores,
    COUNT(*) AS total_apostas,
    SUM(b.casino_last_bet_amount_real) AS total_turnover
FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON b.user_id = u.user_id
WHERE b.label_id = {LABEL_ID}
  AND DATE(b.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND (b.casino_is_rollback IS NULL OR b.casino_is_rollback = FALSE)
  AND (b.casino_is_free_bet IS NULL OR b.casino_is_free_bet = FALSE)
""")
print(f"   Apostadores:  {df_plat_bets['total_apostadores'].iloc[0]:,}")
print(f"   Apostas:      {df_plat_bets['total_apostas'].iloc[0]:,}")
print(f"   Turnover:     R$ {float(df_plat_bets['total_turnover'].iloc[0]):,.2f}")

# ─── 3. Total de casino wins da PLATAFORMA inteira ───────────────────────────
print("\n3. CASINO WINS — Plataforma inteira...")
df_plat_wins = query_bigquery(f"""
SELECT
    SUM(w.casino_last_win_amount_real) AS total_wins
FROM `smartico-bq6.dwh_ext_24105.tr_casino_win` w
WHERE w.label_id = {LABEL_ID}
  AND DATE(w.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND (w.casino_is_rollback IS NULL OR w.casino_is_rollback = FALSE)
""")
print(f"   Wins:         R$ {float(df_plat_wins['total_wins'].iloc[0]):,.2f}")

# ─── 4. Ler totais das campanhas do Excel ─────────────────────────────────────
print("\n4. TOTAIS DAS CAMPANHAS (do Excel gerado)...")
df_resumo = pd.read_excel(
    "analysis/output/retem_consolidado_06a10mar.xlsx",
    sheet_name="Resumo por Segmento"
)

camp_dep_total = df_resumo["Total Depósitos (R$)"].sum()
camp_depositantes = df_resumo["Depositantes"].sum()
camp_turnover = df_resumo["Turnover (R$)"].sum()
camp_apostadores = df_resumo["Apostadores"].sum()
camp_ggr = df_resumo["GGR (R$)"].sum()

print(f"   Depositantes: {camp_depositantes:,.0f} (soma dos segmentos, com overlap)")
print(f"   Depósitos:    R$ {camp_dep_total:,.2f}")
print(f"   Apostadores:  {camp_apostadores:,.0f} (soma dos segmentos, com overlap)")
print(f"   Turnover:     R$ {camp_turnover:,.2f}")
print(f"   GGR:          R$ {camp_ggr:,.2f}")

# ─── 5. Comparação ────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("COMPARAÇÃO")
print("=" * 70)

plat_dep_val = float(df_plat_dep["total_valor"].iloc[0])
plat_dep_n = int(df_plat_dep["total_depositantes"].iloc[0])
plat_turn = float(df_plat_bets["total_turnover"].iloc[0])
plat_apost = int(df_plat_bets["total_apostadores"].iloc[0])
plat_wins = float(df_plat_wins["total_wins"].iloc[0])
plat_ggr = plat_turn - plat_wins

print(f"\n{'Métrica':<25} {'Plataforma':>18} {'Campanhas':>18} {'Camp/Plat %':>12}")
print("-" * 75)
print(f"{'Depositantes':<25} {plat_dep_n:>18,} {camp_depositantes:>18,.0f} {camp_depositantes/plat_dep_n*100:>11.1f}%")
print(f"{'Depósitos (R$)':<25} {plat_dep_val:>18,.2f} {camp_dep_total:>18,.2f} {camp_dep_total/plat_dep_val*100:>11.1f}%")
print(f"{'Apostadores':<25} {plat_apost:>18,} {camp_apostadores:>18,.0f} {camp_apostadores/plat_apost*100:>11.1f}%")
print(f"{'Turnover (R$)':<25} {plat_turn:>18,.2f} {camp_turnover:>18,.2f} {camp_turnover/plat_turn*100:>11.1f}%")
print(f"{'GGR (R$)':<25} {plat_ggr:>18,.2f} {camp_ggr:>18,.2f} {camp_ggr/plat_ggr*100:>11.1f}%")

print("\n⚠️  NOTA: 'Campanhas' soma todos os segmentos — tem overlap de users")
print("   entre segmentos, então % acima de 100% é esperado em algumas métricas.")

# ─── 6. Users únicos impactados vs plataforma ────────────────────────────────
print("\n6. USERS ÚNICOS — Campanhas vs Plataforma...")

# Já temos users únicos impactados por todas as campanhas
df_ent = query_bigquery(f"""
SELECT COUNT(DISTINCT jc.user_ext_id) AS users_unicos_camp
FROM `smartico-bq6.dwh_ext_24105.j_communication` jc
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_resource` r
    ON jc.resource_id = r.resource_id AND r.label_id = {LABEL_ID}
WHERE jc.label_id = {LABEL_ID}
  AND DATE(jc.fact_date) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
  AND UPPER(COALESCE(r.resource_name, '')) LIKE '%RETEM%'
  AND jc.fact_type_id = 2
""")

# Total users ativos na plataforma (que fizeram qualquer depósito ou bet)
df_ativos = query_bigquery(f"""
SELECT COUNT(DISTINCT user_ext_id) AS users_ativos
FROM (
    SELECT CAST(u.user_ext_id AS STRING) AS user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.tr_acc_deposit_approved` d
    JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON d.user_id = u.user_id
    WHERE d.label_id = {LABEL_ID}
      AND DATE(d.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
    UNION DISTINCT
    SELECT CAST(u.user_ext_id AS STRING) AS user_ext_id
    FROM `smartico-bq6.dwh_ext_24105.tr_casino_bet` b
    JOIN `smartico-bq6.dwh_ext_24105.j_user` u ON b.user_id = u.user_id
    WHERE b.label_id = {LABEL_ID}
      AND DATE(b.event_time) BETWEEN '{DATA_INICIO}' AND '{DATA_FIM}'
)
""")

users_camp = int(df_ent["users_unicos_camp"].iloc[0])
users_ativos = int(df_ativos["users_ativos"].iloc[0])

print(f"   Users únicos impactados (RETEM):  {users_camp:,}")
print(f"   Users ativos na plataforma:       {users_ativos:,}")
print(f"   Cobertura:                        {users_camp/users_ativos*100:.1f}% dos ativos foram impactados")

print("\n✅ Validação concluída.")
