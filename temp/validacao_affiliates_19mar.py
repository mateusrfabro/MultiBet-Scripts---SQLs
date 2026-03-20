"""
Validacao cruzada: Affiliate IDs 297657, 445431, 468114
Data: 19/03/2026
Duplo check: bireports_ec2 vs ps_bi para confirmar cada metrica.
"""
import sys, os, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

AFF_IDS = "('297657', '445431', '468114')"
DATA = "2026-03-19"

print("=" * 70)
print(f"VALIDACAO CRUZADA -- Affiliates 297657, 445431, 468114 -- {DATA}")
print("=" * 70)

# ---------------------------------------------------------------
# CHECK 1: Base de players
# ---------------------------------------------------------------
print("\n[CHECK 1] Base de players -- duas fontes")

df_ps = query_athena(f"""
SELECT COUNT(*) AS total FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) IN {AFF_IDS} AND is_test = false
""", database="ps_bi")

df_bi = query_athena(f"""
SELECT COUNT(*) AS total FROM bireports_ec2.tbl_ecr
WHERE CAST(c_affiliate_id AS VARCHAR) IN {AFF_IDS} AND c_test_user = false
""", database="bireports_ec2")

ps_total = int(df_ps.iloc[0, 0])
bi_total = int(df_bi.iloc[0, 0])
print(f"  ps_bi.dim_user:          {ps_total:>10,}")
print(f"  bireports_ec2.tbl_ecr:   {bi_total:>10,}")
diff1 = abs(ps_total - bi_total)
pct1 = diff1 / max(ps_total, 1) * 100
print(f"  Diferenca:               {diff1:>10,}  ({pct1:.2f}%)  {'OK' if pct1 < 1 else 'DIVERGE'}")

# ---------------------------------------------------------------
# CHECK 2: REG
# ---------------------------------------------------------------
print(f"\n[CHECK 2] REG -- cadastros em {DATA}")

df_reg_ps = query_athena(f"""
SELECT COUNT(*) AS reg FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) IN {AFF_IDS}
  AND is_test = false AND registration_date = DATE '{DATA}'
""", database="ps_bi")

df_reg_bi = query_athena(f"""
SELECT COUNT(*) AS reg FROM bireports_ec2.tbl_ecr
WHERE CAST(c_affiliate_id AS VARCHAR) IN {AFF_IDS}
  AND c_test_user = false
  AND CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{DATA}'
""", database="bireports_ec2")

reg_ps = int(df_reg_ps.iloc[0, 0])
reg_bi = int(df_reg_bi.iloc[0, 0])
print(f"  ps_bi (registration_date):     {reg_ps:>8,}")
print(f"  bireports (sign_up BRT):       {reg_bi:>8,}")
print(f"  Diferenca:                      {abs(reg_ps - reg_bi):>8,}")
if reg_ps != reg_bi:
    print(f"  NOTA: ps_bi.registration_date pode usar UTC; bireports converte BRT")
    print(f"  REG bireports ({reg_bi}) e mais confiavel (conversao BRT explicita)")

# ---------------------------------------------------------------
# CHECK 3: FTD
# ---------------------------------------------------------------
print(f"\n[CHECK 3] FTD -- primeiro deposito em {DATA}")

df_ftd_ps = query_athena(f"""
SELECT COUNT(*) AS ftd, COALESCE(SUM(ftd_amount_inhouse), 0) AS ftd_deposit
FROM ps_bi.dim_user
WHERE CAST(affiliate_id AS VARCHAR) IN {AFF_IDS}
  AND is_test = false AND ftd_date = DATE '{DATA}'
""", database="ps_bi")

ftd_ps = int(df_ftd_ps.iloc[0]["ftd"])
ftd_dep_ps = float(df_ftd_ps.iloc[0]["ftd_deposit"])
avg_ftd = ftd_dep_ps / max(ftd_ps, 1)

print(f"  ps_bi.dim_user (ftd_date):     FTD={ftd_ps:>6,}  |  Deposit=R$ {ftd_dep_ps:>12,.2f}")
print(f"  Ticket medio FTD:              R$ {avg_ftd:>8,.2f}  {'(BRL ok)' if avg_ftd < 5000 else '(CENTAVOS!)'}")

# Cross-check: FTD count via fct_player_activity_daily
df_ftd_act = query_athena(f"""
WITH pb AS (
    SELECT ecr_id FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN {AFF_IDS} AND is_test = false
)
SELECT COALESCE(SUM(p.ftd_count), 0) AS ftd
FROM ps_bi.fct_player_activity_daily p
JOIN pb ON p.player_id = pb.ecr_id
WHERE p.activity_date = DATE '{DATA}'
""", database="ps_bi")
ftd_act = int(df_ftd_act.iloc[0]["ftd"])
print(f"  fct_player_activity (ftd_count):  FTD={ftd_act:>6,}")
print(f"  Diferenca vs dim_user:                 {abs(ftd_ps - ftd_act):>6,}  {'OK' if ftd_ps == ftd_act else 'DIVERGE'}")

# ---------------------------------------------------------------
# CHECK 4: Financeiro bireports BI Summary (detalhado)
# ---------------------------------------------------------------
print(f"\n[CHECK 4] Financeiro -- bireports_ec2 BI Summary")

df_fin = query_athena(f"""
WITH pb AS (
    SELECT ecr_id FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN {AFF_IDS} AND is_test = false
)
SELECT
    SUM(s.c_deposit_success_amount) / 100.0 AS dep_amount,
    SUM(s.c_co_success_amount) / 100.0 AS saques,
    SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount) / 100.0 AS ggr_cassino,
    SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount) / 100.0 AS ggr_sport,
    SUM(s.c_bonus_issued_amount) / 100.0 AS bonus_cost,
    SUM(s.c_casino_realcash_bet_amount) / 100.0 AS casino_bet,
    SUM(s.c_casino_realcash_win_amount) / 100.0 AS casino_win,
    SUM(s.c_sb_realcash_bet_amount) / 100.0 AS sb_bet,
    SUM(s.c_sb_realcash_win_amount) / 100.0 AS sb_win,
    COUNT(DISTINCT s.c_ecr_id) AS players_ativos
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
JOIN pb ON s.c_ecr_id = pb.ecr_id
WHERE s.c_created_date = DATE '{DATA}'
""", database="ps_bi")

r = df_fin.iloc[0]
print(f"  Players ativos no dia:  {int(r['players_ativos']):>10,}")
print(f"  ---")
print(f"  Dep Amount:             R$ {float(r['dep_amount']):>12,.2f}")
print(f"  Saques:                 R$ {float(r['saques']):>12,.2f}")
print(f"  ---")
print(f"  Casino Bet (real):      R$ {float(r['casino_bet']):>12,.2f}")
print(f"  Casino Win (real):      R$ {float(r['casino_win']):>12,.2f}")
print(f"  GGR Cassino:            R$ {float(r['ggr_cassino']):>12,.2f}  (bet - win)")
print(f"  ---")
print(f"  SB Bet (real):          R$ {float(r['sb_bet']):>12,.2f}")
print(f"  SB Win (real):          R$ {float(r['sb_win']):>12,.2f}")
print(f"  GGR Sport:              R$ {float(r['ggr_sport']):>12,.2f}  (bet - win)")
print(f"  ---")
ggr_total_bi = float(r['ggr_cassino']) + float(r['ggr_sport'])
bonus_bi = float(r['bonus_cost'])
ngr_bi = ggr_total_bi - bonus_bi
print(f"  GGR Total:              R$ {ggr_total_bi:>12,.2f}")
print(f"  Bonus Cost:             R$ {bonus_bi:>12,.2f}")
print(f"  NGR (GGR - Bonus):      R$ {ngr_bi:>12,.2f}")

# ---------------------------------------------------------------
# CHECK 5: Cross-check COMPLETO via ps_bi.fct_player_activity_daily
# ---------------------------------------------------------------
print(f"\n[CHECK 5] Cross-check COMPLETO -- ps_bi.fct_player_activity_daily")

df_ps_act = query_athena(f"""
WITH pb AS (
    SELECT ecr_id FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN {AFF_IDS} AND is_test = false
)
SELECT
    COALESCE(SUM(p.deposit_success_local), 0) AS dep_amount,
    COALESCE(SUM(p.cashout_success_local), 0) AS saques,
    COALESCE(SUM(p.casino_realbet_local - p.casino_real_win_local), 0) AS ggr_cassino,
    COALESCE(SUM(p.sb_realbet_local - p.sb_real_win_local), 0) AS ggr_sport,
    COALESCE(SUM(p.ggr_local), 0) AS ggr_total,
    COALESCE(SUM(p.ngr_local), 0) AS ngr,
    SUM(p.ftd_count) AS ftd_count,
    SUM(p.nrc_count) AS nrc_count,
    COUNT(DISTINCT p.player_id) AS players_ativos
FROM ps_bi.fct_player_activity_daily p
JOIN pb ON p.player_id = pb.ecr_id
WHERE p.activity_date = DATE '{DATA}'
""", database="ps_bi")

rp = df_ps_act.iloc[0]
print(f"  Players ativos:         {int(rp['players_ativos']):>10,}")
print(f"  NRC (registros):        {int(rp['nrc_count']):>10,}")
print(f"  FTD count:              {int(rp['ftd_count']):>10,}")
print(f"  Dep Amount:             R$ {float(rp['dep_amount']):>12,.2f}")
print(f"  Saques:                 R$ {float(rp['saques']):>12,.2f}")
print(f"  GGR Cassino:            R$ {float(rp['ggr_cassino']):>12,.2f}")
print(f"  GGR Sport:              R$ {float(rp['ggr_sport']):>12,.2f}")
print(f"  GGR Total:              R$ {float(rp['ggr_total']):>12,.2f}")
print(f"  NGR:                    R$ {float(rp['ngr']):>12,.2f}")

# ---------------------------------------------------------------
# COMPARACAO METRICA A METRICA
# ---------------------------------------------------------------
print(f"\n{'='*70}")
print("COMPARACAO bireports_ec2 vs ps_bi (metrica a metrica)")
print(f"{'='*70}")
print(f"  {'Metrica':<20} {'bireports':>14} {'ps_bi':>14} {'Diff':>14} {'%':>8} {'Status'}")
print(f"  {'-'*20} {'-'*14} {'-'*14} {'-'*14} {'-'*8} {'-'*10}")

metrics = [
    ("Dep Amount", float(r['dep_amount']), float(rp['dep_amount'])),
    ("Saques", float(r['saques']), float(rp['saques'])),
    ("GGR Cassino", float(r['ggr_cassino']), float(rp['ggr_cassino'])),
    ("GGR Sport", float(r['ggr_sport']), float(rp['ggr_sport'])),
    ("GGR Total", ggr_total_bi, float(rp['ggr_total'])),
    ("NGR", ngr_bi, float(rp['ngr'])),
]

for name, v_bi, v_ps in metrics:
    diff = abs(v_bi - v_ps)
    pct = diff / max(abs(v_ps), 1) * 100
    status = "OK" if pct < 5 else "DIVERGE"
    print(f"  {name:<20} R${v_bi:>12,.2f} R${v_ps:>12,.2f} R${diff:>12,.2f} {pct:>7.2f}% {status}")

# ---------------------------------------------------------------
# RESUMO FINAL
# ---------------------------------------------------------------
print(f"\n{'='*70}")
print("RESUMO FINAL -- Valores recomendados para entrega")
print(f"{'='*70}")
print(f"  Data:           {DATA}")
print(f"  Affiliates:     297657, 445431, 468114 (consolidado)")
print(f"  Base players:   {ps_total:,}")
print(f"  ---")
print(f"  Saques:         R$ {float(r['saques']):>12,.2f}  (bireports)")
print(f"  REG:            {reg_bi:>12,}  (bireports, BRT)")
print(f"  FTD:            {ftd_ps:>12,}  (ps_bi dim_user)")
print(f"  FTD Deposit:    R$ {ftd_dep_ps:>12,.2f}  (ps_bi, BRL)")
print(f"  Dep Amount:     R$ {float(r['dep_amount']):>12,.2f}  (bireports)")
print(f"  GGR Cassino:    R$ {float(r['ggr_cassino']):>12,.2f}  (bireports)")
print(f"  GGR Sport:      R$ {float(r['ggr_sport']):>12,.2f}  (bireports)")
print(f"  NGR:            R$ {float(rp['ngr']):>12,.2f}  (ps_bi ngr_local)")
print(f"{'='*70}")
print(f"\nRacional:")
print(f"  - Base: ps_bi.dim_user filtrando affiliate_id IN (297657,445431,468114), is_test=false")
print(f"  - REG: bireports_ec2.tbl_ecr com conversao BRT explicita (mais confiavel)")
print(f"  - FTD: ps_bi.dim_user.ftd_date = data alvo (dbt calcula 1o deposito)")
print(f"  - FTD Deposit: ps_bi.dim_user.ftd_amount_inhouse (valores em BRL, ticket medio R${avg_ftd:,.2f})")
print(f"  - Dep/Saques/GGR: bireports_ec2.tbl_ecr_wise_daily_bi_summary (centavos/100)")
print(f"  - GGR = realcash_bet - realcash_win (somente dinheiro real, sem bonus)")
print(f"  - NGR: ps_bi.fct_player_activity_daily.ngr_local (dbt calcula GGR-BTR-RCA)")
print(f"  - Test users excluidos via is_test=false / c_test_user=false")
