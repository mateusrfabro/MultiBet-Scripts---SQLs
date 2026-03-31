"""Validacao: breakdown de produto (casino vs sport) para affiliates Meta."""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\MultiBet")
from db.athena import query_athena

sql = """
WITH base_players AS (
    SELECT DISTINCT ecr_id
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ('532570', '532571', '464673')
      AND is_test = false
)
SELECT
    COALESCE(SUM(s.c_casino_realcash_bet_amount), 0) / 100.0 AS casino_bet,
    COALESCE(SUM(s.c_casino_realcash_win_amount), 0) / 100.0 AS casino_win,
    COALESCE(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_casino,
    COALESCE(SUM(s.c_sb_realcash_bet_amount), 0) / 100.0 AS sport_bet,
    COALESCE(SUM(s.c_sb_realcash_win_amount), 0) / 100.0 AS sport_win,
    COALESCE(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
    COUNT(DISTINCT CASE WHEN s.c_casino_realcash_bet_amount > 0 THEN s.c_ecr_id END) AS jogadores_casino,
    COUNT(DISTINCT CASE WHEN s.c_sb_realcash_bet_amount > 0 THEN s.c_ecr_id END) AS jogadores_sport,
    COUNT(DISTINCT s.c_ecr_id) AS jogadores_total
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
JOIN base_players p ON s.c_ecr_id = p.ecr_id
WHERE s.c_created_date = DATE '2026-03-31'
"""

df = query_athena(sql, database="bireports_ec2")

print("=" * 60)
print("BREAKDOWN PRODUTO — Meta Ads 31/03/2026")
print("Affiliates: 532570, 532571, 464673")
print("=" * 60)

r = df.iloc[0]
print(f"\nJogadores com atividade: {int(r['jogadores_total'])}")
print(f"  Jogaram Casino:      {int(r['jogadores_casino'])}")
print(f"  Jogaram Sportsbook:  {int(r['jogadores_sport'])}")

print(f"\nCasino:")
print(f"  Bet:  R$ {r['casino_bet']:,.2f}")
print(f"  Win:  R$ {r['casino_win']:,.2f}")
print(f"  GGR:  R$ {r['ggr_casino']:,.2f}")

print(f"\nSportsbook:")
print(f"  Bet:  R$ {r['sport_bet']:,.2f}")
print(f"  Win:  R$ {r['sport_win']:,.2f}")
print(f"  GGR:  R$ {r['ggr_sport']:,.2f}")

pct_casino = r['ggr_casino'] / (r['ggr_casino'] + r['ggr_sport']) * 100 if (r['ggr_casino'] + r['ggr_sport']) > 0 else 0
print(f"\n% GGR Casino:     {pct_casino:.1f}%")
print(f"% GGR Sportsbook: {100 - pct_casino:.1f}%")
