"""
1. Auditoria anomalia ROI Out/25 vs Fev/26
2. Criar dim_acquisition_channel (View com Tiering)
3. Notas sobre GGR vs NGR
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

# ============================================================
# 1. AUDITORIA: ROI Out/25 esmagado por orphans?
# ============================================================
print("=" * 80)
print("1. AUDITORIA: Players Out/25 — Google vs Orphans")
print("=" * 80)

rows = execute_supernova("""
    SELECT source, COUNT(*) as players, ROUND(SUM(ggr_d30)::numeric, 0) as ggr_d30
    FROM multibet.agg_cohort_acquisition
    WHERE month_of_ftd = '2025-10'
    GROUP BY source
    ORDER BY players DESC
""", fetch=True)

print(f"{'Source':<25} {'Players':>8} {'GGR D30':>13}")
print("-" * 50)
total_players = 0
total_ggr = 0
for r in rows:
    print(f"{r[0]:<25} {r[1]:>8,} R${float(r[2]):>11,.0f}")
    total_players += r[1]
    total_ggr += float(r[2])
print(f"{'TOTAL':<25} {total_players:>8,} R${total_ggr:>11,.0f}")

# Google vs Orphans em Out/25
google_players = sum(r[1] for r in rows if r[0] == 'google_ads')
orphan_players = sum(r[1] for r in rows if r[0] == 'unmapped_orphans')
google_ggr = sum(float(r[2]) for r in rows if r[0] == 'google_ads')
orphan_ggr = sum(float(r[2]) for r in rows if r[0] == 'unmapped_orphans')

print(f"\nGoogle Ads Out/25: {google_players:,} players ({google_players/total_players*100:.1f}%) | GGR D30: R${google_ggr:,.0f}")
print(f"Orphans Out/25:    {orphan_players:,} players ({orphan_players/total_players*100:.1f}%) | GGR D30: R${orphan_ggr:,.0f}")
print(f"\nDIAGNOSTICO: {'SIM' if orphan_players > google_players else 'NAO'} — orphans dominam Out/25")
print("Se muitos orphans de Out/25 sao Google Ads com IDs antigos,")
print("o GGR real do Google eh MAIOR e o ROI de 6.5% eh SUBESTIMADO.")

# Comparar com Fev/26
print()
rows2 = execute_supernova("""
    SELECT source, COUNT(*) as players, ROUND(SUM(ggr_d30)::numeric, 0) as ggr_d30
    FROM multibet.agg_cohort_acquisition
    WHERE month_of_ftd = '2026-02'
    GROUP BY source
    ORDER BY players DESC
""", fetch=True)

print(f"Comparativo Fev/26:")
for r in rows2:
    if r[0] in ('google_ads', 'unmapped_orphans'):
        pct = r[1] / sum(rr[1] for rr in rows2) * 100
        print(f"  {r[0]:<25} {r[1]:>8,} ({pct:.1f}%) | GGR D30: R${float(r[2]):>11,.0f}")

# ============================================================
# 2. CRIAR dim_acquisition_channel (View com Tiering)
# ============================================================
print()
print("=" * 80)
print("2. CRIANDO dim_acquisition_channel (View com Tiering)")
print("=" * 80)

execute_supernova("DROP VIEW IF EXISTS multibet.vw_acquisition_channel CASCADE;")

execute_supernova("""
CREATE VIEW multibet.vw_acquisition_channel AS
SELECT
    a.dt,
    -- Tiering de canais
    CASE
        WHEN COALESCE(m.source, 'unmapped_orphans') = 'organic' THEN 'Direct / Organic'
        WHEN COALESCE(m.source, 'unmapped_orphans') IN ('google_ads', 'meta_ads', 'tiktok_kwai', 'instagram') THEN 'Paid Media'
        WHEN COALESCE(m.source, 'unmapped_orphans') IN ('influencers', 'portais_midia', 'affiliate_performance') THEN 'Partnerships'
        ELSE 'Unmapped'
    END AS channel_tier,
    COALESCE(m.source, 'unmapped_orphans') AS source,
    SUM(a.qty_registrations) AS qty_registrations,
    SUM(a.qty_ftds) AS qty_ftds,
    SUM(a.ggr) AS ggr,
    SUM(a.marketing_spend) AS marketing_spend,
    CASE WHEN SUM(a.qty_registrations) > 0
         THEN ROUND(SUM(a.qty_ftds)::numeric / SUM(a.qty_registrations) * 100, 2)
         ELSE NULL END AS ftd_rate,
    CASE WHEN SUM(a.marketing_spend) > 0
         THEN ROUND(SUM(a.ggr)::numeric / SUM(a.marketing_spend), 4)
         ELSE NULL END AS roas
FROM multibet.fact_attribution a
LEFT JOIN multibet.dim_marketing_mapping m ON a.c_tracker_id = m.tracker_id
GROUP BY 1, 2, 3
""")
print("View vw_acquisition_channel criada!")

# Mostrar mix de canais
print()
print("=== MIX DE CANAIS (Total periodo) ===")
rows3 = execute_supernova("""
    SELECT channel_tier,
           SUM(qty_registrations) as regs, SUM(qty_ftds) as ftds,
           ROUND(SUM(ggr)::numeric, 0) as ggr,
           ROUND(SUM(marketing_spend)::numeric, 0) as spend
    FROM multibet.vw_acquisition_channel
    GROUP BY 1
    ORDER BY ggr DESC
""", fetch=True)

total_ggr_all = sum(float(r[3]) for r in rows3)
print(f"{'Channel Tier':<20} {'Regs':>8} {'FTDs':>8} {'GGR':>14} {'Spend':>14} {'%GGR':>7}")
print("=" * 75)
for r in rows3:
    pct = float(r[3]) / total_ggr_all * 100 if total_ggr_all > 0 else 0
    print(f"{r[0]:<20} {r[1]:>8,} {r[2]:>8,} R${float(r[3]):>12,.0f} R${float(r[4]):>12,.0f} {pct:>6.1f}%")

# ============================================================
# 3. NOTA SOBRE GGR vs NGR
# ============================================================
print()
print("=" * 80)
print("3. NOTA: GGR vs NGR")
print("=" * 80)
print("""
ATUAL: ROI calculado sobre GGR (Bets - Wins)
IDEAL: ROI sobre NGR = GGR - Bonus Cost - Taxas

Para implementar NGR, precisamos:
  1. Identificar bonus cost em bonus_ec2 (c_txn_type 19=OFFER_BONUS, 20=ISSUE_BONUS)
  2. Subtrair do GGR por player/dia
  3. Isso eh Fase 3 — nao bloqueia a entrega da Prioridade 2

O ROI de 110.5% em Fev/26 pode cair para ~70-80% com NGR,
dependendo da agressividade dos bonus nesse periodo.
""")
