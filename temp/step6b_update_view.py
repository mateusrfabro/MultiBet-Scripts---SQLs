"""Atualiza vw_ad_performance_daily com LEFT JOIN fact_ad_results (CPL, CFTD, REG/FTD)."""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

# vw_ad_performance_daily v2: spend (por campanha) + results (por source)
# Grao: (dt, ad_source, campaign_id)
# REG/FTD ficam iguais pra todas campanhas do mesmo source no mesmo dia
# (ate popular dim_campaign_affiliate pra granularidade por campaign_id).
VIEW_SQL = """
DROP VIEW IF EXISTS multibet.vw_ad_performance_daily;
CREATE VIEW multibet.vw_ad_performance_daily AS
SELECT
    s.dt,
    s.ad_source,
    s.campaign_id,
    s.campaign_name,
    s.channel_type,
    s.affiliate_id,
    -- Fatos de midia (da fact_ad_spend)
    s.cost_brl,
    s.impressions,
    s.clicks,
    s.conversions,
    s.page_views,
    s.reach,
    -- Fatos de negocio (da fact_ad_results, agregado por source-dia)
    -- IMPORTANTE: regs/ftds sao do SOURCE no dia, nao da campanha especifica.
    r.regs,
    r.ftds,
    r.ftd_amount_brl,
    -- KPIs de midia
    CASE WHEN s.clicks > 0 THEN ROUND(s.cost_brl / s.clicks, 2) END AS cpc_brl,
    CASE WHEN s.impressions > 0 THEN ROUND(100.0 * s.clicks / s.impressions, 2) END AS ctr_pct,
    CASE WHEN s.page_views > 0 THEN ROUND(s.cost_brl / s.page_views, 2) END AS cpv_brl,
    CASE WHEN s.impressions > 0 THEN ROUND(1000.0 * s.cost_brl / s.impressions, 2) END AS cpm_brl,
    CASE WHEN s.reach > 0 THEN ROUND(s.impressions::numeric / s.reach, 2) END AS frequency,
    CASE WHEN s.clicks > 0 THEN ROUND(100.0 * s.page_views / s.clicks, 2) END AS landing_rate_pct,
    -- KPIs de negocio (source-level, replicados por campanha do mesmo source-dia)
    CASE WHEN r.regs > 0 THEN ROUND(s.cost_brl / r.regs, 2) END AS cpl_brl,
    CASE WHEN r.ftds > 0 THEN ROUND(s.cost_brl / r.ftds, 2) END AS cftd_brl,
    CASE WHEN r.regs > 0 THEN ROUND(100.0 * r.ftds / r.regs, 2) END AS reg_ftd_pct
FROM multibet.fact_ad_spend s
LEFT JOIN multibet.fact_ad_results r
    ON r.dt = s.dt AND r.ad_source = s.ad_source;
"""

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(VIEW_SQL)
        conn.commit()
        print("CREATE OR REPLACE VIEW vw_ad_performance_daily (v2 com REG/FTD/CPL/CFTD) — OK")

        # Teste: agregado 30d por source
        cur.execute("""
            SELECT ad_source,
                   SUM(cost_brl)::numeric(14,2) cost,
                   SUM(regs) regs, SUM(ftds) ftds,
                   CASE WHEN SUM(regs) > 0 THEN ROUND(SUM(cost_brl)/SUM(regs),2) END cpl,
                   CASE WHEN SUM(ftds) > 0 THEN ROUND(SUM(cost_brl)/SUM(ftds),2) END cftd,
                   CASE WHEN SUM(regs) > 0 THEN ROUND(100.0*SUM(ftds)/SUM(regs),2) END reg_ftd_pct
            FROM multibet.vw_ad_performance_daily
            WHERE dt >= CURRENT_DATE - INTERVAL '30 days'
              AND cost_brl > 0
            GROUP BY ad_source ORDER BY ad_source
        """)
        # Nota: SUM(regs) estara inflado porque regs sao replicados por campanha.
        # Pra agregado correto por source, usar fact_ad_results direto.
        # O valor da view e pra tabela row-level (dt,campaign).
        print("\n=== AGREGADO 30d (via view, SUM regs/ftds INFLADO por n_campanhas) ===")
        print(f"{'source':<12} {'cost':>14} {'regs*':>8} {'ftds*':>8} {'cpl*':>7} {'cftd*':>7}")
        for r in cur.fetchall():
            print(f"{r[0]:<12} R$ {float(r[1] or 0):>11,.2f} {int(r[2] or 0):>8,} {int(r[3] or 0):>8,} "
                  f"{(float(r[4]) if r[4] else 0):>7,.2f} {(float(r[5]) if r[5] else 0):>7,.2f}")

        # Correto: agregar via fact_ad_spend + fact_ad_results separadamente
        cur.execute("""
            WITH s AS (
                SELECT dt, ad_source, SUM(cost_brl) cost
                FROM multibet.fact_ad_spend
                WHERE dt >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY dt, ad_source
            )
            SELECT s.ad_source,
                   SUM(s.cost)::numeric(14,2) cost,
                   SUM(r.regs) regs, SUM(r.ftds) ftds,
                   CASE WHEN SUM(r.regs)>0 THEN ROUND(SUM(s.cost)/SUM(r.regs),2) END cpl,
                   CASE WHEN SUM(r.ftds)>0 THEN ROUND(SUM(s.cost)/SUM(r.ftds),2) END cftd
            FROM s LEFT JOIN multibet.fact_ad_results r USING (dt, ad_source)
            GROUP BY s.ad_source ORDER BY s.ad_source
        """)
        print("\n=== AGREGADO 30d CORRETO (dia-source antes de somar) ===")
        print(f"{'source':<12} {'cost':>14} {'regs':>6} {'ftds':>6} {'cpl':>7} {'cftd':>7}")
        for r in cur.fetchall():
            print(f"{r[0]:<12} R$ {float(r[1] or 0):>11,.2f} {int(r[2] or 0):>6,} {int(r[3] or 0):>6,} "
                  f"{(float(r[4]) if r[4] else 0):>7,.2f} {(float(r[5]) if r[5] else 0):>7,.2f}")
finally:
    conn.close()
    tunnel.stop()
