"""
Passo 6: cria multibet.vw_ad_performance_daily com KPIs derivados de midia.
Nao afeta views existentes (nome novo: vw_ad_performance_daily).
KPIs de negocio (REG, FTD, CPL, CFTD, REG_FTD) virao de fact_ad_results (futuro).
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

VIEW_SQL = """
CREATE OR REPLACE VIEW multibet.vw_ad_performance_daily AS
SELECT
    s.dt,
    s.ad_source,
    s.campaign_id,
    s.campaign_name,
    s.channel_type,
    s.affiliate_id,
    -- Fatos (direto da tabela)
    s.cost_brl,
    s.impressions,
    s.clicks,
    s.conversions,
    s.page_views,
    s.reach,
    -- KPIs derivados de midia
    CASE WHEN s.clicks > 0
         THEN ROUND(s.cost_brl / s.clicks, 2)
         ELSE NULL END AS cpc_brl,
    CASE WHEN s.impressions > 0
         THEN ROUND(100.0 * s.clicks / s.impressions, 2)
         ELSE NULL END AS ctr_pct,
    CASE WHEN s.page_views > 0
         THEN ROUND(s.cost_brl / s.page_views, 2)
         ELSE NULL END AS cpv_brl,               -- custo por landing page view (Meta)
    CASE WHEN s.impressions > 0
         THEN ROUND(1000.0 * s.cost_brl / s.impressions, 2)
         ELSE NULL END AS cpm_brl,               -- custo por mil impressoes
    CASE WHEN s.reach > 0
         THEN ROUND(s.impressions::numeric / s.reach, 2)
         ELSE NULL END AS frequency,             -- vezes que cada pessoa viu o anuncio
    CASE WHEN s.clicks > 0
         THEN ROUND(100.0 * s.page_views / s.clicks, 2)
         ELSE NULL END AS landing_rate_pct       -- % cliques que carregaram a LP
FROM multibet.fact_ad_spend s;
"""

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(VIEW_SQL)
        conn.commit()
        print("CREATE OR REPLACE VIEW multibet.vw_ad_performance_daily — OK")

        # Teste: ultimos 7 dias Meta, top 10 campanhas por spend
        cur.execute("""
            SELECT dt, campaign_name, cost_brl, clicks, page_views,
                   cpc_brl, ctr_pct, cpv_brl, cpm_brl, frequency, landing_rate_pct
            FROM multibet.vw_ad_performance_daily
            WHERE ad_source = 'meta'
              AND dt >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY cost_brl DESC
            LIMIT 10
        """)
        rows = cur.fetchall()
        print("\n=== TOP 10 CAMPANHAS META (ultimos 7 dias por spend) ===")
        print(f"{'dt':<12} {'campanha':<40} {'cost':>10} {'clicks':>7} {'pv':>6} {'cpc':>6} {'ctr%':>6} {'cpv':>6} {'cpm':>7} {'freq':>5} {'lp%':>6}")
        for r in rows:
            name = (r[1] or '-')[:40]
            print(f"{str(r[0]):<12} {name:<40} R${float(r[2]):>7,.0f} {r[3]:>7,} {r[4]:>6,} "
                  f"{(float(r[5]) if r[5] else 0):>6,.2f} {(float(r[6]) if r[6] else 0):>6,.2f} "
                  f"{(float(r[7]) if r[7] else 0):>6,.2f} {(float(r[8]) if r[8] else 0):>7,.2f} "
                  f"{(float(r[9]) if r[9] else 0):>5,.2f} {(float(r[10]) if r[10] else 0):>6,.2f}")

        # Agregado geral por source ultimos 30d
        cur.execute("""
            SELECT ad_source,
                   COUNT(DISTINCT dt) AS dias,
                   SUM(cost_brl)::numeric(14,2) AS cost,
                   SUM(clicks) AS clicks,
                   SUM(impressions) AS imps,
                   SUM(page_views) AS pv,
                   CASE WHEN SUM(clicks) > 0 THEN ROUND(SUM(cost_brl)/SUM(clicks), 2) END AS cpc,
                   CASE WHEN SUM(impressions) > 0 THEN ROUND(100.0*SUM(clicks)/SUM(impressions),2) END AS ctr,
                   CASE WHEN SUM(page_views) > 0 THEN ROUND(SUM(cost_brl)/SUM(page_views),2) END AS cpv
            FROM multibet.vw_ad_performance_daily
            WHERE dt >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY ad_source
            ORDER BY ad_source
        """)
        rows = cur.fetchall()
        print("\n=== AGREGADO POR FONTE (ultimos 30 dias) ===")
        print(f"{'source':<12} {'dias':>5} {'cost':>14} {'clicks':>10} {'imps':>12} {'pv':>10} {'cpc':>7} {'ctr%':>7} {'cpv':>7}")
        for r in rows:
            print(f"{r[0]:<12} {r[1]:>5} R$ {float(r[2] or 0):>11,.2f} {r[3]:>10,} {r[4]:>12,} {r[5]:>10,} "
                  f"{(float(r[6]) if r[6] else 0):>7,.2f} {(float(r[7]) if r[7] else 0):>7,.2f} "
                  f"{(float(r[8]) if r[8] else 0):>7,.2f}")

finally:
    conn.close()
    tunnel.stop()
