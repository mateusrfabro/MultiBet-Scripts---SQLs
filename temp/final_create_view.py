"""
Cria view final vw_ad_daily_summary e limpa estruturas obsoletas.
- Usa tab_user_affiliate + silver_tab_user_ftd (alimentadas por sync_all_aquisicao)
- Classifica via dim_marketing_mapping
- Drop fact_ad_results e vw_ad_performance_daily (obsoletas)
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

DROP_OLD = """
DROP VIEW  IF EXISTS multibet.vw_ad_performance_daily;
DROP TABLE IF EXISTS multibet.fact_ad_results;
"""

CREATE_VIEW = """
CREATE OR REPLACE VIEW multibet.vw_ad_daily_summary AS
WITH mapping AS (
    -- affiliate_id -> ad_source normalizado (alinha com fact_ad_spend.ad_source)
    SELECT affiliate_id,
           CASE source_name
               WHEN 'meta_ads'   THEN 'meta'
               WHEN 'tiktok_ads' THEN 'tiktok'
               WHEN 'kwai_ads'   THEN 'kwai'
               ELSE source_name
           END AS ad_source
    FROM multibet.dim_marketing_mapping
),
spend AS (
    SELECT dt, ad_source,
           SUM(cost_brl)    AS cost_brl,
           SUM(impressions) AS impressions,
           SUM(clicks)      AS clicks,
           SUM(page_views)  AS page_views,
           SUM(reach)       AS reach
    FROM multibet.fact_ad_spend
    GROUP BY dt, ad_source
),
regs AS (
    SELECT t.data_registro AS dt,
           COALESCE(m.ad_source, 'other') AS ad_source,
           COUNT(DISTINCT t.c_ecr_id) AS regs
    FROM multibet.tab_user_affiliate t
    LEFT JOIN mapping m ON m.affiliate_id = t.affiliate_id
    GROUP BY t.data_registro, COALESCE(m.ad_source, 'other')
),
ftds AS (
    SELECT f.ftd_data AS dt,
           COALESCE(m.ad_source, 'other') AS ad_source,
           COUNT(DISTINCT f.c_ecr_id) AS ftds
    FROM multibet.silver_tab_user_ftd f
    LEFT JOIN mapping m ON m.affiliate_id = f.affiliate_id
    GROUP BY f.ftd_data, COALESCE(m.ad_source, 'other')
)
SELECT COALESCE(s.dt, r.dt, f.dt)                 AS dt,
       COALESCE(s.ad_source, r.ad_source, f.ad_source) AS ad_source,
       COALESCE(s.cost_brl, 0)    AS cost_brl,
       COALESCE(s.impressions, 0) AS impressions,
       COALESCE(s.clicks, 0)      AS clicks,
       COALESCE(s.page_views, 0)  AS page_views,
       COALESCE(s.reach, 0)       AS reach,
       COALESCE(r.regs, 0)        AS regs,
       COALESCE(f.ftds, 0)        AS ftds,
       CASE WHEN s.clicks > 0      THEN ROUND(s.cost_brl/s.clicks, 2)                END AS cpc_brl,
       CASE WHEN s.impressions > 0 THEN ROUND(100.0*s.clicks/s.impressions, 2)       END AS ctr_pct,
       CASE WHEN s.page_views > 0  THEN ROUND(s.cost_brl/s.page_views, 2)            END AS cpv_brl,
       CASE WHEN s.impressions > 0 THEN ROUND(1000.0*s.cost_brl/s.impressions, 2)    END AS cpm_brl,
       CASE WHEN s.reach > 0       THEN ROUND(s.impressions::numeric/s.reach, 2)     END AS frequency,
       CASE WHEN s.clicks > 0      THEN ROUND(100.0*s.page_views/s.clicks, 2)        END AS landing_rate_pct,
       CASE WHEN r.regs > 0        THEN ROUND(s.cost_brl/r.regs, 2)                  END AS cpl_brl,
       CASE WHEN f.ftds > 0        THEN ROUND(s.cost_brl/f.ftds, 2)                  END AS cftd_brl,
       CASE WHEN r.regs > 0        THEN ROUND(100.0*f.ftds/r.regs, 2)                END AS reg_ftd_pct
FROM spend s
FULL OUTER JOIN regs r USING (dt, ad_source)
FULL OUTER JOIN ftds f USING (dt, ad_source);
"""

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute(DROP_OLD)
        print("DROP vw_ad_performance_daily + fact_ad_results — OK")
        cur.execute(CREATE_VIEW)
        conn.commit()
        print("CREATE vw_ad_daily_summary — OK")

        # Validar
        cur.execute("""
            SELECT ad_source, COUNT(*) dias,
                   SUM(cost_brl)::numeric(14,2) cost,
                   SUM(regs) regs, SUM(ftds) ftds,
                   ROUND(CASE WHEN SUM(regs)>0 THEN SUM(cost_brl)/SUM(regs) END, 2) cpl,
                   ROUND(CASE WHEN SUM(ftds)>0 THEN SUM(cost_brl)/SUM(ftds) END, 2) cftd,
                   ROUND(CASE WHEN SUM(regs)>0 THEN 100.0*SUM(ftds)/SUM(regs) END, 2) reg_ftd_pct
            FROM multibet.vw_ad_daily_summary
            WHERE dt >= CURRENT_DATE - INTERVAL '30 days'
              AND ad_source IN ('meta', 'google_ads')
            GROUP BY ad_source ORDER BY ad_source
        """)
        print("\n=== AGREGADO 30d (Meta + Google) ===")
        print(f"{'source':<12} {'dias':>5} {'cost':>14} {'regs':>7} {'ftds':>6} {'cpl':>7} {'cftd':>7} {'reg_ftd%':>8}")
        for r in cur.fetchall():
            print(f"{r[0]:<12} {r[1]:>5} R$ {float(r[2]):>11,.2f} {int(r[3]):>7,} {int(r[4]):>6,} "
                  f"{(float(r[5]) if r[5] else 0):>7,.2f} {(float(r[6]) if r[6] else 0):>7,.2f} "
                  f"{(float(r[7]) if r[7] else 0):>7,.2f}%")

        # Check: organic tem REG mas sem cost — valida que FULL OUTER JOIN funciona
        cur.execute("""
            SELECT ad_source, SUM(regs), SUM(ftds), SUM(cost_brl)::numeric(14,2)
            FROM multibet.vw_ad_daily_summary
            WHERE dt >= CURRENT_DATE - INTERVAL '7 days' AND ad_source IN ('organic', 'affiliate_direct')
            GROUP BY ad_source ORDER BY ad_source
        """)
        print("\n=== ORGÂNICO/AFFILIATE 7d (sem spend, com REG) ===")
        for r in cur.fetchall():
            print(f"  {r[0]:<20} regs={int(r[1] or 0):>5,} ftds={int(r[2] or 0):>4,} cost=R$ {float(r[3] or 0):>8,.2f}")
finally:
    conn.close()
    tunnel.stop()
