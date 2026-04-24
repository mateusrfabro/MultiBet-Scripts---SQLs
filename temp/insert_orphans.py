"""
Insere TODOS os affiliate_ids orfaos na dim_marketing_mapping.
- Inferencia forense via click IDs (gclid, fbclid, ttclid, kwai)
- is_validated = FALSE (pendente validacao Marketing)
- Tambem re-insere os 24 mapeamentos manuais do backup v1
"""
import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras
import pandas as pd

# =====================================================================
# 1. Buscar TODOS affiliate_ids com atividade + inferencia forense
# =====================================================================
print("Buscando todos os affiliate_ids do Athena com inferencia de fonte...")

df = query_athena("""
WITH
reg AS (
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        MAX(COALESCE(NULLIF(c_affiliate_name, ''), 'N/A')) AS affiliate_name,
        COUNT(DISTINCT c_ecr_id) AS qty_players
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= TIMESTAMP '2025-10-01'
    GROUP BY 1
),

url_signals AS (
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        COUNT_IF(regexp_like(lower(c_reference_url), 'gclid='))     AS cnt_gclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'fbclid='))    AS cnt_fbclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'ttclid='))    AS cnt_ttclid,
        COUNT_IF(regexp_like(lower(c_reference_url), 'kclid|kwai')) AS cnt_kwai,
        COUNT_IF(regexp_like(lower(c_reference_url), 'afp=|afp1=|afp2=')) AS cnt_afp,
        COUNT_IF(regexp_like(lower(c_reference_url), 'source_id='))AS cnt_source_id,
        COUNT_IF(regexp_like(lower(c_reference_url), 'utm_source='))AS cnt_utm,
        COUNT(*)                                                     AS cnt_urls,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_source=([^&]+)', 1)) AS utm_source_ex,
        MAX(REGEXP_EXTRACT(c_reference_url, 'utm_medium=([^&]+)', 1)) AS utm_medium_ex
    FROM ecr_ec2.tbl_ecr_banner
    WHERE c_affiliate_id IS NOT NULL
    GROUP BY 1
)

SELECT
    r.affiliate_id,
    r.affiliate_name,
    r.qty_players,

    CASE
        WHEN s.cnt_gclid > 0 AND s.cnt_gclid >= COALESCE(s.cnt_fbclid, 0)
            THEN 'google_ads'
        WHEN s.cnt_fbclid > 0 AND s.cnt_fbclid >= COALESCE(s.cnt_gclid, 0)
            THEN 'meta_ads'
        WHEN s.cnt_ttclid > 0 THEN 'tiktok_ads'
        WHEN s.cnt_kwai > 0 THEN 'kwai_ads'
        WHEN s.cnt_afp > 0 THEN 'affiliate_performance'
        WHEN s.cnt_source_id > 0 THEN 'affiliate_direct'
        WHEN s.cnt_utm > 0 THEN 'paid_other'
        ELSE 'unknown'
    END AS suggested_source,

    CASE
        WHEN s.cnt_gclid > 0 OR s.cnt_fbclid > 0 THEN 'High (click_id)'
        WHEN s.cnt_ttclid > 0 THEN 'High (ttclid)'
        WHEN s.cnt_afp > 0 THEN 'High (AFP)'
        WHEN s.cnt_source_id > 0 THEN 'Medium (source_id)'
        WHEN s.cnt_utm > 0 THEN 'Medium (UTM)'
        ELSE 'Low (sem sinal URL)'
    END AS confidence,

    CONCAT(
        'Forense auto | players:', CAST(r.qty_players AS VARCHAR),
        CASE WHEN s.cnt_gclid > 0 THEN CONCAT(' | gclid:', CAST(s.cnt_gclid AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_fbclid > 0 THEN CONCAT(' | fbclid:', CAST(s.cnt_fbclid AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_ttclid > 0 THEN CONCAT(' | ttclid:', CAST(s.cnt_ttclid AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_kwai > 0 THEN CONCAT(' | kwai:', CAST(s.cnt_kwai AS VARCHAR)) ELSE '' END,
        CASE WHEN s.cnt_afp > 0 THEN CONCAT(' | afp:', CAST(s.cnt_afp AS VARCHAR)) ELSE '' END,
        CASE WHEN s.utm_source_ex IS NOT NULL THEN CONCAT(' | utm_source=', s.utm_source_ex) ELSE '' END,
        CASE WHEN s.utm_medium_ex IS NOT NULL THEN CONCAT(' | utm_medium=', s.utm_medium_ex) ELSE '' END
    ) AS evidence

FROM reg r
LEFT JOIN url_signals s ON r.affiliate_id = s.affiliate_id
WHERE r.affiliate_id NOT IN ('0', '468114', '297657', '445431', '464673')
ORDER BY r.qty_players DESC
""", database="bireports_ec2")

print(f"Total de affiliate_ids encontrados: {len(df)}")

# =====================================================================
# 2. Recuperar mapeamentos manuais do backup v1
# =====================================================================
print("\nRecuperando mapeamentos manuais do backup v1...")
rows_bkp = execute_supernova("""
    SELECT tracker_id, campaign_name, source, confidence, mapping_logic
    FROM multibet.dim_marketing_mapping_bkp_20260319
    WHERE tracker_id NOT IN ('0', '468114', '297657', '445431', '464673', 'sem_tracker')
""", fetch=True)

bkp_map = {}
for r in (rows_bkp or []):
    bkp_map[r[0]] = {
        "campaign_name": r[1],
        "source": r[2],
        "confidence": r[3],
        "mapping_logic": r[4],
    }
print(f"Mapeamentos manuais recuperados do backup: {len(bkp_map)}")

# =====================================================================
# 3. Inserir na dim_marketing_mapping
# =====================================================================
insert_sql = """
    INSERT INTO multibet.dim_marketing_mapping
        (affiliate_id, tracker_id, source_name, source, partner_name, is_validated, evidence)
    VALUES (%s, %s, %s, %s, %s, FALSE, %s)
    ON CONFLICT (affiliate_id, tracker_id) DO NOTHING
"""

records = []
for _, row in df.iterrows():
    aff_id = str(row["affiliate_id"])[:50]
    name = str(row["affiliate_name"]) if pd.notna(row["affiliate_name"]) and row["affiliate_name"] != "N/A" else None

    # Se existe no backup v1, usar o source manual (mais confiavel)
    if aff_id in bkp_map:
        src = bkp_map[aff_id]["source"]
        partner = bkp_map[aff_id]["campaign_name"]
        evidence = f"Mapeamento manual v1 (confidence: {bkp_map[aff_id]['confidence']}). {bkp_map[aff_id]['mapping_logic']}"
    else:
        src = str(row["suggested_source"])
        partner = name
        evidence = str(row["evidence"])[:2000] if pd.notna(row["evidence"]) else None

    records.append((
        aff_id,         # affiliate_id
        aff_id,         # tracker_id (= affiliate_id por default)
        src,            # source_name
        src,            # source (retrocompat)
        partner,        # partner_name
        evidence,       # evidence
    ))

# Tambem inserir os tracker_ids especiais do backup (ig, fb, google, google_ads, etc.)
for tid, info in bkp_map.items():
    # Pular os numericos que ja vao pelo df
    if tid.isdigit():
        continue
    records.append((
        tid,                    # affiliate_id = tracker_id
        tid,                    # tracker_id
        info["source"],         # source_name
        info["source"],         # source
        info["campaign_name"],  # partner_name
        f"Mapeamento manual v1 (confidence: {info['confidence']}). {info['mapping_logic']}",
    ))

print(f"\nTotal de registros para inserir: {len(records)}")

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=500)
    conn.commit()
    print("INSERT concluido!")
finally:
    conn.close()
    tunnel.stop()

# =====================================================================
# 4. Validacao
# =====================================================================
print("\n" + "=" * 70)
print("VALIDACAO FINAL")
print("=" * 70)

rows = execute_supernova("""
    SELECT
        source_name,
        is_validated,
        COUNT(*) AS qty
    FROM multibet.dim_marketing_mapping
    GROUP BY 1, 2
    ORDER BY 3 DESC
""", fetch=True)

total = sum(r[2] for r in rows)
validated = sum(r[2] for r in rows if r[1] is True)

print(f"{'source_name':<25} {'validated':<10} {'qty':>6}")
print("-" * 45)
for r in rows:
    print(f"{str(r[0]):<25} {str(r[1]):<10} {r[2]:>6}")
print("-" * 45)
print(f"{'TOTAL':<25} {'':<10} {total:>6}")
print(f"\n  Oficiais (validated=TRUE):  {validated}")
print(f"  Pendentes (validated=FALSE): {total - validated}")

# Top 10 por volume
print("\n--- Top 10 nao-validados por evidencia ---")
rows2 = execute_supernova("""
    SELECT affiliate_id, source_name, partner_name, evidence
    FROM multibet.dim_marketing_mapping
    WHERE is_validated = FALSE
    ORDER BY affiliate_id
    LIMIT 10
""", fetch=True)
for r in rows2:
    ev = str(r[3])[:60] if r[3] else ""
    print(f"  aff={r[0]:<10} src={r[1]:<22} partner={str(r[2] or '')[:20]:<22} {ev}")
