"""
Gap Analysis: tracker_ids e affiliate_ids no Athena vs dim_marketing_mapping
"""
import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.athena import query_athena
import pandas as pd

# =====================================================================
# 1. Contagem de IDs distintos
# =====================================================================
print("=" * 80)
print("QUERY 1: Contagem de IDs distintos (bireports_ec2.tbl_ecr, desde Out/2025)")
print("=" * 80)

df1 = query_athena("""
    SELECT
        COUNT(DISTINCT COALESCE(NULLIF(TRIM(c_tracker_id), ''), 'sem_tracker')) AS distinct_tracker_ids,
        COUNT(DISTINCT CAST(c_affiliate_id AS VARCHAR)) AS distinct_affiliate_ids,
        COUNT(DISTINCT c_ecr_id) AS total_players
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= TIMESTAMP '2025-10-01'
""", database="bireports_ec2")
print(df1.to_string(index=False))

# =====================================================================
# 2. Todos os tracker_ids distintos por volume de players
# =====================================================================
print()
print("=" * 80)
print("QUERY 2: Top 60 tracker_ids por volume (tracker + affiliate_id)")
print("=" * 80)

df2 = query_athena("""
    SELECT
        COALESCE(NULLIF(TRIM(c_tracker_id), ''), 'sem_tracker') AS tracker_id,
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        MAX(COALESCE(NULLIF(c_affiliate_name, ''), 'N/A')) AS aff_name,
        COUNT(DISTINCT c_ecr_id) AS qty_players
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= TIMESTAMP '2025-10-01'
    GROUP BY 1, 2
    ORDER BY 4 DESC
    LIMIT 60
""", database="bireports_ec2")

print(f"{'tracker_id':<32} {'aff_id':<10} {'aff_name':<25} {'players':>8}")
print("-" * 80)
for _, r in df2.iterrows():
    print(f"{str(r['tracker_id'])[:30]:<32} {str(r['affiliate_id']):<10} {str(r['aff_name'])[:24]:<25} {r['qty_players']:>8,}")

# =====================================================================
# 3. Affiliate_ids agregados (sem tracker)
# =====================================================================
print()
print("=" * 80)
print("QUERY 3: Todos os affiliate_ids (agregado) com volume")
print("=" * 80)

df3 = query_athena("""
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        MAX(COALESCE(NULLIF(c_affiliate_name, ''), 'N/A')) AS affiliate_name,
        COUNT(DISTINCT c_ecr_id) AS qty_players,
        COUNT(DISTINCT COALESCE(NULLIF(TRIM(c_tracker_id), ''), 'sem_tracker')) AS distinct_trackers
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= TIMESTAMP '2025-10-01'
    GROUP BY 1
    ORDER BY 3 DESC
    LIMIT 50
""", database="bireports_ec2")

print(f"{'aff_id':<10} {'name':<30} {'players':>8} {'trackers':>8}")
print("-" * 60)
for _, r in df3.iterrows():
    print(f"{str(r['affiliate_id']):<10} {str(r['affiliate_name'])[:28]:<30} {r['qty_players']:>8,} {r['distinct_trackers']:>8}")

# =====================================================================
# 4. IDs na tabela dim_marketing_mapping (os 30)
# =====================================================================
mapped_ids = {
    "467185", "53194", "449235", "522633", "469069", "488468",
    "sem_tracker", "ig", "fb", "qxvideo", "affbrgeov",
    "gazeta-tp-boca", "lance", "MECAAP", "google_ads", "google",
    "0", "468114", "297657", "445431", "464673",
    "474045", "476724", "509759", "452351", "522402",
    "452808", "489203", "502638", "siapesbr"
}

# Comparar tracker_ids do Athena vs mapeados
all_trackers = set(df2["tracker_id"].astype(str).unique())
unmapped = all_trackers - mapped_ids

print()
print("=" * 80)
print(f"GAP ANALYSIS: {len(mapped_ids)} mapeados vs {len(all_trackers)} no top 60 Athena")
print("=" * 80)
print(f"Mapeados:   {len(mapped_ids)}")
print(f"No Athena:  {len(all_trackers)} (top 60)")
print(f"NAO mapeados (gap): {len(unmapped)}")
if unmapped:
    print()
    print("Tracker_ids NO ATHENA que NAO estao na tabela:")
    # Buscar info desses
    for tid in sorted(unmapped):
        row = df2[df2["tracker_id"] == tid].iloc[0]
        print(f"  tracker={tid:<30} aff={row['affiliate_id']:<10} players={row['qty_players']:>8,}")
