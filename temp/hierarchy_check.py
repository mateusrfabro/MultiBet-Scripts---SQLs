"""Verificar hierarquia affiliate → tracker e se tbl_affiliate_tracker_mapping existe."""
import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/MultiBet")
from db.athena import query_athena

# 1. Buscar tabelas com 'affiliate' em todos os databases
print("=== BUSCA: tabelas com 'affiliate' em todos os databases ===")
databases = ['ecr_ec2', 'bireports_ec2', 'mktg_ec2', 'master_ec2', 'segment_ec2',
             'silver', 'ps_bi', 'fund_ec2', 'bonus_ec2', 'cashier_ec2',
             'casino_ec2', 'csm_ec2', 'vendor_ec2', 'fx_ec2', 'regulatory_ec2',
             'risk_ec2', 'messaging_ec2']

for db in databases:
    try:
        df = query_athena(f"SHOW TABLES IN {db}", database=db)
        if not df.empty:
            col = df.columns[0]
            matches = df[df[col].str.contains('affiliate', case=False, na=False)]
            for _, r in matches.iterrows():
                print(f"  {db}.{r[col]}")
    except:
        pass

# 2. Hierarquia real: quantos trackers por affiliate nos top 15
print()
print("=== HIERARQUIA: tracker_ids por affiliate_id (Top 15) ===")
df2 = query_athena("""
    SELECT
        CAST(c_affiliate_id AS VARCHAR) AS affiliate_id,
        COUNT(DISTINCT COALESCE(NULLIF(TRIM(c_tracker_id), ''), 'sem_tracker')) AS distinct_trackers,
        COUNT(DISTINCT c_ecr_id) AS total_players
    FROM bireports_ec2.tbl_ecr
    WHERE c_sign_up_time >= TIMESTAMP '2025-10-01'
      AND CAST(c_affiliate_id AS VARCHAR) IN ('0','297657','445431','53194','464673','449235','467185','468114','522633','508290','469069','458609','474045','509759','476724')
    GROUP BY 1
    ORDER BY 3 DESC
""", database="bireports_ec2")

print(f"{'aff_id':<10} {'trackers':>8} {'players':>10}")
print("-" * 32)
for _, r in df2.iterrows():
    print(f"{str(r['affiliate_id']):<10} {r['distinct_trackers']:>8,} {r['total_players']:>10,}")

# 3. Exemplos de trackers para os top 5 affiliates
print()
print("=== EXEMPLOS de tracker_ids para os Top 5 affiliates ===")
for aff in ['0', '297657', '445431', '468114', '464673']:
    df3 = query_athena(f"""
        SELECT
            COALESCE(NULLIF(TRIM(c_tracker_id), ''), 'sem_tracker') AS tracker_id,
            COUNT(DISTINCT c_ecr_id) AS players
        FROM bireports_ec2.tbl_ecr
        WHERE CAST(c_affiliate_id AS VARCHAR) = '{aff}'
          AND c_sign_up_time >= TIMESTAMP '2025-10-01'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """, database="bireports_ec2")
    print(f"\n  affiliate_id = {aff}:")
    for _, r in df3.iterrows():
        print(f"    tracker={str(r['tracker_id'])[:40]:<42} players={r['players']:>6,}")
