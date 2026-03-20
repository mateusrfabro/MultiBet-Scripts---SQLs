"""
Auditoria Forense: Investigar apagao do Google Ads + validar 2nd deposit
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena
from db.supernova import execute_supernova

print("=" * 80)
print("1. AUDITORIA DE SOBREVIVENCIA — Google Ads Apagao Mar/2026")
print("=" * 80)

# Investigar unmapped_orphans de Mar/2026: tem gclid/gad_source nas URLs?
print()
print("Buscando registros Mar/2026 com gclid nos unmapped_orphans...")
df = query_athena("""
    SELECT
        COALESCE(NULLIF(TRIM(e.c_tracker_id), ''), 'vazio') AS tracker,
        CAST(e.c_affiliate_id AS VARCHAR) AS aff_id,
        COUNT(DISTINCT e.c_ecr_id) AS players
    FROM bireports_ec2.tbl_ecr e
    WHERE e.c_sign_up_time >= TIMESTAMP '2026-03-01'
      AND e.c_sign_up_time < TIMESTAMP '2026-03-19'
      AND (
          e.c_reference_url LIKE '%gclid%'
          OR e.c_reference_url LIKE '%gad_source%'
          OR e.c_reference_url LIKE '%google%'
      )
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 20
""", database="bireports_ec2")

print(f"{'tracker':<30} {'aff_id':<10} {'players':>8}")
print("-" * 55)
for _, r in df.iterrows():
    print(f"{str(r.iloc[0])[:30]:<30} {str(r.iloc[1]):<10} {r.iloc[2]:>8,}")

total_google_mar = df.iloc[:, 2].sum() if not df.empty else 0
print(f"\nTotal jogadores com gclid/google em Mar/2026: {total_google_mar:,}")

# Comparar com Out/2025
print()
print("Comparando com Out/2025...")
df2 = query_athena("""
    SELECT
        COALESCE(NULLIF(TRIM(e.c_tracker_id), ''), 'vazio') AS tracker,
        CAST(e.c_affiliate_id AS VARCHAR) AS aff_id,
        COUNT(DISTINCT e.c_ecr_id) AS players
    FROM bireports_ec2.tbl_ecr e
    WHERE e.c_sign_up_time >= TIMESTAMP '2025-10-01'
      AND e.c_sign_up_time < TIMESTAMP '2025-11-01'
      AND (
          e.c_reference_url LIKE '%gclid%'
          OR e.c_reference_url LIKE '%gad_source%'
          OR e.c_reference_url LIKE '%google%'
      )
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 20
""", database="bireports_ec2")

print(f"{'tracker':<30} {'aff_id':<10} {'players':>8}")
print("-" * 55)
for _, r in df2.iterrows():
    print(f"{str(r.iloc[0])[:30]:<30} {str(r.iloc[1]):<10} {r.iloc[2]:>8,}")

total_google_out = df2.iloc[:, 2].sum() if not df2.empty else 0
print(f"\nTotal jogadores com gclid/google em Out/2025: {total_google_out:,}")
print(f"Queda: {total_google_out:,} -> {total_google_mar:,} ({(total_google_mar/max(total_google_out,1)*100):.1f}%)")

# Verificar quais affiliate_ids novos apareceram com gclid em Mar/2026
print()
print("Novos affiliate_ids com gclid em Mar/2026 (nao existiam em Out/2025):")
df3 = query_athena("""
    WITH mar AS (
        SELECT DISTINCT CAST(c_affiliate_id AS VARCHAR) AS aff_id
        FROM bireports_ec2.tbl_ecr
        WHERE c_sign_up_time >= TIMESTAMP '2026-03-01'
          AND c_reference_url LIKE '%gclid%'
    ),
    out AS (
        SELECT DISTINCT CAST(c_affiliate_id AS VARCHAR) AS aff_id
        FROM bireports_ec2.tbl_ecr
        WHERE c_sign_up_time >= TIMESTAMP '2025-10-01'
          AND c_sign_up_time < TIMESTAMP '2025-11-01'
          AND c_reference_url LIKE '%gclid%'
    )
    SELECT m.aff_id FROM mar m LEFT JOIN out o ON m.aff_id = o.aff_id WHERE o.aff_id IS NULL
""", database="bireports_ec2")
for _, r in df3.iterrows():
    print(f"  NOVO affiliate_id com gclid: {r.iloc[0]}")

print()
print("=" * 80)
print("2. VALIDACAO 2ND DEPOSIT RATE — Qualquer data vs mesmo mes")
print("=" * 80)

# Verificar se 2nd deposit eh em qualquer data
rows = execute_supernova("""
    SELECT
        'Qualquer data' AS tipo,
        ROUND(SUM(is_2nd_depositor)::numeric / COUNT(*) * 100, 2) AS pct
    FROM multibet.agg_cohort_acquisition
""", fetch=True)
print(f"  2nd deposit rate (qualquer data): {float(rows[0][1])}%")
print("  Confirmacao: is_2nd_depositor = 1 se ROW_NUMBER rn=2 existe em cashier_ec2")
print("  Isso significa: o player fez um 2o deposito em QUALQUER momento apos o FTD")
print("  Correto para analise de cohort (maturacao da safra)")

print()
print("=" * 80)
print("3. RESUMO EXECUTIVO")
print("=" * 80)
print(f"""
DESCOBERTA CRITICA:
- Em Out/2025: {total_google_out:,} jogadores vieram do Google (gclid)
- Em Mar/2026: {total_google_mar:,} jogadores vieram do Google (gclid)

SE os jogadores com gclid em Mar/2026 estao com affiliate_id DIFERENTE de 445431,
isso confirma que o rastreamento MUDOU — os jogadores do Google agora estao
sendo atribuidos a outro affiliate_id.

ACAO: Mapear os novos affiliate_ids com gclid na dim_marketing_mapping.
""")
