"""
Cria VIEW multibet.vw_active_player_retention_weekly no Super Nova DB.
Reproduz gráfico: Active Player Retention vs Repeat Depositors

Métricas por semana (dom-sáb):
  - depositantes_semana_atual: depositantes únicos na semana
  - depositantes_semana_anterior: depositantes únicos na semana anterior
  - retidos_da_semana_anterior: depositantes que depositaram em AMBAS as semanas
  - repeat_depositors: depositantes com 2+ depósitos na mesma semana
  - retention_pct: retidos / depositantes semana anterior * 100
  - repeat_depositor_pct: repeat / total depositantes semana * 100

Fonte: bronze_daily_payment_summary (Super Nova DB)
Nota: cores (qual semana é escura/clara) é controle do front-end.
Nota: semana parcial (corrente) é mantida — ETL atualiza diariamente.
"""
import sys
sys.path.insert(0, ".")
from db.supernova import execute_supernova

VIEW_DDL = """
CREATE OR REPLACE VIEW multibet.vw_active_player_retention_weekly AS
WITH
-- Semana dom-sab: shift +1 dia, trunca na segunda, shift -1 dia = domingo
weekly_depositors AS (
    SELECT
        (date_trunc('week', d.c_created_date + INTERVAL '1 day')::date - 1) AS semana,
        d.c_ecr_id,
        SUM(d.c_deposit_count) AS total_deposits_week
    FROM multibet.bronze_daily_payment_summary d
    LEFT JOIN multibet.bronze_ecr_flags f ON f.c_ecr_id = d.c_ecr_id
    WHERE d.c_deposit_count > 0
      AND COALESCE(f.c_test_user, 0) = 0
    GROUP BY 1, 2
),
-- Métricas por semana
weekly_metrics AS (
    SELECT
        w.semana,
        COUNT(DISTINCT w.c_ecr_id) AS depositantes_semana_atual,
        COUNT(DISTINCT CASE
            WHEN w.total_deposits_week >= 2 THEN w.c_ecr_id
        END) AS repeat_depositors,
        COUNT(DISTINCT CASE
            WHEN prev.c_ecr_id IS NOT NULL THEN w.c_ecr_id
        END) AS retidos_da_semana_anterior
    FROM weekly_depositors w
    LEFT JOIN weekly_depositors prev
        ON w.c_ecr_id = prev.c_ecr_id
        AND prev.semana = (w.semana - INTERVAL '7 days')::date
    GROUP BY 1
)
-- Resultado final: cada linha = 1 semana (dom-sab)
SELECT
    m.semana,
    TO_CHAR(m.semana, 'DD/MM') AS semana_label,
    m.depositantes_semana_atual,
    COALESCE(prev_m.depositantes_semana_atual, 0) AS depositantes_semana_anterior,
    m.retidos_da_semana_anterior,
    m.repeat_depositors,
    ROUND(
        m.retidos_da_semana_anterior * 100.0
        / NULLIF(prev_m.depositantes_semana_atual, 0), 1
    ) AS retention_pct,
    ROUND(
        m.repeat_depositors * 100.0
        / NULLIF(m.depositantes_semana_atual, 0), 1
    ) AS repeat_depositor_pct
FROM weekly_metrics m
LEFT JOIN weekly_metrics prev_m
    ON prev_m.semana = (m.semana - INTERVAL '7 days')::date
ORDER BY m.semana
"""

print("Criando view multibet.vw_active_player_retention_weekly...")
execute_supernova(VIEW_DDL)
print("View criada com sucesso!")

# Validar: comparar com dados da imagem (Semana 09/02 a 16/03)
print("\n=== VALIDAÇÃO - Semanas de Fev/Mar 2026 ===")
rows = execute_supernova("""
    SELECT semana_label,
           depositantes_semana_atual,
           depositantes_semana_anterior,
           retention_pct,
           repeat_depositor_pct
    FROM multibet.vw_active_player_retention_weekly
    WHERE semana >= '2026-02-02' AND semana <= '2026-03-23'
    ORDER BY semana
""", fetch=True)

print(f"{'Semana':>10} | {'Atual':>8} | {'Anterior':>8} | {'Ret%':>6} | {'Repeat%':>8}")
print("-" * 55)
for r in rows:
    print(f"{r[0]:>10} | {r[1]:>8,} | {r[2]:>8,} | {r[3] or '-':>6} | {r[4] or '-':>8}")

print("\n=== REFERÊNCIA DA IMAGEM ===")
print("Semana 09/02: Atual=18.921, Anterior=25.207, Ret=47.5%, Repeat=32.8%")
print("Semana 16/02: Atual=18.807, Anterior=18.921, Ret=47.2%, Repeat=40.7%")
print("Semana 23/02: Atual=18.946, Anterior=18.807, Ret=46.5%, Repeat=39.0%")
print("Semana 02/03: Atual=18.946, Anterior=25.145, Ret=39.9%, Repeat=40.1%")
print("Semana 09/03: Atual=23.909, Anterior=25.145, Ret=46.6%, Repeat=33.1%")
print("Semana 16/03: Atual=23.123, Anterior=23.909, Ret=45.9%, Repeat=37.8%")

print("\nDone.")