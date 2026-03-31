"""
Pipeline: Active Player Retention vs Repeat Depositors (semanal)

Fluxo:
  1. Extrai dados direto do Athena (fonte de verdade, sempre atualizado)
  2. Grava resultado no Super Nova DB (tabela destino)
  3. View no Super Nova DB aponta pra tabela → Gusta consome no front

Fonte: Athena cashier_ec2 + bireports_ec2
Destino: multibet.vw_active_player_retention_weekly (Super Nova DB)

Métricas por semana (dom-sáb):
  - depositantes_semana_atual, depositantes_semana_anterior
  - retidos_da_semana_anterior, repeat_depositors
  - retention_pct, repeat_depositor_pct

Mauro: agendar execução diária (sugerido 06:00 BRT)
"""
import sys
sys.path.insert(0, ".")
from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection
import psycopg2.extras

# ============================================================
# 1. DDL — tabela destino + view (idempotente)
# ============================================================
DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.etl_active_player_retention_weekly (
    semana              DATE PRIMARY KEY,
    semana_label        VARCHAR(15),
    depositantes_semana_atual    INTEGER,
    depositantes_semana_anterior INTEGER,
    retidos_da_semana_anterior   INTEGER,
    repeat_depositors            INTEGER,
    retention_pct                NUMERIC(5,1),
    repeat_depositor_pct         NUMERIC(5,1)
);
"""

DDL_VIEW = """
CREATE OR REPLACE VIEW multibet.vw_active_player_retention_weekly AS
SELECT * FROM multibet.etl_active_player_retention_weekly
ORDER BY semana;
"""

# ============================================================
# 2. Query Athena — fonte de verdade, calcula tudo
# ============================================================
ATHENA_SQL = """
WITH
-- Semana dom-sab: shift +1 dia, trunca na segunda, -1 dia = domingo
weekly_depositors AS (
    SELECT
        date_add('day', -1, date_trunc('week', date_add('day', 1, d.c_created_date))) AS semana,
        d.c_ecr_id,
        SUM(d.c_deposit_count) AS total_deposits_week
    FROM cashier_ec2.tbl_cashier_ecr_daily_payment_summary d
    LEFT JOIN bireports_ec2.tbl_ecr e ON e.c_ecr_id = d.c_ecr_id
    WHERE d.c_deposit_count > 0
      AND e.c_test_user = false
    GROUP BY 1, 2
),
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
        AND prev.semana = date_add('day', -7, w.semana)
    GROUP BY 1
)
SELECT
    m.semana,
    CONCAT('Semana ', date_format(m.semana, '%d/%m')) AS semana_label,
    m.depositantes_semana_atual,
    COALESCE(prev_m.depositantes_semana_atual, 0) AS depositantes_semana_anterior,
    m.retidos_da_semana_anterior,
    m.repeat_depositors,
    ROUND(
        CAST(m.retidos_da_semana_anterior AS DOUBLE) * 100.0
        / NULLIF(prev_m.depositantes_semana_atual, 0), 1
    ) AS retention_pct,
    ROUND(
        CAST(m.repeat_depositors AS DOUBLE) * 100.0
        / NULLIF(m.depositantes_semana_atual, 0), 1
    ) AS repeat_depositor_pct
FROM weekly_metrics m
LEFT JOIN weekly_metrics prev_m
    ON prev_m.semana = date_add('day', -7, m.semana)
ORDER BY m.semana
"""


# ============================================================
# 3. Executar pipeline
# ============================================================
def main():
    # DDL
    print("1/4 - Criando tabela destino (idempotente)...")
    execute_supernova(DDL_TABLE)

    print("2/4 - Criando/atualizando view...")
    execute_supernova(DDL_VIEW)

    # Athena
    print("3/4 - Extraindo dados do Athena (pode levar ~60s)...")
    df = query_athena(ATHENA_SQL, database="cashier_ec2")
    print(f"     {len(df)} semanas extraídas ({df['semana'].min()} a {df['semana'].max()})")

    # Carga
    print("4/4 - Carregando no Super Nova DB (TRUNCATE + INSERT)...")
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE multibet.etl_active_player_retention_weekly")
            insert_sql = """
                INSERT INTO multibet.etl_active_player_retention_weekly
                (semana, semana_label, depositantes_semana_atual, depositantes_semana_anterior,
                 retidos_da_semana_anterior, repeat_depositors, retention_pct, repeat_depositor_pct)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            rows = [
                (
                    row.semana, row.semana_label,
                    int(row.depositantes_semana_atual),
                    int(row.depositantes_semana_anterior),
                    int(row.retidos_da_semana_anterior),
                    int(row.repeat_depositors),
                    float(row.retention_pct) if row.retention_pct is not None else None,
                    float(row.repeat_depositor_pct) if row.repeat_depositor_pct is not None else None,
                )
                for row in df.itertuples()
            ]
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=100)
            conn.commit()
            print(f"     {len(rows)} semanas carregadas!")
    finally:
        conn.close()
        tunnel.stop()

    # Resumo
    print("\n=== RESULTADO (via view) ===")
    result = execute_supernova(
        "SELECT semana_label, depositantes_semana_atual, depositantes_semana_anterior, "
        "retention_pct, repeat_depositor_pct "
        "FROM multibet.vw_active_player_retention_weekly "
        "WHERE semana >= DATE '2026-02-01'",
        fetch=True
    )
    print(f"{'Semana':>15} | {'Atual':>7} | {'Anter':>7} | {'Ret%':>5} | {'Rep%':>5}")
    print("-" * 50)
    for r in result:
        print(f"{r[0]:>15} | {r[1]:>7,} | {r[2]:>7,} | {str(r[3] or '-'):>5} | {str(r[4] or '-'):>5}")


if __name__ == "__main__":
    main()