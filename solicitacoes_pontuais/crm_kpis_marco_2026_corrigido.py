"""
Demanda CRM — KPIs de Março 2026 (CORRIGIDO)
=============================================
Exclui users com depósitos antes do ftd_date (contaminação dbt)
"""

import pandas as pd
from datetime import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from db.athena import query_athena

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


# CTE compartilhada: identifica users contaminados (deps antes do ftd_date)
CONTAMINATED_CTE = """
    contaminated AS (
        SELECT DISTINCT d.player_id
        FROM ps_bi.fct_deposits_daily d
        INNER JOIN ps_bi.dim_user u ON d.player_id = u.ecr_id
        WHERE d.success_count > 0
          AND u.has_ftd = 1
          AND u.is_test = false
          AND CAST(u.ftd_date AS DATE) >= DATE '2026-03-01'
          AND CAST(d.created_date AS DATE) < CAST(u.ftd_date AS DATE)
    )
"""

# CTE compartilhada: FTDs limpos de março
MARCH_FTDS_CTE = """
    march_ftds AS (
        SELECT
            ecr_id,
            CAST(ftd_date AS DATE) AS ftd_date,
            CASE
                WHEN CAST(registration_date AS DATE) >= DATE '2026-03-01'
                    THEN 'New_Reg'
                ELSE 'Old_Reg'
            END AS cohort,
            CASE
                WHEN CAST(ftd_date AS DATE) BETWEEN DATE '2026-03-01' AND DATE '2026-03-07'
                    THEN '01a07'
                WHEN CAST(ftd_date AS DATE) BETWEEN DATE '2026-03-08' AND DATE '2026-03-15'
                    THEN '08a15'
            END AS ftd_window
        FROM ps_bi.dim_user
        WHERE has_ftd = 1
          AND is_test = false
          AND CAST(ftd_date AS DATE) >= DATE '2026-03-01'
          AND CAST(ftd_date AS DATE) < current_date
          AND ecr_id NOT IN (SELECT player_id FROM contaminated)
    )
"""


def query_std_3dep():
    """Taxa de STD e 3° Depósito — CORRIGIDO (exclui contaminados)."""
    sql = f"""
    WITH
    {CONTAMINATED_CTE},
    {MARCH_FTDS_CTE},

    -- Depósitos diários agregados
    deposit_daily AS (
        SELECT
            d.player_id,
            d.created_date,
            SUM(d.success_count) AS daily_deps
        FROM ps_bi.fct_deposits_daily d
        INNER JOIN march_ftds f ON d.player_id = f.ecr_id
        WHERE d.success_count > 0
        GROUP BY d.player_id, d.created_date
    ),

    -- Window function sequencial (recomendação arquiteto)
    deposit_cumulative AS (
        SELECT
            player_id,
            SUM(daily_deps) OVER (
                PARTITION BY player_id
                ORDER BY created_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_deps
        FROM deposit_daily
    ),

    -- Milestone por user
    user_milestones AS (
        SELECT player_id, MAX(cumulative_deps) AS total_deps
        FROM deposit_cumulative
        GROUP BY player_id
    )

    -- Março total
    SELECT
        f.cohort,
        'marco' AS periodo,
        COUNT(*) AS ftd_count,
        SUM(CASE WHEN COALESCE(m.total_deps,0) >= 2 THEN 1 ELSE 0 END) AS std_count,
        ROUND(100.0 * SUM(CASE WHEN COALESCE(m.total_deps,0) >= 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS std_rate,
        SUM(CASE WHEN COALESCE(m.total_deps,0) >= 3 THEN 1 ELSE 0 END) AS dep3_count,
        ROUND(100.0 * SUM(CASE WHEN COALESCE(m.total_deps,0) >= 3 THEN 1 ELSE 0 END) / COUNT(*), 2) AS dep3_rate
    FROM march_ftds f
    LEFT JOIN user_milestones m ON f.ecr_id = m.player_id
    GROUP BY f.cohort

    UNION ALL

    -- Janelas parciais
    SELECT
        f.cohort,
        f.ftd_window AS periodo,
        COUNT(*) AS ftd_count,
        SUM(CASE WHEN COALESCE(m.total_deps,0) >= 2 THEN 1 ELSE 0 END) AS std_count,
        ROUND(100.0 * SUM(CASE WHEN COALESCE(m.total_deps,0) >= 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS std_rate,
        SUM(CASE WHEN COALESCE(m.total_deps,0) >= 3 THEN 1 ELSE 0 END) AS dep3_count,
        ROUND(100.0 * SUM(CASE WHEN COALESCE(m.total_deps,0) >= 3 THEN 1 ELSE 0 END) / COUNT(*), 2) AS dep3_rate
    FROM march_ftds f
    LEFT JOIN user_milestones m ON f.ecr_id = m.player_id
    WHERE f.ftd_window IS NOT NULL
    GROUP BY f.cohort, f.ftd_window

    ORDER BY cohort, periodo
    """
    log.info("Query 1/4: STD e 3 Deposito (corrigido)...")
    return query_athena(sql, database='ps_bi')


def query_ltv():
    """LTV = Net Deposit — CORRIGIDO."""
    sql = f"""
    WITH
    {CONTAMINATED_CTE},
    {MARCH_FTDS_CTE},

    user_deps AS (
        SELECT d.player_id, SUM(d.success_amount_local) AS total_dep
        FROM ps_bi.fct_deposits_daily d
        INNER JOIN march_ftds f ON d.player_id = f.ecr_id
        WHERE d.success_count > 0
        GROUP BY d.player_id
    ),
    user_wdr AS (
        SELECT w.player_id, SUM(w.success_amount_local) AS total_wdr
        FROM ps_bi.fct_cashout_daily w
        INNER JOIN march_ftds f ON w.player_id = f.ecr_id
        WHERE w.success_count > 0
        GROUP BY w.player_id
    )

    SELECT
        f.cohort,
        'marco' AS periodo,
        COUNT(*) AS ftd_count,
        ROUND(AVG(COALESCE(d.total_dep, 0)), 2) AS avg_deposit,
        ROUND(AVG(COALESCE(w.total_wdr, 0)), 2) AS avg_withdrawal,
        ROUND(AVG(COALESCE(d.total_dep, 0) - COALESCE(w.total_wdr, 0)), 2) AS avg_ltv,
        ROUND(SUM(COALESCE(d.total_dep, 0) - COALESCE(w.total_wdr, 0)), 2) AS total_ltv,
        COUNT(CASE WHEN COALESCE(d.total_dep,0) - COALESCE(w.total_wdr,0) < 0 THEN 1 END) AS ltv_negativo_count
    FROM march_ftds f
    LEFT JOIN user_deps d ON f.ecr_id = d.player_id
    LEFT JOIN user_wdr w ON f.ecr_id = w.player_id
    GROUP BY f.cohort

    UNION ALL

    SELECT
        f.cohort,
        f.ftd_window AS periodo,
        COUNT(*) AS ftd_count,
        ROUND(AVG(COALESCE(d.total_dep, 0)), 2) AS avg_deposit,
        ROUND(AVG(COALESCE(w.total_wdr, 0)), 2) AS avg_withdrawal,
        ROUND(AVG(COALESCE(d.total_dep, 0) - COALESCE(w.total_wdr, 0)), 2) AS avg_ltv,
        ROUND(SUM(COALESCE(d.total_dep, 0) - COALESCE(w.total_wdr, 0)), 2) AS total_ltv,
        COUNT(CASE WHEN COALESCE(d.total_dep,0) - COALESCE(w.total_wdr,0) < 0 THEN 1 END) AS ltv_negativo_count
    FROM march_ftds f
    LEFT JOIN user_deps d ON f.ecr_id = d.player_id
    LEFT JOIN user_wdr w ON f.ecr_id = w.player_id
    WHERE f.ftd_window IS NOT NULL
    GROUP BY f.cohort, f.ftd_window

    ORDER BY cohort, periodo
    """
    log.info("Query 2/4: LTV (corrigido)...")
    return query_athena(sql, database='ps_bi')


def query_recovery():
    """Taxa de recuperação — sem alteração (não depende de ftd_date)."""
    sql = """
    WITH feb_bettors AS (
        SELECT DISTINCT player_id
        FROM ps_bi.fct_player_activity_daily
        WHERE CAST(activity_date AS DATE) >= DATE '2026-02-01'
          AND CAST(activity_date AS DATE) < DATE '2026-03-01'
          AND bet_count > 0
    ),
    mar_bettors AS (
        SELECT DISTINCT player_id
        FROM ps_bi.fct_player_activity_daily
        WHERE CAST(activity_date AS DATE) >= DATE '2026-03-01'
          AND CAST(activity_date AS DATE) < current_date
          AND bet_count > 0
    ),
    old_users AS (
        SELECT ecr_id
        FROM ps_bi.dim_user
        WHERE is_test = false
          AND CAST(registration_date AS DATE) < DATE '2026-03-01'
    )

    SELECT
        COUNT(*) AS total_old_users,
        COUNT(CASE WHEN fb.player_id IS NULL THEN 1 END) AS sem_aposta_fev,
        COUNT(CASE WHEN fb.player_id IS NULL AND mb.player_id IS NOT NULL THEN 1 END)
            AS recuperados,
        ROUND(
            100.0 * COUNT(CASE WHEN fb.player_id IS NULL AND mb.player_id IS NOT NULL THEN 1 END)
            / NULLIF(COUNT(CASE WHEN fb.player_id IS NULL THEN 1 END), 0),
            2
        ) AS taxa_recuperacao_pct
    FROM old_users ou
    LEFT JOIN feb_bettors fb ON ou.ecr_id = fb.player_id
    LEFT JOIN mar_bettors mb ON ou.ecr_id = mb.player_id
    """
    log.info("Query 3/4: Recuperacao...")
    return query_athena(sql, database='ps_bi')


def query_recovered_financials():
    """Turnover, GGR, NGR dos recuperados — sem alteração."""
    sql = """
    WITH feb_bettors AS (
        SELECT DISTINCT player_id
        FROM ps_bi.fct_player_activity_daily
        WHERE CAST(activity_date AS DATE) >= DATE '2026-02-01'
          AND CAST(activity_date AS DATE) < DATE '2026-03-01'
          AND bet_count > 0
    ),
    mar_bettors AS (
        SELECT DISTINCT player_id
        FROM ps_bi.fct_player_activity_daily
        WHERE CAST(activity_date AS DATE) >= DATE '2026-03-01'
          AND CAST(activity_date AS DATE) < current_date
          AND bet_count > 0
    ),
    old_users AS (
        SELECT ecr_id
        FROM ps_bi.dim_user
        WHERE is_test = false
          AND CAST(registration_date AS DATE) < DATE '2026-03-01'
    ),
    recovered AS (
        SELECT ou.ecr_id
        FROM old_users ou
        LEFT JOIN feb_bettors fb ON ou.ecr_id = fb.player_id
        INNER JOIN mar_bettors mb ON ou.ecr_id = mb.player_id
        WHERE fb.player_id IS NULL
    )

    SELECT
        COUNT(DISTINCT r.ecr_id) AS recovered_count,
        ROUND(SUM(p.real_bet_amount_local), 2) AS turnover_real,
        ROUND(SUM(p.bet_amount_local), 2) AS turnover_total,
        ROUND(SUM(p.ggr_local), 2) AS ggr,
        ROUND(SUM(p.ngr_local), 2) AS ngr,
        ROUND(SUM(p.deposit_success_local), 2) AS total_depositos,
        ROUND(SUM(p.cashout_success_local), 2) AS total_saques,
        ROUND(AVG(p.real_bet_amount_local), 2) AS avg_turnover_dia,
        ROUND(AVG(p.ggr_local), 2) AS avg_ggr_dia
    FROM recovered r
    INNER JOIN ps_bi.fct_player_activity_daily p
        ON r.ecr_id = p.player_id
    WHERE CAST(p.activity_date AS DATE) >= DATE '2026-03-01'
      AND CAST(p.activity_date AS DATE) < current_date
    """
    log.info("Query 4/4: Financeiro Recuperados...")
    return query_athena(sql, database='ps_bi')


if __name__ == '__main__':
    print("=" * 70)
    print("  DEMANDA CRM — KPIs de Marco 2026 (CORRIGIDO)")
    print(f"  Extraido em: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("  Periodo: 01/03 ate 19/03/2026 (dados completos)")
    print("  Filtro: excluidos users com depositos antes do ftd_date")
    print("=" * 70)

    # 1. STD e 3 Deposito
    df_std = query_std_3dep()
    print("\n--- TAXA DE STD E 3o DEPOSITO (corrigido) ---")
    print("Regra: FTD no periodo, conversao ate current_date")
    print(df_std.to_string(index=False))

    # 2. LTV
    df_ltv = query_ltv()
    print("\n--- LTV (NET DEPOSIT) (corrigido) ---")
    print("LTV = depositos - saques | negativo e esperado")
    print(df_ltv.to_string(index=False))

    # 3. Recuperacao (nao depende de ftd_date, sem correcao)
    df_rec = query_recovery()
    print("\n--- TAXA DE RECUPERACAO ---")
    print("Regra: reg < marco, sem aposta em fev, com aposta em marco")
    print(df_rec.to_string(index=False))

    # 4. Financeiro recuperados
    df_fin = query_recovered_financials()
    print("\n--- FINANCEIRO DOS RECUPERADOS ---")
    print(df_fin.to_string(index=False))

    # ----------------------------------------------------------
    # Export Excel
    # ----------------------------------------------------------
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'crm_kpis_marco_2026_FINAL.xlsx')

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_std.to_excel(writer, sheet_name='STD_3Dep', index=False)
        df_ltv.to_excel(writer, sheet_name='LTV', index=False)
        df_rec.to_excel(writer, sheet_name='Recuperacao', index=False)
        df_fin.to_excel(writer, sheet_name='Financeiro_Recuperados', index=False)

    print(f"\nExcel exportado: {os.path.abspath(output_path)}")
    print("=" * 70)
