"""
Script de Validação Final — Backfill CRM Daily Performance
===========================================================
Executa auditoria completa após o backfill das 853 campanhas.

Uso:
    python pipelines/validacao_backfill.py
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova


def fmt_brl(valor) -> str:
    try:
        v = float(valor)
        sinal = "-" if v < 0 else ""
        return f"{sinal}R$ {abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(valor)


def run_validacao():
    print(f"\n{'='*80}")
    print(f"  VALIDACAO FINAL — BACKFILL CRM DAILY PERFORMANCE 2026")
    print(f"{'='*80}\n")

    # =========================================================================
    # 1. CONTAGEM TOTAL
    # =========================================================================
    r = execute_supernova("""
        SELECT
            COUNT(*) as total_registros,
            COUNT(DISTINCT campanha_id) as campanhas,
            COUNT(DISTINCT period) as periodos_tipos,
            MIN(updated_at) as primeiro,
            MAX(updated_at) as ultimo
        FROM multibet.fact_crm_daily_performance
    """, fetch=True)

    total_reg = r[0][0]
    total_camp = r[0][1]
    print(f"  1. CONTAGEM TOTAL")
    print(f"     Registros:            {total_reg}")
    print(f"     Campanhas distintas:  {total_camp}")
    print(f"     Tipos de periodo:     {r[0][2]}")
    print(f"     Primeiro insert:      {r[0][3]}")
    print(f"     Ultimo insert:        {r[0][4]}")

    # Detalhar por período
    r2 = execute_supernova("""
        SELECT period, COUNT(*) as cnt
        FROM multibet.fact_crm_daily_performance
        GROUP BY period ORDER BY period
    """, fetch=True)
    for row in r2:
        print(f"     {row[0]:10s}: {row[1]} linhas")
    print()

    # =========================================================================
    # 2. CHECK DE NULOS (NGR ou ROI nulo pode indicar falha)
    # =========================================================================
    print(f"  2. CHECK DE NULOS")

    # Campanhas DURING com NGR nulo ou zero
    r3 = execute_supernova("""
        SELECT campanha_id, campanha_name,
            financeiro->>'ngr_brl' as ngr,
            financeiro->>'total_users' as users
        FROM multibet.fact_crm_daily_performance
        WHERE period = 'DURING'
          AND (financeiro->>'ngr_brl' IS NULL
               OR (financeiro->>'ngr_brl')::numeric = 0)
        ORDER BY campanha_id
        LIMIT 20
    """, fetch=True)
    print(f"     Campanhas DURING com NGR=0 ou NULL: {len(r3)}")
    if r3:
        for row in r3[:5]:
            print(f"       {row[0]:25s} | {str(row[1])[:40]:40s} | NGR={row[2]} users={row[3]}")
        if len(r3) > 5:
            print(f"       ... e mais {len(r3) - 5}")

    # Campanhas DURING com comparativo vazio ou ROI nulo
    r4 = execute_supernova("""
        SELECT campanha_id, campanha_name,
            comparativo->>'roi' as roi,
            comparativo->>'ngr_incremental' as ngr_inc
        FROM multibet.fact_crm_daily_performance
        WHERE period = 'DURING'
          AND (comparativo = '{}'::jsonb
               OR comparativo->>'roi' IS NULL)
        ORDER BY campanha_id
        LIMIT 20
    """, fetch=True)
    print(f"     Campanhas DURING com ROI nulo:      {len(r4)}")
    if r4:
        for row in r4[:5]:
            print(f"       {row[0]:25s} | {str(row[1])[:40]:40s} | ROI={row[2]} NGR_inc={row[3]}")
        if len(r4) > 5:
            print(f"       ... e mais {len(r4) - 5}")
    print()

    # =========================================================================
    # 3. TOP 5 SUCESSOS vs TOP 5 FALHAS
    # =========================================================================
    print(f"  3. TOP 5 SUCESSOS vs TOP 5 FALHAS")

    # Top 5 maior ROI
    r5 = execute_supernova("""
        SELECT campanha_id, campanha_name,
            (comparativo->>'roi')::numeric as roi,
            (comparativo->>'ngr_incremental')::numeric as ngr_inc,
            (comparativo->>'custo_total')::numeric as custo,
            (financeiro->>'total_users')::int as users
        FROM multibet.fact_crm_daily_performance
        WHERE period = 'DURING'
          AND comparativo->>'roi' IS NOT NULL
        ORDER BY (comparativo->>'roi')::numeric DESC
        LIMIT 5
    """, fetch=True)
    print(f"\n     --- TOP 5 MAIOR ROI ---")
    for row in r5:
        print(f"     {row[0]:25s} | {str(row[1])[:35]:35s} | ROI={float(row[2]):>8.2f}x | NGR_inc={fmt_brl(row[3])} | Custo={fmt_brl(row[4])} | Users={row[5]}")

    # Top 5 menor ROI (piores)
    r6 = execute_supernova("""
        SELECT campanha_id, campanha_name,
            (comparativo->>'roi')::numeric as roi,
            (comparativo->>'ngr_incremental')::numeric as ngr_inc,
            (comparativo->>'custo_total')::numeric as custo,
            (financeiro->>'total_users')::int as users
        FROM multibet.fact_crm_daily_performance
        WHERE period = 'DURING'
          AND comparativo->>'roi' IS NOT NULL
        ORDER BY (comparativo->>'roi')::numeric ASC
        LIMIT 5
    """, fetch=True)
    print(f"\n     --- TOP 5 MENOR ROI (piores) ---")
    for row in r6:
        print(f"     {row[0]:25s} | {str(row[1])[:35]:35s} | ROI={float(row[2]):>8.2f}x | NGR_inc={fmt_brl(row[3])} | Custo={fmt_brl(row[4])} | Users={row[5]}")
    print()

    # =========================================================================
    # 4. LOG DE ALERTAS — Verificar nomes legíveis
    # =========================================================================
    print(f"  4. LOG DE ALERTAS")

    # Campanhas sem nome Smartico (ainda AUTO_MAP)
    r7 = execute_supernova("""
        SELECT COUNT(DISTINCT campanha_id) as sem_nome
        FROM multibet.fact_crm_daily_performance
        WHERE campanha_name LIKE 'AUTO_MAP_%'
    """, fetch=True)
    r8 = execute_supernova("""
        SELECT COUNT(DISTINCT campanha_id) as com_nome
        FROM multibet.fact_crm_daily_performance
        WHERE campanha_name NOT LIKE 'AUTO_MAP_%'
    """, fetch=True)
    print(f"     Com nome Smartico:  {r8[0][0]} campanhas")
    print(f"     Sem nome (fallback):{r7[0][0]} campanhas")

    # Verificar integridade: NGR = GGR - BTR - RCA
    r9 = execute_supernova("""
        SELECT campanha_id,
            (financeiro->>'ggr_brl')::numeric as ggr,
            (financeiro->>'btr_brl')::numeric as btr,
            (financeiro->>'rca_brl')::numeric as rca,
            (financeiro->>'ngr_brl')::numeric as ngr
        FROM multibet.fact_crm_daily_performance
        WHERE period = 'DURING'
          AND ABS(
              (financeiro->>'ngr_brl')::numeric
              - ((financeiro->>'ggr_brl')::numeric
                 - (financeiro->>'btr_brl')::numeric
                 - (financeiro->>'rca_brl')::numeric)
          ) > 0.01
        LIMIT 5
    """, fetch=True)
    if r9:
        print(f"\n     ALERTA: {len(r9)} campanhas com NGR divergente de GGR-BTR-RCA!")
        for row in r9:
            print(f"       {row[0]}: GGR={row[1]} BTR={row[2]} RCA={row[3]} NGR={row[4]}")
    else:
        print(f"     Integridade NGR: OK (todos = GGR - BTR - RCA)")

    print(f"\n{'='*80}")
    print(f"  VALIDACAO CONCLUIDA")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_validacao()
