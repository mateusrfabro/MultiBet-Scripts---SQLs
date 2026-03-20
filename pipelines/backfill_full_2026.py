"""
Backfill: Carga em massa de campanhas CRM 2026 (Motor de Alta Performance)
===========================================================================
Descobre campanhas (entity_id) no BigQuery e processa em paralelo com
ThreadPoolExecutor(max_workers=5).

Otimizações:
  - setup_table() executado 1x antes do loop (não 853x)
  - ThreadPoolExecutor com 5 workers paralelos
  - Nomes capturados do Smartico (dm_audience.audience_name)
  - Custos diferenciados por provider_id (DisparoPro/PushFY/Comtele)

Uso:
    python -u pipelines/backfill_full_2026.py
"""

import logging
import sys
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.bigquery import query_bigquery
from pipelines.crm_daily_performance import run_pipeline, setup_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_WORKERS = 5


# =============================================================================
# Descoberta de campanhas com nomes do Smartico
# =============================================================================
SQL_DISCOVER = """
SELECT
    b.entity_id,
    a.audience_name,
    MIN(DATE(b.fact_date)) AS campanha_start,
    MAX(DATE(b.fact_date)) AS campanha_end,
    COUNT(*)               AS total_bonus_events
FROM `smartico-bq6.dwh_ext_24105.j_bonuses` b
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_audience` a
    ON b.entity_id = a.audience_id
    AND a.label_id = 24105
WHERE DATE(b.fact_date) >= '2026-01-01'
  AND b.bonus_status_id = 3
  AND b.entity_id IS NOT NULL
GROUP BY b.entity_id, a.audience_name
ORDER BY campanha_start, b.entity_id
"""


def discover_campanhas():
    log.info("Consultando BigQuery para descobrir campanhas...")
    df = query_bigquery(SQL_DISCOVER)
    com_nome = df["audience_name"].notna().sum()
    log.info(f"  {len(df)} campanhas ({com_nome} com nome Smartico).")
    return df


# =============================================================================
# Worker: processa 1 campanha (thread-safe)
# =============================================================================
def process_campanha(row, idx, total):
    """Processa uma campanha individual. Chamado por cada thread."""
    entity_id = str(row["entity_id"])
    start = str(row["campanha_start"])
    end = str(row["campanha_end"])

    campanha_id = f"ENTITY_{entity_id}"
    smartico_name = row.get("audience_name")
    if smartico_name and str(smartico_name) != "None" and str(smartico_name).strip():
        campanha_name = str(smartico_name).strip()
    else:
        campanha_name = f"AUTO_MAP_{entity_id}"

    progresso = f"[{idx}/{total}]"
    log.info(f"{progresso} {campanha_id} | {campanha_name[:50]}")

    try:
        result = run_pipeline(
            campanha_id=campanha_id,
            campanha_name=campanha_name,
            campanha_start=start,
            campanha_end=end,
            entity_id=entity_id,
            skip_setup=True,   # setup feito 1x antes do loop
            quiet=True,        # sem print no terminal (paralelo)
        )

        if result is not None and not result.empty:
            log.info(f"{progresso} OK — {campanha_id}")
            return ("OK", campanha_id, campanha_name)
        else:
            log.warning(f"{progresso} SKIP — {campanha_id}")
            return ("SKIP", campanha_id, campanha_name)

    except Exception as e:
        log.error(f"{progresso} ERRO — {campanha_id}: {e}")
        return ("ERRO", campanha_id, str(e))


# =============================================================================
# Backfill principal com ThreadPoolExecutor
# =============================================================================
def backfill():
    inicio = time.time()

    # 1. Descobrir campanhas
    df_campanhas = discover_campanhas()
    if df_campanhas.empty:
        log.warning("Nenhuma campanha encontrada.")
        return

    total = len(df_campanhas)

    # 2. Setup tabelas 1x (não dentro do loop!)
    log.info("Executando setup_table() uma unica vez...")
    setup_table()
    log.info("Setup concluido.")

    print(f"\n{'='*80}")
    print(f"  BACKFILL CRM DAILY PERFORMANCE — 2026 (MOTOR DE ALTA PERFORMANCE)")
    print(f"  Total: {total} campanhas | Workers: {MAX_WORKERS} threads")
    print(f"  Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")

    # 3. Processar em paralelo
    resultados = {"OK": 0, "SKIP": 0, "ERRO": 0}
    falhas = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="worker") as executor:
        futures = {}
        for idx, row in df_campanhas.iterrows():
            future = executor.submit(process_campanha, row, idx + 1, total)
            futures[future] = idx + 1

        for future in as_completed(futures):
            try:
                status, camp_id, detail = future.result()
                resultados[status] += 1
                if status == "ERRO":
                    falhas.append((camp_id, detail))

                # Log de progresso a cada 10 campanhas
                done = sum(resultados.values())
                if done % 10 == 0 or done == total:
                    elapsed = time.time() - inicio
                    rate = done / elapsed * 60 if elapsed > 0 else 0
                    eta_min = (total - done) / rate if rate > 0 else 0
                    log.info(
                        f"  PROGRESSO: {done}/{total} "
                        f"(OK={resultados['OK']} SKIP={resultados['SKIP']} ERRO={resultados['ERRO']}) "
                        f"| {rate:.1f} camp/min | ETA: {eta_min:.0f}min"
                    )
            except Exception as e:
                resultados["ERRO"] += 1
                falhas.append(("UNKNOWN", str(e)))

    # 4. Resumo final
    elapsed = time.time() - inicio
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)

    print(f"\n{'='*80}")
    print(f"  BACKFILL CONCLUIDO")
    print(f"{'='*80}")
    print(f"  Total campanhas:     {total}")
    print(f"  Sucesso:             {resultados['OK']}")
    print(f"  Skipped (vazio):     {resultados['SKIP']}")
    print(f"  Falhas:              {resultados['ERRO']}")
    print(f"  Tempo total:         {mins}m {secs}s")
    print(f"  Performance:         {total / (elapsed/60):.1f} campanhas/min")
    print(f"  Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if falhas:
        print(f"\n  --- FALHAS ({len(falhas)}) ---")
        for camp_id, err in falhas[:20]:  # limitar a 20
            print(f"  {camp_id}: {err[:100]}")
        if len(falhas) > 20:
            print(f"  ... e mais {len(falhas) - 20} falhas")

    print()


if __name__ == "__main__":
    backfill()
