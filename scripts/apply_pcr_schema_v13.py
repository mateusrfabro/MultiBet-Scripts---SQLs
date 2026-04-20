"""
apply_pcr_schema_v13.py
========================
Migracao de schema one-shot para ativar PCR v1.3 (rating NEW) no Super Nova DB.

Executa:
    1. ALTER TABLE multibet.pcr_ratings ALTER COLUMN rating TYPE VARCHAR(10)
       (amplia de VARCHAR(2) pra aceitar 'NEW'; idempotente — no-op se ja aplicado)
    2. DROP VIEW + CREATE VIEW multibet.pcr_atual
       (sem mudanca estrutural, mas garante consistencia com view da tabela ampliada)
    3. DROP VIEW + CREATE VIEW multibet.pcr_resumo
       (inclui 'NEW' no ORDER BY, posicao 7)

NAO toca dados — o snapshot atual fica como esta (sem rating NEW).
O proximo ciclo normal do pipeline PCR vai preencher o rating NEW naturalmente.

Execucao:
    python scripts/apply_pcr_schema_v13.py

Apos rodar, validar:
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = 'multibet' AND table_name = 'pcr_ratings' AND column_name = 'rating';
    -- Esperado: character varying, 10
"""

import sys
import os
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Passo 1: ampliar coluna rating
ALTER_RATING = "ALTER TABLE multibet.pcr_ratings ALTER COLUMN rating TYPE VARCHAR(10);"

# Passo 2: recriar view pcr_atual (sem mudanca, mas reforca consistencia)
DROP_VIEW_PCR_ATUAL = "DROP VIEW IF EXISTS multibet.pcr_atual;"
CREATE_VIEW_PCR_ATUAL = """
CREATE OR REPLACE VIEW multibet.pcr_atual AS
SELECT *
FROM multibet.pcr_ratings
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM multibet.pcr_ratings);
"""

# Passo 3: recriar view pcr_resumo com NEW no ORDER BY
DROP_VIEW_PCR_RESUMO = "DROP VIEW IF EXISTS multibet.pcr_resumo;"
CREATE_VIEW_PCR_RESUMO = """
CREATE OR REPLACE VIEW multibet.pcr_resumo AS
SELECT
    snapshot_date,
    rating,
    COUNT(*)                        AS jogadores,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY snapshot_date), 1) AS pct_base,
    ROUND(SUM(ggr_total), 2)       AS ggr_total,
    ROUND(AVG(ggr_total), 2)       AS ggr_medio,
    ROUND(AVG(total_deposits), 2)  AS deposito_medio,
    ROUND(AVG(num_deposits), 1)    AS num_dep_medio,
    ROUND(AVG(days_active), 1)     AS dias_ativos_medio,
    ROUND(AVG(recency_days), 1)    AS recencia_media,
    ROUND(AVG(pvs), 2)             AS pvs_medio
FROM multibet.pcr_ratings
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM multibet.pcr_ratings)
GROUP BY snapshot_date, rating
ORDER BY
    CASE rating
        WHEN 'S'   THEN 1
        WHEN 'A'   THEN 2
        WHEN 'B'   THEN 3
        WHEN 'C'   THEN 4
        WHEN 'D'   THEN 5
        WHEN 'E'   THEN 6
        WHEN 'NEW' THEN 7
    END;
"""

# Validacao pos-migracao
VALIDATE_SCHEMA = """
SELECT character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'multibet'
  AND table_name = 'pcr_ratings'
  AND column_name = 'rating';
"""

VALIDATE_VIEWS = """
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'multibet'
  AND table_name IN ('pcr_atual', 'pcr_resumo')
ORDER BY table_name;
"""


def main():
    log.info("=== PCR Schema Migration v1.3 — aplicando ===")

    # Ordem correta (PostgreSQL): DROP views -> ALTER coluna -> CREATE views.
    # Nao da pra ALTER coluna enquanto views dependem dela.

    # Passo 1: DROP views (ambas)
    log.info("1/5 DROP view pcr_atual...")
    execute_supernova(DROP_VIEW_PCR_ATUAL)
    log.info("  OK")

    log.info("2/5 DROP view pcr_resumo...")
    execute_supernova(DROP_VIEW_PCR_RESUMO)
    log.info("  OK")

    # Passo 2: ALTER agora funciona (sem views dependendo)
    log.info("3/5 ALTER TABLE rating -> VARCHAR(10)...")
    try:
        execute_supernova(ALTER_RATING)
        log.info("  OK")
    except Exception as e:
        log.warning(f"  ALTER falhou (provavelmente ja aplicado): {e}")

    # Passo 3: CREATE views novamente
    log.info("4/5 CREATE view pcr_atual...")
    execute_supernova(CREATE_VIEW_PCR_ATUAL)
    log.info("  OK")

    log.info("5/5 CREATE view pcr_resumo (com NEW no ORDER BY)...")
    execute_supernova(CREATE_VIEW_PCR_RESUMO)
    log.info("  OK")

    # Validacao
    log.info("--- Validacao pos-migracao ---")
    rows = execute_supernova(VALIDATE_SCHEMA, fetch=True)
    if rows and rows[0][0] == 10:
        log.info(f"  rating: VARCHAR({rows[0][0]}) OK")
    else:
        log.error(f"  rating: VARCHAR({rows[0][0] if rows else '?'}) — esperado 10!")

    rows = execute_supernova(VALIDATE_VIEWS, fetch=True)
    views = [r[0] for r in rows]
    if "pcr_atual" in views and "pcr_resumo" in views:
        log.info(f"  views presentes: {views}")
    else:
        log.error(f"  views faltando: tem {views}, esperado pcr_atual + pcr_resumo")

    log.info("=== Migracao concluida ===")
    log.info("Proximo ciclo do pipeline PCR vai gravar snapshot com rating NEW.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Migracao falhou: {e}", exc_info=True)
        sys.exit(1)
