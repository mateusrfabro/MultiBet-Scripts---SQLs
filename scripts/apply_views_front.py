"""
Aplica em producao (Super Nova DB):
  1. ddl_game_image_mapping_v2.sql  → ALTER TABLE (ADD COLUMN IF NOT EXISTS)
  2. Roda pipeline game_image_mapper para popular as colunas novas
  3. ddl_views_front.sql             → CREATE OR REPLACE VIEW

Tudo idempotente e nao-destrutivo.
"""
import sys, os, logging, subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def aplicar_sql_arquivo(path):
    """Aplica SQL via psycopg2 (suporta multiplos statements separados por ;)."""
    log.info(f"Aplicando: {os.path.basename(path)}")
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    ssh, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        log.info(f"  OK")
    finally:
        conn.close()
        ssh.close()


def smoke_test():
    """Validacao basica: schema novo + cobertura das views."""
    log.info("=== SMOKE TEST ===")

    # Schema atual
    rows = execute_supernova("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='multibet' AND table_name='game_image_mapping'
        ORDER BY ordinal_position
    """, fetch=True)
    log.info(f"game_image_mapping: {len(rows)} colunas")
    for c, t in rows:
        log.info(f"  {c:<25} {t}")

    # Counts por view
    for vw in ["vw_front_top_24h", "vw_front_live_casino", "vw_front_by_vendor",
               "vw_front_by_category", "vw_front_jackpot"]:
        try:
            r = execute_supernova(f"SELECT COUNT(*) FROM multibet.{vw}", fetch=True)
            log.info(f"  {vw:<28} {r[0][0]} linhas")
        except Exception as e:
            log.error(f"  {vw}: ERRO — {e}")


def main():
    # Step 1: ALTER TABLE
    aplicar_sql_arquivo(os.path.join(PROJECT_ROOT, "pipelines", "ddl",
                                      "ddl_game_image_mapping_v2.sql"))

    # Step 2: rodar pipeline (popular as colunas novas)
    log.info("Rodando pipeline game_image_mapper.py para popular colunas novas...")
    res = subprocess.run(
        [sys.executable, os.path.join(PROJECT_ROOT, "pipelines", "game_image_mapper.py")],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=600
    )
    print(res.stdout[-2000:] if len(res.stdout) > 2000 else res.stdout)
    if res.returncode != 0:
        log.error(f"Pipeline falhou:\n{res.stderr[-2000:]}")
        sys.exit(1)

    # Step 3: CREATE VIEWS
    aplicar_sql_arquivo(os.path.join(PROJECT_ROOT, "pipelines", "ddl",
                                      "ddl_views_front.sql"))

    # Step 4: smoke test
    smoke_test()
    log.info("=== Apply concluido ===")


if __name__ == "__main__":
    main()
