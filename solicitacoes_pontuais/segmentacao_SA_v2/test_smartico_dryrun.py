"""
Test isolado do push Smartico — le do CSV gerado pelo pipeline
e roda em DRY-RUN + CANARY pra validar payload sem disparar API.
"""
import sys
import logging
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from pipelines.segmentacao_sa_smartico import publicar_smartico

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

CSV = "output/players_segmento_SA_2026-04-28_FINAL.csv"

# CSV BR: sep=";", decimal=","
df = pd.read_csv(CSV, sep=";", decimal=",", encoding="utf-8-sig", low_memory=False)
print(f"Carregado: {len(df):,} linhas, {len(df.columns)} cols\n")

# Padroniza nome das colunas pra match com publicar_smartico
# (CSV tem UPPER pra alguns; smartico_module espera nomes originais do df)
print("Colunas no CSV:")
for c in df.columns:
    if c.lower() in ("tendencia", "lifecycle_status", "rg_status",
                      "bonus_abuse_flag", "external_id", "rating",
                      "c_category", "pvs", "player_id"):
        print(f"  {c}")

# Roda 3 cenarios
print("\n" + "=" * 70)
print("CENARIO 1: CANARY (1 jogador)")
print("=" * 70)
r1 = publicar_smartico(df, snapshot_date="2026-04-28",
                        dry_run=True, canary=True, skip_cjm=True)
print(f"Resultado: {r1}\n")

print("=" * 70)
print("CENARIO 2: AMOSTRA 100 jogadores (limit=100)")
print("=" * 70)
r2 = publicar_smartico(df, snapshot_date="2026-04-28",
                        dry_run=True, canary=False, limit=100, skip_cjm=True)
print(f"Resultado: {r2}\n")

print("=" * 70)
print("CENARIO 3: FULL — todos os jogadores (DRY-RUN)")
print("=" * 70)
r3 = publicar_smartico(df, snapshot_date="2026-04-28",
                        dry_run=True, canary=False, skip_cjm=True)
print(f"Resultado: {r3}\n")
