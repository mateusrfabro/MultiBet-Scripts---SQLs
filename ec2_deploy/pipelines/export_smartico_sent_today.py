"""
export_smartico_sent_today.py
================================================================
Reproduz a logica de diff do push_risk_to_smartico.py e exporta
CSV com os user_ext_ids que foram enviados ao Smartico hoje.

Uso (na EC2):
    python3 pipelines/export_smartico_sent_today.py
Saida:
    output/smartico_sent_YYYY-MM-DD.csv
================================================================
"""

from __future__ import annotations

import csv
import sys
from datetime import date
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = PROJECT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
sys.path.insert(0, str(PROJECT_DIR))

from pipelines.push_risk_to_smartico import (  # noqa: E402
    load_snapshots,
    df_to_players,
    diff_players,
)


def main():
    df_current, df_previous = load_snapshots()
    current = df_to_players(df_current)
    previous = df_to_players(df_previous) if df_previous is not None else None

    selected = diff_players(current, previous)

    out_file = OUTPUT_DIR / f"smartico_sent_{date.today().isoformat()}.csv"
    with out_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "user_ext_id",
            "user_id_ecr",
            "tier_atual",
            "score_norm",
            "tags_smartico_atual",
            "tags_smartico_anterior",
            "motivo",
        ])
        for p in selected:
            prev = previous.get(p.user_ext_id) if previous else None
            tags_atual = "|".join(p.smartico_tags())
            tags_ant = "|".join(prev.smartico_tags()) if prev else ""
            motivo = "NOVO" if prev is None else "MUDOU_TAGS"
            w.writerow([
                p.user_ext_id,
                p.user_id,
                p.tier,
                f"{p.score_norm:.2f}",
                tags_atual,
                tags_ant,
                motivo,
            ])

    print(f"OK: {len(selected)} jogadores exportados em {out_file}")


if __name__ == "__main__":
    main()
