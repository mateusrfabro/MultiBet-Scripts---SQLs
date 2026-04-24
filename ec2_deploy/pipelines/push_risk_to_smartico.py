"""
push_risk_to_smartico.py — EC2 Edition
================================================================
Publica as tags da Matriz de Risco v2 no Smartico via S2S API.
Versao simplificada para cron diario na EC2.

Comportamento padrao (cron):
    - Compara snapshot atual vs anterior, envia apenas diffs
    - skip_cjm=True SEMPRE (nao dispara automations/jornadas)
    - Log em pipelines/logs/push_smartico_YYYY-MM-DD.log

Uso:
    # Producao (diffs apenas, padrao do cron)
    python3 pipelines/push_risk_to_smartico.py

    # Force (reenvia todos — usar com cuidado)
    python3 pipelines/push_risk_to_smartico.py --force

    # Dry-run (nao chama API)
    python3 pipelines/push_risk_to_smartico.py --dry-run

Dependencias:
    pip install pandas psycopg2-binary sshtunnel python-dotenv requests
================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Setup paths — EC2: /home/ec2-user/multibet/pipelines/push_risk_to_smartico.py
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = PROJECT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(PROJECT_DIR))

from db.smartico_api import SmarticoClient, SmarticoEvent  # noqa: E402
from db.supernova import get_supernova_connection  # noqa: E402

# Logging — arquivo diario + stdout
log_file = LOG_DIR / f"push_smartico_{date.today().isoformat()}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(str(log_file), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("push_smartico")

# ---------------------------------------------------------------------------
# Mapeamento de tags
# ---------------------------------------------------------------------------

TAG_COLUMNS_TO_SMARTICO: Dict[str, str] = {
    "regular_depositor": "RISK_REGULAR_DEPOSITOR",
    "promo_only": "RISK_PROMO_ONLY",
    "zero_risk_player": "RISK_ZERO_RISK_PLAYER",
    "fast_cashout": "RISK_FAST_CASHOUT",
    "sustained_player": "RISK_SUSTAINED_PLAYER",
    "non_bonus_depositor": "RISK_NON_BONUS_DEPOSITOR",
    "promo_chainer": "RISK_PROMO_CHAINER",
    "cashout_and_run": "RISK_CASHOUT_AND_RUN",
    "reinvest_player": "RISK_REINVEST_PLAYER",
    "non_promo_player": "RISK_NON_PROMO_PLAYER",
    "engaged_player": "RISK_ENGAGED_PLAYER",
    "rg_alert_player": "RISK_RG_ALERT_PLAYER",
    "behav_risk_player": "RISK_BEHAV_RISK_PLAYER",
    "potencial_abuser": "RISK_POTENCIAL_ABUSER",
    "player_reengaged": "RISK_PLAYER_REENGAGED",
    "sleeper_low_player": "RISK_SLEEPER_LOW_PLAYER",
    "vip_whale_player": "RISK_VIP_WHALE_PLAYER",
    "winback_hi_val_player": "RISK_WINBACK_HI_VAL_PLAYER",
    "behav_slotgamer": "RISK_BEHAV_SLOTGAMER",
    "multi_game_player": "RISK_MULTI_GAME_PLAYER",
    "rollback_player": "RISK_ROLLBACK_PLAYER",
}

TIER_TO_SMARTICO: Dict[str, str] = {
    "Muito Bom": "RISK_TIER_MUITO_BOM",
    "Bom": "RISK_TIER_BOM",
    "Mediano": "RISK_TIER_MEDIANO",
    "Ruim": "RISK_TIER_RUIM",
    "Muito Ruim": "RISK_TIER_MUITO_RUIM",
}

TAG_COLUMNS = list(TAG_COLUMNS_TO_SMARTICO.keys())
TAG_SELECT_LIST = ", ".join(TAG_COLUMNS)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@dataclass
class PlayerSnapshot:
    user_id: str
    user_ext_id: str
    tier: str
    score_norm: float
    active_tag_columns: List[str]

    def smartico_tags(self) -> List[str]:
        tags: List[str] = []
        tier_tag = TIER_TO_SMARTICO.get(self.tier)
        if tier_tag:
            tags.append(tier_tag)
        for col in self.active_tag_columns:
            mapped = TAG_COLUMNS_TO_SMARTICO.get(col)
            if mapped:
                tags.append(mapped)
        return sorted(set(tags))


def _clean_ext_id(raw) -> Optional[str]:
    if raw is None:
        return None
    try:
        if pd.isna(raw):
            return None
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s or s.lower() in ("none", "nan"):
        return None
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _tag_int(val) -> int:
    if val is None:
        return 0
    try:
        if pd.isna(val):
            return 0
    except (TypeError, ValueError):
        pass
    try:
        return 1 if int(val) != 0 else 0
    except (TypeError, ValueError):
        return 0


def _query_snapshot(cursor, snapshot_date) -> pd.DataFrame:
    sql = f"""
        SELECT user_id, user_ext_id, tier, score_norm, {TAG_SELECT_LIST}
        FROM multibet.risk_tags
        WHERE snapshot_date = %s
          AND user_ext_id IS NOT NULL
          AND tier IS NOT NULL
          AND tier != 'SEM SCORE'
    """
    cursor.execute(sql, (snapshot_date,))
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)


def load_snapshots() -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT snapshot_date FROM multibet.risk_tags "
                "ORDER BY snapshot_date DESC LIMIT 2"
            )
            dates = [r[0] for r in cur.fetchall()]
            if not dates:
                raise RuntimeError("Nenhum snapshot em multibet.risk_tags.")
            log.info("Snapshot dates: %s", dates)

            df_current = _query_snapshot(cur, dates[0])
            log.info("Atual (%s): %d jogadores", dates[0], len(df_current))

            df_previous = None
            if len(dates) > 1:
                df_previous = _query_snapshot(cur, dates[1])
                log.info("Anterior (%s): %d jogadores", dates[1], len(df_previous))
    finally:
        conn.close()
        tunnel.stop()
    return df_current, df_previous


def df_to_players(df: pd.DataFrame) -> Dict[str, PlayerSnapshot]:
    players: Dict[str, PlayerSnapshot] = {}
    for _, row in df.iterrows():
        ext_id = _clean_ext_id(row.get("user_ext_id"))
        if ext_id is None:
            continue
        active = [c for c in TAG_COLUMNS if _tag_int(row.get(c)) == 1]
        players[ext_id] = PlayerSnapshot(
            user_id=str(row["user_id"]),
            user_ext_id=ext_id,
            tier=str(row["tier"]),
            score_norm=float(row.get("score_norm") or 0),
            active_tag_columns=active,
        )
    return players


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


def diff_players(
    current: Dict[str, PlayerSnapshot],
    previous: Optional[Dict[str, PlayerSnapshot]],
) -> List[PlayerSnapshot]:
    if previous is None:
        log.info("Primeiro run (sem anterior) — enviando todos (%d)", len(current))
        return list(current.values())

    changed: List[PlayerSnapshot] = []
    for ext_id, player in current.items():
        prev = previous.get(ext_id)
        if prev is None or set(player.smartico_tags()) != set(prev.smartico_tags()):
            changed.append(player)

    log.info("Diff: %d mudaram de %d total", len(changed), len(current))
    return changed


# ---------------------------------------------------------------------------
# Event building + push
# ---------------------------------------------------------------------------


def build_events(
    client: SmarticoClient, players: List[PlayerSnapshot]
) -> List[SmarticoEvent]:
    events: List[SmarticoEvent] = []
    for p in players:
        tags = p.smartico_tags()
        if not tags:
            continue
        ev = client.build_external_markers_event(
            user_ext_id=p.user_ext_id,
            remove_pattern=["RISK_*"],
            add_tags=tags,
            skip_cjm=True,  # SEMPRE skip_cjm na EC2
        )
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description="Push risk tags to Smartico (EC2)")
    ap.add_argument("--dry-run", action="store_true", help="Nao chama a API")
    ap.add_argument("--force", action="store_true", help="Envia todos (ignora diff)")
    ap.add_argument("--batch-size", type=int, default=1000)
    args = ap.parse_args()

    log.info("=== PUSH RISK MATRIX TO SMARTICO ===")
    log.info("Data: %s | dry_run=%s | force=%s", date.today(), args.dry_run, args.force)

    # Carrega snapshots
    df_current, df_previous = load_snapshots()
    current = df_to_players(df_current)
    previous = df_to_players(df_previous) if df_previous is not None else None

    # Seleciona jogadores
    if args.force:
        selected = list(current.values())
        log.info("--force: enviando todos os %d", len(selected))
    else:
        selected = diff_players(current, previous)

    if not selected:
        log.info("Nenhum jogador com mudancas. Nada a enviar.")
        return

    # Monta e envia
    client = SmarticoClient(dry_run=args.dry_run)
    events = build_events(client, selected)
    log.info("Eventos montados: %d", len(events))

    if args.dry_run:
        log.info("DRY-RUN: nenhuma chamada feita.")
        # Salva amostra
        sample = [e.to_dict() for e in events[:5]]
        out = OUTPUT_DIR / f"smartico_dryrun_{date.today().isoformat()}.json"
        out.write_text(json.dumps(sample, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("Amostra salva em %s", out)
        return

    result = client.send_events(events, batch_size=args.batch_size)
    log.info(
        "RESULTADO: sent=%d failed=%d total=%d",
        result["sent"],
        result["failed"],
        result["total"],
    )

    if result["failed"] > 0:
        log.warning("Houve %d falhas!", result["failed"])
        for err in result.get("errors", [])[:10]:
            log.warning("  erro: %s", err)
        sys.exit(2)

    log.info("=== PUSH COMPLETO ===")


if __name__ == "__main__":
    main()
