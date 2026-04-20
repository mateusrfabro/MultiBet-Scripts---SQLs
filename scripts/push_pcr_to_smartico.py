"""
push_pcr_to_smartico.py
================================================================
Publica os ratings PCR (Player Credit Rating) no Smartico via S2S API.

Arquitetura (replica push_risk_matrix_to_smartico.py):
    - 1 tag de rating por jogador (prefixo PCR_RATING_*):
        PCR_RATING_S | PCR_RATING_A | PCR_RATING_B
        PCR_RATING_C | PCR_RATING_D | PCR_RATING_E
    - Operacao ATOMICA:
        * "^core_external_markers": ["PCR_RATING_*"]   -> remove todas as nossas antigas
        * "+core_external_markers": [tag]              -> adiciona a nova
      Preserva tags de outras integracoes (RISK_*, BONUS_*, etc.).

Fonte:
    multibet.pcr_ratings (Super Nova DB)
    Snapshot mais recente = MAX(snapshot_date)

Dedup (doc Smartico: update_profile so se mudou):
    Por padrao, compara snapshot atual vs anterior e envia apenas jogadores
    cujo rating mudou (ex: B -> A). Use --force pra ignorar o diff.

Modos de uso:
    # Dry-run com 10 jogadores, salva JSON pra review
    python scripts/push_pcr_to_smartico.py --dry-run --limit 10

    # Canary (Fase 1): seleciona 1 jogador seguro (rating B ou C)
    python scripts/push_pcr_to_smartico.py --pick-canary

    # Fase 1: subir 1 jogador especifico
    python scripts/push_pcr_to_smartico.py --user 12345 --skip-cjm --confirm

    # Fase 2: arquivo CSV com lista de user_ext_id
    python scripts/push_pcr_to_smartico.py --file amostra.csv --skip-cjm --confirm

    # Fase 3: producao full (so diffs)
    python scripts/push_pcr_to_smartico.py --skip-cjm --confirm

    # Force (ignora diff)
    python scripts/push_pcr_to_smartico.py --skip-cjm --confirm --force

Dependencias:
    pip install pandas psycopg2-binary sshtunnel python-dotenv requests
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
REPORTS_DIR = PROJECT_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(PROJECT_DIR))

from db.smartico_api import SmarticoClient, SmarticoEvent  # noqa: E402
from db.supernova import get_supernova_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("push_pcr")

# ---------------------------------------------------------------------------
# Rating -> Smartico tag
# ---------------------------------------------------------------------------

RATING_TO_SMARTICO: Dict[str, str] = {
    "S": "PCR_RATING_S",
    "A": "PCR_RATING_A",
    "B": "PCR_RATING_B",
    "C": "PCR_RATING_C",
    "D": "PCR_RATING_D",
    "E": "PCR_RATING_E",
}

TAG_PREFIX_PATTERN = "PCR_RATING_*"  # usado no ^core_external_markers


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@dataclass
class PlayerPcr:
    player_id: str
    user_ext_id: str
    rating: str
    pvs: float
    snapshot_date: Optional[str] = None

    def smartico_tag(self) -> Optional[str]:
        return RATING_TO_SMARTICO.get(self.rating)


def _query_snapshot(cursor, snapshot_date) -> pd.DataFrame:
    sql = """
        SELECT
            player_id,
            external_id AS user_ext_id,
            rating,
            pvs
        FROM multibet.pcr_ratings
        WHERE snapshot_date = %s
          AND external_id IS NOT NULL
          AND rating IN ('S', 'A', 'B', 'C', 'D', 'E')
    """
    cursor.execute(sql, (snapshot_date,))
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    # Postgres retorna NUMERIC como decimal.Decimal - converter pra float
    if "pvs" in df.columns:
        df["pvs"] = pd.to_numeric(df["pvs"], errors="coerce").astype(float)
    return df


def _get_last_two_snapshot_dates(cursor) -> List:
    cursor.execute(
        "SELECT DISTINCT snapshot_date FROM multibet.pcr_ratings "
        "ORDER BY snapshot_date DESC LIMIT 2"
    )
    return [r[0] for r in cursor.fetchall()]


def load_current_and_previous_snapshots() -> Tuple[
    Tuple[pd.DataFrame, str], Optional[Tuple[pd.DataFrame, str]]
]:
    """Retorna ((df_atual, date_atual), (df_anterior, date_anterior) ou None)."""
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            dates = _get_last_two_snapshot_dates(cur)
            if not dates:
                raise RuntimeError("Nenhum snapshot em multibet.pcr_ratings.")
            log.info("Snapshot dates encontrados: %s", dates)

            df_current = _query_snapshot(cur, dates[0])
            log.info("Snapshot atual (%s): %d jogadores", dates[0], len(df_current))

            result_previous = None
            if len(dates) > 1:
                df_previous = _query_snapshot(cur, dates[1])
                log.info(
                    "Snapshot anterior (%s): %d jogadores",
                    dates[1],
                    len(df_previous),
                )
                result_previous = (df_previous, str(dates[1]))
            else:
                log.warning(
                    "So ha 1 snapshot na base - primeiro run (sem diff possivel)."
                )
    finally:
        conn.close()
        tunnel.stop()

    return (df_current, str(dates[0])), result_previous


def _clean_ext_id(raw) -> Optional[str]:
    """Normaliza user_ext_id (trata None/NaN/'12345.0'/espacos)."""
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


def dataframe_to_players(
    df: pd.DataFrame, snapshot_date: Optional[str] = None
) -> Dict[str, PlayerPcr]:
    """Converte df -> dict[user_ext_id -> PlayerPcr]."""
    players: Dict[str, PlayerPcr] = {}
    for _, row in df.iterrows():
        ext_id = _clean_ext_id(row.get("user_ext_id"))
        if ext_id is None:
            continue
        rating = str(row.get("rating") or "").strip().upper()
        if rating not in RATING_TO_SMARTICO:
            continue
        players[ext_id] = PlayerPcr(
            player_id=str(row["player_id"]),
            user_ext_id=ext_id,
            rating=rating,
            pvs=float(row.get("pvs") or 0),
            snapshot_date=snapshot_date,
        )
    return players


# ---------------------------------------------------------------------------
# Diff (doc Smartico: update_profile so se mudou)
# ---------------------------------------------------------------------------


def diff_players(
    current: Dict[str, PlayerPcr],
    previous: Optional[Dict[str, PlayerPcr]],
) -> List[PlayerPcr]:
    """Retorna jogadores cujo rating mudou vs snapshot anterior."""
    if previous is None:
        log.info("Sem snapshot anterior - baseline inicial: enviando todos (%d)", len(current))
        return list(current.values())

    changed: List[PlayerPcr] = []
    for ext_id, player in current.items():
        prev_player = previous.get(ext_id)
        if prev_player is None:
            changed.append(player)  # jogador novo no snapshot
            continue
        if player.rating != prev_player.rating:
            changed.append(player)

    log.info(
        "Diff: %d de %d jogadores mudaram de rating (vs snapshot anterior)",
        len(changed),
        len(current),
    )
    return changed


# ---------------------------------------------------------------------------
# Canary picker
# ---------------------------------------------------------------------------


def pick_canary_user() -> Optional[PlayerPcr]:
    """
    Seleciona 1 jogador SEGURO pra Fase 1:
      - rating = 'B' ou 'C' (meio-termo, nao extremo)
      - user_ext_id nao-nulo/nao-vazio
      - PVS entre P25 e P75 do rating (nao borderline)
    """
    (df_current, current_date), _ = load_current_and_previous_snapshots()

    mask = (
        df_current["rating"].isin(["B", "C"])
        & df_current["user_ext_id"].notna()
    )
    candidates = df_current[mask].copy()
    if candidates.empty:
        log.error("Nenhum candidato canary encontrado (rating B ou C).")
        return None

    # Evita borderline: pega quem tem PVS entre P25 e P75 do proprio rating
    def within_iqr(group: pd.DataFrame) -> pd.DataFrame:
        q1, q3 = group["pvs"].quantile(0.25), group["pvs"].quantile(0.75)
        return group[(group["pvs"] >= q1) & (group["pvs"] <= q3)]

    candidates = (
        candidates.groupby("rating", group_keys=False).apply(within_iqr).reset_index(drop=True)
    )
    if candidates.empty:
        log.error("Nenhum candidato no IQR dos ratings B/C.")
        return None

    row = candidates.sample(n=1, random_state=42).iloc[0]
    ext_id = _clean_ext_id(row.get("user_ext_id"))
    if ext_id is None:
        log.error("Candidato sorteado tem user_ext_id invalido.")
        return None
    return PlayerPcr(
        player_id=str(row["player_id"]),
        user_ext_id=ext_id,
        rating=str(row["rating"]).upper(),
        pvs=float(row.get("pvs") or 0),
        snapshot_date=current_date,
    )


# ---------------------------------------------------------------------------
# Event building
# ---------------------------------------------------------------------------


def build_events(
    client: SmarticoClient,
    players: List[PlayerPcr],
    skip_cjm: bool,
) -> List[SmarticoEvent]:
    """
    Monta 1 evento update_profile por jogador (operacao atomica):
        ^core_external_markers: ["PCR_RATING_*"]
        +core_external_markers: [tag atual]
    """
    events: List[SmarticoEvent] = []
    for p in players:
        tag = p.smartico_tag()
        if not tag:
            log.debug("Pulando %s: rating %s sem mapeamento", p.user_ext_id, p.rating)
            continue
        ev = client.build_external_markers_event(
            user_ext_id=p.user_ext_id,
            remove_pattern=[TAG_PREFIX_PATTERN],
            add_tags=[tag],
            skip_cjm=skip_cjm,
        )
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def save_dry_run_report(events: List[SmarticoEvent], players: List[PlayerPcr]) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = REPORTS_DIR / f"smartico_pcr_dryrun_{ts}.json"
    data = {
        "generated_at": datetime.now().isoformat(),
        "total_events": len(events),
        "sample": [
            {
                "player": {
                    "player_id": p.player_id,
                    "user_ext_id": p.user_ext_id,
                    "rating": p.rating,
                    "pvs": p.pvs,
                    "snapshot_date": p.snapshot_date,
                    "tag_aplicada": p.smartico_tag(),
                },
                "payload": e.to_dict(),
            }
            for p, e in zip(players[:20], events[:20])
        ],
    }
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Dry-run JSON salvo em: %s", out)
    return out


def save_run_log(result: Dict, events_count: int) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = REPORTS_DIR / f"smartico_pcr_push_{ts}.log"
    lines = [
        f"Smartico PCR push run @ {datetime.now().isoformat()}",
        f"Total eventos preparados: {events_count}",
        f"Enviados com sucesso:     {result.get('sent', 0)}",
        f"Falhas:                   {result.get('failed', 0)}",
        "",
        "Erros (amostra):",
        json.dumps(result.get("errors", []), indent=2, ensure_ascii=False),
    ]
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("Log do run salvo em: %s", out)
    return out


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def _lookup_user_any_snapshot(user_ext_id: str) -> Optional[PlayerPcr]:
    """Fallback: busca jogador no snapshot mais recente onde aparece."""
    ext_id = str(user_ext_id).strip()
    ext_variants = [ext_id]
    if not ext_id.endswith(".0"):
        ext_variants.append(ext_id + ".0")

    placeholders = ", ".join(["%s"] * len(ext_variants))
    sql = f"""
        SELECT snapshot_date, player_id, external_id AS user_ext_id, rating, pvs
        FROM multibet.pcr_ratings
        WHERE external_id IN ({placeholders})
          AND rating IN ('S','A','B','C','D','E')
        ORDER BY snapshot_date DESC
        LIMIT 1
    """
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, ext_variants)
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            data = dict(zip(cols, row))
            snap_date = str(data["snapshot_date"])
            clean_id = _clean_ext_id(data["user_ext_id"])
            log.info(
                "Fallback: jogador %s encontrado no snapshot %s (rating=%s)",
                clean_id,
                snap_date,
                data["rating"],
            )
            return PlayerPcr(
                player_id=str(data["player_id"]),
                user_ext_id=clean_id or ext_id,
                rating=str(data["rating"]).upper(),
                pvs=float(data.get("pvs") or 0),
                snapshot_date=snap_date,
            )
    finally:
        conn.close()
        tunnel.stop()


def filter_by_user(
    players: Dict[str, PlayerPcr], user_ext_id: str
) -> List[PlayerPcr]:
    p = players.get(str(user_ext_id).strip())
    if p is not None:
        return [p]
    log.warning(
        "user_ext_id %s nao encontrado no snapshot atual. Buscando historico...",
        user_ext_id,
    )
    fallback = _lookup_user_any_snapshot(user_ext_id)
    if fallback is None:
        log.error("user_ext_id %s nao encontrado em NENHUM snapshot.", user_ext_id)
        return []
    return [fallback]


def filter_by_file(
    players: Dict[str, PlayerPcr], csv_path: Path
) -> List[PlayerPcr]:
    df = pd.read_csv(csv_path)
    col = None
    for candidate in ("user_ext_id", "ext_id", "external_id"):
        if candidate in df.columns:
            col = candidate
            break
    if col is None:
        raise ValueError(f"CSV precisa ter coluna user_ext_id/ext_id/external_id: {csv_path}")
    ids = {str(x).strip() for x in df[col].dropna()}
    result = [players[i] for i in ids if i in players]
    log.info(
        "Filtro por arquivo: %d ids no CSV, %d encontrados no snapshot",
        len(ids),
        len(result),
    )
    return result


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="Sobe ratings PCR (Player Credit Rating) pro Smartico"
    )
    ap.add_argument("--dry-run", action="store_true", help="Nao chama API (gera JSON)")
    ap.add_argument("--confirm", action="store_true", help="Envia de fato (sem isso = dry-run)")
    ap.add_argument("--user", type=str, help="user_ext_id especifico (Fase 1)")
    ap.add_argument("--file", type=str, help="CSV com lista de user_ext_id (Fase 2)")
    ap.add_argument("--limit", type=int, default=0, help="Processar no maximo N jogadores")
    ap.add_argument(
        "--pick-canary",
        action="store_true",
        help="Apenas seleciona jogador canary seguro e imprime no stdout",
    )
    ap.add_argument(
        "--skip-cjm",
        action="store_true",
        help="payload.skip_cjm=true (nao dispara Automation - Fases 1-3)",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Ignora diff vs snapshot anterior (envia todos)",
    )
    ap.add_argument("--batch-size", type=int, default=1000, help="Tamanho do batch")
    args = ap.parse_args()

    # Modo --pick-canary: so seleciona e sai
    if args.pick_canary:
        log.info("Selecionando canary user...")
        player = pick_canary_user()
        if player is None:
            print("\n[ERRO] Nenhum candidato canary encontrado.")
            sys.exit(1)
        print("\n" + "=" * 60)
        print("CANARY USER SELECIONADO (Fase 1)")
        print("=" * 60)
        print(f"player_id:    {player.player_id}")
        print(f"user_ext_id:  {player.user_ext_id}")
        print(f"rating:       {player.rating}")
        print(f"pvs:          {player.pvs}")
        print(f"tag aplicada: {player.smartico_tag()}")
        print("=" * 60)
        print("\nProximo passo (apos OK do Raphael):")
        print(f"  python scripts/push_pcr_to_smartico.py \\")
        print(f"      --user {player.user_ext_id} --skip-cjm --confirm")
        return

    # Carrega snapshots
    (df_current, current_date), prev_result = load_current_and_previous_snapshots()
    current_players = dataframe_to_players(df_current, current_date)
    previous_players = (
        dataframe_to_players(prev_result[0], prev_result[1])
        if prev_result is not None
        else None
    )

    # Seleciona jogadores
    if args.user:
        selected = filter_by_user(current_players, args.user)
    elif args.file:
        selected = filter_by_file(current_players, Path(args.file))
    elif args.force:
        log.info("--force: ignorando diff, enviando TODOS do snapshot atual")
        selected = list(current_players.values())
    else:
        selected = diff_players(current_players, previous_players)

    if args.limit and args.limit > 0:
        selected = selected[: args.limit]
        log.info("Limitado a %d jogadores (--limit)", len(selected))

    if not selected:
        log.info("Nenhum jogador a processar. Nada a fazer.")
        return

    # Dry-run se nao --confirm
    is_dry = args.dry_run or not args.confirm
    if args.confirm and args.dry_run:
        log.warning("--dry-run + --confirm: dry-run prevalece.")
        is_dry = True

    client = SmarticoClient(dry_run=is_dry)
    log.info(
        "Modo: %s | skip_cjm=%s | brand=%s",
        "DRY-RUN" if is_dry else "LIVE",
        args.skip_cjm,
        client.brand,
    )

    # Monta eventos
    events = build_events(client, selected, skip_cjm=args.skip_cjm)
    log.info("Eventos montados: %d", len(events))

    if is_dry:
        save_dry_run_report(events, selected)
        log.info("DRY-RUN finalizado. Nenhuma chamada a API foi feita.")
        return

    # Envio real
    result = client.send_events(events, batch_size=args.batch_size)
    log.info(
        "Envio concluido: sent=%d failed=%d total=%d",
        result["sent"],
        result["failed"],
        result["total"],
    )
    save_run_log(result, len(events))

    if result["failed"] > 0:
        log.warning("Houve %d falhas - verifique o log", result["failed"])
        sys.exit(2)


if __name__ == "__main__":
    main()
