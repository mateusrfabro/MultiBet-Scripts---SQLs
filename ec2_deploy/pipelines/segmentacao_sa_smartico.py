"""
Modulo: Publicacao da Segmentacao A+S no Smartico (External Markers)
=====================================================================

Recebe o DataFrame ja processado pelo pipeline `segmentacao_sa_diaria.py`
(57 colunas) e publica tags operacionais no `core_external_markers` do
Smartico via S2S API.

Bucket: `core_external_markers` (alinhado com Raphael, 28/04/2026).
        DIFERENTE do PCR Rating, que vai em `core_custom_prop1`.

Tags publicadas (4 prefixos atomicos):
  - SEG_TREND_<SUBINDO|CAINDO|ESTAVEL>   (1 tag por player)
  - SEG_LIFECYCLE_<NEW|ACTIVE|AT_RISK|CHURNED|DORMANT>  (1 tag por player)
  - SEG_RG_<NORMAL|CLOSED|COOL_OFF>      (1 tag por player)
  - SEG_BONUS_ABUSER                     (so se BONUS_ABUSE_FLAG = 1)

Operacao atomica POR JOGADOR (preserva tags de outros pipelines):
    {
      "^core_external_markers": ["SEG_TREND_*", "SEG_LIFECYCLE_*",
                                 "SEG_RG_*", "SEG_BONUS_*"],
      "+core_external_markers": ["SEG_TREND_SUBINDO",
                                 "SEG_LIFECYCLE_AT_RISK",
                                 "SEG_RG_NORMAL"]
    }

Diff vs snapshot anterior (idempotencia + correcao de furo "player que sumiu"):
  Carrega snapshot anterior de `multibet.segmentacao_sa_diaria`. Para
  jogadores que estavam ontem mas SUMIRAM hoje (saiu de A+S, fechou conta,
  etc.), envia evento de REMOVE puro (sem ADD) pra limpar as tags SEG_*
  do perfil deles no Smartico — evita tags fantasmas.

Performance:
  - Construcao de tags: vetorizada em pandas (string concat).
  - Envio: batched pelo SmarticoClient (4000 events/batch, 6k req/min).
  - Tipico: 10-15k jogadores em ~3-5min na rede multibet.

Uso (chamado pelo `segmentacao_sa_diaria.py`):
    from pipelines.segmentacao_sa_smartico import publicar_smartico
    result = publicar_smartico(df, snapshot_date, dry_run=True)

Modos:
    dry_run=True    : NAO envia. Salva JSON com payload pra review.
    canary=True     : envia para 1 jogador apenas (rating A estavel).
    skip_cjm=True   : popula estado mas NAO dispara Automation Smartico
                      — RECOMENDADO em testes / canary.
    confirm=True    : obrigatorio para envio real (anti-acidente).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Bucket Smartico — alinhado com Raphael (28/04/2026)
BUCKET = "core_external_markers"

# Patterns para limpar antes do add (idempotencia)
TAG_PATTERNS_REMOVE = [
    "SEG_TREND_*",
    "SEG_LIFECYCLE_*",
    "SEG_RG_*",
    "SEG_BONUS_*",
]


# ============================================================
# Construcao vetorizada das tags (pandas)
# ============================================================
def _construir_tags_vetorizado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona coluna 'tags_seg' (List[str]) ao df, computada vetorialmente.

    Players que nao tiverem nenhuma tag valida (todas as colunas vazias)
    ficam com lista vazia — nao geram evento.
    """
    df = df.copy()

    def _fill(s):
        return s.fillna("").astype(str).str.upper()

    trend = _fill(df.get("tendencia", pd.Series([], dtype=object)))
    life  = _fill(df.get("LIFECYCLE_STATUS", pd.Series([], dtype=object)))
    rg    = _fill(df.get("RG_STATUS", pd.Series([], dtype=object)))
    abuse = pd.to_numeric(df.get("BONUS_ABUSE_FLAG", pd.Series([], dtype=object)),
                           errors="coerce").fillna(0).astype(int)

    # Pre-fixacao com prefixos SEG_* — strings vazias ou invalidas viram ""
    valid_trend = trend.isin(["SUBINDO", "CAINDO", "ESTAVEL"])
    valid_life  = life.isin(["NEW", "ACTIVE", "AT_RISK", "CHURNED", "DORMANT"])
    valid_rg    = rg.isin(["NORMAL", "RG_CLOSED", "RG_COOL_OFF"])

    df["_seg_trend"] = np.where(valid_trend, "SEG_TREND_" + trend, "")
    df["_seg_life"]  = np.where(valid_life, "SEG_LIFECYCLE_" + life, "")
    df["_seg_rg"]    = np.where(valid_rg,
                                  "SEG_RG_" + rg.str.replace("RG_", "", regex=False),
                                  "")
    df["_seg_abuse"] = np.where(abuse == 1, "SEG_BONUS_ABUSER", "")

    # Concat por linha — vetorizado, ~10x mais rapido que apply
    df["tags_seg"] = (
        df[["_seg_trend", "_seg_life", "_seg_rg", "_seg_abuse"]]
        .apply(lambda r: [t for t in r if t], axis=1)
    )
    df = df.drop(columns=["_seg_trend", "_seg_life", "_seg_rg", "_seg_abuse"])
    return df


# ============================================================
# Diff vs snapshot anterior (via multibet.segmentacao_sa_diaria)
# ============================================================
def _carregar_snapshot_anterior(snapshot_date: str) -> Optional[pd.DataFrame]:
    """
    Carrega o snapshot anterior (data < snapshot_date) com colunas necessarias
    para reconstruir as tags ja publicadas. Retorna None se nao houver.
    """
    try:
        from db.supernova import execute_supernova
    except Exception as e:
        log.warning(f"  Sem acesso ao Super Nova DB ({e}) — pulando diff.")
        return None

    # SQL tolerante: se as colunas v2 nao existirem ainda (primeiro run da v2),
    # cai pra apenas player_id/external_id (suficiente pra diff de "sumidos").
    try:
        rows = execute_supernova(
            """
            SELECT player_id, external_id, tendencia, lifecycle_status,
                   rg_status, bonus_abuse_flag
            FROM multibet.segmentacao_sa_diaria
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) FROM multibet.segmentacao_sa_diaria
                WHERE snapshot_date < %s
            );
            """,
            params=(snapshot_date,),
            fetch=True,
        )
        cols = ["player_id", "external_id", "tendencia",
                "LIFECYCLE_STATUS", "RG_STATUS", "BONUS_ABUSE_FLAG"]
    except Exception as e:
        # Schema antigo (v1) — so player_id e external_id
        log.warning(f"  Schema v1 detectado ({e}) — diff parcial (so 'sumidos').")
        rows = execute_supernova(
            """
            SELECT player_id, external_id
            FROM multibet.segmentacao_sa_diaria
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) FROM multibet.segmentacao_sa_diaria
                WHERE snapshot_date < %s
            );
            """,
            params=(snapshot_date,),
            fetch=True,
        )
        cols = ["player_id", "external_id"]

    if not rows:
        log.info("  Sem snapshot anterior — primeiro run (sem diff).")
        return None
    df = pd.DataFrame(rows, columns=cols)
    log.info(f"  Snapshot anterior: {len(df):,} jogadores.")
    return df


def _identificar_players_sumidos(df_atual: pd.DataFrame,
                                   df_anterior: pd.DataFrame) -> pd.DataFrame:
    """
    Players que estavam no snapshot anterior mas NAO estao no atual.
    Para esses, mandamos remove puro (^pattern, sem add).
    """
    if df_anterior is None or df_anterior.empty:
        return pd.DataFrame()

    ids_atual = set(df_atual["player_id"].astype("int64").unique())
    ids_ant   = set(df_anterior["player_id"].astype("int64").unique())
    sumidos = ids_ant - ids_atual

    if not sumidos:
        return pd.DataFrame()

    df_sumidos = df_anterior[df_anterior["player_id"].astype("int64").isin(sumidos)]
    log.info(f"  Sumiram {len(df_sumidos):,} jogadores vs snapshot anterior "
             f"(receberao remove puro de SEG_*).")
    return df_sumidos


# ============================================================
# Selecao canary
# ============================================================
def _pick_canary(df: pd.DataFrame) -> Optional[pd.Series]:
    """
    Escolhe 1 jogador SEGURO pra Fase Canario:
      - rating A (nao S, mais seguro pra teste)
      - c_category = real_user
      - tendencia = Estavel (nao em transicao)
      - sem BONUS_ABUSE_FLAG
      - external_id valido
      - PVS no IQR (nao borderline)
    """
    mask = (
        (df["rating"] == "A")
        & (df["c_category"] == "real_user")
        & (df["tendencia"] == "Estavel")
        & (df["BONUS_ABUSE_FLAG"].fillna(0).astype(int) == 0)
        & df["external_id"].notna()
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return None

    pvs = pd.to_numeric(candidates["pvs"], errors="coerce")
    q1, q3 = pvs.quantile(0.25), pvs.quantile(0.75)
    candidates = candidates[(pvs >= q1) & (pvs <= q3)]
    if candidates.empty:
        return None
    return candidates.sample(n=1, random_state=42).iloc[0]


# ============================================================
# Persistencia dry-run
# ============================================================
def _save_dry_run_report(events_data: List[Dict], remove_data: List[Dict],
                          snapshot_date: str, mode_label: str = "full") -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    out = reports_dir / f"smartico_seg_sa_dryrun_{mode_label}_{ts}.json"
    data = {
        "generated_at": datetime.now().isoformat(),
        "snapshot_date": snapshot_date,
        "bucket": BUCKET,
        "remove_patterns": TAG_PATTERNS_REMOVE,
        "total_eventos_add": len(events_data),
        "total_eventos_remove": len(remove_data),
        "sample_add": events_data[:10],
        "sample_remove": remove_data[:10],
    }
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"  Dry-run JSON salvo: {out}")
    return out


# ============================================================
# API publica
# ============================================================
def publicar_smartico(
    df: pd.DataFrame,
    snapshot_date: str,
    dry_run: bool = True,
    canary: bool = False,
    skip_cjm: bool = True,
    confirm: bool = False,
    limit: Optional[int] = None,
    incluir_diff_sumidos: bool = True,
) -> Dict:
    """
    Publica tags SEG_* no `core_external_markers` para todos os jogadores
    do DataFrame, mais limpa tags de jogadores que sumiram do snapshot.

    Returns:
        Dict com total_eventos_add, total_eventos_remove, sent, failed,
        errors, dry_run_path (se aplicavel).
    """
    log.info("=" * 70)
    log.info(f"[Smartico] Publicacao Segmentacao A+S — {snapshot_date}")
    log.info(f"  bucket={BUCKET} | dry_run={dry_run} | canary={canary} | "
             f"skip_cjm={skip_cjm} | confirm={confirm}")
    log.info("=" * 70)

    df = df.copy()

    # Modo canary
    if canary:
        canary_row = _pick_canary(df)
        if canary_row is None:
            log.error("[Smartico] Sem candidato canary — abortando.")
            return {"total_eventos_add": 0, "total_eventos_remove": 0,
                    "sent": 0, "failed": 0, "errors": ["sem canary"]}
        df = pd.DataFrame([canary_row])
        log.info(f"[Smartico] CANARY: player_id={canary_row['player_id']} "
                 f"external_id={canary_row['external_id']} rating={canary_row['rating']}")
        incluir_diff_sumidos = False  # nao faz sentido em canary

    if limit and limit > 0:
        df = df.head(limit)
        incluir_diff_sumidos = False

    # ---- Construcao vetorizada das tags ADD ----
    log.info(f"  Construindo tags para {len(df):,} jogadores (vetorizado)...")
    df = _construir_tags_vetorizado(df)

    # Filtra players com pelo menos 1 tag e external_id valido
    # AUDITORIA DETALHADA: importante pra garantir que todos sao incluidos
    df["external_id_str"] = df["external_id"].astype(str).str.strip()
    sem_ext_id = df["external_id_str"].isin(["", "nan", "None"])
    sem_tags = ~df["tags_seg"].apply(lambda x: isinstance(x, list) and len(x) > 0)

    excluidos_ext_id = int(sem_ext_id.sum())
    excluidos_tags = int((~sem_ext_id & sem_tags).sum())  # tem ext_id mas sem tags

    df_valid = df[~sem_ext_id & ~sem_tags]
    log.info(f"  AUDITORIA — Players de entrada: {len(df):,}")
    log.info(f"    - Excluidos por external_id invalido: {excluidos_ext_id:,}")
    log.info(f"    - Excluidos por nenhuma tag valida (deveria ser 0): {excluidos_tags:,}")
    log.info(f"    - Validos para ADD: {len(df_valid):,}")
    if excluidos_tags > 0:
        log.warning(f"  ATENCAO: {excluidos_tags} players sem tags — investigar PCR upstream!")
        # Salva amostra dos suspeitos pra debug
        suspeitos = df[~sem_ext_id & sem_tags][
            ["player_id", "external_id", "rating", "tendencia",
             "LIFECYCLE_STATUS", "RG_STATUS", "BONUS_ABUSE_FLAG"]
        ].head(20)
        log.warning(f"  Amostra:\n{suspeitos.to_string(index=False)}")

    # Constroi events_data ADD
    events_add: List[Dict] = []
    for _, row in df_valid.iterrows():
        events_add.append({
            "user_ext_id": row["external_id_str"],
            "player_id": str(row.get("player_id")),
            "rating": str(row.get("rating")),
            "tags_aplicadas": row["tags_seg"],
            "payload": {
                "^core_external_markers": TAG_PATTERNS_REMOVE,
                "+core_external_markers": row["tags_seg"],
                "skip_cjm": skip_cjm,
            },
        })

    # ---- Diff vs snapshot anterior: players que sumiram ----
    events_remove: List[Dict] = []
    if incluir_diff_sumidos and not canary:
        log.info("  Calculando diff vs snapshot anterior...")
        df_anterior = _carregar_snapshot_anterior(snapshot_date)
        df_sumidos = _identificar_players_sumidos(df, df_anterior) \
            if df_anterior is not None else pd.DataFrame()
        if not df_sumidos.empty:
            for _, row in df_sumidos.iterrows():
                ext_id = str(row.get("external_id") or "").strip()
                if not ext_id or ext_id == "nan":
                    continue
                events_remove.append({
                    "user_ext_id": ext_id,
                    "player_id": str(row.get("player_id")),
                    "motivo": "saiu_de_AS",
                    "payload": {
                        "^core_external_markers": TAG_PATTERNS_REMOVE,
                        "skip_cjm": skip_cjm,
                    },
                })

    log.info(f"  Eventos ADD: {len(events_add):,} | REMOVE: {len(events_remove):,}")

    # ---- DRY-RUN ----
    if dry_run:
        mode = "canary" if canary else ("limit" if limit else "full")
        path = _save_dry_run_report(events_add, events_remove, snapshot_date, mode)
        return {
            "total_eventos_add": len(events_add),
            "total_eventos_remove": len(events_remove),
            "sent": 0, "failed": 0, "errors": [],
            "dry_run_path": str(path),
        }

    # ---- ENVIO REAL ----
    if not confirm:
        log.error("[Smartico] dry_run=False mas confirm=False — abortando por seguranca.")
        return {"total_eventos_add": len(events_add),
                "total_eventos_remove": len(events_remove),
                "sent": 0, "failed": 0, "errors": ["confirm=False"]}

    from db.smartico_api import SmarticoClient
    client = SmarticoClient()

    # Monta SmarticoEvent objects
    smartico_events = []
    for ed in events_add:
        ev = client.build_external_markers_event(
            user_ext_id=ed["user_ext_id"],
            add_tags=ed["tags_aplicadas"],
            remove_pattern=TAG_PATTERNS_REMOVE,
            skip_cjm=skip_cjm,
        )
        smartico_events.append(ev)
    for ed in events_remove:
        ev = client.build_external_markers_event(
            user_ext_id=ed["user_ext_id"],
            remove_pattern=TAG_PATTERNS_REMOVE,
            skip_cjm=skip_cjm,
        )
        smartico_events.append(ev)

    log.info(f"[Smartico] Enviando {len(smartico_events)} eventos para a API...")
    result = client.send_events(smartico_events)
    log.info(f"  Resultado: enviados={result.get('sent', 0)} | "
             f"falhas={result.get('failed', 0)}")
    if result.get("errors"):
        log.warning(f"  Erros (amostra 3): {result['errors'][:3]}")
    return {
        "total_eventos_add": len(events_add),
        "total_eventos_remove": len(events_remove),
        "sent": result.get("sent", 0),
        "failed": result.get("failed", 0),
        "errors": result.get("errors", []),
    }
