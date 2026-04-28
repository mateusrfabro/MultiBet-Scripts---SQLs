"""
Modulo: Publicacao da Segmentacao no Smartico (External Markers)
=====================================================================

Recebe o DataFrame ja processado pelo pipeline `segmentacao_sa_diaria.py`
e publica a tag de RATING do PCR no `core_external_markers` do Smartico
via S2S API, para TODA a base (nao so A+S).

Bucket: `core_external_markers` (alinhado com Raphael, 28/04/2026).
        Nesta v3 do PCR push, Raphael decidiu voltar para external_markers
        (saindo de core_custom_prop1 que era a v1.5 de 22/04). Justificativa:
        consolidar tudo em markers para facilitar regras de campanha CRM
        que ja usam markers como input.

ESTE PIPELINE SUBSTITUI o antigo `scripts/push_pcr_to_smartico.py` (que
publicava em core_custom_prop1). O antigo sera desativado no orquestrador
em paralelo a esta migracao.

Tags publicadas (1 prefixo atomico, 1 tag por jogador):
  - PCR_RATING_<S|A|B|C|D|E>   (1 tag por player baseado no rating do PCR)
  - PCR_RATING_NEW             (shadow mode hoje — Raphael alinhar)

Operacao atomica POR JOGADOR (preserva tags de outros pipelines no markers):
    {
      "^core_external_markers": ["PCR_RATING_*"],
      "+core_external_markers": ["PCR_RATING_S"]
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

# Pattern para limpar antes do add (idempotencia atomica)
TAG_PATTERNS_REMOVE = ["PCR_RATING_*"]

# Mapping rating PCR -> tag Smartico
RATING_TO_TAG = {
    "S": "PCR_RATING_S",
    "A": "PCR_RATING_A",
    "B": "PCR_RATING_B",
    "C": "PCR_RATING_C",
    "D": "PCR_RATING_D",
    "E": "PCR_RATING_E",
    # NEW: shadow mode hoje. Quando Raphael liberar, definir aqui.
    # "NEW": "PCR_RATING_NEW",
}


# ============================================================
# Construcao vetorizada das tags (pandas)
# ============================================================
def _construir_tags_vetorizado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona coluna 'tags_seg' (List[str]) ao df, computada vetorialmente.

    1 tag por jogador: PCR_RATING_<rating>. Players sem rating valido
    (ex: NEW em shadow mode hoje) ficam com lista vazia — nao geram evento.
    """
    df = df.copy()
    rating = df.get("rating", pd.Series([""] * len(df))).fillna("").astype(str).str.upper().str.strip()

    # Map vetorizado rating -> tag (None onde rating invalido / shadow mode)
    df["_pcr_tag"] = rating.map(RATING_TO_TAG).fillna("")

    # tags_seg: lista com 1 elemento (ou vazia se shadow mode / invalido)
    df["tags_seg"] = df["_pcr_tag"].apply(lambda t: [t] if t else [])
    df = df.drop(columns=["_pcr_tag"])
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

    # Para detectar 'sumidos' (player que estava ontem mas nao esta hoje na base PCR),
    # so precisamos de player_id + external_id do snapshot anterior.
    # Note: o pipeline grava A+S em segmentacao_sa_diaria, mas o snapshot Smartico
    # vai pra base PCR completa (~136k). O diff de 'sumidos' aqui captura A+S que
    # mudaram de tier (sairam de A+S) — mas como agora subimos PCR_RATING para
    # TODOS os tiers, esse player vai receber o novo PCR_RATING_<X> automaticamente.
    # Mantemos o codigo por defesa: se um player saiu da base ATIVA do PCR (deletado,
    # banido), removemos as tags PCR_RATING_* do perfil dele.
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

    if not rows:
        log.info("  Sem snapshot anterior — primeiro run (sem diff).")
        return None
    df = pd.DataFrame(rows, columns=["player_id", "external_id"])
    log.info(f"  Snapshot anterior (A+S): {len(df):,} jogadores.")
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
# Selecao canary / amostra
# ============================================================
def _pick_canary(df: pd.DataFrame) -> Optional[pd.Series]:
    """
    Escolhe 1 jogador SEGURO pra Fase Canario:
      - rating B ou C (meio-termo, nao extremo) — alinhado com push_pcr antigo
      - c_category = real_user
      - external_id valido
      - PVS no IQR do rating (nao borderline)
    """
    rating_col = df.get("rating", pd.Series([""] * len(df))).fillna("").astype(str)
    cat_col = df.get("c_category", pd.Series([""] * len(df))).fillna("").astype(str)
    mask = (
        rating_col.isin(["B", "C"])
        & (cat_col == "real_user")
        & df["external_id"].notna()
    )
    candidates = df[mask].copy()
    if candidates.empty:
        return None

    # Evita borderline: PVS entre P25 e P75 do proprio rating
    if "pvs" in candidates.columns:
        pvs = pd.to_numeric(candidates["pvs"], errors="coerce")
        candidates = candidates.assign(_pvs_num=pvs)
        out = []
        for r, group in candidates.groupby("rating"):
            q1, q3 = group["_pvs_num"].quantile(0.25), group["_pvs_num"].quantile(0.75)
            sub = group[(group["_pvs_num"] >= q1) & (group["_pvs_num"] <= q3)]
            out.append(sub)
        candidates = pd.concat(out) if out else candidates
        candidates = candidates.drop(columns=["_pvs_num"], errors="ignore")
    if candidates.empty:
        return None
    return candidates.sample(n=1, random_state=42).iloc[0]


def _pick_amostra_diversa(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    Seleciona uma amostra diversa de N jogadores cobrindo todos os ratings
    para validacao manual no painel Smartico.

    Distribuicao alvo (de N=10): 1 S + 2 A + 2 B + 2 C + 1 D + 1 E + 1 NEW.
    Filtros: c_category=real_user, external_id valido.
    Para tiers com poucos jogadores (S, NEW), pega o que tiver disponivel.
    """
    rating_col = df.get("rating", pd.Series([""] * len(df))).fillna("").astype(str)
    cat_col = df.get("c_category", pd.Series([""] * len(df))).fillna("").astype(str)
    base = df[(cat_col == "real_user") & df["external_id"].notna()].copy()
    base["_rating"] = rating_col[base.index]

    # Distribuicao por rating (ajusta proporcional ao N)
    if n == 10:
        plano = {"S": 1, "A": 2, "B": 2, "C": 2, "D": 1, "E": 1, "NEW": 1}
    else:
        # proporcional ao N (mantém pelo menos 1 de cada se possivel)
        plano = {r: max(1, n // 7) for r in ["S", "A", "B", "C", "D", "E", "NEW"]}

    selecionados = []
    for r, qtd in plano.items():
        sub = base[base["_rating"] == r]
        if sub.empty:
            continue
        amostra = sub.sample(n=min(qtd, len(sub)), random_state=42)
        selecionados.append(amostra)

    if not selecionados:
        return pd.DataFrame()
    out = pd.concat(selecionados).drop(columns=["_rating"], errors="ignore")
    log.info(f"  Amostra diversa: {len(out)} jogadores")
    log.info(f"  Distribuicao: {out['rating'].value_counts().to_dict()}")
    return out


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
    amostra: Optional[int] = None,
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

    # Modo canary (1 jogador)
    if canary:
        canary_row = _pick_canary(df)
        if canary_row is None:
            log.error("[Smartico] Sem candidato canary — abortando.")
            return {"total_eventos_add": 0, "total_eventos_remove": 0,
                    "sent": 0, "failed": 0, "errors": ["sem canary"]}
        df = pd.DataFrame([canary_row])
        log.info(f"[Smartico] CANARY: player_id={canary_row['player_id']} "
                 f"external_id={canary_row['external_id']} rating={canary_row['rating']}")
        incluir_diff_sumidos = False

    # Modo amostra diversa (N jogadores cobrindo todos os ratings)
    elif amostra and amostra > 0:
        df = _pick_amostra_diversa(df, n=amostra)
        if df.empty:
            log.error("[Smartico] Sem candidatos amostra — abortando.")
            return {"total_eventos_add": 0, "total_eventos_remove": 0,
                    "sent": 0, "failed": 0, "errors": ["sem amostra"]}
        log.info(f"[Smartico] AMOSTRA DIVERSA — {len(df)} jogadores")
        log.info(f"  IDs (player_id | external_id | rating):")
        for _, r in df.iterrows():
            log.info(f"    {r['player_id']} | {r['external_id']} | {r.get('rating')}")
        incluir_diff_sumidos = False

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
    sent = result.get("sent", 0)
    failed = result.get("failed", 0)
    total = len(smartico_events)
    log.info(f"  Resultado: enviados={sent} | falhas={failed} | total={total}")

    # Reconciliacao: confere se sent + failed == total (sem dropps silenciosos)
    diferenca = total - (sent + failed)
    if diferenca > 0:
        log.warning(f"  ATENCAO: {diferenca} eventos sumiram sem reportar erro "
                    f"(possivel drop silencioso — ver _warn_if_silent_drop nos logs).")

    # Exporta lista de falhas para CSV (auditoria + reprocesso manual)
    errors = result.get("errors", [])
    if errors:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        falhas_path = reports_dir / f"smartico_seg_sa_falhas_{ts}.csv"
        # Mapeia user_ext_id pra player_id pra facilitar investigacao
        ext_to_player = {ed["user_ext_id"]: ed.get("player_id", "")
                          for ed in events_add + events_remove}
        with open(falhas_path, "w", encoding="utf-8") as f:
            f.write("eid;error_code;error_message;user_ext_id;player_id\n")
            for err in errors:
                eid = err.get("eid", "")
                # tenta extrair user_ext_id do eid (formato: <user_ext_id>_<timestamp>)
                ext = eid.split("_")[0] if "_" in eid else eid
                pid = ext_to_player.get(ext, "")
                f.write(f"{eid};{err.get('error_code', '')};"
                         f"{err.get('error_message', '')};"
                         f"{ext};{pid}\n")
        log.warning(f"  Falhas exportadas para: {falhas_path}")
        log.warning(f"  Amostra (3 erros): {errors[:3]}")

    return {
        "total_eventos_add": len(events_add),
        "total_eventos_remove": len(events_remove),
        "sent": sent,
        "failed": failed,
        "errors": errors,
        "reconciliacao_diff": diferenca,
    }
