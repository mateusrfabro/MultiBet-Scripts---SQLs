"""
Cliente HTTP para Smartico S2S External Events API (v2)
========================================================

Endpoint:   POST https://apis6.smartico.ai/api/external/events/v2
Label:      Multibet/Pragmatic (ext_brand_id="multibet")
Auth:       header `Authorization: <token>`
Limites:    12 req paralelos, 6000 req/min, 10MB por batch (~4000 eventos)

Suporta:
    - update_profile com operadores nas propriedades array:
        * (sem prefixo) = REPLACE    (sobrescreve o valor)
        * "+prop"       = ADD        (adiciona itens ao array, preserva existentes)
        * "-prop"       = REMOVE     (remove itens especificos)
        * "^prop"       = REMOVE PATTERN (aceita glob com *, ex: "RISK_*")
        * "!prop": null = CLEAR      (apaga a propriedade)
      E permite COMBINAR operadores no mesmo evento.
    - Batching nativo (envia array de eventos num unico POST)
    - Retry exponencial (1s -> 32s, 5 tentativas)
    - Deduplicacao via `eid` (retry-safe, codigo 20056 = duplicate)
    - Flag skip_cjm=True (popula estado mas nao dispara Automation/Missions)

Variaveis de ambiente (.env):
    SMARTICO_API_URL    (default: https://apis6.smartico.ai/api/external/events/v2)
    SMARTICO_API_TOKEN  (obrigatorio, exceto em dry_run)
    SMARTICO_BRAND_ID   (default: multibet)

Uso minimo:
    from db.smartico_api import SmarticoClient

    client = SmarticoClient()

    # Atomicamente remove todas as tags RISK_* antigas e adiciona as novas.
    ev = client.build_external_markers_event(
        user_ext_id="12345",
        add_tags=["RISK_TIER_MEDIANO", "RISK_FAST_CASHOUT"],
        remove_pattern=["RISK_*"],
        skip_cjm=True,  # nao dispara automation rules (seguro pra testes)
    )

    result = client.send_events([ev])
    print(result)  # {'sent': 1, 'failed': 0, 'errors': [], 'total': 1}
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("smartico_api")

# ---------------------------------------------------------------------------
# Constantes da API (doc Smartico BackOffice, validadas em 2026-04-10)
# ---------------------------------------------------------------------------

SMARTICO_BASE_URL = os.getenv(
    "SMARTICO_API_URL",
    "https://apis6.smartico.ai/api/external/events/v2",
)
SMARTICO_TOKEN = os.getenv("SMARTICO_API_TOKEN", "")
SMARTICO_BRAND = os.getenv("SMARTICO_BRAND_ID", "multibet")

# Limites oficiais
MAX_PARALLEL_REQUESTS = 12
MAX_REQUESTS_PER_MINUTE = 6000
MAX_EVENTS_PER_BATCH = 4000  # ~10MB / ~2.5KB por evento
MAX_BATCH_BYTES = 10 * 1024 * 1024

# Retry policy (doc: 1s -> 2s -> 4s -> 8s -> 16s -> 32s, depois fixo 32s)
INITIAL_RETRY_INTERVAL_S = 1
MAX_RETRY_INTERVAL_S = 32
MAX_RETRIES = 5
DEFAULT_HTTP_TIMEOUT = 30

# Error codes relevantes
ERROR_DUPLICATE_EVENT = 20056  # retry-safe, ignorar
ERROR_USER_NOT_FOUND = 10001
ERROR_API_KEY_WRONG = 10000
ERROR_BRAND_NOT_FOUND = 125


# ---------------------------------------------------------------------------
# Modelo de evento
# ---------------------------------------------------------------------------


@dataclass
class SmarticoEvent:
    """Representa um evento S2S da Smartico pronto pra serializacao."""

    user_ext_id: str
    event_type: str = "update_profile"
    payload: Dict[str, Any] = field(default_factory=dict)
    ext_brand_id: str = SMARTICO_BRAND
    eid: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_date: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "eid": self.eid,
            "event_date": self.event_date,
            "ext_brand_id": self.ext_brand_id,
            "user_ext_id": str(self.user_ext_id),
            "event_type": self.event_type,
            "payload": self.payload,
        }


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------


class SmarticoClient:
    """Cliente S2S para a External Events API v2 da Smartico."""

    def __init__(
        self,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
        brand: Optional[str] = None,
        dry_run: bool = False,
        http_timeout: int = DEFAULT_HTTP_TIMEOUT,
    ):
        self.token = token or SMARTICO_TOKEN
        self.base_url = base_url or SMARTICO_BASE_URL
        self.brand = brand or SMARTICO_BRAND
        self.dry_run = dry_run
        self.http_timeout = http_timeout

        if not dry_run and not self.token:
            raise ValueError(
                "SMARTICO_API_TOKEN nao definido. "
                "Configure no .env ou passe token= no construtor, "
                "ou use dry_run=True."
            )

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": self.token,
                "Content-Type": "application/json",
            }
        )

    # -------------------------------------------------------------------
    # Builders de evento
    # -------------------------------------------------------------------

    def build_external_markers_event(
        self,
        user_ext_id: str,
        add_tags: Optional[List[str]] = None,
        remove_tags: Optional[List[str]] = None,
        remove_pattern: Optional[List[str]] = None,
        replace_with: Optional[List[str]] = None,
        clear_all: bool = False,
        skip_cjm: bool = False,
    ) -> SmarticoEvent:
        """
        Constroi um evento update_profile para a propriedade core_external_markers.

        Regras de uso:
            - `replace_with`: substitui TODAS as tags (REPLACE, cuidado).
            - `clear_all`: apaga a propriedade inteira.
            - `remove_pattern`: remove tags que batam com o glob (ex: "RISK_*").
            - `remove_tags`: remove tags especificas.
            - `add_tags`: adiciona tags preservando as ja existentes.

        Podem ser combinados no mesmo evento (ex: remove_pattern + add_tags).
        `skip_cjm=True` popula o estado mas NAO dispara Automation/Missions.
        """
        payload: Dict[str, Any] = {}

        if clear_all:
            payload["!core_external_markers"] = None
        if replace_with is not None:
            payload["core_external_markers"] = list(replace_with)
        if remove_pattern:
            payload["^core_external_markers"] = list(remove_pattern)
        if remove_tags:
            payload["-core_external_markers"] = list(remove_tags)
        if add_tags:
            payload["+core_external_markers"] = list(add_tags)

        if not payload:
            raise ValueError(
                "Nenhuma operacao foi especificada "
                "(add_tags/remove_tags/remove_pattern/replace_with/clear_all)."
            )

        if skip_cjm:
            payload["skip_cjm"] = True

        return SmarticoEvent(
            user_ext_id=user_ext_id,
            event_type="update_profile",
            payload=payload,
            ext_brand_id=self.brand,
        )

    def build_external_segment_event(
        self,
        user_ext_id: str,
        add_tags: Optional[List[str]] = None,
        remove_tags: Optional[List[str]] = None,
        remove_pattern: Optional[List[str]] = None,
        replace_with: Optional[List[str]] = None,
        clear_all: bool = False,
        skip_cjm: bool = False,
        remove_from_markers: Optional[List[str]] = None,
    ) -> SmarticoEvent:
        """
        Constroi um evento update_profile para a propriedade core_external_segment.

        Mesmo contrato do `build_external_markers_event` (operadores combinaveis),
        mas escreve no bucket `core_external_segment` (segmentos comportamentais),
        separado de `core_external_markers` (tags operacionais/transacionais).

        Parametro extra:
            - `remove_from_markers`: lista de tags para remover tambem do bucket
              `core_external_markers` no MESMO evento. Usado para migrar tags
              que foram escritas no bucket errado. Adiciona ao payload:
              `-core_external_markers: [tags]`.

        Uso tipico (PCR v1.3 — migracao do bucket markers para segment):
            client.build_external_segment_event(
                user_ext_id="12345",
                add_tags=["PCR_RATING_A"],
                remove_from_markers=[
                    "PCR_RATING_S", "PCR_RATING_A", "PCR_RATING_B",
                    "PCR_RATING_C", "PCR_RATING_D", "PCR_RATING_E",
                ],
                skip_cjm=True,
            )

        Nota sobre o operador `^` com pattern (bug descoberto em 20/04/2026):
            `^core_external_segment: ["PCR_*"]` engole o evento inteiro (pd:0)
            quando o pattern nao matcha nada. Preferir `-remove` com tags
            especificas quando possivel. `^` so e seguro quando ha garantia
            de que pelo menos uma tag matcha.
        """
        payload: Dict[str, Any] = {}

        if clear_all:
            payload["!core_external_segment"] = None
        if replace_with is not None:
            payload["core_external_segment"] = list(replace_with)
        if remove_pattern:
            payload["^core_external_segment"] = list(remove_pattern)
        if remove_tags:
            payload["-core_external_segment"] = list(remove_tags)
        if add_tags:
            payload["+core_external_segment"] = list(add_tags)
        if remove_from_markers:
            payload["-core_external_markers"] = list(remove_from_markers)

        if not payload or (len(payload) == 1 and "-core_external_markers" in payload):
            raise ValueError(
                "Nenhuma operacao em core_external_segment foi especificada "
                "(add_tags/remove_tags/remove_pattern/replace_with/clear_all)."
            )

        if skip_cjm:
            payload["skip_cjm"] = True

        return SmarticoEvent(
            user_ext_id=user_ext_id,
            event_type="update_profile",
            payload=payload,
            ext_brand_id=self.brand,
        )

    def build_custom_property_event(
        self,
        user_ext_id: str,
        prop_name: str,
        value: Optional[str],
        skip_cjm: bool = False,
        remove_from_markers: Optional[List[str]] = None,
        remove_from_segment: Optional[List[str]] = None,
    ) -> SmarticoEvent:
        """
        Constroi um evento update_profile para uma Custom Property Smartico
        (core_custom_prop1 ate core_custom_prop20).

        Custom Properties sao campos string livres (1 valor por prop), ideais
        para representar um unico valor categorico por jogador (ex: 1 rating
        PCR por jogador). Diferente de markers/segment (arrays), aqui o push
        simplesmente substitui o valor anterior (REPLACE).

        Parametros:
            prop_name: nome da property (ex: 'core_custom_prop1')
                       validado contra o range prop1..prop20.
            value:     novo valor (string) ou None pra limpar o campo.
            skip_cjm:  True = nao dispara automations/jornadas.
            remove_from_markers: tags a remover de core_external_markers
                                 no mesmo payload (util pra migracao).
            remove_from_segment: tags a remover de core_external_segment
                                 no mesmo payload (util pra migracao).

        Uso tipico (PCR v1.5 — push para core_custom_prop1):
            client.build_custom_property_event(
                user_ext_id="12345",
                prop_name="core_custom_prop1",
                value="PCR_RATING_A",
                skip_cjm=True,
            )

        Uso em migracao (limpa buckets antigos + seta custom prop):
            client.build_custom_property_event(
                user_ext_id="12345",
                prop_name="core_custom_prop1",
                value="PCR_RATING_A",
                remove_from_markers=["PCR_RATING_S","PCR_RATING_A",...],
                remove_from_segment=["PCR_RATING_S","PCR_RATING_A",...],
                skip_cjm=True,
            )
        """
        # Validacao defensiva do nome da prop
        valid_props = {f"core_custom_prop{i}" for i in range(1, 21)}
        if prop_name not in valid_props:
            raise ValueError(
                f"prop_name invalido: {prop_name!r}. "
                f"Deve ser core_custom_prop1 ... core_custom_prop20."
            )

        payload: Dict[str, Any] = {}

        # Set / clear do custom prop
        if value is None:
            payload[f"!{prop_name}"] = None  # clear
        else:
            payload[prop_name] = str(value)  # replace

        # Operacoes auxiliares de migracao (remove dos buckets antigos)
        if remove_from_markers:
            payload["-core_external_markers"] = list(remove_from_markers)
        if remove_from_segment:
            payload["-core_external_segment"] = list(remove_from_segment)

        if skip_cjm:
            payload["skip_cjm"] = True

        return SmarticoEvent(
            user_ext_id=user_ext_id,
            event_type="update_profile",
            payload=payload,
            ext_brand_id=self.brand,
        )

    # -------------------------------------------------------------------
    # Envio
    # -------------------------------------------------------------------

    def send_events(
        self,
        events: List[SmarticoEvent],
        batch_size: int = 1000,
        inter_batch_sleep_s: float = 0.1,
    ) -> Dict[str, Any]:
        """
        Envia uma lista de eventos em batches.

        Retorna dict com:
            - sent:   quantos eventos foram aceitos (HTTP 200 + sem erro individual)
            - failed: quantos falharam (erro individual OU batch todo falhou)
            - total:  len(events)
            - errors: lista com ate 100 erros (para diagnose, sem estourar log)
        """
        total = len(events)
        if total == 0:
            return {"sent": 0, "failed": 0, "total": 0, "errors": []}

        batch_size = max(1, min(batch_size, MAX_EVENTS_PER_BATCH))
        log.info(
            "Enviando %d eventos em batches de %d (dry_run=%s)",
            total,
            batch_size,
            self.dry_run,
        )

        sent, failed = 0, 0
        all_errors: List[Dict[str, Any]] = []

        for i in range(0, total, batch_size):
            batch = events[i : i + batch_size]
            batch_payload = [e.to_dict() for e in batch]

            if self.dry_run:
                log.debug("[DRY-RUN] batch %d: %d eventos", i // batch_size + 1, len(batch))
                sent += len(batch)
                continue

            ok, errors = self._send_batch_with_retry(batch_payload)
            if ok:
                # Falhas individuais dentro do batch (ex: user nao encontrado)
                batch_failed = len(errors)
                sent += len(batch) - batch_failed
                failed += batch_failed
                all_errors.extend(errors[: max(0, 100 - len(all_errors))])
            else:
                failed += len(batch)
                all_errors.append(
                    {"batch_start_index": i, "error": "batch_failed_all_retries"}
                )

            if inter_batch_sleep_s > 0 and i + batch_size < total:
                time.sleep(inter_batch_sleep_s)

        return {
            "sent": sent,
            "failed": failed,
            "total": total,
            "errors": all_errors[:100],
        }

    def _send_batch_with_retry(
        self, batch_payload: List[Dict[str, Any]]
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        POST do batch com retry exponencial.
        Retorna (success_bool, errors_por_evento).
        """
        interval = INITIAL_RETRY_INTERVAL_S

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.post(
                    self.base_url,
                    data=json.dumps(batch_payload),
                    timeout=self.http_timeout,
                )
            except requests.RequestException as e:
                log.warning(
                    "Exception (tentativa %d/%d): %s. Retry em %ds",
                    attempt,
                    MAX_RETRIES,
                    e,
                    interval,
                )
                time.sleep(interval)
                interval = min(interval * 2, MAX_RETRY_INTERVAL_S)
                continue

            if resp.status_code == 200:
                errors = self._parse_individual_errors(resp)
                self._warn_if_silent_drop(resp, len(batch_payload))
                return True, errors

            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning(
                    "HTTP %d (tentativa %d/%d). Retry em %ds",
                    resp.status_code,
                    attempt,
                    MAX_RETRIES,
                    interval,
                )
                time.sleep(interval)
                interval = min(interval * 2, MAX_RETRY_INTERVAL_S)
                continue

            # 4xx fatal (auth, payload invalido) - nao vale retry
            log.error(
                "HTTP %d FATAL: %s",
                resp.status_code,
                resp.text[:500] if resp.text else "<empty body>",
            )
            return False, [
                {
                    "http_status": resp.status_code,
                    "body": (resp.text or "")[:500],
                }
            ]

        return False, [{"error": "max_retries_exceeded"}]

    @staticmethod
    def _warn_if_silent_drop(resp: requests.Response, batch_size: int) -> None:
        """
        Smartico retorna HTTP 200 + err_code:0 + pd:N, onde pd = perfis atualizados.
        Se pd < batch_size, alguns eventos foram descartados silenciosamente
        (ex: operador ^ sem matching, user_ext_id nao encontrado, no-op).
        Emite WARNING para nao ocultar erro silencioso (descoberto 20/04/2026).
        """
        try:
            data = resp.json()
        except ValueError:
            return
        pd_val = data.get("pd")
        if pd_val is None:
            return
        if pd_val < batch_size:
            log.warning(
                "Smartico aceitou o batch mas processou apenas %d/%d perfis "
                "(pd=%d). Eventos restantes foram descartados silenciosamente "
                "(provavel no-op ou user nao encontrado). req_id=%s",
                pd_val,
                batch_size,
                pd_val,
                data.get("req_id"),
            )

    @staticmethod
    def _parse_individual_errors(resp: requests.Response) -> List[Dict[str, Any]]:
        """
        Extrai erros individuais do payload de resposta.
        Estrutura:
            {
              "err_code": 0,
              "event_errors": {
                  "<eid>": [{"error_message": "...", "error_code": 10001}]
              }
            }
        Duplicatas (20056) sao silenciadas por serem retry-safe.
        """
        try:
            data = resp.json()
        except ValueError:
            log.warning("Resposta 200 sem JSON valido: %s", resp.text[:300])
            return []

        errors: List[Dict[str, Any]] = []
        event_errors = data.get("event_errors") or {}
        for eid, err_list in event_errors.items():
            for err in err_list or []:
                if err.get("error_code") == ERROR_DUPLICATE_EVENT:
                    continue
                errors.append(
                    {
                        "eid": eid,
                        "error_code": err.get("error_code"),
                        "error_message": err.get("error_message"),
                    }
                )
        return errors


# ---------------------------------------------------------------------------
# Smoke test manual
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Dry-run: valida que o evento eh montado corretamente sem chamar API.
    client = SmarticoClient(dry_run=True)
    ev = client.build_external_markers_event(
        user_ext_id="999_smoke_test",
        add_tags=["RISK_TIER_MEDIANO", "RISK_FAST_CASHOUT"],
        remove_pattern=["RISK_*"],
        skip_cjm=True,
    )
    print("Evento gerado (dry-run):")
    print(json.dumps(ev.to_dict(), indent=2, ensure_ascii=False))

    result = client.send_events([ev])
    print("\nResultado:", result)
