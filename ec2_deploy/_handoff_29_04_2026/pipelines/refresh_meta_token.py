"""
Pipeline: refresh_meta_token
=============================
Renova o user access token da Meta Marketing API via fb_exchange_token.
Desenhado pra rodar 1x/mes no cron (dia 1, ~02:00 BRT).

Pre-requisitos no .env:
    META_ADS_ACCESS_TOKEN   — token atual (ser refrescado)
    META_APP_ID             — app_id do Meta for Developers
    META_APP_SECRET         — app_secret (chave secreta do app)

Fluxo:
    1. Le .env
    2. debug_token — loga expiracao atual
    3. fb_exchange_token — obtem novo token (60 dias)
    4. Backup .env.bak_YYYYMMDD_HHMMSS
    5. Reescreve SO a linha META_ADS_ACCESS_TOKEN= no .env (preserva outras vars)
    6. debug_token no novo — valida e loga nova expiracao

Execucao:
    python pipelines/refresh_meta_token.py            # executa refresh
    python pipelines/refresh_meta_token.py --dry-run  # so testa, nao escreve

Regras Meta (importantes):
    - Token de input precisa ter >= 24h de idade. Caso contrario a API
      retorna novo token com MESMA expiracao do antigo (sem ganho).
    - Recomendacao oficial: refrescar ~1x/mes. Mais frequente nao ajuda.
    - Endpoint nao publica rate limit; uso 1x/mes nunca encosta no limite.

Referencia completa: memory/reference_meta_marketing_api.md
"""

import sys
import os
import json
import argparse
import logging
import shutil
import urllib.request
import urllib.error
from datetime import datetime, timezone
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

API_VERSION = "v21.0"
ENV_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".env"))


def _api_get(url: str) -> dict:
    """GET na Graph API com erro legivel."""
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            err = json.loads(body)["error"]
            raise RuntimeError(
                f"Meta API erro {err.get('code', '?')} "
                f"(sub {err.get('error_subcode', '-')}): "
                f"{err.get('message', body[:200])}"
            )
        except (json.JSONDecodeError, KeyError):
            raise RuntimeError(f"Meta API HTTP {e.code}: {body[:200]}")


def debug_token(token: str, app_id: str, app_secret: str) -> dict:
    """Retorna metadados do token (expiration, scopes, validade)."""
    url = (
        f"https://graph.facebook.com/{API_VERSION}/debug_token"
        f"?input_token={token}"
        f"&access_token={app_id}|{app_secret}"
    )
    data = _api_get(url).get("data", {})
    return data


def refresh_token(current_token: str, app_id: str, app_secret: str) -> tuple[str, int]:
    """Executa fb_exchange_token e retorna (novo_token, expires_in_segundos)."""
    url = (
        f"https://graph.facebook.com/{API_VERSION}/oauth/access_token"
        f"?grant_type=fb_exchange_token"
        f"&client_id={app_id}"
        f"&client_secret={app_secret}"
        f"&fb_exchange_token={current_token}"
    )
    d = _api_get(url)
    new_token = d.get("access_token")
    expires_in = int(d.get("expires_in", 0))
    if not new_token:
        raise RuntimeError("fb_exchange_token nao retornou access_token")
    return new_token, expires_in


def update_env(new_token: str, env_path: str = ENV_PATH) -> str:
    """Reescreve SO a linha META_ADS_ACCESS_TOKEN= no .env. Retorna path do backup."""
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env nao encontrado em {env_path}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{env_path}.bak_{ts}"
    shutil.copy2(env_path, backup_path)
    log.info(f"Backup do .env criado: {backup_path}")

    with open(env_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated = False
    new_lines = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("META_ADS_ACCESS_TOKEN="):
            # preserva indentacao caso exista
            indent = line[: len(line) - len(stripped)]
            new_lines.append(f"{indent}META_ADS_ACCESS_TOKEN={new_token}\n")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        # nao existia a variavel — adiciona no fim
        new_lines.append(f"META_ADS_ACCESS_TOKEN={new_token}\n")
        log.warning("META_ADS_ACCESS_TOKEN nao existia no .env — adicionada ao final")

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    return backup_path


def main(dry_run: bool = False) -> int:
    load_dotenv(ENV_PATH)

    token = os.getenv("META_ADS_ACCESS_TOKEN")
    app_id = os.getenv("META_APP_ID")
    app_secret = os.getenv("META_APP_SECRET")

    missing = [n for n, v in [
        ("META_ADS_ACCESS_TOKEN", token),
        ("META_APP_ID", app_id),
        ("META_APP_SECRET", app_secret),
    ] if not v]
    if missing:
        log.error(f".env incompleto — faltam: {', '.join(missing)}")
        return 2

    log.info(f"=== refresh_meta_token (dry_run={dry_run}) ===")
    log.info(f"app_id={app_id} | token len={len(token)}")

    # 1. Estado atual
    try:
        info_before = debug_token(token, app_id, app_secret)
    except Exception as e:
        log.error(f"debug_token ANTES falhou: {e}")
        return 3
    exp_before = info_before.get("expires_at", 0)
    if exp_before:
        log.info(
            f"[ANTES] valid={info_before.get('is_valid')} "
            f"expires_at={datetime.fromtimestamp(exp_before, tz=timezone.utc).isoformat()} "
            f"app={info_before.get('application')}"
        )

    # 2. Refresh
    try:
        new_token, expires_in = refresh_token(token, app_id, app_secret)
    except Exception as e:
        log.error(f"fb_exchange_token falhou: {e}")
        return 4
    log.info(f"[REFRESH OK] novo token len={len(new_token)} | expires_in={expires_in//86400} dias")

    # 3. Valida novo
    try:
        info_after = debug_token(new_token, app_id, app_secret)
    except Exception as e:
        log.error(f"debug_token NOVO falhou: {e}")
        return 5
    exp_after = info_after.get("expires_at", 0)
    if exp_after:
        log.info(
            f"[DEPOIS] valid={info_after.get('is_valid')} "
            f"expires_at={datetime.fromtimestamp(exp_after, tz=timezone.utc).isoformat()}"
        )
    delta_dias = (exp_after - exp_before) // 86400 if (exp_after and exp_before) else 0
    log.info(f"Ganho de prazo: {delta_dias} dias")
    if delta_dias <= 0:
        log.warning(
            "Refresh NAO estendeu o prazo. Comum se o token atual tem <24h de idade. "
            "Proximo cron (>24h depois) ja vai estender normalmente."
        )

    # 4. Persiste
    if dry_run:
        log.info("[DRY-RUN] nao escrevendo no .env")
        return 0
    try:
        backup = update_env(new_token)
    except Exception as e:
        log.error(f"Falha ao atualizar .env: {e}")
        return 6

    log.info(f"=== concluido — .env atualizado (backup: {os.path.basename(backup)}) ===")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Refresh Meta user access token")
    parser.add_argument("--dry-run", action="store_true", help="Nao escreve no .env")
    args = parser.parse_args()
    sys.exit(main(dry_run=args.dry_run))
