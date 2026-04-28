"""
Slack Uploader — envia mensagens e arquivos via Slack Bot Token.

Configuracao via .env:
    SLACK_BOT_TOKEN=xoxb-XXXXXXXXX-XXXXXXXXX-XXXXXXXXXXXXXXXXXXXXX
    SLACK_CHANNEL_ID=C0123456789

Setup (uma vez):
    1. Cria Slack App em https://api.slack.com/apps
    2. Add scopes (Bot Token Scopes): files:write, chat:write
    3. Install App no workspace -> copia Bot Token (xoxb-...)
    4. Convida o bot no canal: /invite @nome-do-bot
    5. Pega Channel ID: clica direito no canal -> "Copy link" ->
       parte final apos /archives/ (ex: C0123456789)

Uso:
    from db.slack_uploader import enviar_arquivo_slack
    enviar_arquivo_slack(
        arquivo="output/players_segmento_SA_2026-04-28.csv",
        titulo="Segmentacao A+S diaria — 28/04/2026",
        comentario="Base diaria de jogadores Rating A e S (10.299 jogadores).",
    )

Dependencias:
    pip install slack-sdk
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Iterable, Optional

log = logging.getLogger(__name__)


def _get_client():
    from slack_sdk import WebClient
    token = os.getenv("SLACK_BOT_TOKEN")
    if not token:
        raise RuntimeError("SLACK_BOT_TOKEN nao configurado no .env")
    return WebClient(token=token)


def enviar_mensagem_slack(
    texto: str,
    canal: Optional[str] = None,
    blocks: Optional[list] = None,
) -> bool:
    """Envia mensagem simples (sem arquivo) ao canal."""
    canal = canal or os.getenv("SLACK_CHANNEL_ID")
    if not canal:
        log.error("SLACK_CHANNEL_ID nao configurado.")
        return False
    try:
        client = _get_client()
        resp = client.chat_postMessage(
            channel=canal, text=texto, blocks=blocks,
        )
        log.info(f"[Slack] Mensagem enviada -> {canal}")
        return resp.get("ok", False)
    except Exception as e:
        log.error(f"[Slack] Falha ao enviar mensagem: {e}")
        return False


def enviar_arquivo_slack(
    arquivo: str,
    canal: Optional[str] = None,
    titulo: Optional[str] = None,
    comentario: Optional[str] = None,
) -> bool:
    """
    Sobe 1 arquivo no canal Slack. Usa files.upload_v2 (recomendado).
    """
    canal = canal or os.getenv("SLACK_CHANNEL_ID")
    if not canal:
        log.error("SLACK_CHANNEL_ID nao configurado.")
        return False

    path = Path(arquivo)
    if not path.exists():
        log.error(f"Arquivo nao encontrado: {arquivo}")
        return False

    try:
        client = _get_client()
        log.info(f"[Slack] Upload {path.name} ({path.stat().st_size / 1024:.1f} KB) -> {canal}")
        resp = client.files_upload_v2(
            channel=canal,
            file=str(path),
            filename=path.name,
            title=titulo or path.name,
            initial_comment=comentario,
        )
        log.info(f"[Slack] Upload OK")
        return resp.get("ok", False)
    except Exception as e:
        log.error(f"[Slack] Falha no upload: {e}")
        return False


def enviar_arquivos_slack(
    arquivos: Iterable[str],
    canal: Optional[str] = None,
    comentario: Optional[str] = None,
) -> bool:
    """
    Sobe multiplos arquivos numa unica mensagem. Util pra CSV + legenda juntos.
    """
    canal = canal or os.getenv("SLACK_CHANNEL_ID")
    if not canal:
        log.error("SLACK_CHANNEL_ID nao configurado.")
        return False

    file_uploads = []
    for arq in arquivos:
        p = Path(arq)
        if not p.exists():
            log.warning(f"[Slack] Arquivo ausente: {arq}")
            continue
        file_uploads.append({"file": str(p), "filename": p.name, "title": p.name})

    if not file_uploads:
        log.error("[Slack] Nenhum arquivo valido pra upload.")
        return False

    try:
        client = _get_client()
        log.info(f"[Slack] Upload {len(file_uploads)} arquivos -> {canal}")
        resp = client.files_upload_v2(
            channel=canal,
            file_uploads=file_uploads,
            initial_comment=comentario,
        )
        log.info(f"[Slack] Upload OK")
        return resp.get("ok", False)
    except Exception as e:
        log.error(f"[Slack] Falha no upload: {e}")
        return False
