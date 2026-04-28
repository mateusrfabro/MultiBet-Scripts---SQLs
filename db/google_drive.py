"""
Google Drive uploader — Service Account auth.

Configuracao via .env:
    GDRIVE_CREDENTIALS_JSON=/path/para/service_account.json
    GDRIVE_FOLDER_ID=<id_da_pasta_compartilhada>

Setup:
    1. Cria projeto no Google Cloud (free).
    2. Habilita "Google Drive API".
    3. Cria Service Account, baixa o JSON da chave.
    4. Cria uma pasta no seu Drive pessoal.
    5. Compartilha a pasta com o email do service account (com permissao Editor).
    6. Compartilha a mesma pasta com os destinatarios humanos (Viewer).
    7. Pega o folder_id da URL do Drive (parte apos /folders/).

Uso:
    from db.google_drive import GoogleDriveUploader

    drive = GoogleDriveUploader()
    file_id, web_link = drive.upload_replace(
        local_path="output/players_segmento_SA_2026-04-28.csv",
        remote_name="players_segmento_SA_2026-04-28.csv",
    )
    print(f"Uploaded: {web_link}")

Comportamento:
    - Se ja existe arquivo com o mesmo nome na pasta, SUBSTITUI (update).
    - Se nao existe, CRIA novo.
    - Retorna (file_id, web_link_view) — link clicavel pra abrir no Drive web.

Dependencias:
    pip install google-api-python-client google-auth google-auth-httplib2
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Tuple

log = logging.getLogger(__name__)


class GoogleDriveUploader:
    """Cliente Google Drive via Service Account."""

    def __init__(
        self,
        credentials_path: Optional[str] = None,
        folder_id: Optional[str] = None,
    ):
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        creds_path = credentials_path or os.getenv("GDRIVE_CREDENTIALS_JSON")
        self.folder_id = folder_id or os.getenv("GDRIVE_FOLDER_ID")

        if not creds_path:
            raise RuntimeError(
                "GDRIVE_CREDENTIALS_JSON nao configurado. "
                "Ver db/google_drive.py docstring."
            )
        if not self.folder_id:
            raise RuntimeError(
                "GDRIVE_FOLDER_ID nao configurado. "
                "Pega da URL do Drive (parte apos /folders/)."
            )
        if not Path(creds_path).exists():
            raise RuntimeError(f"Arquivo de credenciais nao encontrado: {creds_path}")

        creds = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        self.service = build("drive", "v3", credentials=creds, cache_discovery=False)
        log.info(f"[Drive] Cliente inicializado (folder={self.folder_id})")

    def _find_existing(self, remote_name: str) -> Optional[str]:
        """Retorna file_id se ja existe arquivo com esse nome na pasta."""
        # Escapa aspas simples no nome
        safe_name = remote_name.replace("'", "\\'")
        query = (
            f"name = '{safe_name}' "
            f"and '{self.folder_id}' in parents "
            f"and trashed = false"
        )
        resp = self.service.files().list(
            q=query, fields="files(id, name)", pageSize=1
        ).execute()
        files = resp.get("files", [])
        return files[0]["id"] if files else None

    def upload_replace(
        self,
        local_path: str,
        remote_name: Optional[str] = None,
        mime_type: str = "text/csv",
    ) -> Tuple[str, str]:
        """
        Upload de um arquivo local. Se ja existe arquivo com o mesmo
        nome na pasta, SUBSTITUI (preserva file_id, atualiza conteudo).
        Senao, CRIA novo.

        Returns:
            (file_id, web_link)
        """
        from googleapiclient.http import MediaFileUpload

        local = Path(local_path)
        if not local.exists():
            raise FileNotFoundError(local_path)

        name = remote_name or local.name
        existing_id = self._find_existing(name)

        media = MediaFileUpload(str(local), mimetype=mime_type, resumable=True)

        if existing_id:
            log.info(f"[Drive] Substituindo {name} (id={existing_id})...")
            updated = self.service.files().update(
                fileId=existing_id,
                media_body=media,
                fields="id, webViewLink",
            ).execute()
            file_id = updated["id"]
            web_link = updated.get("webViewLink", "")
        else:
            log.info(f"[Drive] Criando {name} na pasta {self.folder_id}...")
            metadata = {"name": name, "parents": [self.folder_id]}
            created = self.service.files().create(
                body=metadata,
                media_body=media,
                fields="id, webViewLink",
            ).execute()
            file_id = created["id"]
            web_link = created.get("webViewLink", "")

        log.info(f"[Drive] OK — {name} ({local.stat().st_size / 1024:.1f} KB)")
        log.info(f"[Drive] Link: {web_link}")
        return file_id, web_link

    def upload_many(self, files: list[Tuple[str, str, str]]) -> list[Tuple[str, str]]:
        """
        Upload de varios arquivos. Cada item: (local_path, remote_name, mime_type).
        Retorna lista de (file_id, web_link).
        """
        results = []
        for local_path, remote_name, mime in files:
            results.append(self.upload_replace(local_path, remote_name, mime))
        return results
