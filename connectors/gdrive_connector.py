# ============================================================
# connectors/gdrive_connector.py
# Conector a Google Drive para descarga de archivos Excel
# ============================================================

import io
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from config.settings import settings
from utils.logger import logger


class GoogleDriveConnector:
    """
    Conector a Google Drive API.
    Permite listar y descargar archivos Excel (.xlsx, .xls) de una carpeta.
    """

    EXCEL_MIME_TYPES = [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # .xlsx
        "application/vnd.ms-excel",                                            # .xls
        "application/vnd.google-apps.spreadsheet",                            # Google Sheets
    ]

    def __init__(self):
        self._service = None
        self._credentials = None

    @property
    def service(self):
        if self._service is None:
            self._service = self._authenticate()
        return self._service

    def _authenticate(self):
        """
        Autenticación OAuth2 con Google.
        En el primer uso abrirá el navegador para autorización.
        Después usa el token guardado automáticamente.
        """
        creds = None
        token_path = Path(settings.gdrive.token_path)
        credentials_path = Path(settings.gdrive.credentials_path)

        if not credentials_path.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo de credenciales: {credentials_path}\n"
                "Descárgalo desde Google Cloud Console > APIs & Services > Credentials"
            )

        # Cargar token existente si hay
        if token_path.exists():
            creds = Credentials.from_authorized_user_file(
                str(token_path), settings.gdrive.scopes
            )

        # Si no hay credenciales válidas, autenticar
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Renovando token de Google...")
                creds.refresh(Request())
            else:
                logger.info("Iniciando autenticación OAuth2 con Google Drive...")
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(credentials_path), settings.gdrive.scopes
                )
                creds = flow.run_local_server(port=0)

            # Guardar token para próximos usos
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text(creds.to_json())
            logger.success("Token de Google guardado correctamente.")

        service = build("drive", "v3", credentials=creds)
        logger.success("Conexión a Google Drive establecida.")
        return service

    def list_excel_files(self, folder_id: Optional[str] = None) -> list[dict]:
        """
        Lista todos los archivos Excel en una carpeta de Google Drive.

        Args:
            folder_id: ID de la carpeta. Si no se pasa, usa el de .env

        Returns:
            Lista de dicts con {id, name, mimeType, modifiedTime, size}
        """
        fid = folder_id or settings.gdrive.folder_id
        if not fid:
            raise ValueError("No se especificó GDRIVE_FOLDER_ID en .env")

        mime_query = " or ".join(
            [f"mimeType='{m}'" for m in self.EXCEL_MIME_TYPES]
        )
        query = f"'{fid}' in parents and ({mime_query}) and trashed=false"

        results = []
        page_token = None

        logger.info(f"Listando archivos Excel en carpeta: {fid}")

        while True:
            response = self.service.files().list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
                pageToken=page_token,
                orderBy="modifiedTime desc",
            ).execute()

            files = response.get("files", [])
            results.extend(files)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        logger.info(f"Encontrados {len(results)} archivos Excel.")
        return results

    def download_file(self, file_id: str, file_name: str) -> io.BytesIO:
        """
        Descarga un archivo de Google Drive a memoria (BytesIO).

        Args:
            file_id:   ID del archivo en Drive
            file_name: Nombre del archivo (para logs)

        Returns:
            BytesIO con el contenido del archivo
        """
        logger.info(f"Descargando: {file_name} (id={file_id})")

        # Detectar si es Google Sheets → exportar como xlsx
        file_meta = self.service.files().get(
            fileId=file_id, fields="mimeType"
        ).execute()

        buffer = io.BytesIO()

        if file_meta["mimeType"] == "application/vnd.google-apps.spreadsheet":
            # Google Sheets: exportar como Excel
            request = self.service.files().export_media(
                fileId=file_id,
                mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            # Excel nativo: descargar directo
            request = self.service.files().get_media(fileId=file_id)

        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                logger.debug(f"  Descarga {file_name}: {int(status.progress() * 100)}%")

        buffer.seek(0)
        logger.success(f"Descargado correctamente: {file_name}")
        return buffer

    def get_file_metadata(self, file_id: str) -> dict:
        """Obtiene metadata de un archivo por su ID."""
        return self.service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, modifiedTime, size, parents"
        ).execute()
