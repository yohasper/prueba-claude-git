# ============================================================
# ingestion/bronze/sources/excel_source.py
# Origen de datos: archivos Excel desde Google Drive
# ============================================================

from typing import Iterator, Optional, List

from connectors.gdrive_connector import GoogleDriveConnector
from config.settings import settings
from ingestion.bronze.sources.base_source import BaseSource, SourceRecord
from ingestion.bronze.bronze_loader import BronzeLoader
from utils.excel_reader import ExcelReader
from utils.logger import logger
from datetime import datetime


class ExcelDriveSource(BaseSource):
    """
    Extrae hojas de archivos Excel almacenados en Google Drive.

    Configuración:
        folder_id   : ID de la carpeta en Drive
        file_filter : Texto que debe contener el nombre del archivo
        sheet_filter: Lista de palabras clave para filtrar hojas
    """

    source_type = "excel_gdrive"

    def __init__(
        self,
        folder_id: Optional[str] = None,
        file_filter: Optional[str] = None,
        sheet_filter: Optional[List[str]] = None,
    ):
        self.folder_id   = folder_id or settings.gdrive.folder_id
        self.file_filter = file_filter
        self.sheet_filter = [s.lower() for s in sheet_filter] if sheet_filter else []
        self.drive  = GoogleDriveConnector()
        self.reader = ExcelReader()
        self.loader = BronzeLoader()

    def extract(self) -> Iterator[SourceRecord]:
        """Lista archivos en Drive, filtra y extrae cada hoja como SourceRecord."""

        logger.info(f"[ExcelDriveSource] Listando archivos en carpeta: {self.folder_id}")
        try:
            files = self.drive.list_excel_files(self.folder_id)
        except Exception as e:
            logger.error(f"[ExcelDriveSource] Error listando archivos: {e}")
            return

        if not files:
            logger.warning("[ExcelDriveSource] No se encontraron archivos Excel.")
            return

        # Filtrar por nombre si se especificó
        if self.file_filter:
            files = [f for f in files if self.file_filter in f["name"]]
            if not files:
                logger.warning(
                    f"[ExcelDriveSource] Ningún archivo coincide con '{self.file_filter}'"
                )
                return

        logger.info(f"[ExcelDriveSource] Archivos a procesar: {len(files)}")

        for file_meta in files:
            file_id   = file_meta["id"]
            file_name = file_meta["name"]
            modified  = file_meta.get("modifiedTime")

            modified_at = None
            if modified:
                modified_at = datetime.fromisoformat(modified.replace("Z", "+00:00"))

            logger.info(f"[ExcelDriveSource] Descargando: {file_name}")
            try:
                buffer = self.drive.download_file(file_id, file_name)
                sheets = self.reader.read_excel(buffer, sheet_name=None)
            except Exception as e:
                logger.error(f"[ExcelDriveSource] Error procesando {file_name}: {e}")
                continue

            for sheet_name, df in sheets.items():
                # Filtrar hojas si se especificó
                if self.sheet_filter:
                    sheet_lower = sheet_name.lower().replace(" ", "_")
                    if not any(kw in sheet_lower for kw in self.sheet_filter):
                        logger.info(f"  ⏭ Hoja '{sheet_name}' omitida por filtro")
                        continue

                table_name = self.loader.build_table_name(file_name, sheet_name)

                yield SourceRecord(
                    df=df,
                    source_name=f"{file_name} / {sheet_name}",
                    table_name=table_name,
                    source_id=file_id,
                    modified_at=modified_at,
                )
