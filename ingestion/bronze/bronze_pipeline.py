# ============================================================
# ingestion/bronze/bronze_pipeline.py
# Pipeline principal de ingesta a la capa Bronce
# ============================================================

from datetime import datetime, timezone
from typing import Optional

from connectors.gdrive_connector import GoogleDriveConnector
from connectors.postgres_connector import postgres
from config.settings import settings
from ingestion.bronze.bronze_loader import BronzeLoader
from models.bronze.ingestion_control import ensure_control_table, register_ingestion, was_already_loaded
from utils.excel_reader import ExcelReader
from utils.logger import logger


class BronzePipeline:
    """
    Orquesta la ingesta completa desde Google Drive → PostgreSQL (Bronce).

    Flujo:
        1. Lista archivos Excel en la carpeta de Drive configurada
        2. Por cada archivo: descarga, lee hojas, normaliza
        3. Carga cada hoja como tabla en el schema 'bronze'
        4. Registra cada carga en la tabla de control
    """

    def __init__(self, folder_id: Optional[str] = None):
        self.folder_id = folder_id or settings.gdrive.folder_id
        self.drive = GoogleDriveConnector()
        self.loader = BronzeLoader()
        self.reader = ExcelReader()

    def run(self, force_reload: bool = False) -> dict:
        """
        Ejecuta el pipeline completo de ingesta Bronce.

        Args:
            force_reload: Si True, recarga aunque ya esté cargado anteriormente.

        Returns:
            Resumen de la ejecución {total, success, skipped, errors}
        """
        start_time = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE BRONCE")
        logger.info(f"Carpeta Drive: {self.folder_id}")
        logger.info(f"Schema destino: {settings.db.schema_bronze}")
        logger.info("=" * 60)

        summary = {"total": 0, "success": 0, "skipped": 0, "errors": 0}

        # 1. Preparar infraestructura
        postgres.create_schema_if_not_exists(settings.db.schema_bronze)
        ensure_control_table()

        # 2. Listar archivos
        try:
            files = self.drive.list_excel_files(self.folder_id)
        except Exception as e:
            logger.error(f"No se pudo listar archivos de Drive: {e}")
            return summary

        if not files:
            logger.warning("No se encontraron archivos Excel en la carpeta.")
            return summary

        summary["total"] = len(files)
        logger.info(f"Archivos a procesar: {len(files)}")

        # 3. Procesar cada archivo
        for file_meta in files:
            self._process_file(file_meta, summary, force_reload, start_time)

        # Resumen final
        logger.info("=" * 60)
        logger.info("PIPELINE BRONCE FINALIZADO")
        logger.info(f"  Total archivos : {summary['total']}")
        logger.info(f"  Exitosos       : {summary['success']}")
        logger.info(f"  Omitidos       : {summary['skipped']}")
        logger.info(f"  Con errores    : {summary['errors']}")
        logger.info("=" * 60)

        return summary

    def _process_file(
        self,
        file_meta: dict,
        summary: dict,
        force_reload: bool,
        started_at: datetime,
    ) -> None:
        """Procesa un archivo individual de Drive."""
        file_id = file_meta["id"]
        file_name = file_meta["name"]
        modified_at = file_meta.get("modifiedTime")

        # Parsear fecha de modificación
        source_modified_at = None
        if modified_at:
            source_modified_at = datetime.fromisoformat(modified_at.replace("Z", "+00:00"))

        logger.info(f"\n📄 Procesando: {file_name}")

        # Descargar archivo
        try:
            buffer = self.drive.download_file(file_id, file_name)
        except Exception as e:
            logger.error(f"Error descargando {file_name}: {e}")
            register_ingestion(
                file_name=file_name, sheet_name="ALL",
                target_table="N/A", status="ERROR",
                error_message=str(e), file_id=file_id,
                source_modified_at=source_modified_at,
                started_at=started_at,
            )
            summary["errors"] += 1
            return

        # Leer todas las hojas del Excel
        try:
            sheets = self.reader.read_excel(buffer, sheet_name=None)
        except Exception as e:
            logger.error(f"Error leyendo Excel {file_name}: {e}")
            summary["errors"] += 1
            return

        # Procesar cada hoja
        for sheet_name, df in sheets.items():
            self._process_sheet(
                df=df,
                file_id=file_id,
                file_name=file_name,
                sheet_name=sheet_name,
                source_modified_at=source_modified_at,
                force_reload=force_reload,
                started_at=started_at,
                summary=summary,
            )

    def _process_sheet(
        self,
        df,
        file_id: str,
        file_name: str,
        sheet_name: str,
        source_modified_at,
        force_reload: bool,
        started_at: datetime,
        summary: dict,
    ) -> None:
        """Procesa una hoja individual de un Excel."""

        # Nombre de tabla destino en bronce
        target_table = self.loader.build_table_name(file_name, sheet_name)

        # Verificar si ya fue cargado
        if not force_reload and was_already_loaded(file_name, sheet_name, source_modified_at):
            logger.info(f"  ⏭ Omitido (ya cargado): {sheet_name} → {target_table}")
            register_ingestion(
                file_name=file_name, sheet_name=sheet_name,
                target_table=target_table, status="SKIPPED",
                file_id=file_id, source_modified_at=source_modified_at,
                started_at=started_at,
            )
            summary["skipped"] += 1
            return

        # Normalizar y cargar
        try:
            rows = self.loader.load_sheet(
                df=df,
                file_name=file_name,
                sheet_name=sheet_name,
                target_table=target_table,
            )
            register_ingestion(
                file_name=file_name, sheet_name=sheet_name,
                target_table=target_table, status="SUCCESS",
                rows_loaded=rows, file_id=file_id,
                source_modified_at=source_modified_at,
                started_at=started_at,
            )
            summary["success"] += 1

        except Exception as e:
            logger.error(f"  Error cargando hoja {sheet_name}: {e}")
            register_ingestion(
                file_name=file_name, sheet_name=sheet_name,
                target_table=target_table, status="ERROR",
                error_message=str(e), file_id=file_id,
                source_modified_at=source_modified_at,
                started_at=started_at,
            )
            summary["errors"] += 1
