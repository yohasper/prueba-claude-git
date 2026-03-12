# ============================================================
# ingestion/bronze/bronze_pipeline.py
# Pipeline multi-origen de la capa Bronce
#
# Arquitectura:
#   BronzePipeline recibe una lista de "sources" (orígenes).
#   Cada source implementa BaseSource y sabe cómo extraer sus datos.
#   El pipeline solo se encarga de cargar y registrar.
#
# Orígenes disponibles:
#   - ExcelDriveSource  → archivos Excel desde Google Drive
#   - JsonApiSource     → endpoints JSON/REST
#   (futuro: CsvSource, PostgresSource, S3Source, etc.)
# ============================================================

from datetime import datetime, timezone
from typing import List, Optional

from connectors.postgres_connector import postgres
from config.settings import settings
from ingestion.bronze.bronze_loader import BronzeLoader
from ingestion.bronze.sources.base_source import BaseSource
from ingestion.bronze.sources.excel_source import ExcelDriveSource
from models.bronze.ingestion_control import (
    ensure_control_table,
    register_ingestion,
    was_already_loaded,
)
from utils.logger import logger

# Tablas que siempre se procesan sin importar el control de ingesta
ALWAYS_PROCESS_KEYWORDS = ["clientes", "productos", "ventas"]


class BronzePipeline:
    """
    Orquesta la ingesta de múltiples orígenes hacia la capa Bronce.

    Uso por defecto (Excel sesion 7):
        BronzePipeline().run()

    Uso con múltiples orígenes:
        BronzePipeline(sources=[
            ExcelDriveSource(folder_id="...", file_filter="..."),
            JsonApiSource(endpoints=[{"url": "...", "table_name": "..."}]),
        ]).run()
    """

    def __init__(self, sources: Optional[List[BaseSource]] = None, folder_id: Optional[str] = None):
        self.loader = BronzeLoader()

        if sources is not None:
            self.sources = sources
        else:
            self.sources = [
                ExcelDriveSource(
                    folder_id=folder_id or settings.gdrive.folder_id,
                    file_filter="Ejercicio Sesion #7",
                    sheet_filter=["clientes", "productos", "ventas"],
                )
            ]

    def run(self, force_reload: bool = False) -> dict:
        start_time = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE BRONCE")
        logger.info(f"Origenes configurados : {len(self.sources)}")
        for s in self.sources:
            logger.info(f"  · {s.source_type}")
        logger.info(f"Schema destino        : {settings.db.schema_bronze}")
        logger.info("=" * 60)

        summary = {"total": 0, "success": 0, "skipped": 0, "errors": 0}

        postgres.create_schema_if_not_exists(settings.db.schema_bronze)
        ensure_control_table()
        self.loader.truncate_staging_tables()

        for source in self.sources:
            logger.info(f"\n── Origen: [{source.source_type}] ──")
            self._process_source(source, summary, force_reload, start_time)

        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info("PIPELINE BRONCE FINALIZADO")
        logger.info(f"  Total    : {summary['total']}")
        logger.info(f"  Exitosos : {summary['success']}")
        logger.info(f"  Omitidos : {summary['skipped']}")
        logger.info(f"  Errores  : {summary['errors']}")
        logger.info(f"  Tiempo   : {elapsed:.1f}s")
        logger.info("=" * 60)
        return summary

    def _process_source(
        self,
        source: BaseSource,
        summary: dict,
        force_reload: bool,
        started_at: datetime,
    ) -> None:
        try:
            records = list(source.extract())
        except Exception as e:
            logger.error(f"Error extrayendo [{source.source_type}]: {e}")
            summary["errors"] += 1
            return

        if not records:
            logger.warning(f"[{source.source_type}] Sin registros.")
            return

        summary["total"] += len(records)

        for record in records:
            is_always = any(kw in record.table_name.lower() for kw in ALWAYS_PROCESS_KEYWORDS)
            src_file  = record.source_name.split(" / ")[0]
            src_sheet = record.source_name.split(" / ")[-1]

            if (
                not is_always
                and not force_reload
                and record.modified_at is not None
                and was_already_loaded(src_file, src_sheet, record.modified_at)
            ):
                logger.info(f"  Omitido (sin cambios): {record.source_name}")
                register_ingestion(
                    file_name=src_file, sheet_name=src_sheet,
                    target_table=record.table_name, status="SKIPPED",
                    file_id=record.source_id, source_modified_at=record.modified_at,
                    started_at=started_at,
                )
                summary["skipped"] += 1
                continue

            try:
                logger.info(f"  Cargando: {record.source_name} -> bronze.{record.table_name}")
                rows = self.loader.load_dataframe(
                    df=record.df,
                    source_name=record.source_name,
                    target_table=record.table_name,
                )
                register_ingestion(
                    file_name=src_file, sheet_name=src_sheet,
                    target_table=record.table_name, status="SUCCESS",
                    rows_loaded=rows, file_id=record.source_id,
                    source_modified_at=record.modified_at, started_at=started_at,
                )
                summary["success"] += 1
            except Exception as e:
                logger.error(f"  Error cargando {record.source_name}: {e}")
                register_ingestion(
                    file_name=src_file, sheet_name=src_sheet,
                    target_table=record.table_name, status="ERROR",
                    error_message=str(e), file_id=record.source_id,
                    source_modified_at=record.modified_at, started_at=started_at,
                )
                summary["errors"] += 1
