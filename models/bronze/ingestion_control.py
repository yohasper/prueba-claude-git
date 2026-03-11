# ============================================================
# models/bronze/ingestion_control.py
# Tabla de control de ingesta en la capa Bronce
# ============================================================

from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from connectors.postgres_connector import postgres
from config.settings import settings
from utils.logger import logger


CONTROL_TABLE = "ingestion_control"


def ensure_control_table() -> None:
    """
    Crea la tabla de control de ingesta si no existe.
    Registra cada ejecución de carga: archivo, hoja, estado, errores.
    """
    schema = settings.db.schema_bronze

    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."{CONTROL_TABLE}" (
        id                  SERIAL PRIMARY KEY,
        file_id             VARCHAR(255),
        file_name           VARCHAR(500)    NOT NULL,
        sheet_name          VARCHAR(255),
        target_table        VARCHAR(255),
        rows_loaded         INTEGER         DEFAULT 0,
        status              VARCHAR(50)     NOT NULL,  -- 'SUCCESS', 'ERROR', 'SKIPPED'
        error_message       TEXT,
        started_at          TIMESTAMPTZ     NOT NULL,
        finished_at         TIMESTAMPTZ,
        source_modified_at  TIMESTAMPTZ,
        loaded_by           VARCHAR(100)    DEFAULT current_user,
        env                 VARCHAR(50)
    );
    """

    with postgres.get_connection() as conn:
        from sqlalchemy import text
        conn.execute(text(ddl))

    logger.info(f"Tabla de control '{schema}.{CONTROL_TABLE}' verificada.")


def register_ingestion(
    file_name: str,
    sheet_name: str,
    target_table: str,
    status: str,
    rows_loaded: int = 0,
    error_message: Optional[str] = None,
    file_id: Optional[str] = None,
    source_modified_at: Optional[datetime] = None,
    started_at: Optional[datetime] = None,
) -> None:
    """
    Registra el resultado de una ingesta en la tabla de control.

    Args:
        file_name:           Nombre del archivo fuente
        sheet_name:          Nombre de la hoja cargada
        target_table:        Tabla destino en bronce
        status:              'SUCCESS', 'ERROR' o 'SKIPPED'
        rows_loaded:         Número de filas cargadas
        error_message:       Mensaje de error si aplica
        file_id:             ID del archivo en Google Drive
        source_modified_at:  Fecha de modificación del archivo fuente
        started_at:          Inicio del proceso
    """
    schema = settings.db.schema_bronze
    now = datetime.now(timezone.utc)

    record = pd.DataFrame([{
        "file_id": file_id,
        "file_name": file_name,
        "sheet_name": sheet_name,
        "target_table": target_table,
        "rows_loaded": rows_loaded,
        "status": status,
        "error_message": error_message,
        "started_at": started_at or now,
        "finished_at": now,
        "source_modified_at": source_modified_at,
        "env": settings.ingest.env,
    }])

    postgres.load_dataframe(
        df=record,
        table_name=CONTROL_TABLE,
        schema=schema,
        if_exists="append",
    )

    log_msg = f"[{status}] {file_name} / {sheet_name} → {target_table} ({rows_loaded} filas)"
    if status == "SUCCESS":
        logger.success(log_msg)
    elif status == "ERROR":
        logger.error(log_msg)
    else:
        logger.warning(log_msg)


def was_already_loaded(file_name: str, sheet_name: str, source_modified_at: Optional[datetime]) -> bool:
    """
    Verifica si un archivo/hoja ya fue cargado exitosamente
    con la misma fecha de modificación (evita recargas innecesarias).
    """
    if source_modified_at is None:
        return False

    schema = settings.db.schema_bronze
    query = f"""
        SELECT COUNT(*) FROM "{schema}"."{CONTROL_TABLE}"
        WHERE file_name = :fname
          AND sheet_name = :sname
          AND source_modified_at = :mod_at
          AND status = 'SUCCESS'
    """
    from sqlalchemy import text
    with postgres.get_connection() as conn:
        result = conn.execute(
            text(query),
            {"fname": file_name, "sname": sheet_name, "mod_at": source_modified_at}
        )
        count = result.scalar()

    return count > 0
