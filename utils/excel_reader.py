# ============================================================
# utils/excel_reader.py
# Lectura y normalización de archivos Excel con pandas
# ============================================================

import io
from typing import Optional

import pandas as pd

from utils.logger import logger


class ExcelReader:
    """
    Lee archivos Excel (.xlsx, .xls) y los normaliza
    para la ingesta a la capa Bronce.
    """

    @staticmethod
    def read_excel(
        source: str | io.BytesIO,
        sheet_name: str | int | None = 0,
        header_row: int = 0,
        dtype: Optional[dict] = None,
    ) -> dict[str, pd.DataFrame]:
        """
        Lee un archivo Excel y devuelve un dict con todas las hojas.

        Args:
            source:     Ruta local o BytesIO del archivo
            sheet_name: Hoja específica o None para todas
            header_row: Fila del encabezado (0-indexed)
            dtype:      Tipos de columnas opcionales

        Returns:
            {nombre_hoja: DataFrame}
        """
        try:
            sheets = pd.read_excel(
                source,
                sheet_name=sheet_name if sheet_name is not None else None,
                header=header_row,
                dtype=dtype,
                engine="openpyxl",
            )

            # Normalizar: si viene un solo DataFrame, convertir a dict
            if isinstance(sheets, pd.DataFrame):
                name = sheet_name if isinstance(sheet_name, str) else "Sheet1"
                sheets = {name: sheets}

            logger.info(f"Excel leído: {len(sheets)} hoja(s) → {list(sheets.keys())}")
            return sheets

        except Exception as e:
            logger.error(f"Error leyendo Excel: {e}")
            raise

    @staticmethod
    def normalize_dataframe(df: pd.DataFrame, source_file: str = "", source_sheet: str = "") -> pd.DataFrame:
        """
        Normaliza columnas y agrega metadata de trazabilidad para Bronce.

        Acciones:
        - Limpia nombres de columnas (snake_case, sin espacios ni caracteres especiales)
        - Elimina filas completamente vacías
        - Agrega columnas de auditoría: _source_file, _source_sheet, _ingested_at
        """
        import re
        from datetime import datetime, timezone

        if df.empty:
            logger.warning("DataFrame vacío recibido para normalizar.")
            return df

        # 1. Limpiar nombres de columnas
        df.columns = [
            re.sub(r"[^a-z0-9_]", "_",
                str(col).strip().lower()
                       .replace(" ", "_")
                       .replace("-", "_")
                       .replace(".", "_")
            ).strip("_")
            for col in df.columns
        ]

        # Eliminar columnas duplicadas (por si hay después de normalizar)
        df = df.loc[:, ~df.columns.duplicated()]

        # 2. Eliminar filas completamente vacías
        rows_before = len(df)
        df = df.dropna(how="all")
        rows_dropped = rows_before - len(df)
        if rows_dropped > 0:
            logger.debug(f"Eliminadas {rows_dropped} filas vacías.")

        # 3. Agregar columnas de auditoría (trazabilidad Bronce)
        now = datetime.now(timezone.utc)
        df["_source_file"] = source_file
        df["_source_sheet"] = source_sheet
        df["_ingested_at"] = now

        logger.debug(f"DataFrame normalizado: {len(df)} filas × {len(df.columns)} columnas")
        return df
