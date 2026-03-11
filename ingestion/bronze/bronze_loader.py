# ============================================================
# ingestion/bronze/bronze_loader.py
# Carga de DataFrames normalizados al schema Bronce en PostgreSQL
# ============================================================

import re

import pandas as pd

from connectors.postgres_connector import postgres
from config.settings import settings
from utils.excel_reader import ExcelReader
from utils.logger import logger


class BronzeLoader:
    """
    Responsable de normalizar y cargar datos en la capa Bronce.
    
    En la capa Bronce los datos se cargan tal como vienen de la fuente,
    solo con limpieza mínima de nombres y columnas de auditoría.
    NO se aplican transformaciones de negocio (eso es capa Plata/Oro).
    """

    def __init__(self):
        self.schema = settings.db.schema_bronze
        self.reader = ExcelReader()

    def build_table_name(self, file_name: str, sheet_name: str) -> str:
        """
        Genera el nombre de tabla en Bronce a partir del archivo y hoja.
        Formato: {nombre_archivo}__{nombre_hoja}

        Ejemplo:
            "Ventas 2024.xlsx" + "Enero" → "ventas_2024__enero"
        """
        def clean(text: str) -> str:
            text = str(text).strip().lower()
            text = re.sub(r"[^a-z0-9]+", "_", text)
            text = text.strip("_")
            return text

        # Quitar extensión del archivo
        file_stem = re.sub(r"\.(xlsx|xls|csv)$", "", file_name, flags=re.IGNORECASE)
        table_name = f"{clean(file_stem)}__{clean(sheet_name)}"

        # PostgreSQL tiene límite de 63 chars para nombres de objetos
        if len(table_name) > 63:
            table_name = table_name[:63].rstrip("_")

        return table_name

    def load_sheet(
        self,
        df: pd.DataFrame,
        file_name: str,
        sheet_name: str,
        target_table: str,
    ) -> int:
        """
        Normaliza y carga una hoja Excel al schema Bronce.

        Args:
            df:            DataFrame crudo de la hoja
            file_name:     Nombre del archivo fuente (para auditoría)
            sheet_name:    Nombre de la hoja (para auditoría)
            target_table:  Nombre de tabla destino en PostgreSQL

        Returns:
            Número de filas cargadas
        """
        logger.info(f"  Cargando hoja '{sheet_name}' → {self.schema}.{target_table}")

        # Normalizar DataFrame (snake_case, filas vacías, auditoría)
        df_normalized = self.reader.normalize_dataframe(
            df=df.copy(),
            source_file=file_name,
            source_sheet=sheet_name,
        )

        if df_normalized.empty:
            logger.warning(f"  Hoja '{sheet_name}' vacía, se omite.")
            return 0

        # Todos los datos en bronce se convierten a string
        # para preservar fidelidad con la fuente sin errores de tipo
        audit_cols = ["_source_file", "_source_sheet", "_ingested_at"]
        data_cols = [c for c in df_normalized.columns if c not in audit_cols]
        df_normalized[data_cols] = df_normalized[data_cols].astype(str).replace("nan", None)

        # Cargar a PostgreSQL (replace si ya existe la tabla)
        rows = postgres.load_dataframe(
            df=df_normalized,
            table_name=target_table,
            schema=self.schema,
            if_exists="replace",   # En Bronce: replace completo en cada carga
            chunksize=settings.ingest.batch_size,
        )

        logger.info(f"  ✓ {rows} filas cargadas en {self.schema}.{target_table}")
        return rows
