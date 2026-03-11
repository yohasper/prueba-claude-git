# ============================================================
# ingestion/silver/silver_cleaner.py
# Limpieza y estandarización de datos para la capa Silver
# Reglas:
#   - Campos alfanuméricos → MAYÚSCULAS
#   - Campos numéricos decimales → 2 decimales
#   - Eliminar filas completamente vacías
#   - Eliminar columnas de auditoría de bronce
# ============================================================

import pandas as pd
import numpy as np
from utils.logger import logger


# Columnas de auditoría que vienen de Bronce y no deben procesarse como datos
BRONZE_AUDIT_COLS = {"_source_file", "_source_sheet", "_ingested_at"}


class SilverCleaner:
    """
    Aplica las reglas de limpieza y estandarización para la capa Silver.
    """

    def clean(self, df: pd.DataFrame, table_name: str = "") -> pd.DataFrame:
        """
        Ejecuta el pipeline completo de limpieza.

        Args:
            df:           DataFrame crudo proveniente de Bronce
            table_name:   Nombre de la tabla (para logs)

        Returns:
            DataFrame limpio y estandarizado
        """
        if df.empty:
            logger.warning(f"[{table_name}] DataFrame vacío, nada que limpiar.")
            return df

        logger.info(f"[{table_name}] Iniciando limpieza: {len(df)} filas × {len(df.columns)} cols")

        df = df.copy()

        # 1. Separar columnas de auditoría de bronce
        audit_cols = [c for c in df.columns if c in BRONZE_AUDIT_COLS]
        data_cols  = [c for c in df.columns if c not in BRONZE_AUDIT_COLS]

        # 2. Eliminar filas completamente vacías en columnas de datos
        before = len(df)
        df = df.dropna(subset=data_cols, how="all")
        dropped = before - len(df)
        if dropped:
            logger.debug(f"[{table_name}] Eliminadas {dropped} filas vacías.")

        # 3. Limpiar y convertir columnas de datos
        for col in data_cols:
            df[col] = self._clean_column(df[col], col, table_name)

        # 4. Agregar columna de auditoría Silver
        from datetime import datetime, timezone
        df["_silver_loaded_at"] = datetime.now(timezone.utc)

        # 5. Eliminar columnas de auditoría Bronce (ya no necesarias en Silver)
        df = df.drop(columns=audit_cols, errors="ignore")

        logger.info(f"[{table_name}] Limpieza completada: {len(df)} filas × {len(df.columns)} cols")
        return df

    def _clean_column(self, series: pd.Series, col_name: str, table_name: str) -> pd.Series:
        """
        Limpia una columna individual según su tipo inferido.
        - Numérico decimal → redondear a 2 decimales
        - Texto / alfanumérico → MAYÚSCULAS, strip de espacios
        - Entero → mantener como entero
        """
        # Intentar convertir a numérico
        numeric = pd.to_numeric(series, errors="coerce")
        non_null_ratio = numeric.notna().sum() / max(len(series), 1)

        if non_null_ratio >= 0.5:
            # Es columna numérica
            if self._has_decimals(numeric):
                # Decimal → 2 decimales
                result = numeric.round(2)
                logger.debug(f"[{table_name}] '{col_name}' → numérico decimal (2 dec)")
            else:
                # Entero → mantener sin decimales
                result = numeric
                logger.debug(f"[{table_name}] '{col_name}' → numérico entero")
            return result
        else:
            # Es columna de texto → MAYÚSCULAS + strip
            result = (
                series
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({"NAN": None, "NONE": None, "": None})
            )
            logger.debug(f"[{table_name}] '{col_name}' → texto (MAYÚSCULAS)")
            return result

    @staticmethod
    def _has_decimals(series: pd.Series) -> bool:
        """Detecta si una serie numérica tiene valores decimales reales."""
        clean = series.dropna()
        if clean.empty:
            return False
        return bool((clean % 1 != 0).any())
