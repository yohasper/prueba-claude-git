# ============================================================
# ingestion/bronze/bronze_loader.py
# Carga de datos al schema Bronce con staging de novedades
#
# Estrategia por tabla:
#   - ventas    → INCREMENTAL por idventa  → staging: stg_ventas_nuevos
#   - clientes  → REPLACE completo         → staging: stg_clientes_nuevos (copia total)
#   - productos → REPLACE completo         → staging: stg_productos_nuevos (copia total)
#   - resto     → replace completo sin staging
#
# Flujo por ejecución:
#   1. Al inicio → TRUNCATE de las 3 tablas staging
#   2. ventas    → detecta IDs nuevos → inserta solo nuevos → guarda nuevos en staging
#   3. clientes  → replace completo  → copia todo a staging
#   4. productos → replace completo  → copia todo a staging
#   5. Al final puedes consultar stg_*_nuevos para ver la data cargada
# ============================================================

import re
import pandas as pd
from datetime import datetime, timezone

from connectors.postgres_connector import postgres
from config.settings import settings
from utils.excel_reader import ExcelReader
from utils.logger import logger


# ── Tablas con staging (keyword → staging table) ─────────────
STAGING_TABLES = {
    "clientes":  "stg_clientes_nuevos",
    "productos": "stg_productos_nuevos",
    "ventas":    "stg_ventas_nuevos",
}

# ── Solo ventas usa carga incremental por ID ─────────────────
INCREMENTAL_ID = {
    "ventas": "idventa",
}


class BronzeLoader:
    """
    Normaliza y carga datos en la capa Bronce.
    - ventas    : incremental por ID + staging de nuevos
    - clientes  : replace completo + staging copia total
    - productos : replace completo + staging copia total
    """

    def __init__(self):
        self.schema = settings.db.schema_bronze
        self.reader = ExcelReader()

    # ── Llamar al inicio de cada ejecución ───────────────────
    def truncate_staging_tables(self):
        """Trunca o crea las 3 tablas staging al inicio de cada ejecución."""
        logger.info("Preparando tablas staging de novedades...")
        for keyword, stg_table in STAGING_TABLES.items():
            try:
                if postgres.table_exists(stg_table, self.schema):
                    postgres.execute_ddl(
                        f'TRUNCATE TABLE "{self.schema}"."{stg_table}"'
                    )
                    logger.info(f"  ✓ TRUNCATE {self.schema}.{stg_table}")
                else:
                    postgres.execute_ddl(f'''
                        CREATE TABLE IF NOT EXISTS "{self.schema}"."{stg_table}" (
                            _stg_id        SERIAL PRIMARY KEY,
                            _stg_loaded_at TIMESTAMP WITH TIME ZONE,
                            _source_file   TEXT,
                            _source_sheet  TEXT,
                            _ingested_at   TIMESTAMP WITH TIME ZONE
                        )
                    ''')
                    logger.info(f"  ✓ CREADA {self.schema}.{stg_table}")
            except Exception as e:
                logger.warning(f"  No se pudo preparar staging {stg_table}: {e}")

    def build_table_name(self, file_name: str, sheet_name: str) -> str:
        def clean(text: str) -> str:
            text = str(text).strip().lower()
            text = re.sub(r"[^a-z0-9]+", "_", text)
            return text.strip("_")
        file_stem  = re.sub(r"\.(xlsx|xls|csv)$", "", file_name, flags=re.IGNORECASE)
        table_name = f"{clean(file_stem)}__{clean(sheet_name)}"
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
        logger.info(f"  Cargando hoja '{sheet_name}' → {self.schema}.{target_table}")

        df_normalized = self.reader.normalize_dataframe(
            df=df.copy(),
            source_file=file_name,
            source_sheet=sheet_name,
        )

        if df_normalized.empty:
            logger.warning(f"  Hoja '{sheet_name}' vacía, se omite.")
            return 0

        audit_cols = ["_source_file", "_source_sheet", "_ingested_at"]
        data_cols  = [c for c in df_normalized.columns if c not in audit_cols]
        df_normalized[data_cols] = (
            df_normalized[data_cols].astype(str).replace("nan", None)
        )

        # Detectar qué tipo de carga aplica
        keyword    = self._get_keyword(target_table)
        stg_table  = STAGING_TABLES.get(keyword)
        id_col     = INCREMENTAL_ID.get(keyword)

        if keyword and id_col:
            # ventas → incremental
            rows = self._load_incremental(df_normalized, target_table, id_col, stg_table)
        elif keyword and stg_table:
            # clientes / productos → replace completo + copia a staging
            rows = self._load_replace_with_staging(df_normalized, target_table, stg_table)
        else:
            # resto → replace completo sin staging
            rows = self._load_replace(df_normalized, target_table)

        logger.info(f"  ✓ {rows} filas procesadas en {self.schema}.{target_table}")
        return rows

    # ── VENTAS: carga incremental + staging de nuevos ─────────
    def _load_incremental(
        self,
        df: pd.DataFrame,
        target_table: str,
        id_col: str,
        stg_table: str,
    ) -> int:
        schema = self.schema
        logger.info(f"  Modo incremental activado — clave: '{id_col}'")

        # Tabla no existe → carga completa
        if not postgres.table_exists(target_table, schema):
            logger.info(f"  Tabla nueva, carga completa ({len(df)} registros)")
            rows = postgres.load_dataframe(
                df=df, table_name=target_table, schema=schema,
                if_exists="append", chunksize=settings.ingest.batch_size,
            )
            self._save_to_staging(df, stg_table)
            return rows

        # Obtener IDs existentes
        try:
            existing = postgres.execute_query(
                f'SELECT "{id_col}" FROM "{schema}"."{target_table}"'
            )
            existing_ids = set(existing[id_col].astype(str).str.strip().tolist())
        except Exception as e:
            logger.warning(f"  No se pudieron leer IDs: {e}. Usando replace.")
            return self._load_replace(df, target_table)

        # Filtrar solo nuevos
        df_ids = df[id_col].astype(str).str.strip()
        df_new = df[~df_ids.isin(existing_ids)].copy()

        logger.info(f"  Registros en Excel    : {len(df)}")
        logger.info(f"  Ya existentes en BD   : {len(existing_ids)}")
        logger.info(f"  Omitidos (duplicados) : {len(df) - len(df_new)}")
        logger.info(f"  Nuevos a insertar     : {len(df_new)}")

        if df_new.empty:
            logger.info(f"  Sin registros nuevos — tabla intacta.")
            return 0

        rows = postgres.load_dataframe(
            df=df_new, table_name=target_table, schema=schema,
            if_exists="append", chunksize=settings.ingest.batch_size,
        )
        self._save_to_staging(df_new, stg_table)
        return rows

    # ── CLIENTES / PRODUCTOS: replace completo + staging ──────
    def _load_replace_with_staging(
        self,
        df: pd.DataFrame,
        target_table: str,
        stg_table: str,
    ) -> int:
        logger.info(f"  Modo replace completo con staging")
        rows = postgres.load_dataframe(
            df=df, table_name=target_table, schema=self.schema,
            if_exists="replace", chunksize=settings.ingest.batch_size,
        )
        # Copiar toda la data cargada a staging
        self._save_to_staging(df, stg_table)
        return rows

    # ── Guardar en staging ────────────────────────────────────
    def _save_to_staging(self, df: pd.DataFrame, stg_table: str):
        """Inserta registros en la tabla staging con columna de auditoría."""
        try:
            df_stg = df.copy()
            df_stg["_stg_loaded_at"] = datetime.now(timezone.utc)
            df_stg = df_stg.drop(columns=["_stg_id"], errors="ignore")

            # Asegurar que existen las columnas en staging
            if postgres.table_exists(stg_table, self.schema):
                self._ensure_staging_columns(df_stg, stg_table)

            postgres.load_dataframe(
                df=df_stg, table_name=stg_table, schema=self.schema,
                if_exists="append", chunksize=settings.ingest.batch_size,
            )
            logger.info(f"  ✓ {len(df_stg)} registros en staging {self.schema}.{stg_table}")
        except Exception as e:
            logger.warning(f"  No se pudo guardar en staging {stg_table}: {e}")

    def _ensure_staging_columns(self, df: pd.DataFrame, stg_table: str):
        """Agrega columnas faltantes en la tabla staging con ALTER TABLE."""
        try:
            existing_cols = postgres.execute_query(f'''
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = '{self.schema}' AND table_name = '{stg_table}'
            ''')
            existing_set = set(existing_cols["column_name"].tolist())
            for col in df.columns:
                if col not in existing_set:
                    postgres.execute_ddl(
                        f'ALTER TABLE "{self.schema}"."{stg_table}" '
                        f'ADD COLUMN IF NOT EXISTS "{col}" TEXT'
                    )
        except Exception as e:
            logger.warning(f"  No se pudieron verificar columnas de {stg_table}: {e}")

    # ── Replace simple (tablas sin staging) ──────────────────
    def _load_replace(self, df: pd.DataFrame, target_table: str) -> int:
        logger.info(f"  Modo replace completo")
        return postgres.load_dataframe(
            df=df, table_name=target_table, schema=self.schema,
            if_exists="replace", chunksize=settings.ingest.batch_size,
        )

    # ── Helper: detectar keyword de la tabla ─────────────────
    def _get_keyword(self, table_name: str) -> str | None:
        for keyword in STAGING_TABLES:
            if keyword in table_name.lower():
                return keyword
        return None