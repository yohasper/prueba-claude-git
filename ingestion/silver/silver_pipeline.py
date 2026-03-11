# ============================================================
# ingestion/silver/silver_pipeline.py
# Pipeline principal de la capa Silver
#
# Flujo:
#   1. Lee tablas brutas desde bronze.*
#   2. Aplica limpieza (mayúsculas, 2 decimales, nulls)
#   3. Construye modelo dimensional (dims + fact)
#   4. Carga todo en silver.*
# ============================================================

from datetime import datetime, timezone

from connectors.postgres_connector import postgres
from config.settings import settings
from ingestion.silver.silver_cleaner import SilverCleaner
from models.silver.dimensions import (
    build_dim_clientes,
    build_dim_productos,
    build_dim_tiempo,
    build_fact_ventas,
    BRONZE_CLIENTES, BRONZE_CLIENTES_ALT,
    BRONZE_PRODUCTOS, BRONZE_PRODUCTOS_ALT,
    BRONZE_VENTAS, BRONZE_VENTAS_ALT,
)
from utils.logger import logger

SCHEMA_BRONZE = settings.db.schema_bronze
SCHEMA_SILVER = "silver"


class SilverPipeline:
    """
    Orquesta la construcción de la capa Silver (modelo dimensional)
    a partir de los datos crudos de la capa Bronze.
    """

    def __init__(self):
        self.cleaner = SilverCleaner()

    def run(self) -> dict:
        start = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE SILVER")
        logger.info(f"Fuente : {SCHEMA_BRONZE}")
        logger.info(f"Destino: {SCHEMA_SILVER}")
        logger.info("=" * 60)

        summary = {"total": 4, "success": 0, "errors": 0}

        # Asegurar schema silver
        postgres.create_schema_if_not_exists(SCHEMA_SILVER)

        # ── 1. Leer tablas desde Bronce ──────────────────────
        df_cli  = self._read_bronze(BRONZE_CLIENTES,  BRONZE_CLIENTES_ALT,  "clientes")
        df_prod = self._read_bronze(BRONZE_PRODUCTOS, BRONZE_PRODUCTOS_ALT, "productos")
        df_ven  = self._read_bronze(BRONZE_VENTAS,    BRONZE_VENTAS_ALT,    "ventas")

        if df_cli is None or df_prod is None or df_ven is None:
            logger.error("No se pudieron leer todas las tablas de Bronce. Abortando Silver.")
            summary["errors"] = summary["total"]
            return summary

        # ── 2. Limpiar datos ─────────────────────────────────
        df_cli  = self.cleaner.clean(df_cli,  "clientes")
        df_prod = self.cleaner.clean(df_prod, "productos")
        df_ven  = self.cleaner.clean(df_ven,  "ventas")

        # ── 3. Construir dimensiones ─────────────────────────
        dim_clientes  = build_dim_clientes(df_cli)
        dim_productos = build_dim_productos(df_prod)
        dim_tiempo    = build_dim_tiempo(start_year=2024)
        fact_ventas   = build_fact_ventas(df_ven, dim_clientes, dim_productos, dim_tiempo)

        # ── 4. Cargar en Silver ──────────────────────────────
        tables = {
            "dim_clientes":  dim_clientes,
            "dim_productos": dim_productos,
            "dim_tiempo":    dim_tiempo,
            "fact_ventas":   fact_ventas,
        }

        for table_name, df in tables.items():
            try:
                rows = postgres.load_dataframe(
                    df=df,
                    table_name=table_name,
                    schema=SCHEMA_SILVER,
                    if_exists="replace",
                    chunksize=settings.ingest.batch_size,
                )
                logger.success(f"✓ silver.{table_name} → {rows} filas")
                summary["success"] += 1
            except Exception as e:
                logger.error(f"✗ Error cargando silver.{table_name}: {e}")
                summary["errors"] += 1

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info("=" * 60)
        logger.info("PIPELINE SILVER FINALIZADO")
        logger.info(f"  Tablas exitosas : {summary['success']}/{summary['total']}")
        logger.info(f"  Errores         : {summary['errors']}")
        logger.info(f"  Tiempo total    : {elapsed:.1f}s")
        logger.info("=" * 60)

        return summary

    def _read_bronze(self, table_name: str, alt_name: str, label: str):
        """
        Lee una tabla del schema Bronze. Intenta primero con el nombre
        principal y luego con el alternativo.
        """
        for name in [table_name, alt_name]:
            try:
                if postgres.table_exists(name, SCHEMA_BRONZE):
                    df = postgres.execute_query(
                        f'SELECT * FROM "{SCHEMA_BRONZE}"."{name}"'
                    )
                    logger.info(f"Leído bronze.{name}: {len(df)} filas")
                    return df
            except Exception as e:
                logger.debug(f"No se pudo leer {name}: {e}")

        # Buscar por patrón si los nombres exactos no coinciden
        df = self._search_bronze_table(label)
        if df is not None:
            return df

        logger.error(f"No se encontró la tabla de {label} en bronze.")
        return None

    def _search_bronze_table(self, keyword: str):
        """
        Busca en bronze.* tablas cuyo nombre contenga el keyword.
        Útil cuando el nombre exacto no es conocido.
        """
        try:
            result = postgres.execute_query(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{SCHEMA_BRONZE}'
                  AND table_name ILIKE '%{keyword}%'
                ORDER BY table_name
                LIMIT 1
            """)
            if not result.empty:
                found = result["table_name"].iloc[0]
                logger.info(f"Tabla encontrada por búsqueda: bronze.{found}")
                return postgres.execute_query(
                    f'SELECT * FROM "{SCHEMA_BRONZE}"."{found}"'
                )
        except Exception as e:
            logger.debug(f"Error buscando tabla con keyword '{keyword}': {e}")
        return None
