# ============================================================
# ingestion/gold/gold_pipeline.py
# Pipeline principal de la capa Gold
#
# Flujo:
#   1. Lee tablas del schema silver.*
#   2. Calcula los 3 KPIs
#   3. Carga resultados en gold.*
# ============================================================

from datetime import datetime, timezone

from connectors.postgres_connector import postgres
from config.settings import settings
from models.gold.kpis import (
    build_kpi_ventas_por_periodo,
    build_kpi_ventas_por_cliente,
    build_kpi_ventas_por_producto,
)
from utils.logger import logger

SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"


class GoldPipeline:
    """
    Orquesta la construcción de la capa Gold (KPIs analíticos)
    a partir del modelo dimensional de la capa Silver.
    """

    def run(self) -> dict:
        start = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("INICIANDO PIPELINE GOLD")
        logger.info(f"Fuente : {SCHEMA_SILVER}")
        logger.info(f"Destino: {SCHEMA_GOLD}")
        logger.info("=" * 60)

        summary = {"total": 3, "success": 0, "errors": 0}

        # Asegurar schema gold
        postgres.create_schema_if_not_exists(SCHEMA_GOLD)

        # ── 1. Leer tablas Silver ────────────────────────────
        fact         = self._read_silver("fact_ventas")
        dim_tiempo   = self._read_silver("dim_tiempo")
        dim_clientes = self._read_silver("dim_clientes")
        dim_productos= self._read_silver("dim_productos")

        if any(df is None for df in [fact, dim_tiempo, dim_clientes, dim_productos]):
            logger.error("No se pudieron leer todas las tablas Silver. Abortando Gold.")
            summary["errors"] = summary["total"]
            return summary

        logger.info(f"fact_ventas    : {len(fact)} filas")
        logger.info(f"dim_tiempo     : {len(dim_tiempo)} filas")
        logger.info(f"dim_clientes   : {len(dim_clientes)} filas")
        logger.info(f"dim_productos  : {len(dim_productos)} filas")

        # ── 2. Calcular y cargar KPIs ────────────────────────
        kpis = {
            "kpi_ventas_por_periodo": lambda: build_kpi_ventas_por_periodo(
                fact, dim_tiempo
            ),
            "kpi_ventas_por_cliente": lambda: build_kpi_ventas_por_cliente(
                fact, dim_clientes, dim_tiempo
            ),
            "kpi_ventas_por_producto": lambda: build_kpi_ventas_por_producto(
                fact, dim_productos, dim_tiempo
            ),
        }

        for table_name, builder in kpis.items():
            try:
                df_kpi = builder()
                rows = postgres.load_dataframe(
                    df=df_kpi,
                    table_name=table_name,
                    schema=SCHEMA_GOLD,
                    if_exists="replace",
                    chunksize=settings.ingest.batch_size,
                )
                logger.success(f"✓ gold.{table_name} → {rows} filas")
                summary["success"] += 1
            except Exception as e:
                logger.error(f"✗ Error construyendo gold.{table_name}: {e}")
                summary["errors"] += 1

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info("=" * 60)
        logger.info("PIPELINE GOLD FINALIZADO")
        logger.info(f"  KPIs exitosos : {summary['success']}/{summary['total']}")
        logger.info(f"  Errores       : {summary['errors']}")
        logger.info(f"  Tiempo total  : {elapsed:.1f}s")
        logger.info("=" * 60)

        return summary

    def _read_silver(self, table_name: str):
        """Lee una tabla del schema Silver."""
        try:
            if not postgres.table_exists(table_name, SCHEMA_SILVER):
                logger.error(f"Tabla silver.{table_name} no existe. Ejecuta --layer silver primero.")
                return None
            df = postgres.execute_query(f'SELECT * FROM "{SCHEMA_SILVER}"."{table_name}"')
            return df
        except Exception as e:
            logger.error(f"Error leyendo silver.{table_name}: {e}")
            return None
