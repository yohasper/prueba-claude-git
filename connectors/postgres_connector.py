# ============================================================
# connectors/postgres_connector.py
# Conector a PostgreSQL usando SQLAlchemy
# ============================================================

from contextlib import contextmanager
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from utils.logger import logger


class PostgresConnector:
    """
    Conector centralizado a PostgreSQL.
    Maneja la creación del engine, sesiones y operaciones base.
    """

    def __init__(self, connection_url: Optional[str] = None):
        self._url = connection_url or settings.db.connection_url
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = self._create_engine()
        return self._engine

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _create_engine(self) -> Engine:
        """Crea el engine de SQLAlchemy con reintentos automáticos."""
        logger.info(f"Conectando a PostgreSQL: {settings.db.connection_url_safe}")
        engine = create_engine(
            self._url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,       # Verifica conexión antes de usar
            pool_recycle=3600,        # Recicla conexiones cada hora
        )
        # Test de conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.success("Conexión a PostgreSQL establecida correctamente.")
        return engine

    @contextmanager
    def get_connection(self):
        """Context manager para conexiones individuales."""
        conn = self.engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en transacción PostgreSQL: {e}")
            raise
        finally:
            conn.close()

    def create_schema_if_not_exists(self, schema: str) -> None:
        """Crea un schema si no existe."""
        with self.get_connection() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        logger.info(f"Schema '{schema}' verificado/creado.")

    def table_exists(self, table_name: str, schema: str) -> bool:
        """Verifica si una tabla existe en el schema dado."""
        query = text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = :schema
                AND table_name = :table
            )
        """)
        with self.get_connection() as conn:
            result = conn.execute(query, {"schema": schema, "table": table_name})
            return result.scalar()

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        if_exists: str = "append",
        chunksize: int = 1000,
    ) -> int:
        """
        Carga un DataFrame a PostgreSQL.

        Args:
            df:          DataFrame a cargar
            table_name:  Nombre de la tabla destino
            schema:      Schema destino
            if_exists:   'append', 'replace' o 'fail'
            chunksize:   Tamaño de lote para escritura

        Returns:
            Número de filas cargadas
        """
        if df.empty:
            logger.warning(f"DataFrame vacío, no se carga nada en {schema}.{table_name}")
            return 0

        try:
            rows = len(df)
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi",
            )
            logger.success(f"Cargadas {rows} filas → {schema}.{table_name}")
            return rows

        except SQLAlchemyError as e:
            logger.error(f"Error cargando datos en {schema}.{table_name}: {e}")
            raise

    def execute_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """Ejecuta una consulta SQL y devuelve un DataFrame."""
        with self.get_connection() as conn:
            return pd.read_sql(text(query), conn, params=params)

    def execute_ddl(self, statement: str):
        """Ejecuta un comando DDL o DML sin retorno (CREATE, TRUNCATE, DROP, etc.)."""
        with self.get_connection() as conn:
            conn.execute(text(statement))
            conn.commit()

    def close(self):
        """Cierra el engine y libera conexiones."""
        if self._engine:
            self._engine.dispose()
            logger.info("Conexión PostgreSQL cerrada.")
            self._engine = None


# Instancia global reutilizable
postgres = PostgresConnector()