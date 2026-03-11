#!/usr/bin/env python3
# ============================================================
# scripts/setup_database.py
# Script de inicialización de la base de datos
# Ejecutar UNA VEZ antes del primer pipeline
# ============================================================

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from connectors.postgres_connector import postgres
from config.settings import settings
from utils.logger import logger


def setup():
    logger.info("Iniciando configuración de la base de datos...")
    logger.info(f"  Host : {settings.db.host}:{settings.db.port}")
    logger.info(f"  DB   : {settings.db.name}")
    logger.info(f"  User : {settings.db.user}")

    # 1. Crear schemas de las tres capas
    schemas = [
        settings.db.schema_bronze,
        "silver",   # Para la capa Plata (próximamente)
        "gold",     # Para la capa Oro (próximamente)
    ]

    for schema in schemas:
        postgres.create_schema_if_not_exists(schema)
        logger.info(f"  ✓ Schema '{schema}' listo.")

    # 2. Crear tabla de control en bronce
    from models.bronze.ingestion_control import ensure_control_table
    ensure_control_table()
    logger.info("  ✓ Tabla de control de ingesta lista.")

    # 3. Verificar conexión final
    with postgres.get_connection() as conn:
        result = conn.execute(text("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name IN ('bronze', 'silver', 'gold')
            ORDER BY schema_name
        """))
        found_schemas = [row[0] for row in result]
        logger.info(f"  ✓ Schemas verificados: {found_schemas}")

    logger.success("Base de datos configurada correctamente. ¡Listo para el pipeline!")


if __name__ == "__main__":
    setup()
