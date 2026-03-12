# ============================================================
# main.py
# Punto de entrada principal del DataWarehouse Pipeline
#
# Para agregar un nuevo origen de datos edita _build_sources():
#   - ExcelDriveSource : archivos Excel desde Google Drive
#   - JsonApiSource    : endpoints JSON/REST públicos o privados
# ============================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from utils.logger import setup_logger, logger
from ingestion.bronze.bronze_pipeline import BronzePipeline
from ingestion.bronze.sources.excel_source import ExcelDriveSource
from ingestion.bronze.sources.json_api_source import JsonApiSource
from ingestion.silver.silver_pipeline import SilverPipeline
from ingestion.gold.gold_pipeline import GoldPipeline


def parse_args():
    parser = argparse.ArgumentParser(
        description="DataWarehouse Pipeline - Multi-origen",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python main.py --layer bronze
  python main.py --layer silver
  python main.py --layer gold
  python main.py --layer all
  python main.py --layer bronze --force
        """
    )
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"],
                        default="bronze", help="Capa a ejecutar (default: bronze)")
    parser.add_argument("--folder", type=str,
                        help="ID de carpeta Google Drive (sobreescribe .env)")
    parser.add_argument("--force", action="store_true",
                        help="Forzar recarga aunque no haya cambios")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        default=None, help="Nivel de logging")
    return parser.parse_args()


def _build_sources(folder_id=None):
    """
    ╔══════════════════════════════════════════════════════════════╗
    ║  CONFIGURACIÓN DE ORÍGENES DE DATOS                          ║
    ║                                                              ║
    ║  Agrega o comenta orígenes según los que necesites activar.  ║
    ║  El pipeline procesará todos en orden de arriba hacia abajo. ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    sources = []

    # ── ORIGEN 1: Excel desde Google Drive ───────────────────
    # Carga las hojas clientes, productos y ventas del archivo
    # "Ejercicio Sesion #7" ubicado en la carpeta configurada en .env
    sources.append(
        ExcelDriveSource(
            folder_id=folder_id or settings.gdrive.folder_id,
            file_filter="Ejercicio Sesion #7",
            sheet_filter=["clientes", "productos", "ventas"],
        )
    )

    # ── ORIGEN 2: API pública de países (JSON lista directa) ──
    # URL: https://testapi.devtoolsdaily.com/countries
    # Respuesta: [ {"name": "...", "iso2": "...", "iso3": "..."}, ... ]
    # Como la respuesta es una lista directa, NO se necesita data_key.
    sources.append(
        JsonApiSource(
            endpoints=[
                {
                    "url":        "https://testapi.devtoolsdaily.com/countries",
                    "table_name": "api_paises",
                    # data_key no aplica → la respuesta ya es una lista [ {}, {}, ... ]
                },
            ]
        )
    )

    # ── ORIGEN 3: API pública de posts (JSON con data_key) ────
    # URL: https://dummyjson.com/posts
    # Respuesta: { "posts": [ {...}, {...} ], "total": 150, ... }
    # La lista de registros viene dentro de la key "posts" → se usa data_key.
    sources.append(
        JsonApiSource(
            endpoints=[
                {
                    "url":        "https://dummyjson.com/posts",
                    "table_name": "api_posts",
                    "data_key":   "posts",   # ← la lista está dentro de {"posts": [...]}
                },
            ]
        )
    )

    # ── CÓMO AGREGAR MÁS ORÍGENES ────────────────────────────
    #
    # Caso A: API con respuesta envuelta en un objeto
    # ------------------------------------------------
    # Si la API devuelve: { "data": [ {}, {} ], "total": 250 }
    # debes indicar data_key para que el conector sepa dónde está la lista:
    #
    # sources.append(
    #     JsonApiSource(
    #         endpoints=[
    #             {
    #                 "url":        "https://api.ejemplo.com/v1/ordenes",
    #                 "table_name": "api_ordenes",
    #                 "data_key":   "data",       # ← key que contiene la lista
    #             },
    #         ]
    #     )
    # )
    #
    # Caso B: API privada con autenticación Bearer token
    # ---------------------------------------------------
    # sources.append(
    #     JsonApiSource(
    #         default_headers={"Authorization": "Bearer TU_TOKEN_AQUI"},
    #         endpoints=[
    #             {
    #                 "url":        "https://api.privada.com/clientes",
    #                 "table_name": "api_clientes_ext",
    #                 "data_key":   "results",
    #             },
    #             {
    #                 "url":        "https://api.privada.com/facturas",
    #                 "table_name": "api_facturas",
    #                 "data_key":   "results",
    #             },
    #         ]
    #     )
    # )
    #
    # Caso C: API con API Key en headers
    # ------------------------------------
    # sources.append(
    #     JsonApiSource(
    #         endpoints=[
    #             {
    #                 "url":        "https://api.ejemplo.com/productos",
    #                 "table_name": "api_productos_ext",
    #                 "headers":    {"x-api-key": "TU_API_KEY"},
    #             },
    #         ]
    #     )
    # )
    #
    # Caso D: Otro Excel desde otra carpeta de Drive
    # -----------------------------------------------
    # sources.append(
    #     ExcelDriveSource(
    #         folder_id="ID_OTRA_CARPETA",
    #         file_filter="Reporte Mensual",
    #         sheet_filter=["resumen", "detalle"],
    #     )
    # )

    return sources


def main():
    args = parse_args()

    log_level = args.log_level or settings.ingest.log_level
    setup_logger(log_level=log_level, log_path=settings.ingest.log_path)

    logger.info(f"Entorno : {settings.ingest.env}")
    logger.info(f"DB      : {settings.db.connection_url_safe}")

    summary = {}

    if args.layer in ("bronze", "all"):
        sources = _build_sources(folder_id=args.folder)
        summary["bronze"] = BronzePipeline(sources=sources).run(
            force_reload=args.force
        )

    if args.layer in ("silver", "all"):
        summary["silver"] = SilverPipeline().run()

    if args.layer in ("gold", "all"):
        summary["gold"] = GoldPipeline().run()

    has_errors = any(s.get("errors", 0) > 0 for s in summary.values())
    sys.exit(1 if has_errors else 0)


if __name__ == "__main__":
    main()
