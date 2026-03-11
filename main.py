# ============================================================
# main.py
# Punto de entrada principal del DataWarehouse Pipeline
# ============================================================

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from utils.logger import setup_logger, logger
from ingestion.bronze.bronze_pipeline import BronzePipeline
from ingestion.silver.silver_pipeline import SilverPipeline
from ingestion.gold.gold_pipeline import GoldPipeline


def parse_args():
    parser = argparse.ArgumentParser(
        description="DataWarehouse Pipeline - Ingesta desde Google Drive",
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
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="bronze",
        help="Capa del pipeline a ejecutar (default: bronze)"
    )
    parser.add_argument(
        "--folder",
        type=str,
        help="ID de carpeta en Google Drive (sobreescribe .env)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forzar recarga aunque los archivos ya estén cargados"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Nivel de logging"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    log_level = args.log_level or settings.ingest.log_level
    setup_logger(log_level=log_level, log_path=settings.ingest.log_path)

    logger.info(f"Entorno : {settings.ingest.env}")
    logger.info(f"DB      : {settings.db.connection_url_safe}")

    summary = {}

    # ── BRONCE ────────────────────────────────────────────────
    if args.layer in ("bronze", "all"):
        summary["bronze"] = BronzePipeline(folder_id=args.folder).run(
            force_reload=args.force
        )

    # ── SILVER ────────────────────────────────────────────────
    if args.layer in ("silver", "all"):
        summary["silver"] = SilverPipeline().run()

    # ── GOLD ──────────────────────────────────────────────────
    if args.layer in ("gold", "all"):
        summary["gold"] = GoldPipeline().run()

    # Código de salida
    has_errors = any(s.get("errors", 0) > 0 for s in summary.values())
    sys.exit(1 if has_errors else 0)


if __name__ == "__main__":
    main()
