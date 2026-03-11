# ============================================================
# utils/logger.py
# Logger centralizado con Loguru
# ============================================================

import sys
from pathlib import Path
from loguru import logger as _logger


def setup_logger(log_level: str = "INFO", log_path: str = "logs/") -> object:
    """
    Configura el logger global del proyecto.

    Args:
        log_level: Nivel de log (DEBUG, INFO, WARNING, ERROR)
        log_path:  Directorio donde se guardan los archivos de log

    Returns:
        Instancia configurada de loguru logger
    """
    Path(log_path).mkdir(parents=True, exist_ok=True)

    # Eliminar configuración por defecto
    _logger.remove()

    # Log a consola con colores
    _logger.add(
        sys.stdout,
        level=log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # Log a archivo rotativo diario
    _logger.add(
        f"{log_path}/dw_{{time:YYYY-MM-DD}}.log",
        level=log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation="00:00",       # Rota a medianoche
        retention="30 days",    # Conserva 30 días
        compression="zip",
        encoding="utf-8",
    )

    # Log de errores separado
    _logger.add(
        f"{log_path}/dw_errors.log",
        level="ERROR",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        rotation="10 MB",
        retention="60 days",
        compression="zip",
        encoding="utf-8",
    )

    return _logger


# Logger listo para importar en cualquier módulo
logger = setup_logger()
