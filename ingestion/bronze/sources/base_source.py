# ============================================================
# ingestion/bronze/sources/base_source.py
# Clase base abstracta para todos los orígenes de datos
#
# Cada origen de datos (Excel, JSON/API, DB, CSV, etc.)
# debe heredar de BaseSource e implementar sus métodos.
# ============================================================

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterator
import pandas as pd


@dataclass
class SourceRecord:
    """
    Unidad de datos extraída de cualquier origen.
    Contiene el DataFrame + metadatos del origen.
    """
    df:           pd.DataFrame
    source_name:  str          # Nombre del origen (archivo, endpoint, tabla)
    table_name:   str          # Nombre destino en bronze
    source_id:    str = ""     # ID único del origen (file_id, url, etc.)
    modified_at:  object = None  # Fecha de modificación si aplica


class BaseSource(ABC):
    """
    Clase base para todos los conectores de origen.

    Cada origen implementa:
      - extract() → generador de SourceRecord
      - source_type → identificador del tipo ("excel", "json_api", "csv", etc.)
    """

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Identificador del tipo de origen. Ej: 'excel', 'json_api', 'csv'"""

    @abstractmethod
    def extract(self) -> Iterator[SourceRecord]:
        """
        Extrae datos del origen y los retorna como SourceRecord uno por uno.
        Debe ser un generador para soportar grandes volúmenes.
        """
