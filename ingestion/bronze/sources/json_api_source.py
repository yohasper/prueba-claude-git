# ============================================================
# ingestion/bronze/sources/json_api_source.py
# Origen de datos: JSON consumido desde una API REST (URL)
# ============================================================

import json
from typing import Iterator, Optional, List
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

import pandas as pd

from ingestion.bronze.sources.base_source import BaseSource, SourceRecord
from utils.logger import logger


class JsonApiSource(BaseSource):
    """
    Extrae datos JSON desde una o varias URLs de API REST.

    Soporta:
      - GET sin autenticación
      - GET con headers (Bearer token, API Key, etc.)
      - Respuesta JSON como lista  → [ {}, {}, {} ]
      - Respuesta JSON como objeto → { "data": [ {}, {} ] }
        (usa data_key para indicar qué campo contiene la lista)

    Ejemplo de uso en bronze_pipeline:
        JsonApiSource(endpoints=[
            {
                "url":        "https://api.ejemplo.com/clientes",
                "table_name": "api_clientes",
                "data_key":   "data",           # opcional
                "headers":    {"Authorization": "Bearer TOKEN"},  # opcional
            },
            {
                "url":        "https://api.ejemplo.com/productos",
                "table_name": "api_productos",
            },
        ])
    """

    source_type = "json_api"

    def __init__(
        self,
        endpoints: List[dict],
        default_headers: Optional[dict] = None,
    ):
        """
        Args:
            endpoints: Lista de dicts con keys:
                - url        (requerido)  : URL del endpoint
                - table_name (requerido)  : nombre tabla destino en bronze
                - data_key   (opcional)   : key del JSON que contiene la lista
                - headers    (opcional)   : headers específicos del endpoint
            default_headers: Headers comunes para todos los endpoints
        """
        self.endpoints       = endpoints
        self.default_headers = default_headers or {}

    def extract(self) -> Iterator[SourceRecord]:
        """Llama cada endpoint y retorna su contenido como SourceRecord."""

        for endpoint in self.endpoints:
            url        = endpoint.get("url")
            table_name = endpoint.get("table_name")
            data_key   = endpoint.get("data_key")
            headers    = {**self.default_headers, **endpoint.get("headers", {})}

            if not url or not table_name:
                logger.warning(
                    f"[JsonApiSource] Endpoint sin 'url' o 'table_name', se omite: {endpoint}"
                )
                continue

            logger.info(f"[JsonApiSource] Consultando: {url}")

            try:
                df = self._fetch(url, headers, data_key)
            except Exception as e:
                logger.error(f"[JsonApiSource] Error consultando {url}: {e}")
                continue

            if df.empty:
                logger.warning(f"[JsonApiSource] Respuesta vacía desde {url}")
                continue

            logger.info(f"[JsonApiSource] {len(df)} registros desde {url}")

            yield SourceRecord(
                df=df,
                source_name=url,
                table_name=table_name,
                source_id=url,
                modified_at=None,
            )

    def _fetch(self, url: str, headers: dict, data_key: Optional[str]) -> pd.DataFrame:
        """Hace la llamada HTTP y convierte la respuesta a DataFrame."""

        # Algunos servidores bloquean el User-Agent por defecto de Python
        # Se simula un navegador para evitar el error 403 Forbidden
        default_ua = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        final_headers = {**default_ua, **headers}
        req = Request(url, headers=final_headers, method="GET")

        try:
            with urlopen(req, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as e:
            raise RuntimeError(f"HTTP {e.code}: {e.reason}")
        except URLError as e:
            raise RuntimeError(f"URL error: {e.reason}")

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"JSON inválido: {e}")

        # Extraer la lista de registros
        if data_key:
            if not isinstance(data, dict) or data_key not in data:
                raise RuntimeError(
                    f"data_key '{data_key}' no encontrado en la respuesta. "
                    f"Keys disponibles: {list(data.keys()) if isinstance(data, dict) else 'N/A'}"
                )
            records = data[data_key]
        elif isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Si es un dict sin data_key, intentar convertir directamente
            records = [data]
        else:
            raise RuntimeError(f"Formato de respuesta no soportado: {type(data)}")

        if not isinstance(records, list):
            raise RuntimeError(f"Se esperaba una lista de registros, se obtuvo: {type(records)}")

        return pd.DataFrame(records)