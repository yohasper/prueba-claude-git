# ============================================================
# models/silver/dimensions.py
# ============================================================

import pandas as pd
from datetime import date, datetime, timezone
from utils.logger import logger

# ── TABLAS FUENTE EN BRONCE ──────────────────────────────────
BRONZE_CLIENTES      = "ejercicio_sesion_7_clientes__ejercicio_sesion_7_clientes"
BRONZE_PRODUCTOS     = "ejercicio_sesion_7_productos__ejercicio_sesion_7_productos"
BRONZE_VENTAS        = "ejercicio_sesion_7_ventas__ejercicio_sesion_7_ventas"
BRONZE_CLIENTES_ALT  = "ejercicio_sesion_7_clientes__sheet1"
BRONZE_PRODUCTOS_ALT = "ejercicio_sesion_7_productos__sheet1"
BRONZE_VENTAS_ALT    = "ejercicio_sesion_7_ventas__sheet1"

# ── MAPAS EN ESPAÑOL ─────────────────────────────────────────
MESES_ES = {
    1:"ENERO", 2:"FEBRERO", 3:"MARZO", 4:"ABRIL",
    5:"MAYO", 6:"JUNIO", 7:"JULIO", 8:"AGOSTO",
    9:"SEPTIEMBRE", 10:"OCTUBRE", 11:"NOVIEMBRE", 12:"DICIEMBRE",
}
DIAS_ES = {
    1:"LUNES", 2:"MARTES", 3:"MIÉRCOLES",
    4:"JUEVES", 5:"VIERNES", 6:"SÁBADO", 7:"DOMINGO",
}
TRIMESTRES_ES = {
    1:"PRIMER TRIMESTRE", 2:"SEGUNDO TRIMESTRE",
    3:"TERCER TRIMESTRE", 4:"CUARTO TRIMESTRE",
}


# ── DIMENSIÓN CLIENTES ───────────────────────────────────────
def build_dim_clientes(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Construyendo dim_clientes...")
    df = df.copy()
    id_col = _find_col(df, ["id_cliente", "cliente_id", "id", "codigo_cliente"])
    df = df.reset_index(drop=True)
    df.insert(0, "sk_cliente", range(1, len(df) + 1))
    if id_col:
        df = df.rename(columns={id_col: "nk_cliente"})
    df["_silver_loaded_at"] = datetime.now(timezone.utc)
    logger.info(f"dim_clientes: {len(df)} registros")
    return df


# ── DIMENSIÓN PRODUCTOS ──────────────────────────────────────
def build_dim_productos(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Construyendo dim_productos...")
    df = df.copy()
    id_col = _find_col(df, ["id_producto", "producto_id", "id", "codigo_producto"])
    df = df.reset_index(drop=True)
    df.insert(0, "sk_producto", range(1, len(df) + 1))
    if id_col:
        df = df.rename(columns={id_col: "nk_producto"})
    df["_silver_loaded_at"] = datetime.now(timezone.utc)
    logger.info(f"dim_productos: {len(df)} registros")
    return df


# ── DIMENSIÓN TIEMPO ─────────────────────────────────────────
def build_dim_tiempo(start_year: int = 2024) -> pd.DataFrame:
    logger.info(f"Generando dim_tiempo desde {start_year} hasta hoy...")
    start = date(start_year, 1, 1)
    end   = date.today()
    fechas = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"fecha": fechas})

    df["sk_tiempo"]         = df["fecha"].dt.strftime("%Y%m%d").astype(int)
    df["anio"]              = df["fecha"].dt.year
    df["trimestre"]         = df["fecha"].dt.quarter
    df["trimestre_nombre"]  = df["trimestre"].map(TRIMESTRES_ES)
    df["mes"]               = df["fecha"].dt.month
    df["mes_nombre"]        = df["mes"].map(MESES_ES)
    df["semana_anio"]       = df["fecha"].dt.isocalendar().week.astype(int)
    df["dia_mes"]           = df["fecha"].dt.day
    df["dia_semana"]        = df["fecha"].dt.dayofweek + 1
    df["dia_semana_nombre"] = df["dia_semana"].map(DIAS_ES)
    df["es_fin_semana"]     = df["dia_semana"].isin([6, 7])
    df["es_bisiesto"]       = df["fecha"].dt.is_leap_year
    df["dia_del_anio"]      = df["fecha"].dt.dayofyear
    df["quincena"]          = df["dia_mes"].apply(lambda d: 1 if d <= 15 else 2)
    df["periodo_yyyymm"]    = df["fecha"].dt.strftime("%Y%m").astype(int)
    df["periodo_yyyyqq"]    = df["anio"].astype(str) + "Q" + df["trimestre"].astype(str)

    cols = ["sk_tiempo", "fecha"] + [c for c in df.columns if c not in ("sk_tiempo","fecha")]
    df = df[cols]
    df["_silver_loaded_at"] = datetime.now(timezone.utc)
    logger.info(f"dim_tiempo: {len(df)} días ({start} → {end})")
    return df


# ── TABLA DE HECHOS: VENTAS ──────────────────────────────────
def build_fact_ventas(
    df_ventas: pd.DataFrame,
    df_clientes: pd.DataFrame,
    df_productos: pd.DataFrame,
    df_tiempo: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reglas:
      - fk_cliente  = valor de id_cliente  → se elimina id_cliente
      - fk_producto = valor de id_producto → se elimina id_producto
      - fk_fecha    = fecha como YYYYMMDD  → se elimina columna fecha
    """
    logger.info("Construyendo fact_ventas...")
    df = df_ventas.copy()

    # Log de columnas reales para diagnóstico
    logger.info(f"Columnas recibidas en ventas: {list(df.columns)}")

    # ── fk_cliente ───────────────────────────────────────────
    # Busca cualquier variante del nombre, incluyendo con espacios o mayúsculas
    col_cli = _find_col_flexible(df, ["id_cliente", "idcliente", "cliente_id", "id_cli"])
    if col_cli:
        df["fk_cliente"] = df[col_cli]
        df = df.drop(columns=[col_cli])
        logger.info(f"fk_cliente ← '{col_cli}' (columna original eliminada)")
    else:
        df["fk_cliente"] = None
        logger.warning("No se encontró columna de id_cliente en ventas")

    # ── fk_producto ──────────────────────────────────────────
    col_prod = _find_col_flexible(df, ["id_producto", "idproducto", "producto_id", "id_prod"])
    if col_prod:
        df["fk_producto"] = df[col_prod]
        df = df.drop(columns=[col_prod])
        logger.info(f"fk_producto ← '{col_prod}' (columna original eliminada)")
    else:
        df["fk_producto"] = None
        logger.warning("No se encontró columna de id_producto en ventas")

    # ── fk_fecha ─────────────────────────────────────────────
    col_fecha = _find_col_flexible(df, ["fecha", "fecha_venta", "date", "fecha_factura"])
    if col_fecha:
        df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")
        df["fk_fecha"] = df[col_fecha].dt.strftime("%Y%m%d").astype("Int64")
        df = df.drop(columns=[col_fecha])
        logger.info(f"fk_fecha ← '{col_fecha}' como YYYYMMDD (columna original eliminada)")
    else:
        df["fk_fecha"] = None
        logger.warning("No se encontró columna de fecha en ventas")

    # ── Surrogate key ─────────────────────────────────────────
    df = df.reset_index(drop=True)
    df.insert(0, "sk_venta", range(1, len(df) + 1))

    # ── Ordenar columnas: sk → fks → resto ───────────────────
    fk_cols = ["fk_cliente", "fk_producto", "fk_fecha"]
    other   = [c for c in df.columns if c not in ["sk_venta"] + fk_cols]
    df = df[["sk_venta"] + fk_cols + other]

    logger.info(f"fact_ventas: {len(df)} registros — columnas: {list(df.columns)}")
    df["_silver_loaded_at"] = datetime.now(timezone.utc)
    return df


# ── HELPERS ──────────────────────────────────────────────────
def _find_col(df: pd.DataFrame, candidates: list) -> str | None:
    """Busca la primera columna candidata (exacta) en el DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _find_col_flexible(df: pd.DataFrame, candidates: list) -> str | None:
    """
    Busca columnas ignorando mayúsculas, espacios y guiones bajos.
    Útil cuando los nombres vienen normalizados desde Bronce.
    """
    # Mapa normalizado de columnas reales
    normalized = {
        col.lower().replace(" ", "_").replace("-", "_"): col
        for col in df.columns
    }
    for candidate in candidates:
        key = candidate.lower().replace(" ", "_").replace("-", "_")
        if key in normalized:
            return normalized[key]
    return None
