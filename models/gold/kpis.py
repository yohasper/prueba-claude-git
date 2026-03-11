# ============================================================
# models/gold/kpis.py
# KPIs de la capa Gold
# ============================================================

import pandas as pd
from datetime import datetime, timezone
from utils.logger import logger


# ── KPI 1: VENTAS POR PERÍODO ────────────────────────────────
def build_kpi_ventas_por_periodo(
    fact: pd.DataFrame,
    dim_tiempo: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Construyendo kpi_ventas_por_periodo...")

    df = fact.merge(
        dim_tiempo[[
            "sk_tiempo", "anio", "trimestre", "trimestre_nombre",
            "mes", "mes_nombre", "periodo_yyyymm"
        ]],
        left_on="fk_fecha", right_on="sk_tiempo", how="left"
    )

    col_monto = _find_col(df, ["total", "monto", "valor", "venta", "importe", "precio_total"])
    if col_monto is None:
        logger.warning("No se encontró columna de monto en fact_ventas.")
        df["_monto"] = 0
        col_monto = "_monto"
    df[col_monto] = pd.to_numeric(df[col_monto], errors="coerce").fillna(0)

    kpi = (
        df.groupby(
            ["anio", "trimestre", "trimestre_nombre", "mes", "mes_nombre", "periodo_yyyymm"],
            as_index=False
        ).agg(
            total_ventas=          (col_monto, "sum"),
            cantidad_transacciones=("sk_venta", "count"),
        )
    )

    kpi["ticket_promedio"]          = (kpi["total_ventas"] / kpi["cantidad_transacciones"]).round(2)
    kpi["total_ventas"]             = kpi["total_ventas"].round(2)
    kpi                             = kpi.sort_values("periodo_yyyymm").reset_index(drop=True)
    kpi["variacion_mes_anterior_pct"] = kpi["total_ventas"].pct_change().mul(100).round(2)
    kpi["acumulado_anual"]          = kpi.groupby("anio")["total_ventas"].cumsum().round(2)
    kpi["_gold_loaded_at"]          = datetime.now(timezone.utc)

    logger.info(f"kpi_ventas_por_periodo: {len(kpi)} filas")
    return kpi


# ── KPI 2: VENTAS POR CLIENTE ────────────────────────────────
def build_kpi_ventas_por_cliente(
    fact: pd.DataFrame,
    dim_clientes: pd.DataFrame,
    dim_tiempo: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Construyendo kpi_ventas_por_cliente...")

    # Log para diagnóstico
    logger.info(f"fact.fk_cliente muestra   : {fact['fk_cliente'].dropna().unique()[:5].tolist()}")
    logger.info(f"dim_clientes columnas     : {list(dim_clientes.columns)}")
    if "nk_cliente" in dim_clientes.columns:
        logger.info(f"dim_clientes.nk_cliente   : {dim_clientes['nk_cliente'].unique()[:5].tolist()}")

    # Unir con dim_tiempo
    df = fact.merge(
        dim_tiempo[["sk_tiempo", "anio", "mes", "mes_nombre", "periodo_yyyymm"]],
        left_on="fk_fecha", right_on="sk_tiempo", how="left"
    )

    # ── Cruce con dim_clientes ───────────────────────────────
    # fk_cliente contiene el valor original del Excel (nk_cliente en dim)
    # Convertir ambos lados a string para evitar problemas de tipo int vs str
    attrs_cli = [c for c in dim_clientes.columns
                 if c not in ["sk_cliente", "nk_cliente", "_silver_loaded_at"]]

    if "nk_cliente" in dim_clientes.columns:
        dim_join = dim_clientes[["nk_cliente"] + attrs_cli].copy()
        dim_join["nk_cliente"] = dim_join["nk_cliente"].astype(str).str.strip()
        df["fk_cliente_str"]   = df["fk_cliente"].astype(str).str.strip()

        df = df.merge(dim_join, left_on="fk_cliente_str", right_on="nk_cliente", how="left")
        df = df.drop(columns=["fk_cliente_str", "nk_cliente"], errors="ignore")
    else:
        logger.warning("dim_clientes no tiene columna nk_cliente, se omite el join de atributos.")

    col_monto = _find_col(df, ["total", "monto", "valor", "venta", "importe", "precio_total"])
    if col_monto is None:
        df["_monto"] = 0
        col_monto = "_monto"
    df[col_monto] = pd.to_numeric(df[col_monto], errors="coerce").fillna(0)

    group_cols = ["anio", "mes", "mes_nombre", "periodo_yyyymm", "fk_cliente"] + attrs_cli
    group_cols = [c for c in group_cols if c in df.columns]

    kpi = (
        df.groupby(group_cols, as_index=False)
        .agg(
            total_ventas=          (col_monto, "sum"),
            cantidad_transacciones=("sk_venta", "count"),
        )
    )

    kpi["ticket_promedio"] = (kpi["total_ventas"] / kpi["cantidad_transacciones"]).round(2)
    kpi["total_ventas"]    = kpi["total_ventas"].round(2)

    kpi["ranking_mes"] = (
        kpi.groupby("periodo_yyyymm")["total_ventas"]
        .rank(ascending=False, method="dense").astype(int)
    )

    global_tot = (
        kpi.groupby("fk_cliente")["total_ventas"].sum()
        .rank(ascending=False, method="dense").astype(int)
        .rename("ranking_global")
    )
    kpi = kpi.merge(global_tot, on="fk_cliente", how="left")
    kpi = kpi.sort_values(["periodo_yyyymm", "ranking_mes"]).reset_index(drop=True)
    kpi["_gold_loaded_at"] = datetime.now(timezone.utc)

    logger.info(f"kpi_ventas_por_cliente: {len(kpi)} filas")
    return kpi


# ── KPI 3: VENTAS POR PRODUCTO ───────────────────────────────
def build_kpi_ventas_por_producto(
    fact: pd.DataFrame,
    dim_productos: pd.DataFrame,
    dim_tiempo: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Construyendo kpi_ventas_por_producto...")

    # Log para diagnóstico
    logger.info(f"fact.fk_producto muestra  : {fact['fk_producto'].dropna().unique()[:5].tolist()}")
    logger.info(f"dim_productos columnas    : {list(dim_productos.columns)}")
    if "nk_producto" in dim_productos.columns:
        logger.info(f"dim_productos.nk_producto : {dim_productos['nk_producto'].unique()[:5].tolist()}")

    # Unir con dim_tiempo
    df = fact.merge(
        dim_tiempo[["sk_tiempo", "anio", "mes", "mes_nombre", "periodo_yyyymm"]],
        left_on="fk_fecha", right_on="sk_tiempo", how="left"
    )

    # ── Cruce con dim_productos ──────────────────────────────
    attrs_prod = [c for c in dim_productos.columns
                  if c not in ["sk_producto", "nk_producto", "_silver_loaded_at"]]

    if "nk_producto" in dim_productos.columns:
        dim_join = dim_productos[["nk_producto"] + attrs_prod].copy()
        dim_join["nk_producto"]  = dim_join["nk_producto"].astype(str).str.strip()
        df["fk_producto_str"]    = df["fk_producto"].astype(str).str.strip()

        df = df.merge(dim_join, left_on="fk_producto_str", right_on="nk_producto", how="left")
        df = df.drop(columns=["fk_producto_str", "nk_producto"], errors="ignore")
    else:
        logger.warning("dim_productos no tiene columna nk_producto, se omite el join de atributos.")

    col_monto    = _find_col(df, ["total", "monto", "valor", "venta", "importe", "precio_total"])
    col_cantidad = _find_col(df, ["cantidad", "units", "qty", "cantidad_vendida"])

    if col_monto is None:
        df["_monto"] = 0
        col_monto = "_monto"
    df[col_monto] = pd.to_numeric(df[col_monto], errors="coerce").fillna(0)

    group_cols = ["anio", "mes", "mes_nombre", "periodo_yyyymm", "fk_producto"] + attrs_prod
    group_cols = [c for c in group_cols if c in df.columns]

    agg_dict = {
        "total_ventas":           (col_monto, "sum"),
        "cantidad_transacciones": ("sk_venta", "count"),
    }
    if col_cantidad and col_cantidad in df.columns:
        df[col_cantidad] = pd.to_numeric(df[col_cantidad], errors="coerce").fillna(0)
        agg_dict["cantidad_unidades"] = (col_cantidad, "sum")

    kpi = df.groupby(group_cols, as_index=False).agg(**agg_dict)

    kpi["ticket_promedio"] = (kpi["total_ventas"] / kpi["cantidad_transacciones"]).round(2)
    kpi["total_ventas"]    = kpi["total_ventas"].round(2)

    kpi["ranking_mes"] = (
        kpi.groupby("periodo_yyyymm")["total_ventas"]
        .rank(ascending=False, method="dense").astype(int)
    )

    global_tot = (
        kpi.groupby("fk_producto")["total_ventas"].sum()
        .rank(ascending=False, method="dense").astype(int)
        .rename("ranking_global")
    )
    kpi = kpi.merge(global_tot, on="fk_producto", how="left")

    total_mes = kpi.groupby("periodo_yyyymm")["total_ventas"].transform("sum")
    kpi["participacion_mes_pct"] = ((kpi["total_ventas"] / total_mes) * 100).round(2)

    kpi = kpi.sort_values(["periodo_yyyymm", "ranking_mes"]).reset_index(drop=True)
    kpi["_gold_loaded_at"] = datetime.now(timezone.utc)

    logger.info(f"kpi_ventas_por_producto: {len(kpi)} filas")
    return kpi


# ── HELPER ───────────────────────────────────────────────────
def _find_col(df: pd.DataFrame, candidates: list) -> str | None:
    normalized = {
        col.lower().replace(" ", "_").replace("-", "_"): col
        for col in df.columns
    }
    for c in candidates:
        key = c.lower().replace(" ", "_").replace("-", "_")
        if key in normalized:
            return normalized[key]
    return None
