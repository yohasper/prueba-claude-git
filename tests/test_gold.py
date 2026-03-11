# ============================================================
# tests/test_gold.py
# Tests unitarios para la capa Gold
# ============================================================

import sys, os, pandas as pd
from pathlib import Path

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_NAME", "datawarehouse")
os.environ.setdefault("DB_USER", "dw_user")
os.environ.setdefault("DB_PASSWORD", "test_password")
os.environ.setdefault("DB_SCHEMA_BRONZE", "bronze")
os.environ.setdefault("GDRIVE_FOLDER_ID", "test_folder_id")
os.environ.setdefault("GOOGLE_CREDENTIALS_PATH", "config/credentials/google_credentials.json")
os.environ.setdefault("GOOGLE_TOKEN_PATH", "config/credentials/token.json")
os.environ.setdefault("BATCH_SIZE", "1000")
os.environ.setdefault("LOG_LEVEL", "INFO")
os.environ.setdefault("LOG_PATH", "logs/")
os.environ.setdefault("ENV", "test")

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.silver.dimensions import build_dim_tiempo
from models.gold.kpis import (
    build_kpi_ventas_por_periodo,
    build_kpi_ventas_por_cliente,
    build_kpi_ventas_por_producto,
)


# ── Fixtures reutilizables ────────────────────────────────────
def _make_dim_tiempo():
    return build_dim_tiempo(start_year=2024)


def _make_fact():
    return pd.DataFrame({
        "sk_venta":   [1, 2, 3, 4, 5, 6],
        "fk_cliente": [10, 10, 20, 20, 30, 10],
        "fk_producto":[100, 200, 100, 200, 100, 100],
        "fk_fecha":   [20240115, 20240220, 20240315, 20240415, 20240115, 20240220],
        "total":      [50000.0, 30000.0, 70000.0, 20000.0, 45000.0, 60000.0],
        "cantidad":   [2, 1, 3, 1, 2, 3],
    })


def _make_dim_clientes():
    return pd.DataFrame({
        "sk_cliente":  [1, 2, 3],
        "nk_cliente":  [10, 20, 30],
        "nombre":      ["CLIENTE A", "CLIENTE B", "CLIENTE C"],
        "ciudad":      ["MEDELLIN", "BOGOTA", "CALI"],
    })


def _make_dim_productos():
    return pd.DataFrame({
        "sk_producto":  [1, 2],
        "nk_producto":  [100, 200],
        "nombre":       ["PRODUCTO X", "PRODUCTO Y"],
        "categoria":    ["CAT A", "CAT B"],
    })


# ── KPI 1: Ventas por Período ─────────────────────────────────
class TestKpiVentasPorPeriodo:

    def setup_method(self):
        self.kpi = build_kpi_ventas_por_periodo(_make_fact(), _make_dim_tiempo())

    def test_columnas_requeridas(self):
        for col in ["anio", "mes", "mes_nombre", "periodo_yyyymm",
                    "total_ventas", "cantidad_transacciones",
                    "ticket_promedio", "acumulado_anual"]:
            assert col in self.kpi.columns, f"Falta columna: {col}"

    def test_total_ventas_es_positivo(self):
        assert (self.kpi["total_ventas"] >= 0).all()

    def test_ticket_promedio_con_2_decimales(self):
        for val in self.kpi["ticket_promedio"].dropna():
            assert round(val, 2) == val

    def test_total_ventas_con_2_decimales(self):
        for val in self.kpi["total_ventas"]:
            assert round(val, 2) == val

    def test_ordenado_por_periodo(self):
        periodos = self.kpi["periodo_yyyymm"].tolist()
        assert periodos == sorted(periodos)

    def test_acumulado_anual_crece(self):
        # El acumulado anual dentro del mismo año debe ser creciente
        año_2024 = self.kpi[self.kpi["anio"] == 2024]["acumulado_anual"].tolist()
        assert año_2024 == sorted(año_2024)

    def test_cantidad_transacciones_positiva(self):
        assert (self.kpi["cantidad_transacciones"] > 0).all()

    def test_gold_loaded_at_presente(self):
        assert "_gold_loaded_at" in self.kpi.columns


# ── KPI 2: Ventas por Cliente ─────────────────────────────────
class TestKpiVentasPorCliente:

    def setup_method(self):
        self.kpi = build_kpi_ventas_por_cliente(
            _make_fact(), _make_dim_clientes(), _make_dim_tiempo()
        )

    def test_columnas_requeridas(self):
        for col in ["anio", "mes", "periodo_yyyymm", "fk_cliente",
                    "total_ventas", "cantidad_transacciones",
                    "ticket_promedio", "ranking_mes", "ranking_global"]:
            assert col in self.kpi.columns, f"Falta columna: {col}"

    def test_ranking_mes_empieza_en_1(self):
        for periodo in self.kpi["periodo_yyyymm"].unique():
            min_rank = self.kpi[self.kpi["periodo_yyyymm"] == periodo]["ranking_mes"].min()
            assert min_rank == 1

    def test_ranking_global_empieza_en_1(self):
        assert self.kpi["ranking_global"].min() == 1

    def test_total_ventas_positivo(self):
        assert (self.kpi["total_ventas"] >= 0).all()

    def test_ticket_promedio_2_decimales(self):
        for val in self.kpi["ticket_promedio"].dropna():
            assert round(val, 2) == val

    def test_atributos_cliente_presentes(self):
        # Los atributos de dim_clientes deben estar en el KPI
        assert "nombre" in self.kpi.columns


# ── KPI 3: Ventas por Producto ────────────────────────────────
class TestKpiVentasPorProducto:

    def setup_method(self):
        self.kpi = build_kpi_ventas_por_producto(
            _make_fact(), _make_dim_productos(), _make_dim_tiempo()
        )

    def test_columnas_requeridas(self):
        for col in ["anio", "mes", "periodo_yyyymm", "fk_producto",
                    "total_ventas", "cantidad_transacciones",
                    "ticket_promedio", "ranking_mes", "ranking_global",
                    "participacion_mes_pct"]:
            assert col in self.kpi.columns, f"Falta columna: {col}"

    def test_participacion_suma_100_por_mes(self):
        for periodo in self.kpi["periodo_yyyymm"].unique():
            total_pct = self.kpi[
                self.kpi["periodo_yyyymm"] == periodo
            ]["participacion_mes_pct"].sum()
            assert abs(total_pct - 100.0) < 0.1, \
                f"Participación no suma 100% en {periodo}: {total_pct}"

    def test_ranking_mes_empieza_en_1(self):
        for periodo in self.kpi["periodo_yyyymm"].unique():
            min_rank = self.kpi[self.kpi["periodo_yyyymm"] == periodo]["ranking_mes"].min()
            assert min_rank == 1

    def test_participacion_entre_0_y_100(self):
        assert (self.kpi["participacion_mes_pct"] >= 0).all()
        assert (self.kpi["participacion_mes_pct"] <= 100).all()

    def test_total_ventas_2_decimales(self):
        for val in self.kpi["total_ventas"]:
            assert round(val, 2) == val

    def test_atributos_producto_presentes(self):
        assert "nombre" in self.kpi.columns
        assert "categoria" in self.kpi.columns
