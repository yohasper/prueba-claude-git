# ============================================================
# tests/test_silver.py
# Tests unitarios para la capa Silver
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

from ingestion.silver.silver_cleaner import SilverCleaner
from models.silver.dimensions import (
    build_dim_tiempo, build_dim_clientes,
    build_dim_productos, build_fact_ventas
)


class TestSilverCleaner:

    def setup_method(self):
        self.cleaner = SilverCleaner()

    def test_texto_a_mayusculas(self):
        df = pd.DataFrame({"nombre": ["juan", "maria"]})
        result = self.cleaner.clean(df)
        assert result["nombre"].iloc[0] == "JUAN"

    def test_decimales_a_2_cifras(self):
        df = pd.DataFrame({"precio": [10.5678, 3.14159]})
        result = self.cleaner.clean(df)
        assert result["precio"].iloc[0] == 10.57
        assert result["precio"].iloc[1] == 3.14

    def test_enteros_se_mantienen(self):
        df = pd.DataFrame({"cantidad": [1, 2, 3]})
        result = self.cleaner.clean(df)
        assert result["cantidad"].iloc[0] == 1

    def test_elimina_filas_vacias(self):
        df = pd.DataFrame({"nombre": ["ANA", None, "LUIS"], "edad": [25, None, 30]})
        result = self.cleaner.clean(df)
        assert len(result) == 2

    def test_agrega_columna_auditoria(self):
        df = pd.DataFrame({"col": ["a", "b"]})
        result = self.cleaner.clean(df)
        assert "_silver_loaded_at" in result.columns

    def test_elimina_columnas_bronce(self):
        df = pd.DataFrame({
            "nombre": ["ANA"],
            "_source_file": ["archivo.xlsx"],
            "_source_sheet": ["hoja1"],
            "_ingested_at": ["2024-01-01"],
        })
        result = self.cleaner.clean(df)
        assert "_source_file"  not in result.columns
        assert "_source_sheet" not in result.columns
        assert "_ingested_at"  not in result.columns

    def test_strip_espacios(self):
        df = pd.DataFrame({"ciudad": ["  bogota  ", " medellin "]})
        result = self.cleaner.clean(df)
        assert result["ciudad"].iloc[0] == "BOGOTA"


class TestDimTiempo:

    def setup_method(self):
        self.df = build_dim_tiempo(start_year=2024)

    def test_sk_tiempo_es_yyyymmdd(self):
        row = self.df[self.df["fecha"] == "2024-01-01"]
        assert row["sk_tiempo"].iloc[0] == 20240101

    def test_sk_tiempo_otro_ejemplo(self):
        row = self.df[self.df["fecha"] == "2024-12-31"]
        assert row["sk_tiempo"].iloc[0] == 20241231

    def test_sk_unico(self):
        assert self.df["sk_tiempo"].is_unique

    def test_genera_desde_2024(self):
        assert self.df["anio"].min() == 2024

    def test_columnas_requeridas(self):
        for col in ["sk_tiempo", "fecha", "anio", "trimestre", "trimestre_nombre",
                    "mes", "mes_nombre", "semana_anio", "dia_mes", "dia_semana",
                    "dia_semana_nombre", "es_fin_semana", "es_bisiesto",
                    "dia_del_anio", "quincena", "periodo_yyyymm", "periodo_yyyyqq"]:
            assert col in self.df.columns, f"Falta columna: {col}"

    def test_mes_nombre_en_espanol(self):
        row = self.df[self.df["fecha"] == "2024-01-01"]
        assert row["mes_nombre"].iloc[0] == "ENERO"
        row = self.df[self.df["fecha"] == "2024-12-01"]
        assert row["mes_nombre"].iloc[0] == "DICIEMBRE"

    def test_dia_semana_en_espanol(self):
        row = self.df[self.df["fecha"] == "2024-01-01"]
        assert row["dia_semana_nombre"].iloc[0] == "LUNES"
        row = self.df[self.df["fecha"] == "2024-01-07"]
        assert row["dia_semana_nombre"].iloc[0] == "DOMINGO"

    def test_trimestre_en_espanol(self):
        row = self.df[self.df["fecha"] == "2024-01-01"]
        assert row["trimestre_nombre"].iloc[0] == "PRIMER TRIMESTRE"

    def test_fin_de_semana(self):
        row = self.df[self.df["fecha"] == "2024-01-06"]  # sábado
        assert row["es_fin_semana"].iloc[0] == True
        row = self.df[self.df["fecha"] == "2024-01-01"]  # lunes
        assert row["es_fin_semana"].iloc[0] == False

    def test_quincena(self):
        row = self.df[self.df["fecha"] == "2024-01-10"]
        assert row["quincena"].iloc[0] == 1
        row = self.df[self.df["fecha"] == "2024-01-20"]
        assert row["quincena"].iloc[0] == 2


class TestFactVentas:

    def _make_fact(self):
        df_ventas = pd.DataFrame({
            "id_venta":    [1, 2],
            "fecha":       ["2024-03-15", "2024-07-20"],
            "id_cliente":  [10, 20],
            "id_producto": [100, 200],
            "total":       [50000.0, 30000.0],
        })
        dim_cli  = pd.DataFrame({"sk_cliente":  [1, 2], "nk_cliente":  [10, 20]})
        dim_prod = pd.DataFrame({"sk_producto": [1, 2], "nk_producto": [100, 200]})
        dim_tiem = build_dim_tiempo(start_year=2024)
        return build_fact_ventas(df_ventas, dim_cli, dim_prod, dim_tiem)

    def test_fk_cliente_copia_id_cliente(self):
        fact = self._make_fact()
        # fk_cliente debe contener los valores originales de id_cliente
        assert fact["fk_cliente"].iloc[0] == 10
        assert fact["fk_cliente"].iloc[1] == 20

    def test_fk_producto_copia_id_producto(self):
        fact = self._make_fact()
        # fk_producto debe contener los valores originales de id_producto
        assert fact["fk_producto"].iloc[0] == 100
        assert fact["fk_producto"].iloc[1] == 200

    def test_fk_fecha_es_yyyymmdd(self):
        fact = self._make_fact()
        assert fact["fk_fecha"].iloc[0] == 20240315
        assert fact["fk_fecha"].iloc[1] == 20240720

    def test_id_cliente_eliminado(self):
        fact = self._make_fact()
        assert "id_cliente" not in fact.columns

    def test_id_producto_eliminado(self):
        fact = self._make_fact()
        assert "id_producto" not in fact.columns

    def test_fecha_eliminada(self):
        fact = self._make_fact()
        assert "fecha" not in fact.columns

    def test_fk_tiempo_no_existe(self):
        # La columna debe llamarse fk_fecha, no fk_tiempo
        fact = self._make_fact()
        assert "fk_tiempo" not in fact.columns

    def test_columnas_fk_presentes(self):
        fact = self._make_fact()
        assert "fk_cliente"  in fact.columns
        assert "fk_producto" in fact.columns
        assert "fk_fecha"    in fact.columns

    def test_sk_venta_es_primera_columna(self):
        fact = self._make_fact()
        assert fact.columns[0] == "sk_venta"

    def test_fks_van_despues_del_sk(self):
        fact = self._make_fact()
        assert fact.columns[1] == "fk_cliente"
        assert fact.columns[2] == "fk_producto"
        assert fact.columns[3] == "fk_fecha"


class TestDimClientes:

    def test_agrega_sk(self):
        df = pd.DataFrame({"id_cliente": [1, 2], "nombre": ["ANA", "LUIS"]})
        result = build_dim_clientes(df)
        assert "sk_cliente" in result.columns
        assert result["sk_cliente"].iloc[0] == 1

    def test_renombra_nk(self):
        df = pd.DataFrame({"id_cliente": [10, 20], "nombre": ["ANA", "LUIS"]})
        result = build_dim_clientes(df)
        assert "nk_cliente" in result.columns


class TestDimProductos:

    def test_agrega_sk(self):
        df = pd.DataFrame({"id_producto": [1, 2], "nombre": ["X", "Y"]})
        result = build_dim_productos(df)
        assert "sk_producto" in result.columns

    def test_renombra_nk(self):
        df = pd.DataFrame({"id_producto": [100, 200], "nombre": ["X", "Y"]})
        result = build_dim_productos(df)
        assert "nk_producto" in result.columns
