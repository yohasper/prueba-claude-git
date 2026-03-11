# ============================================================
# tests/test_bronze_loader.py
# Tests unitarios para la capa Bronce
# ============================================================

import io
import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.bronze.bronze_loader import BronzeLoader
from utils.excel_reader import ExcelReader


class TestBronzeLoader:

    def setup_method(self):
        self.loader = BronzeLoader()

    def test_build_table_name_basic(self):
        name = self.loader.build_table_name("Ventas 2024.xlsx", "Enero")
        assert name == "ventas_2024__enero"

    def test_build_table_name_special_chars(self):
        name = self.loader.build_table_name("Mi Reporte (Q1).xlsx", "Hoja 1")
        assert "__" in name
        assert " " not in name
        assert "(" not in name

    def test_build_table_name_max_length(self):
        long_name = "a" * 50
        long_sheet = "b" * 50
        result = self.loader.build_table_name(long_name, long_sheet)
        assert len(result) <= 63

    def test_build_table_name_removes_extension(self):
        name = self.loader.build_table_name("datos.xlsx", "hoja")
        assert "xlsx" not in name


class TestExcelReader:

    def setup_method(self):
        self.reader = ExcelReader()

    def _make_excel_buffer(self, data: dict) -> io.BytesIO:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            for sheet, df in data.items():
                df.to_excel(writer, sheet_name=sheet, index=False)
        buf.seek(0)
        return buf

    def test_read_excel_single_sheet(self):
        df_in = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        buf = self._make_excel_buffer({"Sheet1": df_in})
        result = self.reader.read_excel(buf, sheet_name=0)
        assert "Sheet1" in result
        assert len(result["Sheet1"]) == 2

    def test_read_excel_multiple_sheets(self):
        buf = self._make_excel_buffer({
            "Enero": pd.DataFrame({"v": [1, 2]}),
            "Febrero": pd.DataFrame({"v": [3, 4, 5]}),
        })
        result = self.reader.read_excel(buf, sheet_name=None)
        assert "Enero" in result
        assert "Febrero" in result

    def test_normalize_cleans_column_names(self):
        df = pd.DataFrame({"Nombre Completo": ["Ana"], "Fecha Nac.": ["2000-01-01"]})
        result = self.reader.normalize_dataframe(df)
        assert "nombre_completo" in result.columns
        assert "fecha_nac" in result.columns

    def test_normalize_drops_empty_rows(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [4, None, 6]})
        result = self.reader.normalize_dataframe(df)
        assert len(result) == 2

    def test_normalize_adds_audit_columns(self):
        df = pd.DataFrame({"x": [1, 2]})
        result = self.reader.normalize_dataframe(df, source_file="test.xlsx", source_sheet="Hoja1")
        assert "_source_file" in result.columns
        assert "_source_sheet" in result.columns
        assert "_ingested_at" in result.columns
        assert result["_source_file"].iloc[0] == "test.xlsx"

    def test_normalize_empty_dataframe(self):
        df = pd.DataFrame()
        result = self.reader.normalize_dataframe(df)
        assert result.empty
