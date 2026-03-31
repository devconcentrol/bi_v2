import os
import unittest
from contextlib import AbstractContextManager
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from forecast_consumptions_fact import ForecastConsumptionsFactETL
from utils.config import Config


class FakeConnection:
    def __init__(self):
        self.executed: list[tuple[object, object | None]] = []

    def execute(self, statement, params=None):
        self.executed.append((statement, params))


class FakeBeginContext(AbstractContextManager):
    def __init__(self, connection: FakeConnection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(self, connection: FakeConnection):
        self.connection = connection

    def begin(self):
        return FakeBeginContext(self.connection)


class ForecastConsumptionsFactTests(unittest.TestCase):
    def setUp(self):
        os.environ["HANA_CONNECTION"] = "sqlite:///hana.db"
        os.environ["DW_CONNECTION"] = "sqlite:///dw.db"
        os.environ["COSTING_PATH"] = "/tmp/costing"
        Config._instance = None

    def tearDown(self):
        Config._instance = None

    def test_calculate_cutoff_date_uses_current_month_in_first_half(self):
        cutoff = ForecastConsumptionsFactETL._calculate_cutoff_date(
            datetime(2026, 3, 10, 8, 30, 0)
        )

        self.assertEqual(cutoff, datetime(2026, 3, 1, 0, 0, 0))

    def test_calculate_cutoff_date_uses_next_month_in_second_half(self):
        cutoff = ForecastConsumptionsFactETL._calculate_cutoff_date(
            datetime(2026, 3, 20, 8, 30, 0)
        )

        self.assertEqual(cutoff, datetime(2026, 4, 1, 0, 0, 0))

    def test_transform_results_maps_materials_and_filters_missing_rows(self):
        lookup = MagicMock()
        lookup.get_material_map.return_value = {"MAT1": 1}
        etl = ForecastConsumptionsFactETL(MagicMock(), MagicMock(), lookup)
        source = pd.DataFrame(
            [
                {"WERKS": "1000", "MATNR": "MAT1", "MEINS": "KG", "BDTER": "20260401", "BDMNG": "5.5"},
                {"WERKS": "1000", "MATNR": "UNKNOWN", "MEINS": "KG", "BDTER": "20260402", "BDMNG": "7.5"},
            ]
        )

        transformed, missing_materials = etl._transform_results(source)

        self.assertEqual(missing_materials, ["UNKNOWN"])
        self.assertEqual(len(transformed), 1)
        self.assertEqual(transformed.iloc[0]["MaterialId"], 1)
        self.assertEqual(transformed.iloc[0]["Qty"], 5.5)
        self.assertEqual(str(transformed.iloc[0]["ForecastDate"]), "2026-04-01")

    @patch("forecast_consumptions_fact.pd.read_sql")
    @patch("forecast_consumptions_fact.datetime")
    def test_run_deletes_and_inserts_forecast_rows(self, datetime_mock, read_sql_mock):
        lookup = MagicMock()
        lookup.get_material_map.return_value = {"MAT1": 1}
        dw_connection = FakeConnection()
        etl = ForecastConsumptionsFactETL(FakeEngine(dw_connection), MagicMock(), lookup)
        datetime_mock.now.return_value = datetime(2026, 3, 20, 8, 30, 0)

        read_sql_mock.return_value = pd.DataFrame(
            [
                {"WERKS": "1000", "MATNR": "MAT1", "MEINS": "KG", "BDTER": "20260401", "BDMNG": "5.5"}
            ]
        )

        etl.run()

        self.assertEqual(len(dw_connection.executed), 3)
        delete_params = dw_connection.executed[0][1]
        self.assertIn("cutoff_dw", delete_params)
        self.assertEqual(delete_params["cutoff_dw"], "2026-04-01")
        self.assertEqual(dw_connection.executed[2][1], {"etl_name": "process_forecast_consumptions"})
