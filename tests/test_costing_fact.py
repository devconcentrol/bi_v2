import os
import unittest
from unittest.mock import MagicMock

import pandas as pd

from costing_fact import CostingFactETL
from utils.config import Config


class CostingFactTransformTests(unittest.TestCase):
    def setUp(self):
        os.environ["HANA_CONNECTION"] = "sqlite:///hana.db"
        os.environ["DW_CONNECTION"] = "sqlite:///dw.db"
        os.environ["COSTING_PATH"] = "/tmp/costing"
        Config._instance = None

    def tearDown(self):
        Config._instance = None

    def test_transform_costing_frame_applies_fallback_customer_and_material_lookup(self):
        lookup = MagicMock()
        etl = CostingFactETL(MagicMock(), lookup)
        source = pd.DataFrame(
            [
                {
                    "CostingDate": "20260301",
                    "CustomerCode": "CUST01",
                    "Channel": None,
                    "MaterialCode": "1000A123",
                    "SalesOrganization": "1000",
                    "NetWeight": 1.0,
                    "TotalIncome": 2.0,
                    "TotalCOGS": 3.0,
                    "Transport": 4.0,
                    "Commission": 5.0,
                    "InvoiceDiscount": None,
                }
            ]
        )

        transformed = etl._transform_costing_frame(
            source,
            customer_map={"10001010CUST01": 101},
            material_map={"MA123": 202},
        )

        self.assertEqual(transformed.loc[0, "CustId"], 101)
        self.assertEqual(transformed.loc[0, "MaterialId"], 202)
        self.assertEqual(transformed.loc[0, "InvoiceDiscount"], 0)
        self.assertEqual(str(transformed.loc[0, "CostingDate"]), "2026-03-01")

    def test_get_loaded_dir_is_relative_to_source_directory(self):
        lookup = MagicMock()
        etl = CostingFactETL(MagicMock(), lookup)

        self.assertEqual(etl._get_loaded_dir("/data/incoming"), "/data/incoming/loaded")
