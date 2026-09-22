import unittest
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from utils.new_sale_checker import NewSaleChecker


class NewSaleCheckerTests(unittest.TestCase):
    def setUp(self):
        self.con_dw = MagicMock()
        self.con_sap = MagicMock()
        self.lookup = MagicMock()
        self.sender = MagicMock()
        self.checker = NewSaleChecker(
            self.con_dw,
            self.con_sap,
            self.lookup,
            self.sender,
        )

    def test_find_orders_without_forecast_matches_customer_material_and_month(self):
        self.lookup.get_material_map.return_value = {"MAT1": 11, "MAT2": 22}
        self.lookup.get_customer_map.return_value = {"10001010CUST1": 101}
        orders = pd.DataFrame(
            {
                "KUNNR": ["CUST1", "CUST1"],
                "MATNR": ["MAT1", "MAT2"],
                "VKORG": ["1000", "1000"],
                "VTWEG": ["10", "10"],
                "SPART": ["10", "10"],
                "DELIVERY_DATE": ["20261010", date(2026, 10, 15)],
                "VBELN": ["1", "2"],
            }
        )
        forecast = pd.DataFrame(
            {
                "CustId": [101],
                "MaterialId": [11],
                "ForecastDate": ["2026-10-01"],
            }
        )

        result = self.checker._find_orders_without_forecast(orders, forecast)

        self.assertEqual(result["vbeln"].tolist(), ["2"])

    @patch.object(NewSaleChecker, "_mark_execution")
    @patch.object(NewSaleChecker, "_get_forecast")
    @patch.object(NewSaleChecker, "_get_new_orders")
    @patch.object(NewSaleChecker, "_get_last_execution")
    def test_run_sends_email_only_for_unmatched_orders(
        self,
        get_last_execution,
        get_new_orders,
        get_forecast,
        mark_execution,
    ):
        last_execution = datetime(2026, 9, 18, 8, 30)
        get_last_execution.return_value = last_execution
        get_new_orders.return_value = pd.DataFrame({"VBELN": ["1"]})
        get_forecast.return_value = pd.DataFrame()
        unmatched = pd.DataFrame({"vbeln": ["1"], "erdat": ["20260918"]})

        with patch.object(
            self.checker,
            "_find_orders_without_forecast",
            return_value=unmatched,
        ):
            self.checker.run()

        self.sender.send.assert_called_once()
        self.assertIn("Nuevas ventas fuera", self.sender.send.call_args.args[0])
        mark_execution.assert_called_once_with()

    @patch.object(NewSaleChecker, "_mark_execution")
    @patch.object(NewSaleChecker, "_get_forecast")
    @patch.object(NewSaleChecker, "_get_new_orders")
    @patch.object(NewSaleChecker, "_get_last_execution")
    def test_run_does_not_read_forecast_or_send_when_there_are_no_orders(
        self,
        get_last_execution,
        get_new_orders,
        get_forecast,
        mark_execution,
    ):
        get_last_execution.return_value = datetime(2026, 9, 18, 8, 30)
        get_new_orders.return_value = pd.DataFrame()

        self.checker.run()

        get_forecast.assert_not_called()
        self.sender.send.assert_not_called()
        mark_execution.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
