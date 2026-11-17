import unittest
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from etl.facts.qm_notification_measure_fact import QMNotificationMeasureFactETL


class NotificationMeasureFactTests(unittest.TestCase):
    def setUp(self):
        config = SimpleNamespace(
            TABLE_NOTIFICATION_MEASURES_FACT="NotificationMeasuresFact",
            TABLE_ETL_INFO="ETLInfo",
        )
        with patch("etl.base_fact_etl.Config.get_instance", return_value=config):
            self.etl = QMNotificationMeasureFactETL(
                MagicMock(), MagicMock(), MagicMock()
            )
        self.etl._lookup.get_notification_catalog_map.return_value = {
            "2GROUP0010": 10,
            "2GROUP0020": 20,
        }
        self.etl._lookup.get_notification_type_map.return_value = {"ZI": 1}
        self.conn = self.etl._con_dw.begin.return_value.__enter__.return_value

    @patch("etl.facts.qm_notification_measures_fact.Logger")
    @patch("etl.facts.qm_notification_measures_fact.pd.read_sql")
    def test_upsert_preserves_measures_and_converts_invalid_dates(self, read_sql, _):
        row = {
            "QMNUM": "00000123",
            "MANUM": "0001",
            "ERDAT": "20261113",
            "QMART": "ZI",
            "MNKAT": "2",
            "MNGRP": "GROUP",
            "MNCOD": "0010",
            "PARNR": "0000456",
            "PSTER": "20261114",
            "PETER": "20261115",
            "ERLNAM": None,
            "ERLDAT": "00000000",
            "AEDAT": "20261113",
            "MATXT": "Measure text",
        }
        read_sql.return_value = pd.DataFrame(
            [row, {**row, "MANUM": "0002", "MNCOD": "0020"}]
        )

        self.etl.run()

        query = read_sql.call_args.args[0]
        self.assertIn("WHERE AEDAT = :yesterday", query)
        self.assertEqual(
            read_sql.call_args.kwargs["params"]["yesterday"],
            (date.today() - timedelta(days=1)).strftime("%Y%m%d"),
        )

        calls = self.conn.execute.call_args_list
        statement = str(calls[0].args[0])
        self.assertIn("MERGE INTO NotificationMeasuresFact", statement)
        self.assertIn("ON target.NotificationId = source.NotificationId", statement)
        self.assertIn("AND target.MeasureNum = source.MeasureNum", statement)
        self.assertIn("WHEN MATCHED THEN", statement)
        self.assertIn("WHEN NOT MATCHED THEN", statement)
        records = calls[0].args[1]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["NotificationId"], "00000123")
        self.assertEqual(records[0]["MeasureNum"], "0001")
        self.assertEqual(records[0]["Responsible"], "0000456")
        self.assertEqual(records[0]["CreatedDate"], date(2026, 11, 13))
        self.assertEqual(records[0]["PlannedStartDate"], date(2026, 11, 14))
        self.assertIsNone(records[0]["CompletedDate"])
        self.assertIsNone(records[0]["CompletedBy"])
        self.assertEqual(records[0]["MeasureId"], 10)
        self.assertEqual(records[1]["MeasureNum"], "0002")
        self.assertEqual(records[1]["MeasureId"], 20)
        self.assertEqual(
            calls[1].args[1], {"etl_name": "process_notification_measures"}
        )

    @patch("etl.facts.qm_notification_measures_fact.Logger")
    @patch("etl.facts.qm_notification_measures_fact.pd.read_sql")
    def test_empty_source_only_updates_etl_info(self, read_sql, _):
        read_sql.return_value = pd.DataFrame()

        self.etl.run()

        self.conn.execute.assert_called_once()
        self.assertEqual(
            self.conn.execute.call_args.args[1],
            {"etl_name": "process_notification_measures"},
        )
