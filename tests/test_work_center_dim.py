import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from etl.dimensions.work_center_dim import WorkCenterDim


class WorkCenterDimTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as conn:
            conn.connection.create_function("GETDATE", 0, lambda: "2026-11-09")
            conn.execute(
                text(
                    "CREATE TABLE WorkCenterDim (Id TEXT PRIMARY KEY, "
                    "PlantId TEXT NOT NULL, Code TEXT NOT NULL, Name TEXT)"
                )
            )
            conn.execute(
                text("INSERT INTO WorkCenterDim VALUES ('old', '1000', 'A', 'A')")
            )
            conn.execute(text("CREATE TABLE ETLInfo (ETL TEXT, ProcessDate TEXT)"))
            conn.execute(
                text("INSERT INTO ETLInfo VALUES ('process_work_centers', NULL)")
            )
        with patch(
            "etl.dimensions.work_center_dim.Config.get_instance",
            return_value=SimpleNamespace(
                TABLE_WORK_CENTER_DIM="WorkCenterDim", TABLE_ETL_INFO="ETLInfo"
            ),
        ):
            self.job = WorkCenterDim(self.engine, None)

    def run_snapshot(self, rows):
        snapshot = pd.DataFrame(rows, columns=["OBJID", "WERKS", "ARBPL", "KTEXT"])
        with patch("etl.dimensions.work_center_dim.pd.read_sql", return_value=snapshot):
            self.job.run()

    def rows(self):
        with self.engine.connect() as conn:
            return conn.execute(text("SELECT * FROM WorkCenterDim")).all()

    def test_replaces_snapshot_preserving_string_ids_and_null_name(self):
        self.run_snapshot([["000001", "1000", "WC01", None]])
        self.assertEqual(self.rows(), [("000001", "1000", "WC01", None)])

    def test_empty_snapshot_clears_destination(self):
        self.run_snapshot([])
        self.assertEqual(self.rows(), [])

    def test_insert_failure_rolls_back_deletion(self):
        with self.assertRaises(IntegrityError):
            self.run_snapshot([["000001", None, "WC01", "A"]])
        self.assertEqual(self.rows(), [("old", "1000", "A", "A")])
