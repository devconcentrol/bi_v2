import io
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from utils.config import Config

import main_test


class MainTestCliTests(unittest.TestCase):
    def setUp(self):
        os.environ["HANA_CONNECTION"] = "sqlite:///hana.db"
        os.environ["DW_CONNECTION"] = "sqlite:///dw.db"
        os.environ["COSTING_PATH"] = "/tmp/costing"
        Config._instance = None

    def tearDown(self):
        Config._instance = None

    def test_build_parser_supports_list_and_run_commands(self):
        parser = main_test.build_parser()

        list_args = parser.parse_args(["list"])
        run_args = parser.parse_args(["run", "forecast_consumptions_fact"])

        self.assertEqual(list_args.command, "list")
        self.assertEqual(run_args.command, "run")
        self.assertEqual(run_args.job_name, "forecast_consumptions_fact")

    @patch("main_test._build_runtime")
    def test_list_command_prints_enabled_jobs(self, build_runtime_mock):
        fake_job = MagicMock()
        fake_job.name = "forecast_consumptions_fact"
        fake_job.times = ("04:45",)
        build_runtime_mock.return_value = ([fake_job], None, None)

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = main_test.main(["list"])

        self.assertEqual(exit_code, 0)
        self.assertIn("forecast_consumptions_fact: 04:45", stdout.getvalue())

    @patch("main_test.run_job")
    @patch("main_test._build_runtime")
    def test_run_command_executes_selected_job(self, build_runtime_mock, run_job_mock):
        fake_job = MagicMock()
        fake_job.name = "forecast_consumptions_fact"
        fake_job.runner = MagicMock()
        build_runtime_mock.return_value = ([fake_job], None, None)

        exit_code = main_test.main(["run", "forecast_consumptions_fact"])

        self.assertEqual(exit_code, 0)
        run_job_mock.assert_called_once_with("forecast_consumptions_fact", fake_job.runner)

    @patch("main_test._build_runtime")
    def test_run_command_rejects_unknown_job(self, build_runtime_mock):
        build_runtime_mock.return_value = ([], None, None)

        with self.assertRaises(ValueError):
            main_test.main(["run", "missing_job"])
