import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from job_registry import (
    RuntimeContext,
    build_job_definitions,
    get_job_by_name,
    load_job_config,
    register_jobs,
)
from utils.config import Config


class JobRegistryTests(unittest.TestCase):
    def setUp(self):
        os.environ["HANA_CONNECTION"] = "sqlite:///hana.db"
        os.environ["DW_CONNECTION"] = "sqlite:///dw.db"
        os.environ["COSTING_PATH"] = "/tmp/costing"
        Config._instance = None

    def tearDown(self):
        Config._instance = None

    def test_build_job_definitions_contains_key_jobs(self):
        context = RuntimeContext(
            con_dw=MagicMock(),
            con_sap=MagicMock(),
            lookup=MagicMock(),
        )

        jobs = build_job_definitions(context)

        self.assertGreater(len(jobs), 30)
        self.assertIsNotNone(get_job_by_name(jobs, "forecast_consumptions_fact"))
        self.assertEqual(
            get_job_by_name(jobs, "availability_calculation_fact").times,
            ("03:55", "11:55", "19:55"),
        )

    @patch("job_registry.Logger")
    def test_register_jobs_registers_each_scheduled_time(self, logger_cls):
        scheduler = MagicMock()
        every = scheduler.every.return_value
        day = every.day
        at = day.at
        context = RuntimeContext(
            con_dw=MagicMock(),
            con_sap=MagicMock(),
            lookup=MagicMock(),
        )
        jobs = build_job_definitions(context)

        register_jobs(scheduler, jobs[:2])

        self.assertEqual(at.call_count, 2)
        self.assertEqual(day.at.return_value.do.call_count, 2)
        logger_cls.return_value.info.assert_called()

    def test_load_job_config_rejects_unknown_job_name(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as temp_file:
            temp_file.write(
                '{"jobs": [{"name": "unknown_job", "enabled": true, "times": ["01:00"]}]}'
            )
            temp_path = temp_file.name

        self.addCleanup(lambda: os.remove(temp_path))

        with self.assertRaises(ValueError):
            load_job_config(temp_path, {"known_job"})

    def test_build_job_definitions_skips_disabled_jobs(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as temp_file:
            temp_file.write(
                '{"jobs": ['
                '{"name": "agents_dim", "enabled": false, "times": ["01:00"]},'
                '{"name": "customer_dim", "enabled": true, "times": ["01:05"]}'
                ']}'
            )
            temp_path = temp_file.name

        self.addCleanup(lambda: os.remove(temp_path))

        context = RuntimeContext(
            con_dw=MagicMock(),
            con_sap=MagicMock(),
            lookup=MagicMock(),
        )

        jobs = build_job_definitions(context, temp_path)

        self.assertIsNone(get_job_by_name(jobs, "agents_dim"))
        self.assertIsNotNone(get_job_by_name(jobs, "customer_dim"))

    def test_load_job_config_rejects_invalid_time_format(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as temp_file:
            temp_file.write(
                '{"jobs": [{"name": "agents_dim", "enabled": true, "times": ["25:00"]}]}'
            )
            temp_path = temp_file.name

        self.addCleanup(lambda: os.remove(temp_path))

        with self.assertRaises(ValueError):
            load_job_config(temp_path, {"agents_dim"})
