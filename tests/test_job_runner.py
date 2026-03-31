import unittest
from unittest.mock import patch

from utils.job_runner import run_job, safe_run_job


class JobRunnerTests(unittest.TestCase):
    @patch("utils.job_runner.Logger")
    def test_run_job_logs_success(self, logger_cls):
        calls = []

        def sample_job():
            calls.append("ran")

        run_job("sample_job", sample_job)

        self.assertEqual(calls, ["ran"])
        logger = logger_cls.return_value
        self.assertEqual(logger.info.call_count, 2)
        logger.error.assert_not_called()

    @patch("utils.job_runner.Logger")
    def test_run_job_reraises_failures(self, logger_cls):
        def failing_job():
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            run_job("failing_job", failing_job)

        logger = logger_cls.return_value
        logger.error.assert_called_once()

    @patch("utils.job_runner.Logger")
    def test_safe_run_job_returns_false_on_failure(self, logger_cls):
        result = safe_run_job("failing_job", lambda: (_ for _ in ()).throw(ValueError("x")))

        self.assertFalse(result)
        logger = logger_cls.return_value
        logger.error.assert_called_once()
