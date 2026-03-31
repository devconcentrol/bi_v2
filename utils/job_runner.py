from __future__ import annotations

from time import perf_counter
from typing import Callable

from utils.logger import Logger


def run_job(job_name: str, job_callable: Callable[[], None]) -> None:
    logger = Logger()
    started_at = perf_counter()
    logger.info("Job '%s' started", job_name)

    try:
        job_callable()
    except Exception:
        elapsed = perf_counter() - started_at
        logger.error("Job '%s' failed after %.2fs", job_name, elapsed)
        raise

    elapsed = perf_counter() - started_at
    logger.info("Job '%s' finished successfully in %.2fs", job_name, elapsed)


def safe_run_job(job_name: str, job_callable: Callable[[], None]) -> bool:
    try:
        run_job(job_name, job_callable)
    except Exception:
        return False

    return True
