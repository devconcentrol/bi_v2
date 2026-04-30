import argparse
from collections.abc import Sequence

from sqlalchemy import Engine, create_engine

from job_registry import RuntimeContext, build_job_definitions, get_job_by_name
from utils.config import Config
from utils.dimension_lookup import DimensionLookup
from utils.job_runner import run_job
from utils.logger import Logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run BI ETL jobs manually from the command line.",
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        help="Path to an alternate jobs config JSON file.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List enabled jobs from the active config.")

    run_parser = subparsers.add_parser("run", help="Run a single enabled job.")
    run_parser.add_argument("job_name", help="Name of the job to execute.")

    return parser


def _build_runtime(
    config_path: str | None,
) -> tuple[list, Engine | None, Engine | None]:
    config = Config.get_instance()

    if config.HANA_CONNECTION is None or config.DW_CONNECTION is None:
        raise ValueError("Database connection strings are not configured")

    con_hana = create_engine(config.HANA_CONNECTION)
    con_datawarehouse = create_engine(config.DW_CONNECTION)

    lookup = DimensionLookup(con_datawarehouse)
    context = RuntimeContext(
        con_dw=con_datawarehouse,
        con_sap=con_hana,
        lookup=lookup,
    )
    jobs = build_job_definitions(context, config_path)

    return jobs, con_hana, con_datawarehouse


def _list_jobs(jobs: list) -> int:
    for job in jobs:
        times = ", ".join(job.times)
        print(f"{job.name}: {times}")
    return 0


def _run_job(jobs: list, job_name: str) -> int:
    job = get_job_by_name(jobs, job_name)
    if job is None:
        raise ValueError(f"Unknown or disabled job '{job_name}'")

    run_job(job.name, job.runner)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    con_hana: Engine | None = None
    con_datawarehouse: Engine | None = None

    try:
        jobs, con_hana, con_datawarehouse = _build_runtime(args.config_path)

        if args.command == "list":
            return _list_jobs(jobs)
        if args.command == "run":
            return _run_job(jobs, args.job_name)

        raise ValueError(f"Unsupported command '{args.command}'")

    except Exception as e:
        Logger().exception(f"Critical error in main_test: {e}")
        raise
    finally:
        Logger().info("Closing database connections...")
        if con_hana is not None:
            con_hana.dispose()
        if con_datawarehouse is not None:
            con_datawarehouse.dispose()


if __name__ == "__main__":
    raise SystemExit(main(["run", "qm_adjustment_fact"]))
