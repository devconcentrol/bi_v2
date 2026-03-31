import time

import schedule
from sqlalchemy import Engine, create_engine

from job_registry import RuntimeContext, build_job_definitions, register_jobs
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config


def main() -> None:
    con_hana: Engine | None = None
    con_datawarehouse: Engine | None = None

    try:
        config = Config.get_instance()

        if config.HANA_CONNECTION is None or config.DW_CONNECTION is None:
            raise ValueError("Database connection strings are not configured")

        con_hana = create_engine(config.HANA_CONNECTION)
        con_datawarehouse = create_engine(config.DW_CONNECTION)

        Logger().info("Starting ETL scheduler")

        lookup = DimensionLookup(con_datawarehouse)
        context = RuntimeContext(
            con_dw=con_datawarehouse,
            con_sap=con_hana,
            lookup=lookup,
        )

        jobs = build_job_definitions(context)
        register_jobs(schedule, jobs)

        while True:
            schedule.run_pending()
            time.sleep(10)

    except KeyboardInterrupt:
        Logger().warning("Scheduler interrupted by user")
    except Exception as e:
        Logger().exception(f"Critical error in main: {e}")
        raise
    finally:
        Logger().info("Closing database connections...")
        if con_hana is not None:
            con_hana.dispose()
        if con_datawarehouse is not None:
            con_datawarehouse.dispose()


if __name__ == "__main__":
    main()
