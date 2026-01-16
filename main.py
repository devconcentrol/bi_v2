from sqlalchemy import create_engine, Engine
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from costing_fact import CostingFactETL
from ewm_task import EWMTasksETL
from agent import Agent
import schedule
import time


def main() -> None:
    con_hana: Engine | None = None
    con_datawarehouse: Engine | None = None
    try:
        config = Config.get_instance()
        
        # Create database connections
        con_hana = create_engine(config.HANA_CONNECTION)
        con_datawarehouse = create_engine(config.DW_CONNECTION)

        Logger().info("Starting ETL processes")

        # Initialize dimension lookup
        lookup = DimensionLookup(con_datawarehouse)
        schedule.every().day.at("01:00").do(lookup.invalidate_caches)

        # Process Agents
        agent_processor = Agent(con_datawarehouse, con_hana)
        schedule.every().day.at("03:00").do(agent_processor.run)
        # agent_processor.process()

        # Costing Fact
        # logger.info("Processing costing facts...")
        # process_costing = CostingFactETL(con_datawarehouse, lookup)
        # process_costing.run(costing_path)

        # EWM Tasks
        ewm_tasks_processor = EWMTasksETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:00").do(ewm_tasks_processor.run)
        # ewm_tasks_processor.run()

        while True:
            schedule.run_pending()
            time.sleep(10)
    except Exception as e:
        Logger().error(f"Critical error in main: {e}")
    finally:
        Logger().info("Closing database connections...")
        if con_hana is not None:
            con_hana.dispose()
        if con_datawarehouse is not None:
            con_datawarehouse.dispose()


if __name__ == "__main__":
    main()
