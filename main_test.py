
from sqlalchemy import create_engine, Engine
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from costing_fact import CostingFactETL

def main():
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

        # Process Costing Fact
        costing_fact_processor = CostingFactETL(con_datawarehouse, lookup)
        costing_fact_processor.run()

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