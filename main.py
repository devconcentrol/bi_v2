from sqlalchemy import create_engine, Engine
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from costing_fact import CostingFactETL
import os
from dotenv import load_dotenv
import schedule

def main() -> None:
    try:
        load_dotenv(".env")
        hana_connection = os.getenv("HANA_CONNECTION")
        if not hana_connection:
            raise ValueError(
                "HANA connection string is not set in the environment variables."
            )
        datawarehouse_connection = os.getenv("DW_CONNECTION")
        if not datawarehouse_connection:
            raise ValueError(
                "Data Warehouse connection string is not set in the environment variables."
            )
        
        costing_path = os.getenv("COSTING_PATH")
        if not costing_path:
            raise ValueError(
                "COSTING_PATH is not set in the environment variables."
            )

        # Create database connections
        con_hana: Engine = create_engine(hana_connection)
        con_datawarehouse: Engine = create_engine(datawarehouse_connection)
        
        # Initialize dimension lookup
        lookup = DimensionLookup(con_datawarehouse)
        
        # Costing Fact
        process_costing = CostingFactETL(con_datawarehouse, lookup)
        process_costing.run(costing_path)
    finally:
        if 'con_hana' in locals():
            con_hana.dispose()
        if 'con_datawarehouse' in locals():
            con_datawarehouse.dispose()


if __name__ == "__main__":
    main()