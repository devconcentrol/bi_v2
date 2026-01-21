from sqlalchemy import create_engine, Engine
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from costing_fact import CostingFactETL
from ewm_task_fact import EWMTasksFactETL
from extended_stock_fact import ExtendedStockFactETL
from sales_fact import SalesFactETL
from extended_batch_stock_fact import ExtendedBatchStockFactETL
from dimensions.agent_dim import AgentDim
from dimensions.customer_dim import CustomerDim
from dimensions.material_dim import MaterialDim
from dimensions.vendor_dim import VendorDim
from dimensions.contact_dim import ContactDim
import schedule
import time


def main() -> None:
    con_hana: Engine | None = None
    con_datawarehouse: Engine | None = None
    try:
        config = Config.get_instance()
        
        # # Create database connections
        con_hana = create_engine(config.HANA_CONNECTION)
        con_datawarehouse = create_engine(config.DW_CONNECTION)

        Logger().info("Starting ETL processes")

        # Initialize dimension lookup
        lookup = DimensionLookup(con_datawarehouse)
        schedule.every().day.at("01:00").do(lookup.invalidate_caches)

        # # Process Agents
        agent_dim_processor = AgentDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:05").do(agent_dim_processor.run)
        # agent_dim_processor.run()

        customer_dim_processor = CustomerDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:10").do(customer_dim_processor.run)
        # customer_dim_processor.run()

        material_dim_processor = MaterialDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:15").do(material_dim_processor.run)
        # material_dim_processor.run()

        vendor_dim_processor = VendorDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:20").do(vendor_dim_processor.run)
        # vendor_dim_processor.run()

        contact_dim_processor = ContactDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:25").do(contact_dim_processor.run)
        # contact_dim_processor.run()

        # # Process Costing Fact
        costing_fact_processor = CostingFactETL(con_datawarehouse, lookup)
        schedule.every().day.at("02:00").do(costing_fact_processor.run)       

        # # EWM Tasks
        ewm_tasks_fact_processor = EWMTasksFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:00").do(ewm_tasks_fact_processor.run)
        # # ewm_tasks_processor.run()     

        # # Extended Stock
        extended_stock_fact_processor = ExtendedStockFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:05").do(extended_stock_fact_processor.run)
        # extended_stock_fact_processor.run() 
        
        # # Sales Fact
        sales_fact_processor = SalesFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:10").do(sales_fact_processor.run)
        # sales_fact_processor.run() 

        # # Extended Batch Stock
        extended_batch_stock_fact_processor = ExtendedBatchStockFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:15").do(extended_batch_stock_fact_processor.run)
        # extended_batch_stock_fact_processor.run() 

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
