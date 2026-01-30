from sqlalchemy import create_engine, Engine
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from costing_fact import CostingFactETL
from dimensions.customer_dim import CustomerDim
from customer_price_fact import CustomerPriceFactETL
from planned_orders_qty_fact import PlannedOrdersQtyFactETL


def main():
    con_hana: Engine | None = None
    con_datawarehouse: Engine | None = None
    try:
        config = Config.get_instance()

        # Create database connections
        if config.HANA_CONNECTION is None or config.DW_CONNECTION is None:
            raise ValueError("Database connection strings are not configured")

        con_hana = create_engine(config.HANA_CONNECTION)
        con_datawarehouse = create_engine(config.DW_CONNECTION)

        Logger().info("Starting ETL processes")

        # Initialize dimension lookup
        lookup = DimensionLookup(con_datawarehouse)

        # Process Costing Fact
        # costing_fact_processor = CostingFactETL(con_datawarehouse, lookup)
        # costing_fact_processor.run()

        # Process Customer Dim
        # customer_dim_processor = CustomerDim(con_datawarehouse, con_hana, lookup)
        # customer_dim_processor.run()

        # Process Customer Price Fact
        # customer_price_fact_processor = CustomerPriceFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # customer_price_fact_processor.run()

        # Process Planned Orders Qty Fact
        planned_orders_qty_fact_processor = PlannedOrdersQtyFactETL(
            con_datawarehouse, con_hana, lookup
        )
        planned_orders_qty_fact_processor.run()

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
