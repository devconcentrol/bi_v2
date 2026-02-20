from sqlalchemy import create_engine, Engine
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from costing_fact import CostingFactETL
from dimensions.customer_dim import CustomerDim
from customer_price_fact import CustomerPriceFactETL
from planned_orders_qty_fact import PlannedOrdersQtyFactETL
from sales_open_orders_fact import SalesOpenOrdersFactETL
from production_data_fact import ProductionDataFactETL
from production_orders_state_change_fact import ProductonOrdersStateChangeFactETL
from availability_calculation_fact import AvailabilityCalculationFactETL
from sales_fact import SalesFactETL
from material_real_price_fact import MaterialRealPriceFactETL
from regularization_fact import RegularizationFactETL
from ewm_task_fact import EWMTasksFactETL
from consumption_fact import ConsumptionFactETL
from consumption_ceco_fact import ConsumptionCeCoFactETL
from sample_delivery_fact import SampleDeliveryFactETL
from sales_delivery_date_change_fact import SalesDeliveryDateChangeFactETL


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
        # planned_orders_qty_fact_processor = PlannedOrdersQtyFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # planned_orders_qty_fact_processor.run()

        # Process Sales Open Orders Fact
        # sales_open_orders_fact_processor = SalesOpenOrdersFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # sales_open_orders_fact_processor.run()

        # Process Production Data Fact
        # production_data_fact_processor = ProductionDataFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # production_data_fact_processor.run()

        # Process Production Orders State Change Fact
        # prod_orders_state_change_fact_processor = ProductonOrdersStateChangeFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # prod_orders_state_change_fact_processor.run()

        # Process Sales Fact
        # sales_fact_processor = SalesFactETL(con_datawarehouse, con_hana, lookup)
        # sales_fact_processor.run()

        # availability_calculation_fact_processor = AvailabilityCalculationFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # availability_calculation_fact_processor.run()

        # material_real_price_fact_processor = MaterialRealPriceFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # material_real_price_fact_processor.run()

        # regularization_fact_processor = RegularizationFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # regularization_fact_processor.run()

        # ewm_tasks_fact_processor = EWMTasksFactETL(con_datawarehouse, con_hana, lookup)
        # ewm_tasks_fact_processor.run()

        # consumption_fact_processor = ConsumptionFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # consumption_fact_processor.run()

        # consumption_ceco_fact_processor = ConsumptionCeCoFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # consumption_ceco_fact_processor.run()

        # sample_delivery_fact_processor = SampleDeliveryFactETL(
        #     con_datawarehouse, con_hana, lookup
        # )
        # sample_delivery_fact_processor.run()

        sales_delivery_date_change_fact_processor = SalesDeliveryDateChangeFactETL(
            con_datawarehouse, con_hana, lookup
        )
        sales_delivery_date_change_fact_processor.run()

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
