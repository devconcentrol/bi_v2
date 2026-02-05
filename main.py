from sqlalchemy import create_engine, Engine
import schedule
import time
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from dimensions.agent_dim import AgentDim
from dimensions.customer_dim import CustomerDim
from dimensions.material_dim import MaterialDim
from dimensions.vendor_dim import VendorDim
from dimensions.contact_dim import ContactDim
from costing_fact import CostingFactETL
from ewm_task_fact import EWMTasksFactETL
from extended_stock_fact import ExtendedStockFactETL
from sales_fact import SalesFactETL
from extended_batch_stock_fact import ExtendedBatchStockFactETL
from monitor_stock_fact import MonitorStockFactETL
from qm_adjustment_fact import QMAdjustmentFactETL
from qm_inspection_lot_fact import QMInspectionLotFactETL
from qm_notification_fact import QMNotificationFactETL
from qm_sample_fact import QMSampleFactETL
from customer_price_fact import CustomerPriceFactETL
from sales_open_orders_fact import SalesOpenOrdersFactETL
from planned_orders_qty_fact import PlannedOrdersQtyFactETL
from production_data_fact import ProductionDataFactETL
from production_orders_state_change_fact import ProductonOrdersStateChangeFactETL
from availability_calculation_fact import AvailabilityCalculationFactETL


def main() -> None:
    con_hana: Engine | None = None
    con_datawarehouse: Engine | None = None
    try:
        config = Config.get_instance()

        # # Create database connections
        if config.HANA_CONNECTION is None or config.DW_CONNECTION is None:
            raise ValueError("Database connection strings are not configured")

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
        # ewm_tasks_fact_processor.run()

        # # Extended Stock
        extended_stock_fact_processor = ExtendedStockFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:05").do(extended_stock_fact_processor.run)
        # extended_stock_fact_processor.run()

        # # Sales Fact
        sales_fact_processor = SalesFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:10").do(sales_fact_processor.run)
        # sales_fact_processor.run()

        # # # Extended Batch Stock
        extended_batch_stock_fact_processor = ExtendedBatchStockFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:15").do(extended_batch_stock_fact_processor.run)
        # extended_batch_stock_fact_processor.run()

        # # # QM Adjustment Fact
        qm_adjustment_fact_processor = QMAdjustmentFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:25").do(qm_adjustment_fact_processor.run)
        # qm_adjustment_fact_processor.run()

        # # # QM Notification Fact
        qm_notification_fact_processor = QMNotificationFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:30").do(qm_notification_fact_processor.run)
        # qm_notification_fact_processor.run()

        # # # # QM Sample Fact
        qm_sample_fact_processor = QMSampleFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:35").do(qm_sample_fact_processor.run)
        # qm_sample_fact_processor.run()

        # # # QM Inspection Lot Fact
        qm_inspection_lot_fact_processor = QMInspectionLotFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:40").do(qm_inspection_lot_fact_processor.run)
        # qm_inspection_lot_fact_processor.run()

        # # # # Customer Price Fact
        customer_price_fact_processor = CustomerPriceFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:45").do(customer_price_fact_processor.run)
        # customer_price_fact_processor.run()

        # # # Planned Orders Qty Fact
        planned_orders_qty_fact_processor = PlannedOrdersQtyFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:50").do(planned_orders_qty_fact_processor.run)
        # planned_orders_qty_fact_processor.run()

        # # # Sales Open Orders Fact
        sales_open_orders_fact_processor = SalesOpenOrdersFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:55").do(sales_open_orders_fact_processor.run)
        # sales_open_orders_fact_processor.run()

        # # # Production Data Fact
        production_data_fact_processor = ProductionDataFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:00").do(production_data_fact_processor.run)
        # production_data_fact_processor.run()

        # # # Production Orders State Change Fact
        prod_orders_state_change_fact_processor = ProductonOrdersStateChangeFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:05").do(prod_orders_state_change_fact_processor.run)
        # prod_orders_state_change_fact_processor.run()

        # # # Monitor Stock Fact
        monitor_stock_fact_processor = MonitorStockFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("14:00").do(monitor_stock_fact_processor.run)
        # monitor_stock_fact_processor.run()

        # # # Process availability calculation
        availability_calculation_fact_processor = AvailabilityCalculationFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:55").do(availability_calculation_fact_processor.run)
        schedule.every().day.at("11:55").do(availability_calculation_fact_processor.run)
        schedule.every().day.at("19:55").do(availability_calculation_fact_processor.run)
        # availability_calculation_processor.run()

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
