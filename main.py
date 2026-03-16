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
from ewm_locations_fact import EWMLocationsFactETL
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
from material_real_price_fact import MaterialRealPriceFactETL
from regularization_fact import RegularizationFactETL
from consumption_fact import ConsumptionFactETL
from consumption_ceco_fact import ConsumptionCeCoFactETL
from sample_delivery_fact import SampleDeliveryFactETL
from sales_delivery_date_change_fact import SalesDeliveryDateChangeFactETL
from purchase_pending_orders_fact import PurchasePendingOrdersFactETL
from forecast_consumptions_fact import ForecastConsumptionsFactETL
from document_flow_fact import DocumentFlowFactETL
from purchase_movements_fact import PurchaseMovementsFactETL
from recovery_products_fact import RecoveryProductsFactETL
from purch_delivery_date_fact import PurchDeliveryDateFactETL
from inmobilized_hist_fact import ImmobilizedHistFactETL
from vendor_assesment_fact import VendorAssesmentFactETL
from sustainability_data_fact import SustainabilityDataFactETL
from sales_order_hist_fact import SalesOrderHistFactETL
from purch_average_price_fact import PurchAvgPriceFactETL

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

        # # Process Agents
        agent_dim_processor = AgentDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:00").do(agent_dim_processor.run)

        customer_dim_processor = CustomerDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:05").do(customer_dim_processor.run)

        material_dim_processor = MaterialDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:10").do(material_dim_processor.run)

        vendor_dim_processor = VendorDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:15").do(vendor_dim_processor.run)

        contact_dim_processor = ContactDim(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("01:20").do(contact_dim_processor.run)

        # DimensionLookup invalidate caché after new dimensions loaded.
        schedule.every().day.at("01:25").do(lookup.invalidate_caches)

        # # Process Costing Fact
        costing_fact_processor = CostingFactETL(con_datawarehouse, lookup)
        schedule.every().day.at("02:00").do(costing_fact_processor.run)

        # # Document Flow Fact
        document_flow_fact_processor = DocumentFlowFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:02").do(document_flow_fact_processor.run)

        # # Purchase Movements Fact
        purchase_movements_fact_processor = PurchaseMovementsFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:04").do(purchase_movements_fact_processor.run)

        # # Recovery Products Fact
        recovery_products_fact_processor = RecoveryProductsFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:06").do(recovery_products_fact_processor.run)

        # # Purchase Delivery Date Fact
        purch_delivery_date_fact_processor = PurchDeliveryDateFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:07").do(purch_delivery_date_fact_processor.run)

        # # Immobilized History Fact
        immobilized_hist_fact_processor = ImmobilizedHistFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:08").do(immobilized_hist_fact_processor.run)

        # # Vendor Assessment Fact
        vendor_assesment_fact_processor = VendorAssesmentFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:09").do(vendor_assesment_fact_processor.run)

        # # Sustainability Data Fact
        sustainability_data_processor = SustainabilityDataFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:10").do(sustainability_data_processor.run)

        # # Sales Order Historic Fact
        sales_order_hist_fact_processor = SalesOrderHistFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:11").do(sales_order_hist_fact_processor.run)

        # # Purchase Average Price Fact
        purch_average_price_fact_processor = PurchAvgPriceFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("02:12").do(purch_average_price_fact_processor.run)

        # # EWM Tasks
        ewm_tasks_fact_processor = EWMTasksFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:00").do(ewm_tasks_fact_processor.run)

        # # EWM Locations
        ewm_locations_fact_processor = EWMLocationsFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:02").do(ewm_locations_fact_processor.run)

        # # Extended Stock
        extended_stock_fact_processor = ExtendedStockFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:05").do(extended_stock_fact_processor.run)

        # # Sales Fact
        sales_fact_processor = SalesFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:10").do(sales_fact_processor.run)

        # # # Extended Batch Stock
        extended_batch_stock_fact_processor = ExtendedBatchStockFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:15").do(extended_batch_stock_fact_processor.run)

        # # # QM Adjustment Fact
        qm_adjustment_fact_processor = QMAdjustmentFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:25").do(qm_adjustment_fact_processor.run)

        # # # QM Notification Fact
        qm_notification_fact_processor = QMNotificationFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:30").do(qm_notification_fact_processor.run)

        # # # # QM Sample Fact
        qm_sample_fact_processor = QMSampleFactETL(con_datawarehouse, con_hana, lookup)
        schedule.every().day.at("03:35").do(qm_sample_fact_processor.run)

        # # # QM Inspection Lot Fact
        qm_inspection_lot_fact_processor = QMInspectionLotFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:40").do(qm_inspection_lot_fact_processor.run)

        # # # # Customer Price Fact
        customer_price_fact_processor = CustomerPriceFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:45").do(customer_price_fact_processor.run)

        # # # Planned Orders Qty Fact
        planned_orders_qty_fact_processor = PlannedOrdersQtyFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:50").do(planned_orders_qty_fact_processor.run)

        # # # Sales Open Orders Fact
        sales_open_orders_fact_processor = SalesOpenOrdersFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("03:55").do(sales_open_orders_fact_processor.run)

        # # # Production Data Fact
        production_data_fact_processor = ProductionDataFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:00").do(production_data_fact_processor.run)

        # # # Production Orders State Change Fact
        prod_orders_state_change_fact_processor = ProductonOrdersStateChangeFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:05").do(prod_orders_state_change_fact_processor.run)

        # # # Material Real Price Fact
        material_real_price_fact_processor = MaterialRealPriceFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:10").do(material_real_price_fact_processor.run)

        # # # Regularization Fact
        regularization_fact_processor = RegularizationFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:15").do(regularization_fact_processor.run)

        # # # Consumption Fact
        consumption_fact_processor = ConsumptionFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:20").do(consumption_fact_processor.run)

        # # # Consumption CeCo Fact
        consumption_ceco_fact_processor = ConsumptionCeCoFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:25").do(consumption_ceco_fact_processor.run)

        # # # Sample Delivery Fact
        sample_delivery_fact_processor = SampleDeliveryFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:30").do(sample_delivery_fact_processor.run)

        # # # Sales Delivery Date Change Fact
        sales_delivery_date_change_fact_processor = SalesDeliveryDateChangeFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:35").do(
            sales_delivery_date_change_fact_processor.run
        )

        # # # Purchase Pending Orders Fact
        purchase_pending_orders_fact_processor = PurchasePendingOrdersFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:40").do(purchase_pending_orders_fact_processor.run)
        schedule.every().day.at("13:05").do(purchase_pending_orders_fact_processor.run)

        # # # Forecast Consumptions Fact
        forecast_consumptions_fact_processor = ForecastConsumptionsFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("04:45").do(forecast_consumptions_fact_processor.run)

        # # # Monitor Stock Fact
        monitor_stock_fact_processor = MonitorStockFactETL(
            con_datawarehouse, con_hana, lookup
        )
        schedule.every().day.at("14:00").do(monitor_stock_fact_processor.run)

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
