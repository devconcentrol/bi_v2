import os
from dotenv import load_dotenv


class Config:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        load_dotenv(".env")

        # Connections
        self.HANA_CONNECTION = os.getenv("HANA_CONNECTION")
        self.DW_CONNECTION = os.getenv("DW_CONNECTION")

        # Paths
        self.COSTING_PATH = os.getenv("COSTING_PATH")

        # Validation
        if not self.HANA_CONNECTION:
            raise ValueError("HANA_CONNECTION not set in environment.")
        if not self.DW_CONNECTION:
            raise ValueError("DW_CONNECTION not set in environment.")
        if not self.COSTING_PATH:
            raise ValueError("COSTING_PATH not set in environment.")

        # ETL Table Names
        self.TABLE_COSTING_FACT = "CostingFact"
        self.TABLE_AGENT_DIM = "AgentDim"
        self.TABLE_ETL_INFO = "ETLInfo"
        self.TABLE_CUSTOMER_DIM = "CustomerDim"
        self.TABLE_MATERIAL_DIM = "MaterialsDim"
        self.TABLE_VENDOR_DIM = "VendorDim"
        self.TABLE_CONTACT_DIM = "ContactDim"
        self.TABLE_EWM_TASK_FACT = "EWMTaskFact"
        self.TABLE_EXTENDED_STOCK_FACT = "ExtendedStockFact"
        self.TABLE_EXTENDED_BATCH_STOCK_FACT = "ExtendedBatchStockFact"
        self.TABLE_SALES_FACT = "SalesFact"
        self.TABLE_MONITOR_STOCK_FACT = "StockLocationFact"
        self.TABLE_QM_ADJUSTMENT_FACT = "QMAdjustmentFact"
        self.TABLE_QM_INSPECTION_LOT_FACT = "QMInspectionLot"
        self.TABLE_QM_NOTIFICATION_FACT = "QMNotificationFact"
        self.TABLE_QM_SAMPLE_ANALYSIS_FACT = "QMSampleAnalysisFact"
        self.TABLE_CUSTOMER_PRICE_FACT = "CustomerPriceFact"
        self.TABLE_PLANNED_ORDERS_QTY_FACT = "ProcessOrderQtyFact"
        self.TABLE_SALES_OPEN_ORDERS_FACT = "SalesOpenOrdersFact"
        self.TABLE_SALES_OPEN_ORDERS_FACT_HIST = "SalesOpenOrdersFactHist"
        self.TABLE_PRODUCTION_DATA_FACT = "ProductionDataFact"
        self.TABLE_PRODUCTION_OF_STATE_CHANGE_FACT = "ProductionOFStateChangeFact"

        # Standard Constants
        self.DEFAULT_SALES_ORG = "1000"
        self.DEFAULT_CHANNEL = "10"
        self.DEFAULT_DIVISION = "10"

        # Economic Sales Type
        self.ECONOMIC_SALES_TYPE = "ZCRP"

    @classmethod
    def get_instance(cls):
        return cls()
