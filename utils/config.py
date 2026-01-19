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

        # Shared Constants
        self.DEFAULT_CHANNEL = "10"
        self.DEFAULT_DIVISION = "10"
        
        # ETL Table Names
        self.TABLE_COSTING_FACT = "CostingFact"
        self.TABLE_AGENT_DIM = "AgentDim"
        self.TABLE_EWM_TASK_FACT = "EWMTaskFact"
        self.TABLE_ETL_INFO = "ETLInfo"
        self.TABLE_CUSTOMER_DIM = "CustomerDim"
        self.TABLE_MATERIAL_DIM = "MaterialsDim"
        self.TABLE_VENDOR_DIM = "VendorDim"

    @classmethod
    def get_instance(cls):
        return cls()
