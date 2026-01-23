import pandas as pd
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from base_fact_etl import BaseFactETL
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Engine,
    DECIMAL,
    insert,
    text,
    Insert,
)


class MonitorStockFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "MaterialId": "MaterialId",
        "lgnum": "EwmStorageLocation",
        "lgtyp": "StorageLocationType",
        "lgpla": "Location",
        "charg": "BatchNumber",
        "quan": "Quantity",
        "unit": "UnitId",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Monitor Stock Fact...")

        sql_get_stock: str = """
            SELECT MATNR,                                        
                   LGNUM,
                   LGTYP,
                   LGPLA,
                   CHARG,
                   QUAN,
                   UNIT
            FROM SAPSR3.ZCON_V_MONITOR
            """

        results: pd.DataFrame = pd.read_sql(sql_get_stock, con=self._con_sap)

        if results.empty:
            Logger().info("No monitor stock data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn,'process_monitor_stock')
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # Define the table
        metadata: MetaData = MetaData()

        stock_location_fact_table: Table = Table(
            self._config.TABLE_MONITOR_STOCK_FACT,
            metadata,
            Column("EwmStorageLocation", String(10)),
            Column("StorageLocationType", String(15)),
            Column("Location", String(100)),
            Column("MaterialId", Integer),
            Column("Quantity", DECIMAL(15, 4)),
            Column("BatchNumber", String(100)),
            Column("UnitId", String(10)),
        )

        # Convert numeric columns safely
        if "quan" in results.columns:
            results["quan"] = pd.to_numeric(results["quan"], errors="coerce").fillna(0)

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        # Usually matnr is numeric-string, ensure it's comparable to key in map
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Filter out missing materials
        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
            Logger().warning(
                f"Dropped {len(missing_materials)} rows due to missing MaterialId."
            )

        results = results.dropna(subset=["MaterialId"])

        # Use COLUMN_MAPPING for renaming
        results = results.rename(columns=self.COLUMN_MAPPING)

        # Select only the columns that exist in the target table (based on mapping)
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = results[final_cols].where(pd.notnull(results), None).to_dict(orient="records")

        # Database Operations
        stmt_insert_stock: Insert = insert(stock_location_fact_table)

        with self._con_dw.begin() as conn:
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} monitor stock records.")
                conn.execute(stmt_insert_stock, insert_records)

            self._update_etl_info(conn,'process_monitor_stock')
