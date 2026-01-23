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
    insert,
    text,
    Insert,
    DECIMAL,
)


class ExtendedBatchStockFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "lgort": "StorageLocation",
        "clabs": "UnrestrictedStock",
        "cinsm": "QualityInspectionStock",
        "cspem": "BlockedStock",
        "cretm": "ReturnStock",
        "ceinm": "RestrictedStock",
        "werks": "Plant",
        "unit_cost": "UnitCost",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Processing Extended Batch Stock Fact...")

        sql_get_stock = """
                        SELECT WERKS,
                               MATNR,
                               CHARG,
                               LGORT,
                               CLABS,
                               CINSM,
                               CSPEM,
                               CRETM,
                               CEINM,
                               UNIT_COST
                        FROM SAPSR3.ZCON_V_STOCK_CHARG
                        """

        results: pd.DataFrame = pd.read_sql(sql_get_stock, con=self._con_sap)

        if results.empty:
            Logger().info("No batch stock data found.")            
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_batch_stock")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # Define the table
        metadata: MetaData = MetaData()
        extended_stock_table: Table = Table(
            self._config.TABLE_EXTENDED_BATCH_STOCK_FACT,
            metadata,
            Column("MaterialId", Integer),
            Column("BatchNumber", String(25)),
            Column("StorageLocation", String(200)),
            Column("UnrestrictedStock", DECIMAL(15, 4)),
            Column("QualityInspectionStock", DECIMAL(15, 4)),
            Column("BlockedStock", DECIMAL(15, 4)),
            Column("ReturnStock", DECIMAL(15, 4)),
            Column("RestrictedStock", DECIMAL(15, 4)),
            Column("UnitCost", DECIMAL(15, 4)),
            Column("Plant", String(10)),
        )

        # Convert numeric columns safely
        numeric_cols = ["clabs", "cinsm", "cspem", "cretm", "ceinm", "unit_cost"]
        for col in numeric_cols:
            if col in results.columns:
                results[col] = pd.to_numeric(results[col], errors="coerce").fillna(0)

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        # Ensure matnr is string
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Log missing materials logic
        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
            Logger().warning(
                f"Dropped {len(missing_materials)} rows due to missing MaterialId."
            )

        results = results.dropna(subset=["MaterialId"])

        # Rename columns to match database schema
        results = results.rename(columns=self.COLUMN_MAPPING)
        
        # Select only relevant columns
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = results[final_cols].where(pd.notnull(results), None).to_dict(orient="records")

        stmt_insert_stock: Insert = insert(extended_stock_table)

        # In batch stock, we truncate and reload usually (snapshot of current state)
        stmt_truncate_stock_table = text(
            f"TRUNCATE TABLE {self._config.TABLE_EXTENDED_BATCH_STOCK_FACT}"
        )

        with self._con_dw.begin() as conn:
            conn.execute(stmt_truncate_stock_table)
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} batch stock records.")
                conn.execute(stmt_insert_stock, insert_records)

            self._update_etl_info(conn, "process_batch_stock")
