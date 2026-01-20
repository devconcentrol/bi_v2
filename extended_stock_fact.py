import pandas as pd
from datetime import date
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
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
    Date,
)


class ExtendedStockFactETL:
    COLUMN_MAPPING = {
        "MaterialId": "MaterialId",
        "lgort":"StorageLocation" ,
        "labst":"UnrestrictedStock",
        "insme":"QualityInspectionStock",
        "speme":"BlockedStock",
        "retme":"ReturnStock",
        "einme":"RestrictedStock",
        "unit_cost":"UnitCost",
        "werks":"Plant",        
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Processing Extended Stock Fact...")
        
        sql_get_stock = """
                        SELECT WERKS,
                               MATNR,
                               LGORT,
                               LABST,
                               INSME,
                               SPEME,
                               RETME,
                               EINME,
                               UNIT_COST
                        FROM SAPSR3.ZCON_V_STOCK
                        WHERE (COALESCE(LABST, 0) + COALESCE(INSME, 0) + COALESCE(SPEME, 0) + COALESCE(RETME, 0) + COALESCE(EINME, 0)) > 0
                        """

        results: pd.DataFrame = pd.read_sql(sql_get_stock, con=self._con_sap)

        if results.empty:
             Logger().info("No stock data found.")
             # Update ETL info even if empty? Usually yes.
             stmt_update_etl_empty = text(
                f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_stock'"
             )
             with self._con_dw.begin() as conn:
                 conn.execute(stmt_update_etl_empty)
             return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # # Define the table
        metadata: MetaData = MetaData()

        # Define VendorDim Table
        extended_stock_table: Table = Table(
            self._config.TABLE_EXTENDED_STOCK_FACT,
            metadata,
            Column("MaterialId", Integer),
            Column("StorageLocation", String(200)),
            Column("UnrestrictedStock", DECIMAL(15, 4)),
            Column("QualityInspectionStock", DECIMAL(15, 4)),
            Column("BlockedStock", DECIMAL(15, 4)),
            Column("ReturnStock", DECIMAL(15, 4)),
            Column("RestrictedStock", DECIMAL(15, 4)),
            Column("UnitCost", DECIMAL(15, 4)),
            Column("Plant", String(10)),
            Column("StockDate", Date),
        )
        
        # Convert numeric columns safely
        numeric_cols = ["labst", "insme", "speme", "retme", "einme", "unit_cost"]
        for col in numeric_cols:
            if col in results.columns:
                results[col] = pd.to_numeric(results[col], errors="coerce").fillna(0)

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Filter out missing materials (was printed in loop before)
        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
             Logger().warning(f"Dropped {len(missing_materials)} rows due to missing MaterialId.")
        
        results = results.dropna(subset=["MaterialId"])
        
        if results.empty:
             Logger().info("No valid stock data after material lookup.")
             return
        
        # Use COLUMN_MAPPING for renaming
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = {
            db_col
            for df_col,db_col in self.COLUMN_MAPPING.items()            
        }
        
        insert_records = results[list(final_cols)].to_dict(orient="records")

        # Database Operations
        stmt_insert_stock: Insert = insert(extended_stock_table)
        
        # Delete only today's data before inserting (Snapshot logic)
        
        stmt_delete_stock = text(
            f"DELETE FROM {self._config.TABLE_EXTENDED_STOCK_FACT} WHERE StockDate = CAST(GETDATE() as DATE)"
        )
        
        stmt_update_etl = text(
            f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_stock'"
        )

        with self._con_dw.begin() as conn:
            conn.execute(stmt_delete_stock)
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} stock records.")
                conn.execute(stmt_insert_stock, insert_records) # type: ignore

            conn.execute(stmt_update_etl)
