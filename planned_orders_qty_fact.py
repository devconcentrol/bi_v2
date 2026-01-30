## Solo se usa para el debe haber del BI de bi_ordenes.
import pandas as pd
from datetime import date
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
    DECIMAL,
    Date,
    insert,
    text,
    Insert,
    Engine
)

class PlannedOrdersQtyFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "werks": "Plant",
        "MaterialId": "MaterialId",
        "qty_of": "Qty",
        "bdmng": "ReqQty",
        "FactDate": "FactDate"
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        super().__init__(con_dw, con_sap, lookup)

    @error_handler
    def run(self):
        Logger().info("Processing Planned Orders Qty Fact...")
        
        # 1. Fetch Orders Data
        sql_get_qty_on_order = """
                        SELECT 
                            WERKS,
                            MATNR,
                            QTY_OF
                        FROM SAPSR3.ZCON_V_CANTIDAD_OF                                                                            
                        WHERE MANDT = '500'
                        """
        orders_results: pd.DataFrame = pd.read_sql(sql_get_qty_on_order, con=self._con_sap)
        orders_results.columns = orders_results.columns.str.lower()

        # 2. Fetch Reservations Data
        sql_get_qty_reserva = """
            SELECT WERKS,
                   MATNR,
                   BDMNG
            FROM SAPSR3.ZCON_V_RESERVAS            
        """
        reservations_results: pd.DataFrame = pd.read_sql(
            sql_get_qty_reserva, con=self._con_sap
        )
        reservations_results.columns = reservations_results.columns.str.lower()

        # 3. Merge Data
        results: pd.DataFrame = pd.merge(
            orders_results, reservations_results, on=["werks", "matnr"], how="outer"
        )

        if results.empty:
             Logger().info("No planned orders/reservations data found.")             
             with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_planned_orders")
             return

        # 4. Data Transformation
        # Fill NaNs for quantities
        results["qty_of"] = results["qty_of"].fillna(0)
        results["bdmng"] = results["bdmng"].fillna(0)

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        # Ensure matnr is string
        results["matnr"] = results["matnr"].fillna("").astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Log missing materials
        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
            # Optional: Log count or details. Previous standard logs warning.
            Logger().warning(f"Dropped {len(missing_materials)} rows due to missing MaterialId.")

        # Drop rows without MaterialId as per standard practice (unless we want to keep them with null?)
        # extended_stock_fact drops them. planned_orders_qty_fact previous code dropped them (if material_id is None).
        results = results.dropna(subset=["MaterialId"])

        if results.empty:
             Logger().info("No valid data after material lookup.")
             with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_planned_orders")
             return            

        # Add FactDate
        results["FactDate"] = date.today()

        # Rename and Select Columns
        # Ensure all columns in mapping exist (or are created)
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())
        
        # Prepare for insertion
        insert_records = results[final_cols].where(pd.notnull(results), None).to_dict(orient="records")

        # 5. Define Table Schema
        metadata: MetaData = MetaData()
        target_table: Table = Table(
            self._config.TABLE_PLANNED_ORDERS_QTY_FACT,
            metadata,
            Column("Plant", String(10)),
            Column("MaterialId", Integer),
            Column("FactDate", Date),
            Column("Qty", DECIMAL(15, 4)),
            Column("ReqQty", DECIMAL(15, 4)),
        )

        # 6. Database Operations (Delete Today + Insert)
        stmt_delete = text(
            f"DELETE FROM {self._config.TABLE_PLANNED_ORDERS_QTY_FACT} WHERE FactDate = CAST(GETDATE() as DATE)"
        )
        
        stmt_insert: Insert = insert(target_table)

        with self._con_dw.begin() as conn:
            # Delete existing for today
            conn.execute(stmt_delete)
            
            # Insert new
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} planned order records.")
                conn.execute(stmt_insert, insert_records)
            
            # Update ETL Info
            self._update_etl_info(conn, "process_planned_orders")

