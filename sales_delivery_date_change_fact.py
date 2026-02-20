import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DECIMAL,
    Date,
    insert,
    Insert,
)

from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL


class SalesDeliveryDateChangeFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "vbeln": "SalesId",
        "CustomerId": "CustomerId",
        "MaterialId": "MaterialId",
        "erdat": "CreatedDate",
        "fecha_anterior": "OldDeliveryDate",
        "fecha_actual": "NewDeliveryDate",
        "audat": "DocumentDate",
        "old_qty": "OldConfirmedQty",
        "new_qty": "NewConfirmedQty",
        "wadat": "GoodsIssueDate",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Sales Delivery Date Change Fact...")
        config = self._config

        # Define table metadata
        metadata: MetaData = MetaData()
        sales_date_change_table: Table = Table(
            config.TABLE_SALES_DATE_CHANGE_FACT,
            metadata,
            Column("SalesId", String(45)),
            Column("CustomerId", Integer),
            Column("MaterialId", Integer),
            Column("CreatedDate", Date),
            Column("OldDeliveryDate", Date),
            Column("NewDeliveryDate", Date),
            Column("DocumentDate", Date),
            Column("OldConfirmedQty", DECIMAL(15, 4)),
            Column("NewConfirmedQty", DECIMAL(15, 4)),
            Column("GoodsIssueDate", Date),
        )

        # Get current date in SAP format
        # Using today's date for 'ERDAT = CURRENT_DATE' logic from original query
        today_sap = pd.Timestamp.now().strftime("%Y%m%d")

        sql_get_sales = """
            SELECT ERDAT,
                   VBELN,
                   MATNR,
                   VKORG,
                   VTWEG,
                   SPART,
                   KUNNR,
                   FECHA_ANTERIOR,
                   FECHA_ACTUAL,
                   AUDAT,
                   OLD_QTY,
                   NEW_QTY,
                   WADAT
            FROM SAPSR3.ZCON_V_SALES_DELIVERY_DATE_CHANGE ZVF   
            WHERE ERDAT = :today                        
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_sales,
            con=self._con_sap,
            params={"today": today_sap},
            dtype_backend="numpy_nullable",
        )

        if results.empty:
            Logger().info("No sales delivery date changes found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_sales_date_changes")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # Date Conversions
        # In Pandas 2.0, we keep them as datetime objects and handle the NaT -> None
        # translation during the final dictionary conversion for SQL compatibility.
        date_cols = ["erdat", "fecha_anterior", "fecha_actual", "audat", "wadat"]
        for col in date_cols:
            if col in results.columns:
                results[col] = pd.to_datetime(
                    results[col], format="%Y%m%d", errors="coerce"
                ).convert_dtypes()

        # Numeric Conversions
        # Using .fillna(0) to ensure no NA/NaN values reach SQL DECIMAL columns
        numeric_cols = ["old_qty", "new_qty"]
        for col in numeric_cols:
            if col in results.columns:
                results[col] = pd.to_numeric(results[col], errors="coerce").fillna(0)

        # Lookups
        customer_map = self._lookup.get_customer_map()
        material_map = self._lookup.get_material_map()

        # Customer Lookup
        results["vkorg_str"] = results["vkorg"].astype(str)
        results["vtweg_str"] = results["vtweg"].astype(str)
        results["spart_str"] = results["spart"].astype(str)
        results["kunnr_str"] = results["kunnr"].astype(str)

        key1 = (
            results["vkorg_str"]
            + results["vtweg_str"]
            + results["spart_str"]
            + results["kunnr_str"]
        )
        key2 = (
            results["vkorg_str"]
            + config.DEFAULT_CHANNEL
            + results["spart_str"]
            + results["kunnr_str"]
        )

        results["CustomerId"] = key1.map(customer_map)
        # Apply fallback logic
        results["CustomerId_Fallback"] = key2.map(customer_map)
        results["CustomerId"] = results["CustomerId"].fillna(
            results["CustomerId_Fallback"]
        )

        # Material Lookup
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Rename columns to match target table
        results = results.rename(columns=self.COLUMN_MAPPING)

        # Select final columns
        final_cols = list(self.COLUMN_MAPPING.values())

        insert_records = results[final_cols].to_dict(orient="records")

        # Insert
        stm_insert: Insert = insert(sales_date_change_table)

        with self._con_dw.begin() as conn:
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} sales delivery date change records."
                )
                conn.execute(stm_insert, insert_records)

            self._update_etl_info(conn, "process_sales_date_changes")
