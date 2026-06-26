import pandas as pd
from utils.error_handler import error_handler
from utils.logger import Logger
from etl.base_fact_etl import BaseFactETL
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


class FinanceOpenItemsFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "CustId": "CustId",
        "FactDate": "FactDate",
        "wrbtr": "Amount",
        "waers": "Currency",
        "bukrs": "CompanyCode",
        "max_days": "Days",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Finance Open Items Fact...")

        # 1. Fetch New Data from SAP
        Logger().info("Fetching new data from SAP...")
        sql_get_open_orders = """
                        SELECT KUNNR,
                               WRBTR,
                               WAERS,
                               BUKRS,
                               MAX_DAYS                               
                        FROM SAPSR3.ZCON_V_CARTERA
                        WHERE BUKRS = '1000'
                        """
        results: pd.DataFrame = pd.read_sql(sql_get_open_orders, con=self._con_sap)

        if results.empty:
            Logger().info("No data found in SAP.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_fi_open_items")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # 4. Data Transformation
        Logger().info("Transforming data...")

        results["FactDate"] = pd.Timestamp.today().strftime("%Y-%m-%d")

        # Lookup Customer
        customer_map = self._lookup.get_customer_map()

        key1 = (
            results["bukrs"]
            + self._config.DEFAULT_CHANNEL
            + self._config.DEFAULT_DIVISION
            + results["kunnr"]
        )

        results["CustId"] = key1.map(customer_map)

        # Rename and Select Columns
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        # Ensure all columns exist (mapped ones usually do)
        # Filter for existence just in case, or let it fail if mapping is wrong (better to fail)
        insert_df = results[final_cols].copy()

        # Replace NaNs with None for SQL insertion
        insert_records = insert_df.where(pd.notnull(insert_df), None).to_dict(
            orient="records"
        )

        # 5. Define Table Schema
        metadata: MetaData = MetaData()
        target_table: Table = Table(
            self._config.TABLE_FI_OPEN_ITEMS_FACT,
            metadata,
            Column("CustId", Integer),
            Column("FactDate", Date),
            Column("Amount", DECIMAL(15, 4)),
            Column("Currency", String(10)),
            Column("CompanyCode", String(4)),
            Column("Days", Integer),
        )

        stmt_insert: Insert = insert(target_table)

        with self._con_dw.begin() as conn:
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} finance open items records."
                )
                conn.execute(stmt_insert, insert_records)

            # Update ETL Info
            self._update_etl_info(conn, "process_fi_open_items")
