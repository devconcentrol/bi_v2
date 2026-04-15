import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    insert,
    text,
    Insert,
)
from utils.error_handler import error_handler
from utils.logger import Logger
from etl.base_fact_etl import BaseFactETL


class CustomerDMFactETL(BaseFactETL):
    COLUMN_MAPPING = {"KUNNR": "CustId", "KUNNR_DM": "CustDMId"}

    @error_handler
    def run(self):
        Logger().info("Processing Customer DM Fact...")

        sql_get_customer_dm = """
                        SELECT KUNNR,
                               KUNNR_DM
                        FROM SAPSR3.ZCON_V_DM
                        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_customer_dm, con=self._con_sap, dtype_backend="numpy_nullable"
        )

        if results.empty:
            Logger().info("No customer price data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_customer_dm")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # # Define the table
        metadata: MetaData = MetaData()

        customer_dm_table: Table = Table(
            self._config.TABLE_CUSTOMER_DM,
            metadata,
            Column("CustId", Integer),
            Column("CustDMId", Integer),
        )
        # Lookup CustId
        customer_map = self._lookup.get_customer_map()
        # Create composite key: 1000 + 10 + 10 + KUNNR
        # Ensure kunnr is string
        results["kunnr"] = results["kunnr"].astype(str)
        results["cust_key"] = (
            self._config.DEFAULT_SALES_ORG
            + self._config.DEFAULT_CHANNEL
            + self._config.DEFAULT_DIVISION
            + results["kunnr"]
        )
        results["CustId"] = results["cust_key"].map(customer_map)

        results["kunnr_dm"] = results["kunnr_dm"].astype(str)
        results["cust_key2"] = (
            self._config.DEFAULT_SALES_ORG
            + self._config.DEFAULT_CHANNEL
            + self._config.DEFAULT_DIVISION
            + results["kunnr_dm"]
        )
        results["CustDMId"] = results["cust_key"].map(customer_map)

        # Rename columns, not necessary
        # results = results.rename(columns=self.COLUMN_MAPPING)

        # Select only required columns
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # Database Operations
        stmt_truncate = text(f"TRUNCATE TABLE {self._config.TABLE_CUSTOMER_DM}")
        stmt_insert: Insert = insert(customer_dm_table)

        with self._con_dw.begin() as conn:
            conn.execute(stmt_truncate)
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} customer DM records.")
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_customer_dm")
