import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    insert,
    text,
    Insert,
    Date,
    DECIMAL,
)
from utils.error_handler import error_handler
from utils.logger import Logger
from etl.base_fact_etl import BaseFactETL


class LastSalesPriceFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "CustId": "CustId",
        "MaterialId": "MaterialId",
        "kbetr": "SalesPrice",
        "kpein": "Per",
        "kmein": "UnitId",
        "waers": "Currency",
        "kdatu": "PriceDate",
        "vbeln": "SalesId",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Last Sales Price Fact...")

        sql_get_customer_prices = """
                        SELECT KUNNR,                               
                               MATNR,
                               KBETR,
                               KPEIN,
                               KMEIN,
                               WAERS,
                               KDATU,
                               VBELN,
                               VKORG,
                               VTWEG,
                               SPART
                        FROM SAPSR3.ZCON_V_LAST_PRICE
                        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_customer_prices, con=self._con_sap, dtype_backend="numpy_nullable"
        )

        if results.empty:
            Logger().info("No last sales price data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_last_sales_prices")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # # Define the table
        metadata: MetaData = MetaData()

        last_sales_prices_table: Table = Table(
            self._config.TABLE_LAST_SALES_PRICE_FACT,
            metadata,
            Column("CustId", Integer),
            Column("MaterialId", Integer),
            Column("SalesPrice", DECIMAL(15, 4)),
            Column("Per", Integer),
            Column("UnitId", String(5)),
            Column("Currency", String(5)),
            Column("PriceDate", Date),
            Column("SalesId", String(20)),
        )

        results["kdatu"] = pd.to_datetime(
            results["kdatu"], format="%Y%m%d", errors="coerce"
        ).dt.date

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Lookup CustId
        customer_map = self._lookup.get_customer_map()
        # Ensure kunnr is string
        results["kunnr"] = results["kunnr"].astype(str)

        results["CustKey"] = (
            results["vkorg"].astype(str)
            + results["vtweg"].astype(str)
            + results["spart"].astype(str)
            + results["kunnr"].astype(str)
        )
        results["CustId"] = results["CustKey"].map(customer_map)

        results["CustKey2"] = (
            self._config.DEFAULT_SALES_ORG
            + self._config.DEFAULT_CHANNEL
            + self._config.DEFAULT_DIVISION
            + results["kunnr"].astype(str)
        )

        results["CustId_Fallback"] = results["CustKey2"].map(customer_map)
        results["CustId"] = results["CustId"].fillna(results["CustId_Fallback"])

        # Log warning for missing IDs
        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
            Logger().warning(
                f"Dropped {len(missing_materials)} rows due to missing MaterialId."
            )

        missing_customers = results[results["CustId"].isna()]
        if not missing_customers.empty:
            Logger().warning(
                f"Dropped {len(missing_customers)} rows due to missing CustId."
            )

        # Filter out rows where either ID is missing
        # If we want strict integrity.
        results = results.dropna(subset=["MaterialId", "CustId"])

        if results.empty:
            Logger().info("No valid last sales price data after lookups.")
            return

        # Rename columns
        results = results.rename(columns=self.COLUMN_MAPPING)

        # Select only required columns
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # Database Operations
        stmt_truncate = text(
            f"TRUNCATE TABLE {self._config.TABLE_LAST_SALES_PRICE_FACT}"
        )
        stmt_insert: Insert = insert(last_sales_prices_table)

        with self._con_dw.begin() as conn:
            conn.execute(stmt_truncate)
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} last sales price records."
                )
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_last_sales_prices")
