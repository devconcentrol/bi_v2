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
from base_fact_etl import BaseFactETL


class CustomerPriceFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "CustId": "CustId",
        "MaterialId": "MaterialId",
        "datab": "FromDate",
        "datbi": "ToDate",
        "ztar_kbetr": "SalesPrice",
        "ztar_konwa": "SalesPriceCurrency",
        "ztar_for_qty": "SalesPriceQuantity",
        "ztar_for_unit": "SalesPriceUnit",
        "ztra_kbetr": "TransportPrice",
        "ztra_konwa": "TransportPriceCurrency",
        "ztra_for_qty": "TransportPriceQuantity",
        "ztra_for_unit": "TransportPriceUnit",
        "precio_max": "MaxPrice",
        "precio_med": "MidPrice",
        "precio_min": "MinPrice",
        "cost_price": "CostPrice",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Customer Price Fact...")

        sql_get_customer_prices = """
                        SELECT KUNNR,                               
                               MATNR,
                               DATAB,
                               DATBI,
                               ZTAR_KBETR,
                               ZTAR_KONWA,
                               ZTAR_FOR_QTY,
                               ZTAR_FOR_UNIT,
                               ZTRA_KBETR,
                               ZTRA_KONWA,
                               ZTRA_FOR_QTY,
                               ZTRA_FOR_UNIT,
                               PRECIO_MAX,
                               PRECIO_MED,
                               PRECIO_MIN,
                               COST_PRICE
                        FROM SAPSR3.ZCON_V_CUSTOMER_PRICES                                                                                    
                        """
        results: pd.DataFrame = pd.read_sql(sql_get_customer_prices, con=self._con_sap, dtype_backend="numpy_nullable")

        if results.empty:
            Logger().info("No customer price data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_customer_prices")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # # Define the table
        metadata: MetaData = MetaData()

        customer_prices_table: Table = Table(
            self._config.TABLE_CUSTOMER_PRICE_FACT,
            metadata,
            Column("CustId", Integer),
            Column("MaterialId", Integer),
            Column("FromDate", Date),
            Column("ToDate", Date),
            Column("SalesPrice", DECIMAL(15, 4)),
            Column("SalesPriceCurrency", String(5)),
            Column("SalesPriceQuantity", DECIMAL(15, 4)),
            Column("SalesPriceUnit", String(5)),
            Column("TransportPrice", DECIMAL(15, 4)),
            Column("TransportPriceCurrency", String(5)),
            Column("TransportPriceQuantity", DECIMAL(15, 4)),
            Column("TransportPriceUnit", String(5)),
            Column("MaxPrice", DECIMAL(15, 4)),
            Column("MidPrice", DECIMAL(15, 4)),
            Column("MinPrice", DECIMAL(15, 4)),
            Column("CostPrice", DECIMAL(15, 4)),
        )

        # Convert date columns
        # "00000000" will fail format match and become NaT with errors='coerce'
        results["datab"] = pd.to_datetime(
            results["datab"], format="%Y%m%d", errors="coerce"
        ).dt.date
        results["datbi"] = pd.to_datetime(
            results["datbi"], format="%Y%m%d", errors="coerce"
        ).dt.date

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

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
            Logger().info("No valid customer price data after lookups.")
            return

        # Rename columns
        results = results.rename(columns=self.COLUMN_MAPPING)

        # Select only required columns
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = (
            results[final_cols].where(pd.notnull(results), None).to_dict(orient="records")
        )

        # Database Operations
        stmt_truncate = text(f"TRUNCATE TABLE {self._config.TABLE_CUSTOMER_PRICE_FACT}")
        stmt_insert: Insert = insert(customer_prices_table)

        with self._con_dw.begin() as conn:
            conn.execute(stmt_truncate)
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} customer price records.")
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_customer_prices")
