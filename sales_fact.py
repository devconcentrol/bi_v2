from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
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
    REAL,
    Date,
    DECIMAL,
)
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config


class SalesFactETL:
    COLUMN_MAPPING = {
        "vkorg": "Organization",
        "vbtyp": "InvoiceType",
        "PayerId": "PayerId",
        "MaterialId": "MaterialId",
        "prdha": "Hierarchy",
        "fkdat": "InvoiceDate",
        "fkimg": "SalesQuantity",
        "vrkme": "SalesUnit",
        "ntgew": "NetWeight",
        "gewei": "WeightUnit",
        "sales_volume": "SalesVolume",
        "net_sales_cost": "NetSalesCost",
        "net_profit_margin": "NetProfitMargin",
        "vbeln": "InvoiceId",
        "vtweg": "Channel",
        "spart": "Division",
        "stcur": "ConRate",
        "waerk": "Currency",
        "erdat": "CreatedDate",
        "NetRealSalesCost": "NetRealSalesCost",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Processing Sales Fact...")
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

        stmt_update_etl = text(
            f"""UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_sales'"""
        )

        sql_get_sales = """
            SELECT
                ZVF.VKORG,
                ZVF.VBTYP,
                ZVF.KUNRG,
                ZVF.MATNR,
                M.PRDHA,
                ZVF.FKDAT,
                CASE
                    WHEN ZVF.VBTYP IN ('M', 'P', '5', 'S') THEN ZVF.FKIMG
                    ELSE ZVF.FKIMG * -1
                END AS FKIMG,
                VRKME,
                CASE
                    WHEN ZVF.VBTYP IN ('M', 'P', '5', 'S') THEN ZVF.NTGEW
                    ELSE ZVF.NTGEW * -1
                END AS NTGEW,
                ZVF.GEWEI,
                CASE
                    WHEN ZVF.VBTYP IN ('M', 'P', '5', 'S') THEN ZVF.NETWR
                    ELSE ZVF.NETWR * -1
                END AS SALES_VOLUME,
                CASE
                    WHEN ZVF.VBTYP IN ('M', 'P', '5', 'S') THEN ZVF.WAVWR
                    ELSE ZVF.WAVWR * -1
                END AS NET_SALES_COST,
                CASE
                    WHEN ZVF.VBTYP IN ('M', 'P', '5', 'S') THEN ZVF.NETWR - ZVF.WAVWR
                    ELSE (ZVF.NETWR * -1) - (ZVF.WAVWR * -1)
                END AS NET_PROFIT_MARGIN,
                ZVF.VBELN,
                ZVF.VTWEG,
                ZVF.SPART,
                ZVF.STCUR,
                ZVF.WAERK,
                ZVF.ERDAT,
                ZVF.VBELV,
                ZVF.AUART
            FROM
                SAPSR3.ZCON_V_FACTURACION ZVF
            INNER JOIN SAPSR3.MARA M ON
                ZVF.MATNR = M.MATNR
                AND M.MANDT = '500'
            WHERE
                ZVF.VBTYP <> 'U'
                AND ERDAT = :yesterday
        """

        results: pd.DataFrame = pd.read_sql(sql_get_sales, con=self._con_sap,params={"yesterday": yesterday}    )

        if results.empty:
            Logger().info("No sales data found.")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_update_etl)
            return

        # Define the table
        metadata: MetaData = MetaData()

        sales_table: Table = Table(
            self._config.TABLE_SALES_FACT,
            metadata,
            Column("Organization", String(200)),
            Column("InvoiceType", String(200)),
            Column("PayerId", Integer),
            Column("MaterialId", Integer),
            Column("Hierarchy", String(200)),
            Column("InvoiceDate", Date),
            Column("SalesQuantity", DECIMAL(15, 4)),
            Column("SalesUnit", String(200)),
            Column("NetWeight", DECIMAL(15, 4)),
            Column("WeightUnit", String(200)),
            Column("SalesVolume", DECIMAL(15, 4)),
            Column("NetSalesCost", DECIMAL(15, 4)),
            Column("NetProfitMargin", DECIMAL(15, 4)),
            Column("InvoiceId", String(10)),
            Column("Channel", String(10)),
            Column("Division", String(10)),
            Column("ConRate", REAL),
            Column("Currency", String(10)),
            Column("CreatedDate", Date),
            Column("NetRealSalesCost", DECIMAL(15, 4)),
        )

        # Normalize columns
        results.columns = results.columns.str.lower()

        # Vectorized transformations
        
        # Matnr replacement
        results["matnr"] = results["matnr"].str.replace("1000A", "MA")
        
        # Date parsing
        results["fkdat"] = pd.to_datetime(results["fkdat"], format="%Y%m%d", errors="coerce").dt.date
        results["erdat"] = pd.to_datetime(results["erdat"], format="%Y%m%d", errors="coerce").dt.date

        # Logic for ECONOMIC_SALES_TYPE (ntgew = 0 if auart == ECONOMIC_SALES_TYPE)
        results["ntgew"] = np.where(
            results["auart"] == self._config.ECONOMIC_SALES_TYPE,
            0,
            results["ntgew"],
        )

        # Lookups
        customer_map = self._lookup.get_customer_map()
        material_map = self._lookup.get_material_map()

        # Material ID
        results["MaterialId"] = results["matnr"].map(material_map)
        
        # Customer ID Logic (with fallback)
        results["vtweg_str"] = results["vtweg"].astype(str)
        results["vkorg_str"] = results["vkorg"].astype(str)
        results["spart_str"] = results["spart"].astype(str)
        results["kunrg_str"] = results["kunrg"].astype(str)

        key1 = results["vkorg_str"] + results["vtweg_str"] + results["spart_str"] + results["kunrg_str"]
        key2 = results["vkorg_str"] + self._config.DEFAULT_CHANNEL + results["spart_str"] + results["kunrg_str"]

        results["PayerId"] = key1.map(customer_map)
        results["PayerId_Fallback"] = key2.map(customer_map)
        
        results["PayerId"] = results["PayerId"].fillna(results["PayerId_Fallback"])
        
        # Drop helper columns
        results.drop(columns=["vtweg_str", "vkorg_str", "spart_str", "kunrg_str", "PayerId_Fallback"], inplace=True)

        # Rename columns to match Table definition (using mapping)
        results = results.rename(columns=self.COLUMN_MAPPING)

        # Add NetRealSalesCost as None if not present (it is in table def)
        if "NetRealSalesCost" not in results.columns:
             results["NetRealSalesCost"] = None

        # Filter only columns present in the table definition
        final_cols = {
            db_col
            for df_col,db_col in self.COLUMN_MAPPING.items()            
        } 
        
        # Ensure we only have valid columns
        results = results[list(final_cols)]
        
        # replace NaN with None for SQL insertion
        results = results.astype(object).where(pd.notnull(results), None)

        converted_results = results.to_dict(orient="records")

        stm_insert_sales: Insert = insert(sales_table)

        with self._con_dw.begin() as conn:
            if len(converted_results) > 0:
                Logger().info(f"Inserting {len(converted_results)} sales records.")
                conn.execute(stm_insert_sales, converted_results)

            conn.execute(stmt_update_etl)
