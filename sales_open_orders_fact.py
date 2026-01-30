import pandas as pd
from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    REAL,
    DECIMAL,
    Date,
    insert,
    text,
    Insert,
)


class SalesOpenOrdersFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "vbeln": "SalesId",
        "CustId": "CustId",
        "MaterialId": "MaterialId",
        "delivery_date": "DeliveryDate",
        "loading_date": "LoadingDate",
        "goods_issue_date": "GoodsIssueDate",
        "initial_delivery_date": "InitialDeliveryDate",
        "initial_loading_date": "InitialLoadingDate",
        "initial_goods_issue_date": "InitialGoodsIssueDate",
        "qty": "QtyOrdered",
        "conf_qty": "QtyConfirmed",
        "meins": "UnitId",
        "netwr": "LineAmount",
        "waerk": "Currency",
        "audat": "CreatedDate",
        "auart": "SalesType",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Sales Open Orders Fact...")

        # 1. Fetch New Data from SAP
        Logger().info("Fetching new data from SAP...")
        sql_get_open_orders = """
                        SELECT KUNNR,
                               VBELN,
                               MATNR,
                               DELIVERY_DATE,
                               LOADING_DATE,
                               GOODS_ISSUE_DATE,
                               INITIAL_DELIVERY_DATE,
                               INITIAL_LOADING_DATE,
                               INITIAL_GOODS_ISSUE_DATE,
                               QTY,
                               CONF_QTY,
                               MEINS,
                               VKORG,
                               VTWEG,
                               SPART,
                               NETWR,
                               WAERK,
                               AUDAT,
                               AUART
                        FROM SAPSR3.ZCON_V_OPEN_SALES_ORDERS                                                    
                        WHERE VKORG = 1000                        
                        """
        results: pd.DataFrame = pd.read_sql(sql_get_open_orders, con=self._con_sap)

        if results.empty:
            Logger().info("No open orders data found in SAP.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_open_orders")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # 4. Data Transformation
        Logger().info("Transforming data...")

        # Date Parsing
        date_cols = [
            "delivery_date",
            "loading_date",
            "goods_issue_date",
            "initial_delivery_date",
            "initial_loading_date",
            "initial_goods_issue_date",
            "audat",
        ]

        for col in date_cols:
            if col in results.columns:
                # SAP dates are YYYYMMDD. "00000000" or invalid becomes NaT.
                results[col] = pd.to_datetime(
                    results[col], format="%Y%m%d", errors="coerce"
                ).dt.date

        # Prepare Keys for Lookups
        # Ensure string types for key components
        key_cols = ["vkorg", "vtweg", "spart", "kunnr", "matnr"]
        for col in key_cols:
            if col in results.columns:
                results[col] = results[col].fillna("").astype(str)

        # Lookup Material
        material_map = self._lookup.get_material_map()
        results["MaterialId"] = results["matnr"].map(material_map)

        # Lookup Customer
        # Primary Lookup: vkorg + vtweg + spart + kunnr
        customer_map = self._lookup.get_customer_map()

        key1 = results["vkorg"] + results["vtweg"] + results["spart"] + results["kunnr"]
        key2 = (
            results["vkorg"]
            + self._config.DEFAULT_CHANNEL
            + results["spart"]
            + results["kunnr"]
        )

        results["CustId"] = key1.map(customer_map)
        results["CustId"] = results["CustId"].fillna(key2.map(customer_map))

        # Handle numeric columns (sometimes read as object if mixed)
        results["qty"] = pd.to_numeric(results["qty"], errors="coerce").fillna(0)
        results["conf_qty"] = pd.to_numeric(
            results["conf_qty"], errors="coerce"
        ).fillna(0)
        results["netwr"] = pd.to_numeric(results["netwr"], errors="coerce").fillna(0)

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
            self._config.TABLE_SALES_OPEN_ORDERS_FACT,
            metadata,
            Column("SalesId", String(45)),
            Column("CustId", Integer),
            Column("MaterialId", Integer),
            Column("DeliveryDate", Date),
            Column("LoadingDate", Date),
            Column("GoodsIssueDate", Date),
            Column("InitialDeliveryDate", Date),
            Column("InitialLoadingDate", Date),
            Column("InitialGoodsIssueDate", Date),
            Column("QtyOrdered", REAL),
            Column("QtyConfirmed", REAL),
            Column("UnitId", String(10)),
            Column("LineAmount", DECIMAL(15, 4)),
            Column("Currency", String(10)),
            Column("CreatedDate", Date),
            Column("SalesType", String(10)),
        )

        # 6. Database Operations (Backup -> Truncate -> Insert)
        table_fields: str = ",".join(self.COLUMN_MAPPING.values())
        stmt_backup = text(
            f"INSERT INTO {self._config.TABLE_SALES_OPEN_ORDERS_FACT_HIST} ({table_fields}) SELECT {table_fields} FROM {self._config.TABLE_SALES_OPEN_ORDERS_FACT}"
        )
        stmt_truncate = text(
            f"TRUNCATE TABLE {self._config.TABLE_SALES_OPEN_ORDERS_FACT}"
        )
        stmt_insert: Insert = insert(target_table)

        with self._con_dw.begin() as conn:
            # 1. Backup
            Logger().info("Backing up current open orders to history...")
            conn.execute(stmt_backup)

            # 2. Truncate
            Logger().info("Truncating current table...")
            conn.execute(stmt_truncate)

            # 3. Insert
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} open order records.")
                conn.execute(stmt_insert, insert_records)

            # 4. Update ETL Info
            self._update_etl_info(conn, "process_open_orders")
