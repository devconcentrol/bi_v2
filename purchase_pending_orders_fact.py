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
    text,
    Insert,
)

from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL


class PurchasePendingOrdersFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "ordertype": "OrderType",
        "ordernumber": "OrderNumber",
        "purchaseorderline": "PurchaseOrderLine",
        "orderquantity": "OrderQuantity",
        "schedulelinenumber": "ScheduleLineNumber",
        "pendingdate": "PendingDate",
        "schedulelinequantity": "ScheduleLineQuantity",
        "pendingquantity": "PendingQuantity",
        "reservationnumber": "ReservationNumber",
        "MaterialId": "MaterialId",
        "VendId": "VendId",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Purchase Pending Orders Fact...")
        config = self._config

        # 1. Fetch Data from SAP
        sql_get_prod_orders: str = """
            SELECT OrderType,
                   OrderNumber, 
                   PurchaseOrderLine,
                   MATNR,                                        
                   OrderQuantity,
                   ScheduleLineNumber, 
                   PendingDate, 
                   ScheduleLineQuantity, 
                   PendingQuantity,
                   ReservationNumber,
                   LIFNR
            FROM SAPSR3.ZCON_V_RESB_OF
            """

        sql_get_purchase_orders: str = """
            WITH R AS
                (
                    SELECT ROW_NUMBER() OVER(PARTITION BY EBELN, EBELP ORDER BY ETENR DESC) AS NROW, EBELN, EBELP, ETENR, EINDT, MANDT, MENGE 
                    FROM SAPSR3.EKET
                ),
            P1 AS
                (
                    SELECT EBELN, EBELP, MANDT, BWART, 
                    CASE 
                    	WHEN BWART IN ('101','162') THEN MENGE
                    	ELSE -MENGE
                    END AS MENGE 
                    FROM SAPSR3.EKBE 
                    WHERE BEWTP = 'E' AND BWART IN ('101','102', '161','162') 
                ),
            P AS (
            	SELECT EBELN, EBELP,MANDT,SUM(MENGE) AS MENGE
            	FROM P1            	  
            	GROUP BY EBELN, EBELP, MANDT
            ),
            K AS
                (
                    SELECT LIFNR, EBELN, MANDT, BSART FROM SAPSR3.EKKO
                )
            SELECT 'ORDRE DE COMPRA' AS OrderType,
                   DOC.EBELN as OrderNumber,
                   DOC.EBELP as PurchaseOrderLine, 
                CASE
                    WHEN DOC.MATNR LIKE '0%' THEN CAST(CAST(DOC.MATNR AS BIGINT) AS VARCHAR(18))
                    ELSE DOC.MATNR
                END AS MATNR, 
                DOC.MENGE AS OrderQuantity,
                R.ETENR as ScheduleLineNumber,
                R.EINDT as PendingDate, 
                R.MENGE as ScheduleLineQuantity, 
                P.MENGE AS PendingQuantity,
                NULL as ReservationNumber,
                K.LIFNR
            FROM SAPSR3.EKPO DOC
                LEFT OUTER JOIN R ON DOC.EBELN = R.EBELN AND DOC.EBELP = R.EBELP AND DOC.MANDT = R.MANDT AND R.NROW = 1
                LEFT OUTER JOIN P ON DOC.EBELN = P.EBELN AND DOC.EBELP = P.EBELP AND DOC.MANDT = P.MANDT 
                LEFT OUTER JOIN K ON DOC.EBELN = K.EBELN AND DOC.MANDT = K.MANDT
            WHERE DOC.MANDT = '500' AND R.MENGE > IFNULL(ABS(P.MENGE),0) AND DOC.LOEKZ <> 'L' AND DOC.ELIKZ <> 'X'
        """

        Logger().info("Fetching production orders from SAP...")
        prod_order_results: pd.DataFrame = pd.read_sql(
            sql_get_prod_orders, con=self._con_sap, dtype_backend="numpy_nullable"
        )

        Logger().info("Fetching purchase orders from SAP...")
        purchase_order_results: pd.DataFrame = pd.read_sql(
            sql_get_purchase_orders, con=self._con_sap, dtype_backend="numpy_nullable"
        )

        # 2. Normalize and Transform
        prod_order_results.columns = prod_order_results.columns.str.lower()
        purchase_order_results.columns = purchase_order_results.columns.str.lower()

        results = pd.concat([prod_order_results, purchase_order_results])

        if results.empty:
            Logger().info("No pending orders found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_pending_orders")
            return

        # Material Code cleaning
        results["matnr"] = results["matnr"].astype(str).str.replace("1000A", "MA")

        # Date conversion
        results["pendingdate"] = pd.to_datetime(
            results["pendingdate"], format="%Y%m%d", errors="coerce"
        ).convert_dtypes()

        # Numeric fields
        numeric_cols = ["orderquantity", "schedulelinequantity", "pendingquantity"]
        for col in numeric_cols:
            results[col] = pd.to_numeric(results[col], errors="coerce").fillna(0)

        # 3. Lookups
        material_map = self._lookup.get_material_map()
        vendor_map = self._lookup.get_vendor_map()

        results["MaterialId"] = results["matnr"].map(material_map)
        results["VendId"] = (
            results["lifnr"]
            .astype(str)
            .str.lstrip("0")
            .map(vendor_map)
            .convert_dtypes()
        )

        # 4. Map and Select Columns
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        # Ensure only columns in mapping are included
        insert_df = results[final_cols].copy()

        # Replace NaNs with None for SQL
        insert_records = insert_df.where(pd.notnull(insert_df), None).to_dict(
            orient="records"
        )

        # 5. Define Table Schema
        metadata: MetaData = MetaData()
        pending_orders_table: Table = Table(
            config.TABLE_PENDING_ORDERS_FACT,
            metadata,
            Column("MaterialId", Integer),
            Column("OrderType", String(50)),
            Column("OrderNumber", String(25)),
            Column("OrderQuantity", DECIMAL(15, 4)),
            Column("PendingQuantity", DECIMAL(15, 4)),
            Column("PendingDate", Date),
            Column("PurchaseOrderLine", String(10)),
            Column("ScheduleLineNumber", String(10)),
            Column("ScheduleLineQuantity", DECIMAL(15, 4)),
            Column("ReservationNumber", String(20)),
            Column("VendId", Integer),
        )

        # 6. Database Operations
        stmt_truncate = text(f"TRUNCATE TABLE {config.TABLE_PENDING_ORDERS_FACT}")
        stmt_insert: Insert = insert(pending_orders_table)

        with self._con_dw.begin() as conn:
            Logger().info(f"Truncating {config.TABLE_PENDING_ORDERS_FACT}...")
            conn.execute(stmt_truncate)

            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} pending order records.")
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_pending_orders")
