import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    insert,
    REAL,
    Date,
    delete,
    bindparam,
)

from utils.error_handler import error_handler
from utils.logger import Logger
from etl.base_fact_etl import BaseFactETL


class SalesOrderHistFactETL(BaseFactETL):
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
    }

    @error_handler
    def run(self):
        Logger().info("Processing Sales Order Historic Fact...")
        config = self._config
        yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y%m%d")

        # 1. Fetch from SAP
        sql_get_historic = """
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
                   SPART
            FROM SAPSR3.ZCON_V_SALES_ORDER_DELAY                                                   
            WHERE VKORG = 1000                             
              AND AEDAT = :yesterday
        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_historic, con=self._con_sap, params={"yesterday": yesterday}
        )

        if results.empty:
            Logger().info("No Historic Sales Orders found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_sales_orders")
            return

        results.columns = results.columns.str.lower()

        # 2. Extract Keys from Data Warehouse to handle Delete before Insert
        sales_orders_query = (
            f"SELECT DISTINCT SalesId FROM {config.TABLE_SALES_ORDER_FACT}"
        )
        df_existing_orders = pd.read_sql(sales_orders_query, con=self._con_dw)

        # Parse Dates
        date_cols = [
            "delivery_date",
            "loading_date",
            "goods_issue_date",
            "initial_delivery_date",
            "initial_loading_date",
            "initial_goods_issue_date",
        ]
        for col in date_cols:
            results[col] = pd.to_datetime(
                results[col].astype(str),
                format="%Y%m%d",
                errors="coerce",
            ).dt.strftime("%Y-%m-%d")

        results["matnr"] = results["matnr"].astype(str)

        material_map = self._lookup.get_material_map()
        results["MaterialId"] = results["matnr"].map(material_map)
        missing_materials = results[results["MaterialId"].isna()]["matnr"].unique()
        for mm in missing_materials:
            if mm.strip():
                Logger().warning(f"Material no encontrado en DW: {mm}")

        # Customer logic
        results["kunnr"] = results["kunnr"].astype(str)
        results["vkorg"] = results["vkorg"].astype(str)
        results["vtweg"] = results["vtweg"].astype(str)
        results["spart"] = results["spart"].astype(str)

        customer_map = self._lookup.get_customer_map()

        direct_keys = (
            results["vkorg"] + results["vtweg"] + results["spart"] + results["kunnr"]
        )
        fallback_keys = (
            results["vkorg"]
            + config.DEFAULT_CHANNEL
            + results["spart"]
            + results["kunnr"]
        )

        results["CustId"] = direct_keys.map(customer_map)
        results["CustId"] = results["CustId"].fillna(fallback_keys.map(customer_map))

        results["CustId"] = results["CustId"].astype("Int64")
        results["MaterialId"] = results["MaterialId"].astype("Int64")

        # Rename columns to map to DB
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        final_results = results[final_cols].copy()

        metadata: MetaData = MetaData()
        sales_orders_table: Table = Table(
            config.TABLE_SALES_ORDER_FACT,
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
        )

        # Merge with existing orders to find which ones to delete
        sales_to_delete = final_results.merge(
            df_existing_orders, on="SalesId", how="inner"
        )[["SalesId"]].drop_duplicates()

        delete_records = sales_to_delete.rename(
            columns={"SalesId": "b_SalesId"}
        ).to_dict(orient="records")

        insert_records = final_results.where(pd.notnull(final_results), None).to_dict(
            orient="records"
        )

        stmt_delete = delete(sales_orders_table).where(
            sales_orders_table.c.SalesId == bindparam("b_SalesId")
        )
        stmt_insert = insert(sales_orders_table)

        with self._con_dw.begin() as conn:
            if delete_records:
                conn.execute(stmt_delete, delete_records)
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} historic sales order records."
                )
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_sales_orders")
