import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    insert,
    Insert,
    Date,
    DECIMAL,
    delete,
    bindparam,
)

from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL


class SampleDeliveryFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "vbeln": "DeliveryId",
        "lfart": "DeliveryType",
        "wadat": "GoodsIssueDate",
        "wadat_ist": "RealGoodsIssueDate",
        "wbstk": "Status",
        "ModifiedDate": "ModifiedDate",
        "CustId": "CustId",
        "MaterialId": "MaterialId",
        "vkorg": "SalesOrganization",
        "salesid": "SalesId",
        "lfimg": "Qty",
        "lsmeng": "QtyOrdered",
        "erdat": "CreatedDate",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Sample Delivery Fact...")

        config = self._config

        # Define table metadata
        metadata: MetaData = MetaData()
        sample_delivery_table: Table = Table(
            config.TABLE_SAMPLE_DELIVERY_FACT,
            metadata,
            Column("DeliveryId", String(50)),
            Column("DeliveryType", String(50)),
            Column("GoodsIssueDate", Date),
            Column("RealGoodsIssueDate", Date),
            Column("Status", String(10)),
            Column("ModifiedDate", Date),
            Column("CustId", Integer),
            Column("MaterialId", Integer),
            Column("SalesOrganization", String(10)),
            Column("SalesId", String(50)),
            Column("Qty", DECIMAL(15, 4)),
            Column("QtyOrdered", DECIMAL(15, 4)),
            Column("CreatedDate", Date),
        )

        # ---------------------------------------------------------
        # 1. Synchronization: Detect records deleted in SAP
        # ---------------------------------------------------------

        # Calculate dates in Python
        today = pd.Timestamp.now()
        # Start of current month, one year ago
        first_day_current = today.replace(day=1)
        start_date = first_day_current - pd.DateOffset(years=1)
        yesterday = today - pd.DateOffset(days=1)

        start_date_str = start_date.strftime("%Y-%m-%d")
        start_date_sap = start_date.strftime("%Y%m%d")
        yesterday_sap = yesterday.strftime("%Y%m%d")

        # Existing deliveries in DW (recent window)
        open_deliveries_query = f"""
            SELECT DISTINCT SalesId FROM {config.TABLE_SAMPLE_DELIVERY_FACT}
            WHERE CreatedDate >= '{start_date_str}'
        """
        df_existing = pd.read_sql(open_deliveries_query, con=self._con_dw)

        # Normalize columns
        df_existing.columns = df_existing.columns.str.lower()

        # All deliveries in SAP (same window)
        sql_all_deliveries = """
            SELECT DISTINCT SalesId
            FROM SAPSR3.ZCON_V_SAMPLE_ORDERS
            WHERE ERDAT > :cuttoff_date
              AND VKORG = '1000'
        """
        df_all = pd.read_sql(
            sql_all_deliveries,
            con=self._con_sap,
            params={"cuttoff_date": start_date_sap},
        )

        # Normalize columns
        df_all.columns = df_all.columns.str.lower()

        all_ids = set(df_all["salesid"])
        existing_ids = set(df_existing["salesid"])

        ids_to_delete = existing_ids.difference(all_ids)

        # ---------------------------------------------------------
        # 2. Delta Load: Fetch modified/new records from SAP
        # ---------------------------------------------------------
        sql_get_deliveries = """
            SELECT LFART, VBELN, MATNR, WADAT, WADAT_IST, WBSTK, KUNNR, VKORG, SPART, VTWEG, SALESID, LFIMG, AEDAT, ERDAT, LSMENG
            FROM SAPSR3.ZCON_V_SAMPLE_ORDERS
            WHERE VKORG = '1000' AND AEDAT = :yesterday
        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_deliveries,
            con=self._con_sap,
            params={"yesterday": yesterday_sap},
        )

        if results.empty and not ids_to_delete:
            Logger().info("No sample deliveries found to update or delete.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_sample_deliveries")
            return

        # Prepare for processing updates
        insert_records = []
        if not results.empty:
            # Normalize column names
            results.columns = results.columns.str.lower()

            # Add updated SalesIds to deletion list (to avoid duplicates)
            if "salesid" in results.columns:
                ids_to_delete.update(results["salesid"].astype(str).dropna().tolist())

            # Data Transformations
            results["wadat"] = pd.to_datetime(
                results["wadat"], format="%Y%m%d", errors="coerce"
            ).dt.date
            results["wadat_ist"] = pd.to_datetime(
                results["wadat_ist"], format="%Y%m%d", errors="coerce"
            ).dt.date
            results["erdat"] = pd.to_datetime(
                results["erdat"], format="%Y%m%d", errors="coerce"
            ).dt.date
            results["aedat"] = pd.to_datetime(
                results["aedat"], format="%Y%m%d", errors="coerce"
            ).dt.date

            results["lfimg"] = pd.to_numeric(results["lfimg"], errors="coerce").fillna(
                0
            )
            results["lsmeng"] = pd.to_numeric(
                results["lsmeng"], errors="coerce"
            ).fillna(0)

            # Lookups
            # Customer Lookup
            customer_map = self._lookup.get_customer_map()

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

            results["CustId"] = key1.map(customer_map)
            # Apply fallback logic
            results["CustId_Fallback"] = key2.map(customer_map)
            results["CustId"] = results["CustId"].fillna(results["CustId_Fallback"])

            # Material Lookup
            material_map = self._lookup.get_material_map()
            results["matnr"] = results["matnr"].astype(str)
            results["MaterialId"] = results["matnr"].map(material_map)

            # Calculated Fields
            results["wbstk"] = results["wbstk"].fillna("A")
            results["ModifiedDate"] = results["aedat"].where(
                results["aedat"].notna(), results["erdat"]
            )

            # Map Columns
            results = results.rename(columns=self.COLUMN_MAPPING)

            # Select final columns
            final_cols = list(self.COLUMN_MAPPING.values())
            # Ensure we only pick columns that exist (though they should all exist after rename/calculation)
            insert_df = results[final_cols].where(pd.notnull(results[final_cols]), None)
            insert_records = insert_df.to_dict(orient="records")

        # ---------------------------------------------------------
        # 3. Commit Changes
        # ---------------------------------------------------------

        # Prepare delete parameters
        delete_params = [{"b_SalesId": sid} for sid in ids_to_delete]

        stm_insert: Insert = insert(sample_delivery_table)
        stm_delete = delete(sample_delivery_table).where(
            sample_delivery_table.c.SalesId == bindparam("b_SalesId")
        )

        with self._con_dw.begin() as conn:
            if delete_params:
                Logger().info(f"Deleting {len(delete_params)} records.")
                conn.execute(stm_delete, delete_params)

            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} sample delivery records."
                )
                conn.execute(stm_insert, insert_records)

            self._update_etl_info(conn, "process_sample_deliveries")
