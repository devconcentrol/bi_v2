import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Date,
    insert,
    update,
    bindparam,
    and_,
)
from base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class PurchDeliveryDateFactETL(BaseFactETL):
    """
    ETL for Purchase Delivery Date Fact.
    Inherits from BaseFactETL and uses vectorized operations for efficiency.
    """

    # Mapping: SAP Column (lowercase) -> Table Column
    COLUMN_MAPPING = {
        "company": "Company",
        "plant": "Plant",
        "purchid": "PurchId",
        "position": "Position",
        "VendId": "VendId",
        "MaterialId": "MaterialId",
        "fecha_confirmada": "ConfirmedDate",
        "fecha_contabilizacion": "PostedDate",
        "fecha_modificacion": "ModifiedDate",
        "loekz": "Deleted",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Purchase Delivery Date Fact...")
        config = self._config

        yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y%m%d")

        # 1. Extract Data from SAP
        # Extract changes since yesterday
        sql_get_purchase_orders = """
            SELECT BUKRS,
                   WERKS,
                   EBELN,
                   EBELP,
                   LIFNR,
                   MATNR,
                   FECHA_CONFIRMADA,
                   FECHA_CONTABILIZACION,
                   FECHA_MODIFICACION,
                   LOEKZ
            FROM SAPSR3.ZCON_V_FECHAS_ENTREGA_COMPRAS   
            WHERE FECHA_MODIFICACION = :yesterday                     
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_purchase_orders,
            con=self._con_sap,
            params={"yesterday": yesterday},
            dtype_backend="numpy_nullable",
        )

        if results.empty:
            Logger().info("No purchase order delivery date changes found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "purch_delivery_dates")
            return

        # 2. Extract Keys from Data Warehouse to handle Upsert
        dw_keys_query = f"SELECT Company, Plant, PurchId, Position FROM {config.TABLE_PURCH_DELV_DATE_FACT}"
        df_dw_keys = pd.read_sql(dw_keys_query, con=self._con_dw)
        df_dw_keys["exists_in_dw"] = True

        # 3. Transform Data
        results.columns = results.columns.str.lower()

        # Material Code cleaning
        results["matnr"] = results["matnr"].astype(str).str.replace("1000A", "MA")

        # Date transformations
        date_cols = ["fecha_confirmada", "fecha_contabilizacion", "fecha_modificacion"]
        for col in date_cols:
            results[col] = (
                pd.to_datetime(
                    results[col].astype(str), format="%Y%m%d", errors="coerce"
                )
                .dt.strftime("%Y%m%d")
                .convert_dtypes()
            )

        # Lookups
        material_map = self._lookup.get_material_map()
        vendor_map = self._lookup.get_vendor_map()

        results["MaterialId"] = results["matnr"].astype(str).map(material_map)
        results["VendId"] = results["lifnr"].astype(str).map(vendor_map)

        # Map SAP columns to DW column names (lowercase) for merging and insertion
        results = results.rename(
            columns={
                "bukrs": "company",
                "werks": "plant",
                "ebeln": "purchid",
                "ebelp": "position",
            }
        )

        # Merge with DW keys to separate Insert from Update
        # Prepare join keys in DW
        df_dw_keys.columns = df_dw_keys.columns.str.lower()
        # Join on company, plant, purchid, position
        results = results.merge(
            df_dw_keys, on=["company", "plant", "purchid", "position"], how="left"
        )
        results["exists_in_dw"] = results["exists_in_dw"].notna()

        # 4. Prepare Load operations
        metadata: MetaData = MetaData()
        purch_delivery_dates_table: Table = Table(
            config.TABLE_PURCH_DELV_DATE_FACT,
            metadata,
            Column("Company", String(10)),
            Column("Plant", String(10)),
            Column("PurchId", String(20)),
            Column("Position", String(10)),
            Column("VendId", Integer),
            Column("MaterialId", Integer),
            Column("ConfirmedDate", Date),
            Column("PostedDate", Date),
            Column("ModifiedDate", Date),
            Column("Deleted", String(1)),
        )

        # Inserts
        insert_df = results[~results["exists_in_dw"]].copy()
        insert_df = insert_df.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = (
            insert_df[final_cols]
            .where(pd.notnull(insert_df[final_cols]), None)
            .to_dict(orient="records")
        )

        # Updates
        update_df = results[results["exists_in_dw"]].copy()
        # We need to map to 'b_' prefixed parameters for the update statement
        update_records = []
        for _, row in update_df.iterrows():
            update_records.append(
                {
                    "b_Company": row["company"],
                    "b_Plant": row["plant"],
                    "b_PurchId": row["purchid"],
                    "b_Position": row["position"],
                    "b_ConfirmedDate": row["fecha_confirmada"]
                    if pd.notnull(row["fecha_confirmada"])
                    else None,
                    "b_PostedDate": row["fecha_contabilizacion"]
                    if pd.notnull(row["fecha_contabilizacion"])
                    else None,
                    "b_ModifiedDate": row["fecha_modificacion"]
                    if pd.notnull(row["fecha_modificacion"])
                    else None,
                    "b_Deleted": row["loekz"],
                }
            )

        # 5. Database operations
        stmt_insert = insert(purch_delivery_dates_table)
        stmt_update = (
            update(purch_delivery_dates_table)
            .where(
                and_(
                    purch_delivery_dates_table.c.Company == bindparam("b_Company"),
                    purch_delivery_dates_table.c.Plant == bindparam("b_Plant"),
                    purch_delivery_dates_table.c.PurchId == bindparam("b_PurchId"),
                    purch_delivery_dates_table.c.Position == bindparam("b_Position"),
                )
            )
            .values(
                ConfirmedDate=bindparam("b_ConfirmedDate"),
                PostedDate=bindparam("b_PostedDate"),
                ModifiedDate=bindparam("b_ModifiedDate"),
                Deleted=bindparam("b_Deleted"),
            )
        )

        with self._con_dw.begin() as conn:
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} new purchase delivery records."
                )
                conn.execute(stmt_insert, insert_records)

            if update_records:
                Logger().info(
                    f"Updating {len(update_records)} existing purchase delivery records."
                )
                conn.execute(stmt_update, update_records)

            self._update_etl_info(conn, "purch_delivery_dates")
