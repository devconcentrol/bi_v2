import pandas as pd
import numpy as np
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    insert,
    text,
    Insert,
)

from utils.error_handler import error_handler
from utils.logger import Logger
from etl.base_fact_etl import BaseFactETL


class VendorAssesmentFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "VendId": "VendId",
        "AssesmentType": "AssesmentType",
        "AssesmentValue": "AssesmentValue",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Vendor Assessment Fact...")
        config = self._config

        PROV_CAT: list = ["08", "17", "43", "25"]

        vendor_query = """SELECT VendCode, VendId, CountryCode, RegionCode, isUE 
                          FROM VendorDim V
                          LEFT JOIN COUNTRYDIM c ON C.CountryId = V.CountryId 
                          LEFT JOIN RegionDim rd ON V.REGIONiD = rd.RegionId 
                      """
        df_vend = pd.read_sql(vendor_query, con=self._con_dw)

        vendor_in_delivery_query = (
            f"SELECT DISTINCT VendId FROM {config.TABLE_PURCH_DELV_DATE_FACT}"
        )
        df_vend_in_delivery = pd.read_sql(vendor_in_delivery_query, con=self._con_dw)

        year_ausvn = pd.Timestamp.now().year - 1

        sql_get_iso = """
                        SELECT LIFNR,
                               CASE 
                                 WHEN DOKAR = 'Z02' THEN 'ISO 14001'
                                 WHEN DOKAR = 'Z05' THEN 'ISO 9001'
                               END AS ASSESMENT_TYPE,
                               CASE 
                                 WHEN DOKAR = 'Z02' THEN 5
                                 WHEN DOKAR = 'Z05' THEN 10
                               END AS ASSESMENT_VALUE                                                               
                        FROM SAPSR3.ZCON_V_AV_PROV_ISO                           
                        """
        sql_get_inc = """
                        SELECT LIFNR,
                               'INC' AS ASSESMENT_TYPE,
                               (NUM_INC * -5) AS ASSESMENT_VALUE
                        FROM SAPSR3.ZCON_V_AV_PROV_INCIDENCIAS
                        WHERE YEAR_AUSVN = :year_ausvn
        """

        iso_results: pd.DataFrame = pd.read_sql(sql_get_iso, con=self._con_sap)
        inc_results: pd.DataFrame = pd.read_sql(
            sql_get_inc, con=self._con_sap, params={"year_ausvn": year_ausvn}
        )

        results: pd.DataFrame = pd.concat([iso_results, inc_results])

        if results.empty:
            Logger().info("No Vendor Assessment data found.")

        results.columns = results.columns.str.lower()

        vendor_map = self._lookup.get_vendor_map()
        results["VendId"] = results["lifnr"].astype(str).map(vendor_map)

        missing_vendors = results[results["VendId"].isna()]["lifnr"].unique()
        for mv in missing_vendors:
            Logger().warning(f"El proveedor {mv} no esta dado de alta en el DW")

        results = results.rename(
            columns={
                "assesment_type": "AssesmentType",
                "assesment_value": "AssesmentValue",
            }
        )

        # The ISO and INC part
        df_tmp = results[["VendId", "AssesmentType", "AssesmentValue"]].copy()

        # Carbon calculation
        vendor_list: list = (
            df_vend_in_delivery["VendId"].tolist() + df_tmp["VendId"].dropna().tolist()
        )
        unique_vendor_list = list(set([v for v in vendor_list if pd.notna(v)]))

        unique_vendors_df = pd.DataFrame({"VendId": unique_vendor_list})

        # Merge with df_vend to get RegionCode, CountryCode, isUE
        carbon_df = unique_vendors_df.merge(df_vend, on="VendId", how="left")

        conditions = [
            carbon_df["RegionCode"].isin(PROV_CAT),
            carbon_df["CountryCode"] == "ES",
            carbon_df["isUE"] == 1,
        ]
        choices = [5, 3, 2]  # Valoraciones

        carbon_df["AssesmentValue"] = np.select(conditions, choices, default=1)
        carbon_df["AssesmentType"] = "PETJADA CARBONI"

        carbon_results = carbon_df[["VendId", "AssesmentType", "AssesmentValue"]]

        final_results = pd.concat([df_tmp, carbon_results], ignore_index=True)
        final_results = final_results.dropna(subset=["VendId"])
        final_results["VendId"] = final_results["VendId"].astype(int)

        metadata: MetaData = MetaData()

        # Define Table
        vendor_assesment_table: Table = Table(
            config.TABLE_VENDOR_ASSESMENT_FACT,
            metadata,
            Column("VendId", Integer),
            Column("AssesmentType", String(10)),
            Column("AssesmentValue", Integer),
        )

        insert_records = final_results.where(pd.notnull(final_results), None).to_dict(
            orient="records"
        )

        stmt_insert_assesment: Insert = insert(vendor_assesment_table)
        stmt_truncate = text(f"TRUNCATE TABLE {config.TABLE_VENDOR_ASSESMENT_FACT}")

        with self._con_dw.begin() as conn:
            conn.execute(stmt_truncate)
            if len(insert_records) > 0:
                Logger().info(
                    f"Inserting {len(insert_records)} vendor assessment records."
                )
                conn.execute(stmt_insert_assesment, insert_records)

            self._update_etl_info(conn, "vendor_assesment")
