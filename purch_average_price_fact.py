import pandas as pd
from sqlalchemy import (
    DECIMAL,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    text,
)

from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL


class PurchAvgPriceFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "ekorg": "PurchOrganization",
        "werks": "Plant",
        "VendId": "VendId",
        "MaterialId": "MaterialId",
        "avg_purch_price": "AvgPrice",
        "avg_purch_price_vendor": "AvgPurchPrice",
        "bprme": "PriceUnit",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Purchase Average Price Fact...")
        config = self._config

        sql_get_avg_price = """
            SELECT EKORG,
                   WERKS,                               
                   LIFNR,
                   MATNR,
                   AVG_PURCH_PRICE,
                   AVG_PURCH_PRICE_VENDOR,
                   BPRME
            FROM SAPSR3.ZCON_V_VAR_PRECIO_COMPRA                           
        """
        results: pd.DataFrame = pd.read_sql(sql_get_avg_price, con=self._con_sap)

        if results.empty:
            Logger().info("No Purchase Average Price data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_purch_avg_prices")
            return

        results.columns = results.columns.str.lower()

        # Clean material code
        results["matnr"] = results["matnr"].astype(str).str.replace("1000A", "MA")
        results["lifnr"] = results["lifnr"].astype(str)

        material_map = self._lookup.get_material_map()
        vendor_map = self._lookup.get_vendor_map()

        results["MaterialId"] = results["matnr"].map(material_map)
        missing_materials = results[results["MaterialId"].isna()]["matnr"].unique()
        for mm in missing_materials:
            if mm.strip():
                Logger().warning(f"Material {mm} no dado de alta en el DW")

        results["VendId"] = results["lifnr"].map(vendor_map)
        missing_vendors = results[results["VendId"].isna()]["lifnr"].unique()
        for mv in missing_vendors:
            if mv.strip():
                Logger().warning(f"El proveedor {mv} no esta dado de alta en el DW")

        results = results.rename(columns=self.COLUMN_MAPPING)

        final_cols = list(self.COLUMN_MAPPING.values())
        final_results = results[final_cols].copy()

        metadata: MetaData = MetaData()

        purch_avg_price_table: Table = Table(
            config.TABLE_PURCH_AVG_PRICE_FACT,
            metadata,
            Column("PurchOrganization", String(10)),
            Column("Plant", String(10)),
            Column("VendId", Integer),
            Column("MaterialId", Integer),
            Column("AvgPrice", DECIMAL(15, 4)),
            Column("AvgPurchPrice", DECIMAL(15, 4)),
            Column("PriceUnit", String(10)),
        )

        insert_records = final_results.where(pd.notnull(final_results), None).to_dict(
            orient="records"
        )

        stmt_insert = insert(purch_avg_price_table)
        stmt_truncate = text(f"TRUNCATE TABLE {config.TABLE_PURCH_AVG_PRICE_FACT}")

        with self._con_dw.begin() as conn:
            conn.execute(stmt_truncate)
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} purchase average price records.")
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_purch_avg_prices")

