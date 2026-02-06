import pandas as pd
from utils.error_handler import error_handler
from utils.logger import Logger
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    DECIMAL,
    Insert,
    text,
    Integer,
    Date,
    insert,
)
from base_fact_etl import BaseFactETL


class MaterialRealPriceFactETL(BaseFactETL):
    # Rename and select columns to match EWMTaskFact
    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "bwkey": "PlantId",
        "MaterialId": "MaterialId",
        "curtp": "CurrencyType",
        "pvprs": "RealPrice",
        "peinh": "PriceQuantity",
        "period": "PriceDate",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Material Real Price...")

        # Get the cutoff date
        today = pd.Timestamp.now()
        first_day_current = today.replace(day=1)
        cutoff_date = first_day_current - pd.DateOffset(months=3)
        cutoff_date_sap = cutoff_date.strftime("%Y%m%d")

        sql_get_material_real_price = """
                            SELECT bwkey,
                                matnr,
                                curtp,
                                pvprs,
                                peinh,
                                period
                            FROM SAPSR3.ZCON_V_MATERIAL_REAL_PRICE
                            WHERE period >= :cutoff_date_sap
                        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_material_real_price,
            con=self._con_sap,
            params={"cutoff_date_sap": cutoff_date_sap},
        )

        if results.empty:
            Logger().info("No material real price data found in SAP.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_material_real_price")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # 2. Data Transformation
        Logger().info("Transforming data...")

        # Matnr replacement
        results["matnr"] = results["matnr"].str.replace("1000A", "MA")

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].fillna("").astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Filter out missing materials (was printed in loop before)
        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
            Logger().warning(
                f"Dropped {len(missing_materials)} rows due to missing MaterialId."
            )
        results = results.dropna(subset=["MaterialId"])

        # Date parsing
        Logger().info("Parsing period to date...")
        results["period"] = pd.to_datetime(
            results["period"], format="%Y%m%d", errors="coerce"
        ).dt.date

        metadata = MetaData()
        material_real_price_table = Table(
            self._config.TABLE_MATERIAL_REAL_PRICE_FACT,
            metadata,
            Column("PlantId", Integer),
            Column("MaterialId", Integer),
            Column("CurrencyType", String(10)),
            Column("RealPrice", DECIMAL(15, 4)),
            Column("PriceQuantity", DECIMAL(15, 4)),
            Column("PriceDate", Date),
        )

        # Rename and Select Columns
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        insert_records = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # Delete x months back
        stmt_delete = text(f"""
                           DELETE FROM {self._config.TABLE_MATERIAL_REAL_PRICE_FACT}
                           WHERE PriceDate >= :cutoff_date
                           """)

        # Insert new records
        stmt_insert: Insert = insert(material_real_price_table)
        with self._con_dw.begin() as conn:
            conn.execute(stmt_delete, {"cutoff_date": cutoff_date.strftime("%Y-%m-%d")})
            if len(insert_records) > 0:
                Logger().info(
                    f"Inserting {len(insert_records)} records into Material Real Price Fact."
                )
                conn.execute(stmt_insert, insert_records)
            self._update_etl_info(conn, "process_material_real_price")

            Logger().info("Material Real Price ETL completed successfully.")
