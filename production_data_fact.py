import pandas as pd
from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Delete,
    delete,
    insert,
    Insert,
    DECIMAL,
    Date,
    Integer,
    Boolean,
    bindparam,
)


class ProductionDataFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "gltri": "ProductionDate",
        "auart": "OrderType",
        "totalkg": "Qty",
        "numbatches": "NumBatches",
        "ProductionAreaId": "ProductionAreaId",
        "MaterialId": "MaterialId",
        "lgort": "StorageLocation",
        "isOR": "isOR",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Production Data Fact...")

        # 1. Fetch Data from SAP
        # Calculate cutoff date: Start of current month - 2 months
        # Equivalent to the original SQL logic which looked for data > Last Day of (Prev Month - 2 Months)
        today = pd.Timestamp.now()
        first_day_current = today.replace(day=1)
        cutoff_date = (first_day_current - pd.DateOffset(months=2)).strftime("%Y%m%d")

        sql_get_zsem_quantities = """
                            SELECT WERKS,
                                   DISPO,
                                   AUART,
                                   LGORT,
                                   GMEIN,
                                   GLTRI,
                                   MATNR,
                                   VERID,
                                   COUNT(*) as NumBatches,
                                   SUM(WEMNG) as TotalKg                                  
                            FROM SAPSR3.ZCON_V_PRODUCTION_QUANTITIES                             
                            WHERE AUART IN('ZSEM','ZPA')
                              AND CHARG <> ''
                              AND GLTRI >= :cutoff_date
                            GROUP BY WERKS,DISPO,AUART,LGORT,GMEIN,GLTRI,MATNR,VERID
                        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_zsem_quantities,
            con=self._con_sap,
            params={"cutoff_date": cutoff_date},
        )

        if results.empty:
            Logger().info("No production data found in SAP.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_production_data")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # 2. Data Transformation
        Logger().info("Transforming data...")

        # Date Parsing
        results["gltri"] = pd.to_datetime(
            results["gltri"], format="%Y%m%d", errors="coerce"
        ).dt.date

        # Lookup ProductionAreaId
        # Key: PlantId (werks) + ProductionAreaCode (dispo)
        production_area_map = self._lookup.get_production_area_map()

        # Ensure proper types for mapping key
        results["werks"] = results["werks"].fillna("").astype(str)
        results["dispo"] = results["dispo"].fillna("").astype(str)

        results["ProductionAreaId"] = (results["werks"] + results["dispo"]).map(
            production_area_map
        )

        # Log missing Production Areas
        missing_areas = results[results["ProductionAreaId"].isna()]
        if not missing_areas.empty:
            # Drop duplicates to log unique missing combinations
            missing_unique = missing_areas[["werks", "dispo"]].drop_duplicates()
            for _, row in missing_unique.iterrows():
                Logger().warning(
                    f"Production Area not found for PlantId: {row['werks']}, ProductionAreaCode: {row['dispo']}"
                )

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].fillna("").astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Boolean isOR
        results["isOR"] = results["verid"] == "OR"

        # Rename and Select Columns
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        # Ensure we have all columns, map result to dicts
        # Drop rows where critical keys might be missing if necessary?
        # Original code didn't explicitly drop, but logic implies we need MaterialId for deletion logic.
        # Let's drop if MaterialId or ProductionDate is missing as they form the composite key for update/delete logic roughly speaking.

        # Original logic deleted based on (ProductionDate, MaterialId) pairs found in source.
        # So we must have Valid MaterialId and ProductionDate.
        results = results.dropna(subset=["MaterialId", "ProductionDate"])

        if results.empty:
            Logger().info("No valid data after transformations.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_production_data")
            return

        insert_records = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # Prepare deletion parameters: distinct pairs of (ProductionDate, MaterialId)
        delete_params = (
            results[["ProductionDate", "MaterialId"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        # Rename keys to match bindparams if needed, or adjust bindparams.
        # SQLAlchemy bindparam style: "b_ProductionDate", "b_MaterialId"
        delete_params_mapped = [
            {"b_ProductionDate": r["ProductionDate"], "b_MaterialId": r["MaterialId"]}
            for r in delete_params
        ]

        # 3. Define Table Schema
        metadata: MetaData = MetaData()
        target_table: Table = Table(
            self._config.TABLE_PRODUCTION_DATA_FACT,
            metadata,
            Column("ProductionDate", Date),
            Column("OrderType", String(15)),
            Column("Qty", DECIMAL(15, 4)),
            Column("NumBatches", DECIMAL(15, 4)),
            Column("ProductionAreaId", Integer),
            Column("MaterialId", Integer),
            Column("StorageLocation", String(200)),
            Column("isOR", Boolean),
        )

        # 4. Database Operations
        stmt_insert: Insert = insert(target_table)

        # Delete existing records matching the (Date, Material) pairs we are about to insert/update
        # This is an incremental update strategy.
        stmt_delete: Delete = (
            delete(target_table)
            .where(target_table.c.ProductionDate == bindparam("b_ProductionDate"))
            .where(target_table.c.MaterialId == bindparam("b_MaterialId"))
        )

        with self._con_dw.begin() as conn:
            # Delete old records
            if delete_params_mapped:
                conn.execute(stmt_delete, delete_params_mapped)

            # Insert new records
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} production data records."
                )
                conn.execute(stmt_insert, insert_records)

            # Update ETL Info
            self._update_etl_info(conn, "process_production_data")
