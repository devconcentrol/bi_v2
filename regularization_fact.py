import pandas as pd
from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    insert,
    text,
    Insert,
    DECIMAL,
    Date,
)


class RegularizationFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "werks": "PlantId",
        "lgort": "StorageLocation",
        "MaterialId": "MaterialId",
        "RegularizationDate": "RegularizationDate",
        "menge": "Qty",
        "meins": "UnitId",
        "bwart": "MovementType",
        "usnam_mkpf": "CreatedUser",
        "menge_kg": "QtyKg",
        "aufnr": "ProductionOrderNumber",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Regularization Fact...")

        # Calculate start date in Python (Start of current month - 2 months)
        # Example: If today is 2026-02-06 -> Start Month: 2026-02-01 -> -2 Months: 2025-12-01
        today = pd.Timestamp.now()
        first_day_current = today.replace(day=1)
        cutoff_date = first_day_current - pd.DateOffset(months=3)
        cutoff_date_sap = cutoff_date.strftime("%Y%m%d")

        sql_get_regularization = """
                            SELECT WERKS, 
                                   LGORT,
                                   MATNR,                                   
                                   MEINS,
                                   BUDAT_MKPF,
                                   MENGE,
                                   BWART,
                                   USNAM_MKPF,
                                   MENGE_KG,
                                   AUFNR
                            FROM SAPSR3.ZCON_V_REGULARIZATIONS                             
                            WHERE BUDAT_MKPF >= :cutoff_date_sap
                        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_regularization,
            con=self._con_sap,
            params={"cutoff_date_sap": cutoff_date_sap},
        )

        if results.empty:
            Logger().info("No regularization data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_regularizations")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # Define the table
        metadata: MetaData = MetaData()

        regularization_table: Table = Table(
            self._config.TABLE_REGULARIZATION_FACT,
            metadata,
            Column("PlantId", String(10)),
            Column("RegularizationDate", Date),
            Column("MaterialId", String(15)),
            Column("Qty", DECIMAL(15, 4)),
            Column("UnitId", String(10)),
            Column("MovementType", String(5)),
            Column("CreatedUser", String(100)),
            Column("QtyKg", DECIMAL(15, 4)),
            Column("ProductionOrderNumber", String(15)),
            Column("StorageLocation", String(15)),
        )

        # Vectorized Date Parsing
        results["RegularizationDate"] = pd.to_datetime(
            results["budat_mkpf"], format="%Y%m%d", errors="coerce"
        ).dt.date

        # Numeric Conversions
        numeric_cols = ["menge", "menge_kg"]
        for col in numeric_cols:
            if col in results.columns:
                results[col] = pd.to_numeric(results[col], errors="coerce").fillna(0)

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
            Logger().warning(
                f"Dropped {len(missing_materials)} rows due to missing MaterialId."
            )
        results = results.dropna(subset=["MaterialId"])

        if results.empty:
            Logger().info("No valid regularization data after material lookup.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_regularizations")
            return

        # Rename and Select Columns
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        insert_records = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # Database Operations
        stmt_insert: Insert = insert(regularization_table)

        stmt_delete = text(
            f"DELETE FROM {self._config.TABLE_REGULARIZATION_FACT} WHERE RegularizationDate >= :cutoff_date"
        )

        with self._con_dw.begin() as conn:
            conn.execute(stmt_delete, {"cutoff_date": cutoff_date.strftime("%Y-%m-%d")})
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} regularization records."
                )
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_regularizations")
