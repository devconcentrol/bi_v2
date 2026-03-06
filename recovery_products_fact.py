import pandas as pd
from datetime import datetime
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Integer,
    DECIMAL,
    Date,
    insert,
    text,
)
from base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class RecoveryProductsFactETL(BaseFactETL):
    """
    ETL for Recovery Products Fact.
    Inherits from BaseFactETL and uses vectorized operations for efficiency.
    """

    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "DateFact": "DateFact",
        "MaterialId": "MaterialId",
        "qty": "Quantity",
        "meins": "Unit",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Recovery Products Fact...")

        # 1. Date Calculation in Python
        # Logic: From the 1st of 2 months ago
        now = datetime.now()
        first_day_current = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        # Calculate beginning of 2 months ago
        if now.month > 2:
            cutoff_date = first_day_current.replace(month=now.month - 2)
        elif now.month == 2:
            cutoff_date = first_day_current.replace(year=now.year - 1, month=12)
        else:  # month == 1
            cutoff_date = first_day_current.replace(year=now.year - 1, month=11)

        cutoff_sap = cutoff_date.strftime("%Y%m%d")
        cutoff_dw = cutoff_date.strftime("%Y-%m-%d")

        # 2. Extract Data from SAP
        sql_get_PR_data = """
            SELECT BUDAT,
                   MATNR,                                   
                   SUM(ERFMG) AS QTY,
                   MEINS                                   
            FROM SAPSR3.ZCON_V_PR_101                             
            WHERE BUDAT >= :cutoff_sap                            
            GROUP BY BUDAT, MATNR, MEINS
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_PR_data,
            con=self._con_sap,
            params={"cutoff_sap": cutoff_sap},
            dtype_backend="numpy_nullable",
        )

        # 3. Handle Deletion in Data Warehouse
        stmt_delete = text(
            f"DELETE FROM {self._config.TABLE_RECOVERY_PRODUCTS_FACT} WHERE DateFact >= :cutoff_dw"
        )

        if results.empty:
            Logger().info("No recovery product records found.")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
                self._update_etl_info(conn, "process_recovery_products_data")
            return

        # 4. Transform Data
        results.columns = results.columns.str.lower()

        # Material ID Mapping
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Date transformations
        results["DateFact"] = (
            pd.to_datetime(results["budat"].astype(str), errors="coerce")
            .dt.strftime("%Y-%m-%d")
            .convert_dtypes()
        )

        # Ensure numeric columns
        results["qty"] = pd.to_numeric(results["qty"], errors="coerce").fillna(0)

        # Rename and Select Columns
        results_final = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        insert_records = (
            results_final[final_cols]
            .where(pd.notnull(results_final[final_cols]), None)
            .to_dict(orient="records")
        )

        # 5. Load Data to Data Warehouse
        metadata: MetaData = MetaData()
        recovery_products_table: Table = Table(
            self._config.TABLE_RECOVERY_PRODUCTS_FACT,
            metadata,
            Column("DateFact", Date),
            Column("MaterialId", Integer),
            Column("Quantity", DECIMAL(15, 4)),
            Column("Unit", String(10)),
        )

        with self._con_dw.begin() as conn:
            Logger().info(
                f"Deleting and inserting {len(insert_records)} recovery product records."
            )
            conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
            if insert_records:
                conn.execute(insert(recovery_products_table), insert_records)

            self._update_etl_info(conn, "process_recovery_products_data")
