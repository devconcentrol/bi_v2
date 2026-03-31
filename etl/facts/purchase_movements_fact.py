import pandas as pd
from datetime import datetime
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    DECIMAL,
    Date,
    insert,
    text,
)
from etl.base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class PurchaseMovementsFactETL(BaseFactETL):
    """
    ETL for Purchase Movements Fact.
    Inherits from BaseFactETL and uses vectorized operations for efficiency.
    """

    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "werks": "PlantId",
        "MaterialId": "MaterialId",
        "PurchaseDate": "PurchaseDate",
        "menge": "Qty",
        "meins": "UnitId",
        "ebeln": "PurchaseOrder",
        "charg": "BatchNumber",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Purchase Movements Fact...")

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
        sql_get_purchase_movements = """
            SELECT WERKS,                                                                                                                                            
                   MATNR,                                   
                   MEINS,
                   BUDAT_MKPF,
                   MENGE,
                   EBELN,
                   CHARG
            FROM SAPSR3.ZCON_V_PURCHASE_MOV                             
            WHERE BUDAT_MKPF >= :cutoff_sap
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_purchase_movements,
            con=self._con_sap,
            params={"cutoff_sap": cutoff_sap},
            dtype_backend="numpy_nullable",
        )

        # 3. Handle Deletion in Data Warehouse
        stmt_delete = text(
            f"DELETE FROM {self._config.TABLE_PURCHASE_MOVEMENTS_FACT} WHERE PurchaseDate >= :cutoff_dw"
        )

        if results.empty:
            Logger().info("No purchase movement records found.")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
                self._update_etl_info(conn, "process_purchase_movements")
            return

        # 4. Transform Data
        results.columns = results.columns.str.lower()

        # Material ID Mapping
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Date transformations
        results["PurchaseDate"] = (
            pd.to_datetime(results["budat_mkpf"].astype(str), errors="coerce")
            .dt.strftime("%Y-%m-%d")
            .convert_dtypes()
        )

        # Ensure numeric columns
        results["menge"] = pd.to_numeric(results["menge"], errors="coerce").fillna(0)

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
        purchase_movements_table: Table = Table(
            self._config.TABLE_PURCHASE_MOVEMENTS_FACT,
            metadata,
            Column("PlantId", String(10)),
            Column("PurchaseDate", Date),
            Column("MaterialId", String(15)),
            Column("Qty", DECIMAL(15, 4)),
            Column("UnitId", String(10)),
            Column("PurchaseOrder", String(15)),
            Column("BatchNumber", String(50)),
        )

        with self._con_dw.begin() as conn:
            Logger().info(
                f"Deleting and inserting {len(insert_records)} purchase movement records."
            )
            conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
            if insert_records:
                conn.execute(insert(purchase_movements_table), insert_records)

            self._update_etl_info(conn, "process_purchase_movements")
