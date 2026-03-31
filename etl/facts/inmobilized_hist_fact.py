import pandas as pd
from datetime import datetime
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    DECIMAL,
    Date,
    String,
    insert,
)
from etl.base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class ImmobilizedHistFactETL(BaseFactETL):
    """
    ETL for Immobilized History Fact.
    Inherits from BaseFactETL. Processes new immobilized materials on the first day of the month.
    """

    COLUMN_MAPPING = {
        "MaterialId": "MaterialId",
        "QtyStock": "QtyStock",
        "LastConsumptionDate": "LastConsumptionDate",
    }

    @error_handler
    def run(self) -> None:
        if datetime.now().day != 1:
            Logger().info(
                "Today is not the first day of the month. Skipping Immobilized Hist Fact ETL."
            )
            return

        Logger().info("Processing Immobilized History Fact...")

        # 1. Extract Data from Data Warehouse
        sql_get_new_immobilized = """
            SELECT MaterialId,
                   QtyStock,
                   LastConsumptionDate
            FROM V_DW_NUEVOS_IMMOBILIZADOS
        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_new_immobilized, con=self._con_dw, dtype_backend="numpy_nullable"
        )

        if results.empty:
            Logger().info("No new immobilized records found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_immobilized_hist")
            return

        # 2. Transform Data (Rename columns to match target table)
        results_final = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        insert_records = (
            results_final[final_cols]
            .where(pd.notnull(results_final[final_cols]), None)
            .to_dict(orient="records")
        )

        # 3. Load Data to Data Warehouse (Append)
        metadata: MetaData = MetaData()
        immobilized_hist_table: Table = Table(
            self._config.TABLE_IMMOBILIZED_HIST_FACT,
            metadata,
            Column("MaterialId", String(50)),
            Column("QtyStock", DECIMAL(15, 4)),
            Column("LastConsumptionDate", Date),
            Column("CreatedDate", Date),
        )

        with self._con_dw.begin() as conn:
            Logger().info(
                f"Inserting {len(insert_records)} records into {self._config.TABLE_IMMOBILIZED_HIST_FACT}."
            )
            if insert_records:
                conn.execute(insert(immobilized_hist_table), insert_records)

            self._update_etl_info(conn, "process_immobilized_hist")
