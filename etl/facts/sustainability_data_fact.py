import pandas as pd
from sqlalchemy import (
    bindparam,
    MetaData,
    Table,
    Column,
    String,
    delete,
    insert,
    DECIMAL,
    Date,
    Integer,
)

from utils.error_handler import error_handler
from utils.logger import Logger
from etl.base_fact_etl import BaseFactETL


class SustainabilityDataFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "budat_mkpf": "DateFact",
        "MaterialId": "MaterialId",
        "movement_type": "MovementType",
        "qty": "Quantity",
        "meins": "Unit",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Sustainability Data Fact...")
        config = self._config

        today = pd.Timestamp.now()
        first_day_this_month = today.replace(day=1)
        cutoff_date = (first_day_this_month - pd.DateOffset(months=2)).strftime(
            "%Y%m%d"
        )

        sql_get_sustainability_data = """
            SELECT BUDAT_MKPF,
                   MATNR,
                   MOVEMENT_TYPE,
                   QTY,
                   MEINS                                   
            FROM SAPSR3.ZCON_V_SUSTAINABILITY                             
            WHERE BUDAT_MKPF >= :cutoff_date
        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_sustainability_data,
            con=self._con_sap,
            params={"cutoff_date": cutoff_date},
        )

        if results.empty:
            Logger().info("No Sustainability Data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_sustainability_data")
            return

        results.columns = results.columns.str.lower()

        # Parse Dates
        results["budat_mkpf"] = pd.to_datetime(
            results["budat_mkpf"], format="%Y%m%d", errors="coerce"
        ).dt.date

        results["matnr"] = results["matnr"].astype(str)

        material_map = self._lookup.get_material_map()
        results["MaterialId"] = results["matnr"].map(material_map)
        missing_materials = results[results["MaterialId"].isna()]["matnr"].unique()
        for mm in missing_materials:
            Logger().warning(f"Material no encontrado en DW: {mm}")

        results = results.dropna(subset=["MaterialId"])
        results["MaterialId"] = results["MaterialId"].astype(int)

        results = results.rename(columns=self.COLUMN_MAPPING)

        metadata: MetaData = MetaData()
        sustainability_data_table: Table = Table(
            config.TABLE_SUSTAINABILITY_DATA_FACT,
            metadata,
            Column("DateFact", Date),
            Column("MaterialId", Integer),
            Column("MovementType", String(25)),
            Column("Quantity", DECIMAL(15, 4)),
            Column("Unit", String(10)),
        )

        # Prepare records for insert and delete
        final_cols = list(self.COLUMN_MAPPING.values())
        final_results = results[final_cols].copy()

        insert_records = final_results.where(pd.notnull(final_results), None).to_dict(
            orient="records"
        )

        delete_records = (
            final_results[["DateFact", "MaterialId", "MovementType"]]
            .rename(
                columns={
                    "DateFact": "b_DateFact",
                    "MaterialId": "b_MaterialId",
                    "MovementType": "b_MovementType",
                }
            )
            .to_dict(orient="records")
        )

        stmt_insert = insert(sustainability_data_table)
        stmt_delete = (
            delete(sustainability_data_table)
            .where(sustainability_data_table.c.DateFact == bindparam("b_DateFact"))
            .where(sustainability_data_table.c.MaterialId == bindparam("b_MaterialId"))
            .where(
                sustainability_data_table.c.MovementType == bindparam("b_MovementType")
            )
        )

        with self._con_dw.begin() as conn:
            if delete_records:
                conn.execute(stmt_delete, delete_records)
            if insert_records:
                Logger().info(
                    f"Inserting {len(insert_records)} sustainability data records."
                )
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_sustainability_data")
