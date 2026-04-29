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


class ForecastConsumptionsFactETL(BaseFactETL):
    """
    ETL for Consumption Forecast Fact.
    Inherits from BaseFactETL and uses vectorized operations for efficiency.
    Dates are calculated in Python for better clarity.
    """

    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "werks": "PlantId",
        "MaterialId": "MaterialId",
        "ForecastDate": "ForecastDate",
        "bdmng": "Qty",
        "meins": "UnitId",
    }

    @staticmethod
    def _calculate_cutoff_date(now: datetime) -> datetime:
        first_day_current = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        if now.day <= 15:
            return first_day_current
        if now.month == 12:
            return first_day_current.replace(year=now.year + 1, month=1)
        return first_day_current.replace(month=now.month + 1)

    def _transform_results(
        self, results: pd.DataFrame, cutoff_date: datetime
    ) -> tuple[pd.DataFrame, list[str]]:
        results = results.copy()
        results.columns = results.columns.str.lower()

        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        missing_materials = (
            results.loc[results["MaterialId"].isna(), "matnr"]
            .astype(str)
            .unique()
            .tolist()
        )
        results = results.dropna(subset=["MaterialId"])

        if results.empty:
            return results, missing_materials

        results["ForecastDate"] = pd.to_datetime(
            results["bdter"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.date
        results.loc[results["ForecastDate"] < cutoff_date.date(), "ForecastDate"] = (
            cutoff_date.date()
        )
        results["bdmng"] = pd.to_numeric(results["bdmng"], errors="coerce").fillna(0)
        results = results.rename(columns=self.COLUMN_MAPPING)

        return results[list(self.COLUMN_MAPPING.values())], missing_materials

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Forecast Consumptions Fact...")

        # 1. Date Calculation in Python
        now = datetime.now()
        cutoff_date = self._calculate_cutoff_date(now)

        cutoff_sap = cutoff_date.strftime("%Y%m%d")
        cutoff_dw = cutoff_date.strftime("%Y-%m-%d")

        # 2. Extract Data from SAP
        sql_get_forecast = """
            SELECT WERKS,                                                                                                                                            
                   MATNR,                                   
                   MEINS,
                   BDTER,
                   BDMNG
            FROM SAPSR3.ZCON_V_CONSUMPTION_FORECAST                                         
        """
        # -- WHERE BDTER >= :cutoff_sap

        results: pd.DataFrame = pd.read_sql(
            sql_get_forecast,
            con=self._con_sap,
            params={"cutoff_sap": cutoff_sap},
            dtype_backend="numpy_nullable",
        )

        # 3. Handle Deletion in Data Warehouse
        stmt_delete = text(
            f"DELETE FROM {self._config.TABLE_CONSUMPTION_FORECAST_FACT} WHERE ForecastDate >= :cutoff_dw"
        )

        if results.empty:
            Logger().info("No forecast consumptions found.")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
                self._update_etl_info(conn, "process_forecast_consumptions")
            return

        # 4. Transform Data
        results, missing_materials = self._transform_results(results, cutoff_date)
        if missing_materials:
            Logger().warning(
                "Dropped %s rows due to missing MaterialId (%s).",
                len(missing_materials),
                missing_materials,
            )

        if results.empty:
            Logger().warning("No records remaining after material lookup.")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
                self._update_etl_info(conn, "process_forecast_consumptions")
            return

        insert_records = results.where(pd.notnull(results), None).to_dict(
            orient="records"
        )

        # 5. Load Data to Data Warehouse
        metadata: MetaData = MetaData()
        forecast_consumptions_table: Table = Table(
            self._config.TABLE_CONSUMPTION_FORECAST_FACT,
            metadata,
            Column("PlantId", String(10)),
            Column("ForecastDate", Date),
            Column("MaterialId", String(15)),
            Column("Qty", DECIMAL(15, 4)),
            Column("UnitId", String(10)),
        )

        with self._con_dw.begin() as conn:
            Logger().info(
                f"Deleting and inserting {len(insert_records)} forecast consumption records."
            )
            conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
            if insert_records:
                conn.execute(insert(forecast_consumptions_table), insert_records)  # type: ignore

            self._update_etl_info(conn, "process_forecast_consumptions")
