import pandas as pd
from sqlalchemy import Column, Engine, MetaData, String, Table, delete, insert, text

from utils.config import Config
from utils.error_handler import error_handler
from utils.logger import Logger


class WorkCenterDim:
    COLUMN_MAPPING = {
        "objid": "Id",
        "werks": "PlantId",
        "arbpl": "Code",
        "ktext": "Name",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._config = Config.get_instance()

    @error_handler
    def run(self) -> None:
        """Replace all work centers with the current SAP snapshot atomically."""
        Logger().info("Starting work center processing")
        results = pd.read_sql(
            text("SELECT OBJID, WERKS, ARBPL, KTEXT FROM SAPSR3.ZCON_V_WORK_CENTER"),
            self._con_sap,
        )
        results.columns = results.columns.str.lower()
        records = results.rename(columns=self.COLUMN_MAPPING)[
            list(self.COLUMN_MAPPING.values())
        ]
        records = records.astype(object).where(pd.notna(records), None)
        insert_records = [
            {str(column): value for column, value in record.items()}
            for record in records.to_dict(orient="records")
        ]

        work_center_table = Table(
            self._config.TABLE_WORK_CENTER_DIM,
            MetaData(),
            Column("Id", String(10), primary_key=True),
            Column("PlantId", String(10), nullable=False),
            Column("Code", String(50), nullable=False),
            Column("Name", String(1)),
        )

        with self._con_dw.begin() as conn:
            conn.execute(delete(work_center_table))
            if insert_records:
                conn.execute(insert(work_center_table), insert_records)
            conn.execute(
                text(
                    f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() "
                    "WHERE ETL = 'process_work_centers'"
                )
            )

        Logger().info(f"Loaded {len(records)} work centers")
