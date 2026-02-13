import pandas as pd
from pandas._libs import Timestamp
from datetime import date, timedelta
from zoneinfo import ZoneInfo
from utils.error_handler import error_handler
from utils.logger import Logger
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    insert,
    text,
    Integer,
    Date,
    Time,
)
from base_fact_etl import BaseFactETL


class EWMTasksFactETL(BaseFactETL):
    # Rename and select columns to match EWMTaskFact
    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "who": "OrderNum",
        "tanum": "TaskNum",
        "hdr_procty": "ClProcAlm",
        "queue": "Cola",
        "trart": "Trart",
        "vlpla": "UbicOrigen",
        "nlpla": "UbicDestino",
        "cat": "TipoCat",
        "vlenr": "UMPOrigen",
        "nlenr": "UMPDestino",
        "letyp": "TpUMP",
        "charg": "Batch",
        "zewmusu": "UsuExt",
        "created_by": "CreatedBy",
        "confirmed_by": "ConfirmedBy",
        "vltyp": "Tp",
        "vlber": "Sec",
        "nltyp": "Tipo",
        "nlber": "Area",
        "prod_order": "ProductionOrder",
        "tostat": "Status",
        "MaterialId": "MaterialId",
        "ConfirmedDate": "ConfirmedDate",
        "ConfirmedTime": "ConfirmedTime",
        "CreatedDate": "CreatedDate",
        "CreatedTime": "CreatedTime",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing EWM tasks...")
        # Date calculation in Python: 1 months ago from the start of the current month
        today = date.today()
        first_day_this_month = today.replace(day=1)
        filter_date = (first_day_this_month - timedelta(days=1)).replace(day=1)
        filter_date_sap = filter_date.strftime("%Y%m%d")

        sql_get_tasks = """
                            SELECT WHO,
                                   TANUM,
                                   HDR_PROCTY,
                                   ZEWMUSU,
                                   QUEUE,
                                   TRART,
                                   VLPLA,
                                   NLPLA,
                                   CAT,
                                   VLENR,
                                   NLENR,
                                   LETYP,
                                   CHARG,
                                   MATNR,
                                   Created_by,
                                   CREATED_AT,
                                   CONFIRMED_BY,
                                   CONFIRMED_AT,
                                   VLTYP,    
                                   VLBER,
                                   NLTYP,
                                   NLBER,
                                   PROD_ORDER,
                                   TOSTAT
                            FROM SAPSR3.ZCON_EWM_TASK                             
                            WHERE FILTER_CREATE_DATE >= :filter_date_sap
                        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_tasks,
            con=self._con_sap,
            params={"filter_date_sap": filter_date_sap},
        )

        if results.empty:
            Logger().info("No EWM tasks found.")
            with self._con_dw.connect() as conn:
                self._update_etl_info(conn, "process_ewm_tasks")
            return

        # normalize column names to lowercase to make downstream accesses predictable
        results.columns = results.columns.str.lower()

        results["prod_order"] = (
            results.sort_values(["who", "prod_order"], ascending=False)
            .groupby("who")["prod_order"]
            .transform("first")
        )

        # Vectorized mapping for MaterialId
        material_map = self._lookup.get_material_map()
        results["MaterialId"] = results["matnr"].map(material_map)

        created_dt = self.convert_sap_ts(results["created_at"])
        results["CreatedDate"] = created_dt.dt.date
        results["CreatedTime"] = created_dt.dt.time

        confirmed_dt = self.convert_sap_ts(results["confirmed_at"])
        results["ConfirmedDate"] = confirmed_dt.dt.date
        results["ConfirmedTime"] = confirmed_dt.dt.time

        results = results.rename(columns=self.COLUMN_MAPPING)

        final_cols = list(self.COLUMN_MAPPING.values())
        insert_data = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # Define the table for insertion
        metadata: MetaData = MetaData()
        ewm_tasks_table: Table = Table(
            self._config.TABLE_EWM_TASK_FACT,
            metadata,
            Column("OrderNum", String(15)),
            Column("TaskNum", String(15)),
            Column("ClProcAlm", String(10)),
            Column("Cola", String(15)),
            Column("Trart", String(5)),
            Column("UbicOrigen", String(25)),
            Column("UbicDestino", String(25)),
            Column("TipoCat", String(10)),
            Column("UMPOrigen", String(25)),
            Column("UMPDestino", String(25)),
            Column("TpUMP", String(10)),
            Column("MaterialId", Integer),
            Column("Batch", String(25)),
            Column("UsuExt", String(100)),
            Column("CreatedBy", String(100)),
            Column("CreatedDate", Date),
            Column("CreatedTime", Time),
            Column("ConfirmedBy", String(100)),
            Column("ConfirmedDate", Date),
            Column("ConfirmedTime", Time),
            Column("Tp", String(15)),
            Column("Sec", String(15)),
            Column("Tipo", String(15)),
            Column("Area", String(15)),
            Column("ProductionOrder", String(15)),
            Column("Status", String(1)),
        )

        stmt_delete = text(f"""
                        DELETE FROM {self._config.TABLE_EWM_TASK_FACT}
                        WHERE CreatedDate >= '{filter_date.strftime("%Y-%m-%d")}'
                        """)

        with self._con_dw.begin() as conn:
            if len(insert_data) > 0:
                conn.execute(stmt_delete)
                conn.execute(insert(ewm_tasks_table), insert_data)  # type: ignore
            self._update_etl_info(conn, "process_ewm_tasks")

    def convert_sap_ts(self, ts_series: pd.Series) -> pd.Series:
        # Vectorized timestamp conversion
        tz_utc = ZoneInfo("UTC")
        tz_local = ZoneInfo("Europe/Madrid")
        # SAP timestamps are strings like '20231219205500', but may come as floats from DB
        # Convert to int first to remove decimals, then to string
        dt_series = pd.to_datetime(
            ts_series.fillna(0).astype(int).astype(str),
            format="%Y%m%d%H%M%S",
            errors="coerce",
        )
        # Localize to UTC and then convert to Europe/Madrid
        return dt_series.dt.tz_localize(tz_utc).dt.tz_convert(tz_local)
