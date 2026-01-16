import pandas as pd
from datetime import date, timedelta
from zoneinfo import ZoneInfo
from utils.dimension_lookup import DimensionLookup
from utils.error_handler import error_handler
from utils.logger import Logger
from utils.config import Config
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Engine,
    insert,
    text,
    Integer,
    Date,
    Time,
)


class EWMTasksFactETL:
    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw: Engine = con_dw
        self._con_sap: Engine = con_sap
        self._lookup: DimensionLookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self) -> None:
        Logger().info("Processing EWM tasks...")
        # Date calculation in Python: 2 months ago from the start of the current month
        today = date.today()
        first_day_this_month = today.replace(day=1)
        filter_date = (first_day_this_month - timedelta(days=1)).replace(
            day=1
        ) - timedelta(days=1)
        filter_date = filter_date.replace(day=1)
        filter_date_sap = filter_date.strftime("%Y%m%d")

        sql_get_tasks = f"""
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
                            WHERE FILTER_CREATE_DATE >= '{filter_date_sap}'
                        """
        results: pd.DataFrame = pd.read_sql(sql_get_tasks, con=self._con_sap)
        # normalize column names to lowercase to make downstream accesses predictable
        results.columns = results.columns.str.lower()

        results["prod_order"] = (
            results.sort_values(["who", "prod_order"], ascending=False)
            .groupby("who")["prod_order"]
            .transform("first")
        )

        insert_data = []

        if not results.empty:
            # Vectorized mapping for MaterialId
            material_map = self._lookup.get_material_map()
            results["MaterialId"] = results["matnr"].map(material_map)

            created_dt = self.convert_sap_ts(results["created_at"])
            results["CreatedDate"] = created_dt.dt.date
            results["CreatedTime"] = created_dt.dt.time

            confirmed_dt = self.convert_sap_ts(results["confirmed_at"])
            results["ConfirmedDate"] = confirmed_dt.dt.date
            results["ConfirmedTime"] = confirmed_dt.dt.time

            # Rename and select columns to match EWMTaskFact
            # Mapping: SAP Column -> Table Column
            mapping = {
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
            }

            results = results.rename(columns=mapping)

            # Prepare for insertion
            final_cols = [
                "OrderNum",
                "TaskNum",
                "ClProcAlm",
                "Cola",
                "Trart",
                "UbicOrigen",
                "UbicDestino",
                "TipoCat",
                "UMPOrigen",
                "UMPDestino",
                "TpUMP",
                "MaterialId",
                "Batch",
                "UsuExt",
                "CreatedBy",
                "CreatedDate",
                "CreatedTime",
                "ConfirmedBy",
                "ConfirmedDate",
                "ConfirmedTime",
                "Tp",
                "Sec",
                "Tipo",
                "Area",
                "ProductionOrder",
                "Status",
            ]

            insert_data = results[final_cols].to_dict(orient="records")

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

        stmt_update_etl = text(
            f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_ewm_tasks'"
        )
        with self._con_dw.begin() as conn:
            if len(insert_data) > 0:
                conn.execute(stmt_delete)
                conn.execute(insert(ewm_tasks_table), insert_data)  # type: ignore
            conn.execute(stmt_update_etl)

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
