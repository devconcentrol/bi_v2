import pandas as pd
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.config import Config
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Engine,
    insert,
    text,
    Insert,
    Date,
)
from base_fact_etl import BaseFactETL


class QMNotificationFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "qmnum": "NotificationId",
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "qmdat": "NotificationDate",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Processing QM Notifications Fact...")

        sql_get_notifications = """
            SELECT QMNUM,
                  QMART,
                  QMGRP,
                  MATNR,
                  CHARG,
                  QMDAT                  
            FROM SAPSR3.ZCON_V_NOTIFICATIONS
            WHERE QMART = 'ZI'
              AND QMGRP = 'INC'
              AND QMCOD = 'IN01'                                 
        """

        results: pd.DataFrame = pd.read_sql(sql_get_notifications, con=self._con_sap)

        if results.empty:
            Logger().info("No QM Notifications data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, 'process_qm_notification')
            return
        
        results.columns = results.columns.str.lower()
        
        results["qmdat"] = pd.to_datetime(results["qmdat"], format="%Y%m%d", errors="coerce").dt.date
        
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)
        
        results = results.rename(columns=self.COLUMN_MAPPING)

        metadata = MetaData()
        qm_notification_table = Table(
            self._config.TABLE_QM_NOTIFICATION_FACT,
            metadata,
            Column("NotificationId", String(25)),
            Column("MaterialId", Integer),
            Column("BatchNumber", String(50)),
            Column("NotificationDate", Date),
        )        
        
        final_cols = list(self.COLUMN_MAPPING.values())        
        insert_records = results[final_cols].where(pd.notnull(results), None).to_dict(orient="records")

        stmt_insert: Insert = insert(qm_notification_table)
        stmt_truncate = text(f"TRUNCATE TABLE {self._config.TABLE_QM_NOTIFICATION_FACT}")

        with self._con_dw.begin() as conn:
            if insert_records:
                conn.execute(stmt_truncate)
                Logger().info(f"Inserting {len(insert_records)} records into QM Notification Fact.")
                conn.execute(stmt_insert, insert_records)
            
            self._update_etl_info(conn, 'process_qm_notification')
