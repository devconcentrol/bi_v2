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
    DECIMAL,
)
from etl.base_fact_etl import BaseFactETL


class QMNotificationFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "qmnum": "NotificationId",
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "qmdat": "NotificationDate",
        "qmcod": "Codification",
        "qmgrp": "CodeGroup",
        "qmart": "Type",
        "rkmng": "Qty",
        "DefectId": "DefectId",
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
                  QMCOD,
                  FEGRP,
                  FECOD,
                  MATNR,
                  CHARG,
                  QMDAT,
                  RKMNG                 
            FROM SAPSR3.ZCON_V_NOTIFICATIONS 
            WHERE (QMART = 'ZI' AND QMCOD = 'IN01')
               OR (QMART = 'ZC' AND QMCOD = 'RC2')
                                        
        """

        results: pd.DataFrame = pd.read_sql(sql_get_notifications, con=self._con_sap)

        if results.empty:
            Logger().info("No QM Notifications data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_qm_notification")
            return

        results.columns = results.columns.str.lower()

        results["qmdat"] = pd.to_datetime(
            results["qmdat"], format="%Y%m%d", errors="coerce"
        ).dt.date

        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map).convert_dtypes()

        defect_map = self._lookup.get_notification_defect_map()
        results["fegrp"] = results["fegrp"].astype(str)
        results["fecod"] = results["fecod"].astype(str)
        results["defect_key"] = results["fegrp"] + results["fecod"]
        results["DefectId"] = results["defect_key"].map(defect_map).convert_dtypes()

        results = results.rename(columns=self.COLUMN_MAPPING)

        metadata = MetaData()
        qm_notification_table = Table(
            self._config.TABLE_QM_NOTIFICATION_FACT,
            metadata,
            Column("NotificationId", String(25)),
            Column("MaterialId", Integer),
            Column("BatchNumber", String(50)),
            Column("NotificationDate", Date),
            Column("CodeGroup", String(100)),
            Column("Type", String(100)),
            Column("Codification", String(100)),
            Column("Qty", DECIMAL),
            Column("DefectId", Integer),
        )

        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = (
            results[final_cols]
            .where(pd.notnull(results[final_cols]), None)
            .to_dict(orient="records")
        )

        #        for record in insert_records:
        #            if pd.isna(record["MaterialId"]):
        #                record["MaterialId"] = None

        stmt_insert: Insert = insert(qm_notification_table)
        stmt_truncate = text(
            f"TRUNCATE TABLE {self._config.TABLE_QM_NOTIFICATION_FACT}"
        )

        with self._con_dw.begin() as conn:
            if insert_records:
                conn.execute(stmt_truncate)
                Logger().info(
                    f"Inserting {len(insert_records)} records into QM Notification Fact."
                )
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_qm_notification")
