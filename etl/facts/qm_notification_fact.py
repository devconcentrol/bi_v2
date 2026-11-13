import pandas as pd
from sqlalchemy import (
    DECIMAL,
    Column,
    Date,
    Engine,
    Insert,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    text,
)

from etl.base_fact_etl import BaseFactETL
from utils.config import Config
from utils.dimension_lookup import DimensionLookup
from utils.error_handler import error_handler
from utils.logger import Logger


class QMNotificationFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "qmnum": "NotificationId",
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "qmdat": "NotificationDate",
        "rkmng": "Qty",
        "DefectId": "DefectId",
        "NotificationTypeId": "NotificationTypeId",
        "CodificationId": "CodificationId",
        "WorkCenterId": "WorkCenterId",
        "dftxt": "DefectText",
        "urtxt": "CauseText",
        "CauseId": "CauseId",
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
                  RKMNG,
                  ARBPL,
                  URGRP,
                  URCOD,
                  DFTXT,
                  URTXT                 
            FROM SAPSR3.ZCON_V_NOTIFICATIONS
            WHERE QMART IN ('ZI','ZM','ZO','ZS','ZC')
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

        catalog_map = self._lookup.get_notification_catalog_map()
        results["fegrp"] = results["fegrp"].astype(str)
        results["fecod"] = results["fecod"].astype(str)
        results["defect_key"] = (
            self._config.QM_DEFECT_CATALOG + results["fegrp"] + results["fecod"]
        )
        results["DefectId"] = results["defect_key"].map(catalog_map).convert_dtypes()

        results["qmgrp"] = results["qmgrp"].astype(str)
        results["qmcod"] = results["qmcod"].astype(str)
        results["codification_key"] = (
            self._config.QM_CODIFICATION_CATALOG + results["qmgrp"] + results["qmcod"]
        )
        results["CodificationId"] = (
            results["codification_key"].map(catalog_map).convert_dtypes()
        )

        results["CauseId"] = (
            (self._config.QM_CAUSE_CATALOG + results["urgrp"] + results["urcod"])
            .map(catalog_map)
            .convert_dtypes()
        )

        notification_type_map = self._lookup.get_notification_type_map()

        results["NotificationTypeId"] = (
            results["qmart"].astype(str).map(notification_type_map).convert_dtypes()
        )

        results["WorkCenterId"] = (
            results["arbpl"].map(self._lookup.get_work_center_map()).convert_dtypes()
        )

        results = results.rename(columns=self.COLUMN_MAPPING)

        metadata = MetaData()
        qm_notification_table = Table(
            self._config.TABLE_QM_NOTIFICATION_FACT,
            metadata,
            Column("NotificationId", String(25)),
            Column("MaterialId", Integer),
            Column("BatchNumber", String(50)),
            Column("NotificationDate", Date),
            Column("NotificationTypeId", String(100)),
            Column("Codification", String(100)),
            Column("Qty", DECIMAL),
            Column("DefectId", Integer),
            Column("CodificationId", Integer),
            Column("WorkCenterId", Integer),
            Column("DefectText", String),
            Column("CauseText", String),
            Column("CauseId", Integer),
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
                conn.execute(stmt_insert, insert_records)  # type: ignore

            self._update_etl_info(conn, "process_qm_notification")
