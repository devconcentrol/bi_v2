from datetime import date, timedelta

import pandas as pd
from sqlalchemy import Column, Date, Integer, MetaData, String, Table, text

from etl.base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class QMNotificationMeasureFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "qmnum": "NotificationId",
        "manum": "MeasureNum",
        "erdat": "CreatedDate",
        "NotificationTypeId": "NotificationTypeId",
        "MeasureId": "MeasureId",
        "parnr": "Responsible",
        "pster": "PlannedStartDate",
        "peter": "PlannedEndDate",
        "erlnam": "CompletedBy",
        "erldat": "CompletedDate",
        "aedat": "ChangedDate",
        "matxt": "Text",
    }
    DATE_COLUMNS = ("erdat", "pster", "peter", "erldat", "aedat")

    @error_handler
    def run(self):
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

        Logger().info(f"Processing Notification Measures for date: {yesterday}")
        query = """
            SELECT QMNUM,
                   MANUM,
                   ERDAT,
                   QMART,
                   MNKAT,
                   MNGRP,
                   MNCOD,
                   PARNR,
                   PSTER,
                   PETER,
                   ERLNAM,
                   ERLDAT,
                   AEDAT,
                   MATXT
            FROM SAPSR3.ZCON_V_NOTIFICATION_MEASURE            
            --WHERE AEDAT = :yesterday
        """
        results = pd.read_sql(query, con=self._con_sap, params={"yesterday": yesterday})

        if results.empty:
            Logger().info("No Notification Measures data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_notification_measures")
            return

        results.columns = results.columns.str.lower()
        for column in self.DATE_COLUMNS:
            results[column] = pd.to_datetime(
                results[column], format="%Y%m%d", errors="coerce"
            ).dt.date

        results["cause_key"] = (
            results["mnkat"].astype(str)
            + results["mngrp"].astype(str)
            + results["mncod"].astype(str)
        )

        catalog_map = self._lookup.get_notification_catalog_map()
        results["MeasureId"] = results["cause_key"].map(catalog_map).convert_dtypes()

        results["NotificationTypeId"] = (
            results["qmart"]
            .map(self._lookup.get_notification_type_map())
            .convert_dtypes()
        )

        results = results.rename(columns=self.COLUMN_MAPPING)
        table = Table(
            self._config.TABLE_NOTIFICATION_MEASURE_FACT,
            MetaData(),
            Column("NotificationId", String(25)),
            Column("MeasureNum", Integer),
            Column("CreatedDate", Date),
            Column("NotificationTypeId", Integer),
            Column("MeasureId", Integer),
            Column("Responsible", String(25)),
            Column("PlannedStartDate", Date),
            Column("PlannedEndDate", Date),
            Column("CompletedBy", String(25)),
            Column("CompletedDate", Date),
            Column("ChangedDate", Date),
            Column("Text", String(255)),
        )
        records = results[list(self.COLUMN_MAPPING.values())].astype(object)
        insert_records = records.where(pd.notnull(records), None).to_dict(
            orient="records"
        )

        columns = list(self.COLUMN_MAPPING.values())
        key_columns = ("NotificationId", "MeasureNum")
        source_values = ", ".join(f":{column}" for column in columns)
        source_columns = ", ".join(columns)
        update_columns = [column for column in columns if column not in key_columns]
        update_assignments = ", ".join(
            f"target.{column} = source.{column}" for column in update_columns
        )
        insert_columns = ", ".join(columns)
        insert_values = ", ".join(f"source.{column}" for column in columns)
        stmt_upsert = text(f"""
            MERGE INTO {table.name} WITH (HOLDLOCK) AS target
            USING (VALUES ({source_values})) AS source ({source_columns})
            ON target.NotificationId = source.NotificationId
               AND target.MeasureNum = source.MeasureNum
            WHEN MATCHED THEN
                UPDATE SET {update_assignments}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns}) VALUES ({insert_values});
            """)

        with self._con_dw.begin() as conn:
            Logger().info(
                f"Upserting {len(insert_records)} records "
                "into Notification Measures Fact."
            )
            conn.execute(stmt_upsert, insert_records)
            self._update_etl_info(conn, "process_notification_measures")
