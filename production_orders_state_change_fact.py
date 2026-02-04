import pandas as pd
from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    insert,
    Insert,
    Integer,
    Date,
    Time,
)


class ProductonOrdersStateChangeFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "aufnr": "OrderNumber",
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "usnam": "UserName",
        "ChangeDate": "ChangeDate",
        "ChangeTime": "ChangeTime",
        "stat": "State",
    }

    @error_handler
    def run(self):
        Logger().info("Processing Production Orders State Change Fact...")

        yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime("%Y%m%d")
        # 1. Fetch Data from SAP
        sql_get_state_change = """
            SELECT AUFNR,
                  CHARG,                  
                  MATNR,                                                      
                  UDATE,
                  UTIME,                  
                  USNAM,
                  STAT
            FROM SAPSR3.ZCON_V_OF_STATE_CHANGE                           
            WHERE UDATE = :yesterday            
              AND STAT = 'E0001'
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_state_change, con=self._con_sap, params={"yesterday": yesterday}
        )

        if results.empty:
            Logger().info("No production state change data found in SAP for yesterday.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_prod_of_state_change")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # 2. Data Transformation
        Logger().info("Transforming data...")

        # Parse Date and Time
        results["ChangeDate"] = pd.to_datetime(
            results["udate"], format="%Y%m%d", errors="coerce"
        ).dt.date

        # Parse Time using string manipulation for vectorized speed
        # Ensure it's string, pad if needed (though SAP times are usually 6 digits HHMMSS)
        # Assuming HHMMSS string format
        results["utime"] = results["utime"].fillna("000000").astype(str).str.zfill(6)
        results["ChangeTime"] = pd.to_datetime(
            results["utime"], format="%H%M%S", errors="coerce"
        ).dt.time

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].fillna("").astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Rename and Select Columns
        results = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        insert_records = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # 3. Define Table Schema
        metadata: MetaData = MetaData()
        target_table: Table = Table(
            self._config.TABLE_PRODUCTION_OF_STATE_CHANGE_FACT,
            metadata,
            Column("OrderNumber", String(25)),
            Column("MaterialId", Integer),
            Column("BatchNumber", String(50)),
            Column("UserName", String(25)),
            Column("ChangeDate", Date),
            Column("ChangeTime", Time),
            Column("State", String(15)),
        )

        # 4. Database Operations
        stmt_insert: Insert = insert(target_table)

        with self._con_dw.begin() as conn:
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} state change records.")
                conn.execute(stmt_insert, insert_records)

            # Update ETL Info
            self._update_etl_info(conn, "process_prod_of_state_change")
