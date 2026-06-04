import pandas as pd
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.config import Config
from utils.logger import Logger
from datetime import date, timedelta
from sqlalchemy import (
    bindparam,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    update,
    Engine,
    insert,
    text,
    Insert,
    Update,
)

CRLF: str = "\r\n"


class NotificationDefectDim:
    # Mapping from DataFrame columns (lowercase from SQL) to DB columns
    COLUMN_MAPPING = {
        "GroupCode": "codegruppe",
        "Code": "code",
        "Description": "kurztext",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        Logger().info(f"Processing defects for date: {yesterday}")

        stmt_update_etl = text(
            f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_notification_defects'"
        )

        sql_get_defects = """
                            SELECT CODEGRUPPE, CODE, KURZTEXT
                            FROM SAPSR3.ZCON_V_NOTIFICATIONS_DEFECT
                            WHERE MODIFIEDDATE = :yesterday                            
                        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_defects,
            self._con_sap,
            params={"yesterday": yesterday},
            dtype_backend="numpy_nullable",
        )

        if results.empty:
            Logger().info("No defects found for processing")
            # Always update ETL Info even if no data
            with self._con_dw.begin() as conn:
                conn.execute(stmt_update_etl)
            return

        # normalize column names to lowercase to make downstream accesses predictable
        results.columns = results.columns.str.lower()

        metadata: MetaData = MetaData()
        # Define NotificationDefectDim Table
        defect_table: Table = Table(
            self._config.TABLE_NOTIFICATION_DEFECT_DIM,
            metadata,
            Column("DefectId", Integer, primary_key=True, autoincrement=True),
            Column("GroupCode", Integer, primary_key=True),
            Column("Code", String(25)),
            Column("Description", String),
        )

        # Prepare maps
        defect_map = self._lookup.get_notification_defect_map()

        # Lookups
        results["search_key"] = results["codegruppe"] + results["code"]
        results["DefectId"] = results["search_key"].map(defect_map)

        # Transformation
        results["kurztext"] = results["kurztext"].str.replace(CRLF, "", regex=False)

        # Split into updates and inserts
        updates_df = results[results["DefectId"].notna()]
        inserts_df = results[results["DefectId"].isna()]

        stmt_insert_defects: Insert = insert(defect_table)

        # Build update values dynamically using COLUMN_MAPPING
        update_values = {
            db_col: bindparam(f"b_{db_col}")
            for db_col in self.COLUMN_MAPPING.keys()
            if db_col not in ["codegruppe", "code"]
        }
        stmt_update_defects: Update = (
            update(defect_table)
            .where(defect_table.c.DefectId == bindparam("b_DefectId"))
            .values(**update_values)
        )

        with self._con_dw.begin() as conn:
            if not updates_df.empty:
                # Prepare update data
                # We rename DataFrame columns to match bindparams (b_ColName)
                rename_dict = {
                    df_col: f"b_{db_col}"
                    for db_col, df_col in self.COLUMN_MAPPING.items()
                    if db_col not in ["codegruppe", "code"]
                }
                rename_dict["DefectId"] = "b_DefectId"  # Key for update

                update_data = updates_df.rename(columns=rename_dict)[
                    list(rename_dict.values())
                ].to_dict(orient="records")

                conn.execute(stmt_update_defects, update_data)  # type: ignore

            if not inserts_df.empty:
                Logger().info(f"Inserting {len(inserts_df)} new defects")
                # Prepare insert data
                # We simply map DataFrame columns to DB columns for insert
                rename_dict_insert = {
                    df_col: db_col for db_col, df_col in self.COLUMN_MAPPING.items()
                }

                insert_data = inserts_df.rename(columns=rename_dict_insert)[
                    list(rename_dict_insert.values())
                ].to_dict(orient="records")

                conn.execute(stmt_insert_defects, insert_data)  # type: ignore

            conn.execute(stmt_update_etl)
