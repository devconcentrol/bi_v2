import pandas as pd
from datetime import datetime, timedelta
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
    Delete,
    delete,
    bindparam,
    Date,
)
from base_fact_etl import BaseFactETL


class QMSampleFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "phynr": "SampleId",
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "prart": "SampleType",
        "pn_nr": "SampleDrawNumber",
        "seconds": "DurationSeconds",
        "anldt": "CreatedDate",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Processing QM Sample Fact...")
        
        yesterday = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

        sql_get_sample_analysis = """
            SELECT PHYNR,
                   MATNR,
                   CHARG,
                   PRART,
                   PN_NR,
                   SECONDS,
                   ANLDT                               
            FROM SAPSR3.ZCON_V_SAMPLE_TIME_ANALYSIS    
            WHERE AENDT = :yesterday         
        """

        results: pd.DataFrame = pd.read_sql(sql_get_sample_analysis, con=self._con_sap, params={"yesterday": yesterday})

        if results.empty:
            Logger().info("No QM Sample data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, 'process_qm_sample_analysis')
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # Parse Dates
        results["anldt"] = pd.to_datetime(results["anldt"], format="%Y%m%d", errors="coerce").dt.date

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Rename columns
        results = results.rename(columns=self.COLUMN_MAPPING)

        # Define table
        metadata = MetaData()
        qm_sample_analysis_table = Table(
            self._config.TABLE_QM_SAMPLE_ANALYSIS_FACT,
            metadata,
            Column("SampleId", String(25)),
            Column("MaterialId", Integer),
            Column("BatchNumber", String(50)),
            Column("SampleType", String(10)),
            Column("SampleDrawNumber", String(25)),
            Column("DurationSeconds", Integer),
            Column("CreatedDate", Date),
        )

        # Select relevant columns
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = results[final_cols].where(pd.notnull(results), None).to_dict(orient="records")

        delete_records = [{"b_SampleId": row["SampleId"]} for row in insert_records]

        stmt_insert: Insert = insert(qm_sample_analysis_table)
        stmt_delete: Delete = delete(qm_sample_analysis_table).where(
            qm_sample_analysis_table.c.SampleId == bindparam("b_SampleId")
        )

        with self._con_dw.begin() as conn:
            if delete_records:
                conn.execute(stmt_delete, delete_records)
            
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} records into QM Sample Fact.")
                conn.execute(stmt_insert, insert_records)
            
            self._update_etl_info(conn, 'process_qm_sample_analysis')


