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
    Date,
    Float,
)
from base_fact_etl import BaseFactETL


class QMInspectionLotFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "vwerks": "Plant",
        "prueflos": "InspectionLot",
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "vornr": "Operation",
        "verwmerkm": "Characteristic",
        "kurztext": "CharacteristicDesc",
        "minimo": "MinValue",
        "maximo": "MaxValue",
        "unidad": "Unit",
        "especificacion": "Specification",
        "result": "Result",
        "valoracion": "Valoration",
        "decision_empleo": "UsageDecision",
        "vdatum": "UsageDecisionDate",
        "pruefdatub": "InspectionEndDate",
        "desc_inspeccion": "InspectionNotes",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Processing QM Inspection Lot Fact...")

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        sql_get_lotes_inspeccion = """
            SELECT VWERKS,
                  PRUEFLOS,
                  MATNR,
                  CHARG,
                  VORNR,
                  VERWMERKM,
                  KURZTEXT,
                  MINIMO,
                  MAXIMO,
                  UNIDAD,
                  ESPECIFICACION,
                  RESULT,
                  VALORACION,
                  DESC_INSPECCION,
                  DECISION_EMPLEO,                  
                  VDATUM,
                  PRUEFDATUB
            FROM SAPSR3.ZCON_V_INSPECTION_LOT            
            WHERE VDATUM = :yesterday
        """

        results: pd.DataFrame = pd.read_sql(sql_get_lotes_inspeccion, con=self._con_sap, params={"yesterday": yesterday})

        if results.empty:
            Logger().info("No Inspection Lot data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, 'process_inspection_lot')
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # Parse Dates
        results["vdatum"] = pd.to_datetime(results["vdatum"], format="%Y%m%d", errors="coerce").dt.date
        results["pruefdatub"] = pd.to_datetime(results["pruefdatub"], format="%Y%m%d", errors="coerce").dt.date

        # Lookup Material
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        # Rename columns
        results = results.rename(columns=self.COLUMN_MAPPING)

        metadata = MetaData()
        inspection_lot_table = Table(
            self._config.TABLE_QM_INSPECTION_LOT_FACT,
            metadata,
            Column("Plant", String(10)),
            Column("InspectionLot", String(25)),
            Column("MaterialId", Integer),
            Column("BatchNumber", String(50)),
            Column("Operation", String(10)),
            Column("Characteristic", String(10)),
            Column("CharacteristicDesc", String(100)),
            Column("MinValue", Float),
            Column("MaxValue", Float),
            Column("Unit", String(10)),
            Column("Specification", String(100)),
            Column("Result", String(100)),
            Column("Valoration", String(100)),
            Column("UsageDecision", String(100)),
            Column("UsageDecisionDate", Date),
            Column("InspectionEndDate", Date),
            Column("InspectionNotes", String),
        )

        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = results[final_cols].where(pd.notnull(results), None).to_dict(orient="records")
        stmt_insert: Insert = insert(inspection_lot_table)

        with self._con_dw.begin() as conn:
            if insert_records:
                Logger().info(f"Inserting {len(insert_records)} records into QM Inspection Lot Fact.")
                conn.execute(stmt_insert, insert_records)
            
            self._update_etl_info(conn, 'process_inspection_lot')

