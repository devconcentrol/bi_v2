import pandas as pd
from datetime import date, timedelta
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.config import Config
from utils.logger import Logger
from utils.date_utils import parse_date
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
    DECIMAL,
    Date,
    Float,
)


class MaterialDim:
    COLUMN_MAPPING = {
        "MaterialCode": "matnr",
        "MaterialName": "maktx",
        "MaterialType": "mtart",
        "PurchaseManager": "ekgrp",
        # "TechnicalManager": None, # Not present in source
        "SafetyStock": "eisbe",
        "MinimumStock": "minbe",
        # "MainSupplier": None, # Not present in source
        "PMR": "plifz",
        "GammaId": "GammaId",  # Mapped
        "DivisionId": "DivisionId",  # Mapped
        "FamilyId": "FamilyId",  # Mapped
        "SubFamilyId": "SubFamilyId",  # Mapped
        "MarkDeleted": "lvorm",
        "NetWeight": "netweight",
        "CreatedDate": "ersda",
        "MaterialStatusId": "MaterialStatusId",  # Mapped
        "ShelfLife": "mhdhb",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Starting materials processing")
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

        stmt_update_etl = text(
            f"""UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_materials'"""
        )        

        # Load Source Data
        sql_get_materials = """
                            SELECT MATNR, MAKTX, MTART, EISBE, MINBE, MABST, EKGRP, PRDHA, LVORM, NETWEIGHT ,
                                   ERSDA, PLIFZ, MSTAE, MHDHB
                            FROM SAPSR3.ZCON_V_MATERIAL                              
                            WHERE LAEDA = :yesterday
                        """
        results: pd.DataFrame = pd.read_sql(sql_get_materials, con=self._con_sap, params={"yesterday": yesterday})

        if results.empty:
            Logger().info("No materials found for processing")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_update_etl)
            return

        # Load Maps
        gamma_map = self._lookup.get_gamma_map()
        division_map = self._lookup.get_division_map()
        family_map = self._lookup.get_family_map()
        subfamily_map = self._lookup.get_subfamily_map()
        status_map = self._lookup.get_material_status_map()

        # Normalize columns
        results.columns = results.columns.str.lower()

        metadata: MetaData = MetaData()
        material_table: Table = Table(
            self._config.TABLE_MATERIAL_DIM,
            metadata,
            Column("MaterialId", Integer, primary_key=True),
            Column("MaterialCode", String(25)),
            Column("MaterialName", String),
            Column("MaterialType", String(200)),
            Column("PurchaseManager", String(200)),
            Column("TechnicalManager", String(200)),
            Column("SafetyStock", DECIMAL(15, 4)),
            Column("MinimumStock", DECIMAL(15, 4)),
            Column("MainSupplier", String(200)),
            Column("PMR", Integer),
            Column("GammaId", Integer),
            Column("DivisionId", Integer),
            Column("FamilyId", Integer),
            Column("SubFamilyId", Integer),
            Column("MarkDeleted", String(1)),
            Column("NetWeight", Float),
            Column("CreatedDate", Date),
            Column("MaterialStatusId", Integer),
            Column("ShelfLife", Integer),
        )

        # Transformation & Vectorization
        results["matnr"] = results["matnr"].astype(str)
        
        # Product Hierarchy Parsing
        # PRDHA structure: Gamma(2) + Division(3) + Family(4) + SubFamily(remaining)
        # We need to handle cases where PRDHA might be too short or null safely
        results["prdha"] = results["prdha"].fillna("").astype(str)
        
        results["gamma_code"] = results["prdha"].str.slice(0, 2)
        results["division_code"] = results["prdha"].str.slice(2, 5)
        results["family_code"] = results["prdha"].str.slice(5, 9)
        results["subfamily_code"] = results["prdha"].str.slice(9)

        # Map Hierarchy IDs
        results["GammaId"] = results["gamma_code"].map(gamma_map)
        
        # Composite keys for deeper levels
        results["division_key"] = results["gamma_code"] + results["division_code"]
        results["DivisionId"] = results["division_key"].map(division_map)

        results["family_key"] = results["division_key"] + results["family_code"]
        results["FamilyId"] = results["family_key"].map(family_map)

        results["subfamily_key"] = results["family_key"] + results["subfamily_code"]
        results["SubFamilyId"] = results["subfamily_key"].map(subfamily_map)

        # Other fields
        results["MaterialStatusId"] = results["mstae"].map(status_map)
        
        # Dates and Numeric conversions
        results["ersda"] = results["ersda"].apply(lambda x: parse_date(x))
        # Ensure numeric fields are numeric
        results["minbe"] = pd.to_numeric(results["minbe"], errors='coerce').fillna(0) / 1000
        results["eisbe"] = pd.to_numeric(results["eisbe"], errors='coerce')
        results["mabst"] = pd.to_numeric(results["mabst"], errors='coerce')
        results["plifz"] = pd.to_numeric(results["plifz"], errors='coerce')
        results["netweight"] = pd.to_numeric(results["netweight"], errors='coerce')
        results["mhdhb"] = pd.to_numeric(results["mhdhb"], errors='coerce')

        # Material Lookup
        material_map = self._lookup.get_material_map()
        results["MaterialId"] = results["matnr"].map(material_map)

        # Split Updates/Inserts
        updates_df = results[results["MaterialId"].notna()]
        inserts_df = results[results["MaterialId"].isna()]

        stmt_insert_materials: Insert = insert(material_table)

        # Build update values dynamically using COLUMN_MAPPING
        update_values = {
            db_col: bindparam(f"b_{db_col}") 
            for db_col, df_col in self.COLUMN_MAPPING.items() if db_col not in ["MaterialCode","CreatedDate"]            
        }
        
        stmt_update_materials: Update = (
            update(material_table)
            .where(material_table.c.MaterialId == bindparam("b_MaterialId"))
            .values(**update_values)
        )

        with self._con_dw.begin() as conn:
            if not updates_df.empty:
                rename_dict = {
                    df_col: f"b_{db_col}"
                    for db_col, df_col in self.COLUMN_MAPPING.items() if db_col not in ["MaterialCode","CreatedDate"]
                }
                rename_dict["MaterialId"] = "b_MaterialId"

                updates_df.replace(float('nan'), None, inplace=True)
                update_data = updates_df.rename(columns=rename_dict)[
                    list(rename_dict.values())
                ].to_dict(orient="records")
                
                conn.execute(stmt_update_materials, update_data) # type: ignore

            if not inserts_df.empty:
                Logger().info(f"Inserting {len(inserts_df)} new materials")
                rename_dict_insert = {
                    df_col: db_col 
                    for db_col, df_col in self.COLUMN_MAPPING.items()
                    if df_col is not None
                }
                
                inserts_df.replace(float('nan'), None, inplace=True)
                insert_data = inserts_df.rename(columns=rename_dict_insert)[
                    list(rename_dict_insert.values())
                ].to_dict(orient="records")
                
                conn.execute(stmt_insert_materials, insert_data) # type: ignore

            conn.execute(stmt_update_etl)        
