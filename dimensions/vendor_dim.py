import pandas as pd
from datetime import date, timedelta
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.config import Config
from utils.logger import Logger
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

class VendorDim:
    COLUMN_MAPPING = {
        "VendCode": "lifnr",
        "VendName": "name",
        "CountryId": "CountryId",  # Mapped
        "RegionId": "RegionId",  # Mapped
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Starting vendor processing")
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

        stmt_update_etl = text(
             f"""UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_vendors'"""
        )

        # Load Source Data
        sql_get_vendors = """
                            SELECT LIFNR, NAME, LAND1, REGIO
                            FROM SAPSR3.ZCON_V_VENDOR                                                       
                            WHERE CHDAT = :yesterday
                        """

        results: pd.DataFrame = pd.read_sql(sql_get_vendors, con=self._con_sap, params={"yesterday": yesterday})

        if results.empty:
            Logger().info("No vendors found for processing")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_update_etl)
            return

        # Normalize columns
        results.columns = results.columns.str.lower()
        
        # Load Maps
        country_map = self._lookup.get_country_map()
        region_map = self._lookup.get_region_map()

        # Define the table
        metadata: MetaData = MetaData()

        # Define VendorDim Table
        vendor_table: Table = Table(
            self._config.TABLE_VENDOR_DIM,
            metadata,
            Column("VendId", Integer, primary_key=True),
            Column("VendCode", String(25)),
            Column("VendName", String),
            Column("CountryId", Integer),
            Column("RegionId", Integer),
        )

        # Transformation & Vectorization
        results["lifnr"] = results["lifnr"].astype(str)
        results["name"] = results["name"].fillna("")
        
        # Lookups
        results["CountryId"] = results["land1"].map(country_map)
        
        # Region lookup requires composite key: CountryId (from map) + RegionCode (from source)
        # Handle cases where CountryId might be null (NaN) which converts to float. 
        # Safest is to use the mapped column if available, else fillna.
        results["temp_country_id"] = results["CountryId"].fillna(0).astype("Int64").astype(str)
        results["region_search_key"] = results["temp_country_id"] + results["regio"]
        results["RegionId"] = results["region_search_key"].map(region_map)
        
        # Vendor Lookup for Insert/Update
        vendor_map = self._lookup.get_vendor_map()
        results["VendId"] = results["lifnr"].map(vendor_map)

        # Split Updates/Inserts
        updates_df = results[results["VendId"].notna()]
        inserts_df = results[results["VendId"].isna()]

        stmt_insert_vendors: Insert = insert(vendor_table)

        # Build update values dynamically using COLUMN_MAPPING
        update_values = {
            db_col: bindparam(f"b_{db_col}") 
            for db_col, df_col in self.COLUMN_MAPPING.items() 
            if df_col is not None 
        }
        
        stmt_update_vendors: Update = (
            update(vendor_table)
            .where(vendor_table.c.VendId == bindparam("b_VendId"))
            .values(**update_values)
        )

        with self._con_dw.begin() as conn:
            if not updates_df.empty:
                rename_dict = {
                    df_col: f"b_{db_col}"
                    for db_col, df_col in self.COLUMN_MAPPING.items()
                    if df_col is not None
                }
                rename_dict["VendId"] = "b_VendId"

                update_data = updates_df.rename(columns=rename_dict)[
                    list(rename_dict.values())
                ].to_dict(orient="records")
                
                conn.execute(stmt_update_vendors, update_data) # type: ignore

            if not inserts_df.empty:
                Logger().info(f"Inserting {len(inserts_df)} new vendors")
                rename_dict_insert = {
                    df_col: db_col 
                    for db_col, df_col in self.COLUMN_MAPPING.items()
                    if df_col is not None
                }

                insert_data = inserts_df.rename(columns=rename_dict_insert)[
                    list(rename_dict_insert.values())
                ].to_dict(orient="records")
                
                conn.execute(stmt_insert_vendors, insert_data) # type: ignore

            conn.execute(stmt_update_etl)
        
        self._lookup.invalidate_vendor_cache()
