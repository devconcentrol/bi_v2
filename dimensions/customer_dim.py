import pandas as pd
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.config import Config
from utils.logger import Logger
from utils.date_utils import parse_date
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
    Date,
)

CRLF: str = "\r\n"


class CustomerDim:
    # Mapping from DataFrame columns (lowercase from SQL) to DB columns
    COLUMN_MAPPING = {
        "CustCode": "kunnr",
        "CustName": "name",
        "ZoneId": "ZoneId",  # Mapped column
        "CustGroupId": "CustGroupId",  # Mapped column
        "ZRAgentId": "ZRAgentId",  # Mapped column
        "ZEAgentId": "ZEAgentId",  # Mapped column
        "Country": "land1",
        "SalesOrganization": "vkorg",
        "Channel": "vtweg",
        "Division": "spart",
        "CreatedDate": "crdat",
        "CountryId": "CountryId",  # Mapped column
        "RegionId": "RegionId",  # Mapped column
        "PurchaseGroupId": "PurchaseGroupId",  # Mapped column
        "TaxNum": "taxnum",
        "ZipCode": "pstlz",
        "City": "ort01",
        "Address": "address",
        "Type": "ktokd",
        "CustDivisionId": "CustDivisionId",  # Mapped column
        "ModifiedDate": "chdat",
        # "CustId" is handled separately as it determines insert vs update
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):       
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
        Logger().info(f"Processing customers for date: {yesterday}")

        stmt_update_etl = text(
            f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_customers'"
        )

        sql_get_customers = f"""
                            SELECT KUNNR, NAME, VKORG, VTWEG, SPART, LAND1, BZIRK, KDGRP, ZR, ZE, CRDAT, REGIO, HKUNNR, 
                                   KTOKD, TAXNUM, PSTLZ, ORT01, KVGR2, ADDRESS, CHDAT
                            FROM SAPSR3.ZCON_V_CUSTOMER                            
                            WHERE CHDAT = :yesterday                            
                        """
        results: pd.DataFrame = pd.read_sql(sql_get_customers, self._con_sap, params = {"yesterday": yesterday})

        if results.empty:
            Logger().info("No customers found for processing")
            # Always update ETL Info even if no data
            with self._con_dw.begin() as conn:
                conn.execute(stmt_update_etl)
            return

        # normalize column names to lowercase to make downstream accesses predictable
        results.columns = results.columns.str.lower()       

        metadata: MetaData = MetaData()
        # Define CustomerDim Table
        customer_table: Table = Table(
            self._config.TABLE_CUSTOMER_DIM,
            metadata,
            Column("CustId", Integer, primary_key=True),
            Column("CustCode", String(25)),
            Column("CustName", String),
            Column("ZoneId", Integer),
            Column("CustGroupId", Integer),
            Column("ZRAgentId", Integer),
            Column("ZEAgentId", Integer),
            Column("Country", String(10)),
            Column("SalesOrganization", String(10)),
            Column("Channel", String(10)),
            Column("Division", String(10)),
            Column("CreatedDate", Date),
            Column("CountryId", Integer),
            Column("RegionId", Integer),
            Column("PurchaseGroupId", Integer),
            Column("TaxNum", String(50)),
            Column("ZipCode", String(50)),
            Column("City", String(150)),
            Column("Address", String),
            Column("Type", String(10)),
            Column("ModifiedDate", Date),
            Column("CustDivisionId", Integer, nullable=True),
        )

        # Prepare maps
        customer_map = self._lookup.get_customer_map()
        zone_map = self._lookup.get_zone_map()
        group_map = self._lookup.get_group_map()
        agent_map = self._lookup.get_agent_map()
        country_map = self._lookup.get_country_map()
        division_map = self._lookup.get_cust_division_map()
        region_map = self._lookup.get_region_map()

        # Transformation
        results["name"] = results["name"].str.replace(CRLF, "", regex=False)
        results["crdat"] = results["crdat"].apply(lambda x: parse_date(x))
        results["chdat"] = results["chdat"].apply(lambda x: parse_date(x))

        # Lookups
        results["search_key"] = (
            results["vkorg"] + results["vtweg"] + results["spart"] + results["kunnr"]
        )
        results["CustId"] = results["search_key"].map(customer_map)

        results["CustDivisionId"] = results["kvgr2"].map(division_map)

        results["purch_search_key"] = (
            results["vkorg"] + results["vtweg"] + results["spart"] + results["hkunnr"]
        ).where(results["hkunnr"].notna() & (results["hkunnr"] != ""), None)
        results["PurchaseGroupId"] = results["purch_search_key"].map(customer_map)

        results["ZoneId"] = results["bzirk"].map(zone_map)
        results["CustGroupId"] = results["kdgrp"].map(group_map)

        results["ZRAgentId"] = (
            ("ZR" + results["zr"])
            .where(results["zr"].notna() & (results["zr"] != ""), None)
            .map(agent_map)
        )

        results["ZEAgentId"] = (
            ("ZE" + results["ze"])
            .where(results["ze"].notna() & (results["ze"] != ""), None)
            .map(agent_map)
        )

        results["CountryId"] = results["land1"].map(country_map)

        # Region Key Logic Improvement
        # Safely convert CountryId to string for key generation
        # 'Int64' roughly matches nullable integer behavior in recent pandas versions,
        # preventing '.0' suffixes from float conversions of integers-with-NaNs.
        results["region_key"] = (
            results["CountryId"].astype("Int64").astype(str) + results["regio"]
        ).where(
            results["CountryId"].notna()
            & results["regio"].notna()
            & (results["regio"] != ""),
            None,
        )
        results["RegionId"] = results["region_key"].map(region_map)

        # Split into updates and inserts
        updates_df = results[results["CustId"].notna()]
        inserts_df = results[results["CustId"].isna()]

        stmt_insert_customers: Insert = insert(customer_table)

        # Build update values dynamically using COLUMN_MAPPING
        update_values = {
            db_col: bindparam(f"b_{db_col}") for db_col in self.COLUMN_MAPPING.keys() if db_col not in ["CustCode", "CreatedDate"]
        }
        stmt_update_customers: Update = (
            update(customer_table)
            .where(customer_table.c.CustId == bindparam("b_CustId"))
            .values(**update_values)
        )

        with self._con_dw.begin() as conn:
            if not updates_df.empty:
                # Prepare update data
                # We rename DataFrame columns to match bindparams (b_ColName)
                rename_dict = {
                    df_col: f"b_{db_col}"
                    for db_col, df_col in self.COLUMN_MAPPING.items() if db_col not in ["CustCode", "CreatedDate"]
                }
                rename_dict["CustId"] = "b_CustId"  # Key for update
                
                updates_df.replace(float('nan'), None, inplace=True)
                update_data = updates_df.rename(columns=rename_dict)[
                    list(rename_dict.values())
                ].to_dict(orient="records")

                conn.execute(stmt_update_customers, update_data)  # type: ignore

            if not inserts_df.empty:
                Logger().info(f"Inserting {len(inserts_df)} new customers")
                # Prepare insert data
                # We simply map DataFrame columns to DB columns for insert
                rename_dict_insert = {
                    df_col: db_col for db_col, df_col in self.COLUMN_MAPPING.items()
                }

                inserts_df.replace(float('nan'), None, inplace=True)
                insert_data = inserts_df.rename(columns=rename_dict_insert)[
                    list(rename_dict_insert.values())
                ].to_dict(orient="records")

                conn.execute(stmt_insert_customers, insert_data)  # type: ignore

            conn.execute(stmt_update_etl)
