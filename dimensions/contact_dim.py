import pandas as pd
from datetime import date
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from utils.config import Config
from utils.logger import Logger
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
)


class ContactDim:
    COLUMN_MAPPING = {
        "ContactCode": "partner",
        "ContactName": "name_first",
        "CustId": "CustId",
        "ContactEmail": "smtp_address",
        "CreatedDate": "crdat",
        "PhoneNumber": "tel_number",
        "VIP": "pavip",
        "ZipCode": "post_code1",
        "City": "city1",
        "Address": "address",
        "CountryId": "CountryId",
    }

    def __init__(self, con_dw: Engine, con_sap: Engine, lookup: DimensionLookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    @error_handler
    def run(self):
        Logger().info("Starting contact processing")
        
        stmt_update_etl = text(
             f"""UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_contacts'"""
        )

        sql_get_customers = """
                            SELECT PARTNER,
                                   NAME_FIRST,
                                   KUNNR,                                   
                                   SMTP_ADDRESS,
                                   TEL_NUMBER,
                                   PAVIP,      
                                   POST_CODE1,                             
                                   CITY1,
                                   ADDRESS,
                                   COUNTRY,
                                   CRDAT       
                            FROM SAPSR3.ZCON_V_CUSTOMER_CONTACTS                                                        
                        """

        results: pd.DataFrame = pd.read_sql(sql_get_customers, con=self._con_sap)

        if results.empty:
            Logger().info("No contacts found for processing")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_update_etl)
            return

        #Normalize columns
        results.columns = results.columns.str.lower()
        
        # Load Maps
        customer_map = self._lookup.get_customer_map()
        country_map = self._lookup.get_country_map()

        # Define the table
        metadata: MetaData = MetaData()

        # Define ContactDim Table
        contacts_table: Table = Table(
            self._config.TABLE_CONTACT_DIM,
            metadata,
            Column("ContactId", Integer, primary_key=True),
            Column("ContactCode", String(25)),
            Column("ContactName", String),
            Column("CustId", Integer),
            Column("ContactEmail", String),
            Column("CreatedDate", Date),
            Column("PhoneNumber", String(25)),
            Column("VIP", String(10)),
            Column("ZipCode", String(50)),
            Column("City", String(150)),
            Column("Address", String),
            Column("CountryId", Integer),
        )

        # Transformation
        results["crdat"] = pd.to_datetime(
            results["crdat"], format="%Y%m%d", errors="coerce"
        ).dt.date
        results["address"] = results["address"].str.strip()

        # Lookups
        # Complete customer search key: Standard Constants + KUNNR
        results["customer_search_key"] = (
            self._config.DEFAULT_SALES_ORG 
            + self._config.DEFAULT_CHANNEL 
            + self._config.DEFAULT_DIVISION 
            + results["kunnr"]
        )

        results["CustId"] = results["customer_search_key"].map(customer_map)
        results["CountryId"] = results["country"].map(country_map)
        
# Replace nan values in float columns
        results.replace(float("nan"), None, inplace=True)
        
        # Truncate and Insert logic
        stmt_insert_contacts: Insert = insert(contacts_table)

        with self._con_dw.begin() as conn:
            # Truncate Table
            conn.execute(text(f"TRUNCATE TABLE {self._config.TABLE_CONTACT_DIM}"))
            
            Logger().info(f"Inserting {len(results)} contacts")
            
            rename_dict_insert = {
                df_col: db_col 
                for db_col, df_col in self.COLUMN_MAPPING.items()
                if df_col is not None
            }
            
            insert_data = results.rename(columns=rename_dict_insert)[
                list(rename_dict_insert.values())
            ]
            
            # Replace empty strings with None
            insert_data = insert_data.replace({"": None})
            insert_records = insert_data.to_dict(orient="records")
            
            if insert_records:
                conn.execute(stmt_insert_contacts, insert_records) # type: ignore

            conn.execute(stmt_update_etl)
