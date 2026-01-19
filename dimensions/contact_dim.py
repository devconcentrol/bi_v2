import pandas as pd
from datetime import datetime
from utils.error_handler import error_handler
from utils.dimension_lookup import DimensionLookup
from sqlalchemy import (
    Delete,
    bindparam,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    delete,
    update,
    Engine,
    insert,
    text,
    Insert,
    Update,
    Date,
)


class Contacts:
    @staticmethod
    @error_handler
    def process_contacts(con_dw: Engine, con_sap: Engine):
        STANDARD_SALES_ORG = "1000"
        STANDARD_CHANNEL = "10"
        STANDARD_DIVISION = "10"

        dimension_lookup = DimensionLookup(con_dw=con_dw)

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

        results: pd.DataFrame = pd.read_sql(sql_get_customers, con_sap)

        # # Define the table
        metadata: MetaData = MetaData()

        # Define ContactDim Table
        contacts_table: Table = Table(
            "ContactDim",
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

        if results.empty:
            with con_dw.connect() as conn:
                conn.execute(text("TRUNCATE TABLE ContactDim"))
                conn.execute(
                    text(
                        "UPDATE ETLInfo SET ProcessDate = GETDATE() WHERE ETL = 'process_contacts'"
                    )
                )
                conn.commit()
            return

        # Prepare maps
        customer_map = dimension_lookup.get_customer_map()
        country_map = dimension_lookup.get_country_map()

        # Transformation
        results["crdat"] = pd.to_datetime(
            results["crdat"], format="%Y%m%d", errors="coerce"
        ).dt.date
        results["address"] = results["address"].str.strip()

        # Lookups
        # Complete customer search key: VKORG (1000) + CHANNEL (10) + DIVISION (10) + KUNNR
        results["customer_search_key"] = (
            STANDARD_SALES_ORG + STANDARD_CHANNEL + STANDARD_DIVISION + results["kunnr"]
        )
        results["CustId"] = results["customer_search_key"].map(customer_map)
        results["CountryId"] = results["country"].map(country_map)

        # Prepare insert data
        insert_data = results.rename(
            columns={
                "partner": "ContactCode",
                "name_first": "ContactName",
                "smtp_address": "ContactEmail",
                "crdat": "CreatedDate",
                "tel_number": "PhoneNumber",
                "pavip": "VIP",
                "post_code1": "ZipCode",
                "city1": "City",
                "address": "Address",
            }
        )[
            [
                "ContactCode",
                "ContactName",
                "CustId",
                "ContactEmail",
                "CreatedDate",
                "PhoneNumber",
                "VIP",
                "ZipCode",
                "City",
                "Address",
                "CountryId",
            ]
        ]
        
        # Replace empty strings with None for database compatibility
        insert_data = insert_data.replace({"": None})
        insert_records = insert_data.to_dict(orient="records")

        stmt_insert_contacts: Insert = insert(contacts_table)
        stmt_truncate = text("TRUNCATE TABLE ContactDim")
        stmt_update_etl = text(
            "UPDATE ETLInfo SET ProcessDate = GETDATE() WHERE ETL = 'process_contacts'"
        )

        with con_dw.connect() as conn:
            conn.execute(stmt_truncate)
            if len(insert_records) > 0:
                conn.execute(stmt_insert_contacts, insert_records)

            conn.execute(stmt_update_etl)
            conn.commit()
