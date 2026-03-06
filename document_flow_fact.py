import pandas as pd
from datetime import datetime
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Integer,
    DECIMAL,
    Date,
    insert,
    text,
)
from base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class DocumentFlowFactETL(BaseFactETL):
    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "vkorg": "SalesOrganization",
        "CustId": "CustId",
        "salesid": "SalesId",
        "vbeln": "Document",
        "vbtyp_n": "DocumentType",
        "DocumentDate": "DocumentDate",
        "MaterialId": "MaterialId",
        "klmeng": "Qty",
        "meins": "UnitId",
        "netwr": "NetValue",
        "waerk": "Currency",
        "kbetr": "Price",
        "kmein": "PriceUnit",
        "waers": "PriceCurrency",
        "CreatedDate": "CreatedDate",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Document Flow Fact...")

        # 1. Date Calculation in Python
        # Logic: From the 1st of 2 months ago
        now = datetime.now()
        first_day_current = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        # Calculate beginning of 2 months ago
        if now.month > 2:
            cutoff_date = first_day_current.replace(month=now.month - 2)
        elif now.month == 2:
            cutoff_date = first_day_current.replace(year=now.year - 1, month=12)
        else:  # month == 1
            cutoff_date = first_day_current.replace(year=now.year - 1, month=11)

        cutoff_sap = cutoff_date.strftime("%Y%m%d")
        cutoff_dw = cutoff_date.strftime("%Y-%m-%d")

        # 2. Extract Data from SAP
        sql_get_document_flow = """
            SELECT VKORG,
                   VTWEG,
                   SPART,
                   KUNNR,
                   SALESID,
                   VBELN,
                   VBTYP_N,
                   FKDAT,
                   KLMENG,
                   MEINS,                                                                                                                                            
                   MATNR,
                   NETWR,
                   WAERK,
                   KBETR,
                   KMEIN,
                   WAERS,
                   ERDAT                                                                      
            FROM SAPSR3.ZCON_V_DOCUMENT_FLOW                             
            WHERE ERDAT >= :cutoff_sap
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_document_flow,
            con=self._con_sap,
            params={"cutoff_sap": cutoff_sap},
            dtype_backend="numpy_nullable",
        )

        # 3. Handle Deletion in Data Warehouse
        stmt_delete = text(
            f"DELETE FROM {self._config.TABLE_DOCUMENT_FLOW_FACT} WHERE CreatedDate >= :cutoff_dw"
        )

        if results.empty:
            Logger().info("No document flow records found.")
            with self._con_dw.begin() as conn:
                conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
                self._update_etl_info(conn, "process_document_flow")
            return

        # 4. Transform Data
        results.columns = results.columns.str.lower()

        # Material ID Mapping
        material_map = self._lookup.get_material_map()
        results["matnr"] = (
            results["matnr"].astype(str).str.replace("1000A", "MA", regex=False)
        )
        results["MaterialId"] = results["matnr"].map(material_map)

        # Customer ID Mapping
        customer_map = self._lookup.get_customer_map()
        # Key: SalesOrganization + Channel + Division + CustCode
        results["CustKey"] = (
            results["vkorg"].fillna("").astype(str)
            + results["vtweg"].fillna("").astype(str)
            + results["spart"].fillna("").astype(str)
            + results["kunnr"].fillna("").astype(str)
        )
        results["CustId"] = results["CustKey"].map(customer_map)

        results["CustKey2"] = (
            results["vkorg"].astype(str)
            + self._config.DEFAULT_CHANNEL
            + self._config.DEFAULT_DIVISION
            + results["kunnr"].astype(str)
        )
        results["CustId_Fallback"] = results["CustKey2"].map(customer_map)
        results["CustId"] = results["CustId"].fillna(results["CustId_Fallback"])

        # Date transformations
        results["DocumentDate"] = (
            pd.to_datetime(results["fkdat"].astype(str), errors="coerce")
            .dt.strftime("%Y%m%d")
            .convert_dtypes()
        )

        results["CreatedDate"] = (
            pd.to_datetime(results["erdat"].astype(str), errors="coerce")
            .dt.strftime("%Y%m%d")
            .convert_dtypes()
        )

        # Ensure numeric columns
        numeric_cols = ["klmeng", "netwr", "kbetr"]
        for col in numeric_cols:
            results[col] = pd.to_numeric(results[col], errors="coerce").fillna(0)

        # Rename and Select Columns
        results_final = results.rename(columns=self.COLUMN_MAPPING)
        final_cols = list(self.COLUMN_MAPPING.values())

        insert_records = (
            results_final[final_cols]
            .where(pd.notnull(results_final[final_cols]), None)
            .to_dict(orient="records")
        )

        # 5. Load Data to Data Warehouse
        metadata: MetaData = MetaData()
        document_flow_table: Table = Table(
            self._config.TABLE_DOCUMENT_FLOW_FACT,
            metadata,
            Column("SalesOrganization", String(10)),
            Column("CustId", Integer),
            Column("SalesId", String(25)),
            Column("Document", String(25)),
            Column("DocumentType", String(2)),
            Column("DocumentDate", Date),
            Column("MaterialId", Integer),
            Column("Qty", DECIMAL(15, 4)),
            Column("UnitId", String(10)),
            Column("NetValue", DECIMAL(15, 4)),
            Column("Currency", String(10)),
            Column("Price", DECIMAL(15, 4)),
            Column("PriceUnit", String(10)),
            Column("PriceCurrency", String(10)),
            Column("CreatedDate", Date),
        )

        with self._con_dw.begin() as conn:
            Logger().info(
                f"Deleting and inserting {len(insert_records)} document flow records."
            )
            conn.execute(stmt_delete, {"cutoff_dw": cutoff_dw})
            if insert_records:
                conn.execute(insert(document_flow_table), insert_records)

            self._update_etl_info(conn, "process_document_flow")
