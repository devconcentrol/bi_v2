import pandas as pd
from utils.error_handler import error_handler
from utils.logger import Logger
from base_fact_etl import BaseFactETL
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    insert,
    text,
    Insert,
    Date,
    DECIMAL,
    DateTime,
)


class QMAdjustmentFactETL(BaseFactETL):
    COLUMN_MAPPING = {
        "aufnr": "OrderNumber",
        "MaterialId": "MaterialId",
        "charg": "BatchNumber",
        "gstrp": "AdjustmentDate",
        "auart": "OrderType",
        "MaterialAdjId": "MaterialAdjId",
        "bdmng": "Qty",
        "meins": "UnitId",
        "cpd_updat": "CreatedDateTime",
    }

    @error_handler
    def run(self):
        Logger().info("Processing QM Adjustments Fact...")

        # Calculate cutoff date: Start of current month minus 2 months
        today = pd.Timestamp.now()
        first_day_current = today.replace(day=1)
        cutoff_date = first_day_current - pd.DateOffset(months=2)
        cutoff_date_sap = cutoff_date.strftime("%Y%m%d")

        sql_get_adjustments = """
            SELECT AUFNR,
                  AUART,
                  GSTRP,
                  MATNR,
                  CHARG,
                  MATNR_ADJ,
                  BDMNG,
                  MEINS,
                  CPD_UPDAT                                    
            FROM SAPSR3.ZCON_V_QM_ADJUSTMENT            
            WHERE GSTRP >= :cutoff_date
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_adjustments,
            con=self._con_sap,
            params={"cutoff_date": cutoff_date_sap},
        )

        if results.empty:
            Logger().info("No QM Adjustments data found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_qm_adjustment")
            return

        results.columns = results.columns.str.lower()

        # Parse Dates
        results["gstrp"] = pd.to_datetime(
            results["gstrp"], format="%Y%m%d", errors="coerce"
        ).dt.date

        # Parse DateTime
        # Original: datetime.strptime(str(int(row["cpd_updat"])), "%Y%m%d%H%M%S")
        # Ensure cpd_updat is string or numeric first
        if "cpd_updat" in results.columns:
            results["cpd_updat"] = (
                pd.to_numeric(results["cpd_updat"], errors="coerce")
                .fillna(0)
                .astype(int)
                .astype(str)
            )
            # Pad with leading zeros if necessary to ensure length is 14? %Y%m%d%H%M%S is 14 chars.
            # SAP timestamps are usually 14 digits.
            results["cpd_updat"] = pd.to_datetime(
                results["cpd_updat"], format="%Y%m%d%H%M%S", errors="coerce"
            )

        if "bdmng" in results.columns:
            results["bdmng"] = pd.to_numeric(results["bdmng"], errors="coerce").fillna(
                0
            )

        # Lookup Material
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)

        results["matnr_adj"] = results["matnr_adj"].astype(str)
        results["MaterialAdjId"] = results["matnr_adj"].map(material_map)

        results = results.rename(columns=self.COLUMN_MAPPING)

        metadata = MetaData()
        qm_adjustment_table = Table(
            self._config.TABLE_QM_ADJUSTMENT_FACT,
            metadata,
            Column("OrderNumber", String(25)),
            Column("MaterialId", Integer),
            Column("BatchNumber", String(50)),
            Column("AdjustmentDate", Date),
            Column("Qty", DECIMAL(15, 4)),
            Column("UnitId", String(10)),
            Column("MaterialAdjId", Integer),
            Column("OrderType", String(10)),
            Column("CreatedDateTime", DateTime),
        )

        final_cols = list(self.COLUMN_MAPPING.values())
        insert_records = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )
        stmt_insert: Insert = insert(qm_adjustment_table)

        # Delete 2 months back
        stmt_delete = text(f"""
                           DELETE FROM {self._config.TABLE_QM_ADJUSTMENT_FACT}
                           WHERE AdjustmentDate >= :cutoff_date
                           """)

        with self._con_dw.begin() as conn:
            if insert_records:
                conn.execute(
                    stmt_delete, {"cutoff_date": cutoff_date.strftime("%Y-%m-%d")}
                )
                Logger().info(
                    f"Inserting {len(insert_records)} records into QM Adjustment Fact."
                )
                conn.execute(stmt_insert, insert_records)

            self._update_etl_info(conn, "process_qm_adjustment")
