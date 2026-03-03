import pandas as pd
from datetime import date
from zoneinfo import ZoneInfo
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Integer,
    DECIMAL,
    Date,
    Time,
    insert,
    text,
)
from base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class EWMLocationsFactETL(BaseFactETL):
    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "lgnum": "WarehouseNumber",
        "lgtyp": "StorageType",
        "lgpla": "StorageBin",
        "IsEmpty": "IsEmpty",
        "IsFull": "IsFull",
        "btanr": "LastTask",
        "IsBlockedForRemoval": "IsBlockedForRemoval",
        "IsBlockedForPutAway": "IsBlockedForPutAway",
        "MovedAt": "MovedAt",
        "MovedTime": "MovedTime",
        "MaterialId": "MaterialId",
        "charg": "Batch",
        "vsolm": "Qty",
        "meins": "UnitId",
        "FactDate": "FactDate",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing EWM Locations Fact...")

        # SQL to retrieve information from SAP view
        sql_get_locations = """
            SELECT LGNUM,
                   LGPLA,
                   LGTYP,
                   KZLER, --Vacía
                   KZVOL, --Completa
	               BTANR, --Última tarea
	               SKZUA, --Bloqueado salida de stock
	               SKZUE, --Bloqueado entrada de stock
	               MOVED_AT, --Datetime confirmación con UTC aplicada,                   
                   MATNR,
                   CHARG,
                   VSOLM,
                   MEINS
            FROM SAPSR3.ZCON_V_EWM_LOCATIONS
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_locations, con=self._con_sap, dtype_backend="numpy_nullable"
        )

        if results.empty:
            Logger().info("No EWM locations found.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_ewm_locations")
            return

        # Normalize column names to lowercase
        results.columns = results.columns.str.lower()

        # Data Transformations
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].astype(str)
        results["MaterialId"] = results["matnr"].map(material_map).convert_dtypes()

        results["IsEmpty"] = results["kzler"] == "X"
        results["IsFull"] = results["kzvol"] == "X"
        results["IsBlockedForRemoval"] = results["skzua"] == "X"
        results["IsBlockedForPutAway"] = results["skzue"] == "X"

        # Handle MOVED_AT timestamp transformation
        # If it's already a datetime from SAP HANA, we just convert timezone.
        # Assuming convert_sap_ts pattern if it's a string, but the comment says "Datetime confirmación".
        # Let's use pd.to_datetime and convert to local timezone.
        if "moved_at" in results.columns:
            moved_dt = pd.to_datetime(
                results["moved_at"].fillna(0).astype(int).astype(str),
                format="%Y%m%d%H%M%S",
                errors="coerce",
            ).convert_dtypes()
            # Localize to Europe/Madrid if it's naive UTC as the comment suggest "UTC aplicada"
            tz_utc = ZoneInfo("UTC")
            tz_local = ZoneInfo("Europe/Madrid")

            if moved_dt.dt.tz is None:
                moved_dt = moved_dt.dt.tz_localize(tz_utc).dt.tz_convert(tz_local)
            else:
                moved_dt = moved_dt.dt.tz_convert(tz_local)

            results["MovedAt"] = moved_dt.dt.date
            results["MovedTime"] = moved_dt.dt.time
        else:
            results["MovedAt"] = None
            results["MovedTime"] = None

        results["FactDate"] = date.today()

        # Ensure numeric Qty
        results["vsolm"] = pd.to_numeric(results["vsolm"], errors="coerce").fillna(0)

        # Rename columns based on mapping
        results = results.rename(columns=self.COLUMN_MAPPING)

        # Select columns that are in the mapping values
        final_cols = list(self.COLUMN_MAPPING.values())
        insert_data = (
            results[final_cols]
            .where(pd.notnull(results), None)
            .to_dict(orient="records")
        )

        # Define the table for insertion
        metadata: MetaData = MetaData()
        ewm_locations_table: Table = Table(
            self._config.TABLE_EWM_LOCATIONS_FACT,
            metadata,
            Column("WarehouseNumber", String(10)),
            Column("StorageType", String(10)),
            Column("StorageBin", String(25)),
            Column("IsEmpty", String(1)),
            Column("IsFull", String(1)),
            Column("LastTask", String(15)),
            Column("IsBlockedForRemoval", String(1)),
            Column("IsBlockedForPutAway", String(1)),
            Column("MovedAt", Date),
            Column("MovedTime", Time),
            Column("MaterialId", Integer),
            Column("Batch", String(25)),
            Column("Qty", DECIMAL(15, 4)),
            Column("UnitId", String(10)),
            Column("FactDate", Date),
        )

        # Truncate and Insert strategy
        stmt_truncate = text(f"TRUNCATE TABLE {self._config.TABLE_EWM_LOCATIONS_FACT}")

        with self._con_dw.begin() as conn:
            Logger().info(f"Inserting {len(insert_data)} EWM locations...")
            conn.execute(stmt_truncate)
            if insert_data:
                conn.execute(insert(ewm_locations_table), insert_data)  # type: ignore
            self._update_etl_info(conn, "process_ewm_locations")
