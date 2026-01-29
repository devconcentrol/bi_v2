import os
import shutil
import glob
import pandas as pd
from sqlalchemy import Engine, text
from utils.dimension_lookup import DimensionLookup
from utils.logger import Logger
from utils.error_handler import error_handler
from utils.config import Config


class CostingFactETL:
    def __init__(self, engine: Engine, lookup: DimensionLookup):        
        self._engine: Engine = engine
        self._lookup: DimensionLookup = lookup
        self._config = Config.get_instance()

        self.LOADED_DIR = "loaded"

    @error_handler
    def run(self):
        """
        Processes all files matching the pattern in the given directory,
        transforms them, uploads to DB, and moves them to 'loaded'.
        """
        directory = self._config.COSTING_PATH
        pattern = os.path.join(directory, "*.xlsx")
        files = glob.glob(pattern)

        if not files:
            Logger().info(f"No files found matching pattern: {pattern}")
            return

        os.makedirs(self.LOADED_DIR, exist_ok=True)

        # Initialize maps once for efficiency
        customer_map = self._lookup.get_customer_map()
        material_map = self._lookup.get_material_map()

        for file_path in files:
            # Skip directories or Zone.Identifier files
            if os.path.isdir(file_path):
                continue

            Logger().info(f"Processing file: {file_path}")

            # 1. Load Excel Data
            df = pd.read_excel(
                file_path,
                names=[
                    "CostingDate",
                    "CustomerCode",
                    "MaterialCode",
                    "SalesOrganization",
                    "NetWeight",
                    "TotalIncome",
                    "TotalCOGS",
                    "Transport",
                    "Commission",
                ],
            )

            # 2. Transform Data            
            df["CostingDate"] = pd.to_datetime(df["CostingDate"], format="%Y%m%d", errors="coerce").dt.date

            # Map Customers
            df["CustKey"] = (
                df["SalesOrganization"].astype(str)
                + self._config.DEFAULT_CHANNEL
                + self._config.DEFAULT_DIVISION
                + df["CustomerCode"].astype(str)
            )
            df["CustId"] = df["CustKey"].map(customer_map)

            # Map Materials
            df["MaterialId"] = df["MaterialCode"].astype(str).map(material_map)

            # Handle missing IDs
            missing_customers = df[df["CustId"].isna()]["CustomerCode"].unique()
            if len(missing_customers) > 0:
                Logger().warning(
                    f"Missing customer IDs in {os.path.basename(file_path)}: {missing_customers}"
                )

            missing_materials = df[df["MaterialId"].isna()]["MaterialCode"].unique()
            if len(missing_materials) > 0:
                Logger().warning(
                    f"Missing material IDs in {os.path.basename(file_path)}: {missing_materials}"
                )

            # Select and order columns
            final_cols = [
                "CostingDate",
                "CustId",
                "MaterialId",
                "SalesOrganization",
                "NetWeight",
                "TotalIncome",
                "TotalCOGS",
                "Transport",
                "Commission",
            ]
            df_upload = df[final_cols].copy()

            # 3. Upload to SQL Server
            Logger().info(
                f"Uploading {len(df_upload)} rows from {os.path.basename(file_path)}..."
            )
            df_upload.to_sql(
                self._config.TABLE_COSTING_FACT, self._engine, if_exists="append", index=False
            )

            # 4. Move to loaded
            dest_path = os.path.join(self.LOADED_DIR, os.path.basename(file_path))
            if os.path.exists(dest_path):
                os.remove(dest_path)
            shutil.move(file_path, dest_path)
            Logger().info(f"Processed and moved: {os.path.basename(file_path)}")

        # Update ETLInfo
        stmt_update_etl = text(
            f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = 'process_costing'"
        )
        with self._engine.begin() as conn:
            conn.execute(stmt_update_etl)            
