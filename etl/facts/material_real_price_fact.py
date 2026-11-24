import pandas as pd
from sqlalchemy import text

from etl.base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class MaterialRealPriceFactETL(BaseFactETL):
    # Rename and select columns to match EWMTaskFact
    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "bwkey": "PlantId",
        "MaterialId": "MaterialId",
        "curtp": "CurrencyType",
        "pvprs": "RealPrice",
        "peinh": "PriceQuantity",
        "period": "PriceDate",
        "CanCalculate": "CanCalculate",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Material Real Price...")

        # Get the cutoff date
        today = pd.Timestamp.now()
        first_day_current = today.replace(day=1)
        cutoff_date = first_day_current - pd.DateOffset(months=3)
        cutoff_date_sap = cutoff_date.strftime("%Y%m%d")

        sql_get_material_real_price = """
                            SELECT bwkey,
                                matnr,
                                curtp,
                                pvprs,
                                peinh,
                                period,
                                xclose
                            FROM SAPSR3.ZCON_V_MATERIAL_REAL_PRICE
                            WHERE period >= :cutoff_date_sap                            
                        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_material_real_price,
            con=self._con_sap,
            params={"cutoff_date_sap": cutoff_date_sap},
        )

        # Logger().info(
        #     "SAP returned %d material real price rows since %s:\n%s",
        #     len(results),
        #     cutoff_date_sap,
        #     results.to_string(index=False),
        # )

        if results.empty:
            Logger().info("No material real price data found in SAP.")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_material_real_price")
            return

        # Normalize columns
        results.columns = results.columns.str.lower()

        # 2. Data Transformation
        Logger().info("Transforming data...")

        # Matnr replacement
        results["matnr"] = results["matnr"].str.replace("1000A", "MA")

        # Lookup MaterialId
        material_map = self._lookup.get_material_map()
        results["matnr"] = results["matnr"].fillna("").astype(str)
        results["MaterialId"] = results["matnr"].map(material_map)
        results["CanCalculate"] = results["xclose"].astype(str).eq("X").fillna(False)

        # Filter out missing materials (was printed in loop before)
        missing_materials = results[results["MaterialId"].isna()]
        if not missing_materials.empty:
            Logger().warning(
                f"Dropped {len(missing_materials)} rows due to missing MaterialId."
            )
        results = results.dropna(subset=["MaterialId"])

        # Date parsing
        Logger().info("Parsing period to date...")
        results["period"] = pd.to_datetime(
            results["period"], format="%Y%m%d", errors="coerce"
        ).dt.date

        table_name = self._config.TABLE_MATERIAL_REAL_PRICE_FACT
        columns = list(self.COLUMN_MAPPING.values())
        key_columns = ["PlantId", "MaterialId", "CurrencyType", "PriceDate"]
        records = (
            results.rename(columns=self.COLUMN_MAPPING)[columns]
            .drop_duplicates(subset=key_columns, keep="last")
            .astype(object)
        )
        rows = list(
            records.where(records.notna(), None).itertuples(index=False, name=None)
        )

        column_list = ", ".join(columns)
        key_list = ", ".join(key_columns)
        source_columns = ", ".join(f"source.{column}" for column in columns)
        match_key = " AND ".join(
            f"target.{column} = source.{column}" for column in key_columns
        )

        with self._con_dw.begin() as conn:
            if rows:
                Logger().info(f"Staging {len(rows)} material real prices.")
                conn.execute(text(f"""
                    SELECT TOP (0) {column_list}
                    INTO #MaterialRealPriceStage
                    FROM {table_name}
                    """))

                # Seven parameters per row, below SQL Server's 2,100 limit.
                batch_size = 250
                for offset in range(0, len(rows), batch_size):
                    batch = rows[offset : offset + batch_size]
                    parameters = {
                        f"p{row_index}_{column_index}": value
                        for row_index, row in enumerate(batch)
                        for column_index, value in enumerate(row)
                    }
                    values = ", ".join(
                        "("
                        + ", ".join(
                            f":p{row_index}_{column_index}"
                            for column_index in range(len(columns))
                        )
                        + ")"
                        for row_index in range(len(batch))
                    )
                    conn.execute(
                        text(f"""
                            INSERT INTO #MaterialRealPriceStage ({column_list})
                            VALUES {values}
                            """),
                        parameters,
                    )

                conn.execute(text(f"""
                    CREATE UNIQUE CLUSTERED INDEX IX_MaterialRealPriceStage
                    ON #MaterialRealPriceStage ({key_list})
                    """))
                Logger().info("Applying staged material real prices.")
                conn.execute(text(f"""
                    UPDATE target
                    SET RealPrice = source.RealPrice,
                        PriceQuantity = source.PriceQuantity,
                        CanCalculate = source.CanCalculate
                    FROM {table_name} AS target WITH (UPDLOCK, HOLDLOCK)
                    JOIN #MaterialRealPriceStage AS source ON {match_key}
                    WHERE target.CalculatedDate IS NULL
                      AND EXISTS (
                          SELECT source.RealPrice, source.PriceQuantity,
                                 source.CanCalculate
                          EXCEPT
                          SELECT target.RealPrice, target.PriceQuantity,
                                 target.CanCalculate
                      )
                    """))
                conn.execute(text(f"""
                    INSERT INTO {table_name} ({column_list})
                    SELECT {source_columns}
                    FROM #MaterialRealPriceStage AS source
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {table_name} AS target WITH (UPDLOCK, HOLDLOCK)
                        WHERE {match_key}
                    )
                    """))
                conn.execute(text("DROP TABLE #MaterialRealPriceStage"))

            self._update_etl_info(conn, "process_material_real_price")
            Logger().info("Material Real Price ETL completed successfully.")
