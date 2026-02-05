import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DECIMAL,
    Date,
    insert,
    text,
)
from base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class AvailabilityCalculationFactETL(BaseFactETL):
    @error_handler
    def run(self):
        Logger().info("Processing Availability Calculation Fact...")

        # 1. Fetch Sales Orders (Demand)
        # ---------------------------------------------------------------------
        sql_get_sales_orders = """
                            SELECT SalesId,
                                   MaterialId,
                                   CustId,
                                   CAST(GoodsIssueDate AS DATE) AS GoodsIssueDate,
                                   QtyOrdered,
                                   QtyConfirmed
                              FROM V_DW_NEXT_WEEK_SALES                              
                              ORDER BY GoodsIssueDate, QtyConfirmed ASC
                        """
        orders: pd.DataFrame = pd.read_sql(sql_get_sales_orders, con=self._con_dw)

        # Ensure types
        orders["MaterialId"] = pd.to_numeric(orders["MaterialId"], errors="coerce")
        orders["QtyConfirmed"] = pd.to_numeric(
            orders["QtyConfirmed"], errors="coerce"
        ).fillna(0)

        # 2. Fetch Current Stock
        # ---------------------------------------------------------------------
        sql_get_stock = """
                     SELECT MaterialId,
                            UnrestrictedStock,
                            QtyStock                                                 
                     FROM V_DW_DAILY_STOCK
                     WHERE StockDate = CAST(GETDATE() AS DATE)
                       AND Plant = '1000'
                     """
        today_stock_df: pd.DataFrame = pd.read_sql(sql_get_stock, con=self._con_dw)

        # Prepare Stock Map: MaterialId -> UnrestrictedStock
        # We assume one row per material per plant=1000 based on the query structure (StockDate=Today)
        if not today_stock_df.empty:
            # Handle potential duplicates if any (though query suggests unique per material/plant/date)
            today_stock_df = today_stock_df.groupby("MaterialId")[
                ["UnrestrictedStock"]
            ].sum()
            stock_map = today_stock_df["UnrestrictedStock"].to_dict()
        else:
            stock_map = {}

        # 3. Vectorized Stock Allocation (Identify Shortages)
        # ---------------------------------------------------------------------
        # Logic: First Come First Served based on formatting in 'orders' (which is ORDER BY GoodsIssueDate)
        # We calculate cumulative demand per Material.

        # Calculate cumulative demand per material
        orders["CumulativeDemand"] = orders.groupby("MaterialId")[
            "QtyConfirmed"
        ].cumsum()

        # Map available stock to orders
        orders["AvailableStock"] = orders["MaterialId"].map(stock_map).fillna(0)

        # Determine if order is covered by stock
        # Original logic: If QtyConfirmed <= RemainingStock, it is covered. Else, it goes to "ToProcess".
        # In Vectorized terms: If CumulativeDemand <= AvailableStock, it means this order fits entirely in the initial stock block.
        orders["CoveredByStock"] = (
            orders["CumulativeDemand"] <= orders["AvailableStock"]
        )

        # Filter orders that need OF allocation
        # These are the ones where NOT CoveredByStock
        orders_to_process = (
            orders[~orders["CoveredByStock"]].copy().reset_index(drop=True)
        )

        # Note: The original logic dropped the "covered" orders entirely from the result.
        # So we only care about `orders_to_process`.

        # 4. Fetch OFs (Future Supply)
        # ---------------------------------------------------------------------
        sql_get_ofs_liberadas = """
                     SELECT AUFNR_ZSEM,
                            AUFNR_ZPA,
                            MATNR_ZSEM,
                            MATNR_ZPA,
                            GLTRS,
                            GLTRI,
                            GAMNG,
                            WEMNG,
                            CHARG,
                            LGORT,
                            0 AS USED_QTY                            
                     FROM SAPSR3.ZCON_V_OF_LIBERADAS  
                     WHERE GLTRS <= ADD_DAYS(CURRENT_DATE,7)                     
                     ORDER BY MATNR_ZPA, GLTRS                  
                     """
        ofs_liberadas = pd.read_sql(
            sql_get_ofs_liberadas, con=self._con_sap, dtype_backend="numpy_nullable"
        )

        # Transform OFs Vectorized
        if not ofs_liberadas.empty:
            ofs_liberadas.columns = ofs_liberadas.columns.str.lower()

            # Map Material Codes to IDs using Dimensions Lookup
            material_map = self._lookup.get_material_map()
            ofs_liberadas["matnr_zpa"] = (
                ofs_liberadas["matnr_zpa"].fillna("").astype(str)
            )
            ofs_liberadas["material_id"] = ofs_liberadas["matnr_zpa"].map(material_map)

            # Calculate Remaining Qty
            ofs_liberadas["gamng"] = pd.to_numeric(
                ofs_liberadas["gamng"], errors="coerce"
            ).fillna(0)
            ofs_liberadas["wemng"] = pd.to_numeric(
                ofs_liberadas["wemng"], errors="coerce"
            ).fillna(0)
            ofs_liberadas["remain_qty"] = (
                ofs_liberadas["gamng"] - ofs_liberadas["wemng"]
            )

            # Parse Dates
            # Original: datetime.strptime(row["gltrs"], "%Y%m%d").date()
            ofs_liberadas["release_date"] = pd.to_datetime(
                ofs_liberadas["gltrs"], format="%Y%m%d", errors="coerce"
            ).dt.date
            ofs_liberadas["finish_date"] = pd.to_datetime(
                ofs_liberadas["gltri"], format="%Y%m%d", errors="coerce"
            ).dt.date

            # Replace NaT with None for SQLAlchemy compatibility
            ofs_liberadas["release_date"] = ofs_liberadas["release_date"].where(
                pd.notnull(ofs_liberadas["release_date"]), None
            )
            ofs_liberadas["finish_date"] = ofs_liberadas["finish_date"].where(
                pd.notnull(ofs_liberadas["finish_date"]), None
            )

            # Prepare OFs DataFrame for allocation loop
            # Columns needed: aufnr_zpa, aufnr_zsem, material_id, qty (gamng-wemng), release_date, finish_date, batch (charg), invent_location (lgort), remain_qty

            # Rename for convenience
            ofs = ofs_liberadas.rename(
                columns={"charg": "batch", "lgort": "invent_location"}
            )
            ofs["qty"] = ofs["remain_qty"]  # Current Quantity is the remaining one

            # Filter valid materials
            ofs = ofs.dropna(subset=["material_id"])

            # Use specific columns
            ofs_cols = [
                "aufnr_zpa",
                "aufnr_zsem",
                "material_id",
                "qty",
                "release_date",
                "finish_date",
                "batch",
                "invent_location",
                "remain_qty",
            ]
            ofs = ofs[ofs_cols].copy()

            # Sort to ensure FIFO allocation matches original logic (Order by MATNR_ZPA matches material_id roughly, GLTRS matches release_date)
            # Original SQL had ORDER BY MATNR_ZPA, GLTRS. We should maintain sort by Date.
            ofs = ofs.sort_values(by=["material_id", "release_date"])
        else:
            ofs = pd.DataFrame(
                columns=[
                    "aufnr_zpa",
                    "aufnr_zsem",
                    "material_id",
                    "qty",
                    "release_date",
                    "finish_date",
                    "batch",
                    "invent_location",
                    "remain_qty",
                ]
            )

        # 5. Allocation Loop (OFs vs Remaining Demand)
        # ---------------------------------------------------------------------
        # We must iterate here because a single supply line can split across multiple orders,
        # and a single order can split across multiple supply lines.

        insert_results = []

        # Optimize: Group OFs by material to avoid scanning full DF every time
        # We can create a dictionary of DataFrames or Lists for faster access
        ofs_dict = {k: v for k, v in ofs.groupby("material_id")}

        # Helper to track OF index consumption per material
        # Since we modify 'remain_qty' in place or logically, we need to track state.
        # We will use simple lists of dictionaries for mutable state per material.
        ofs_state = {}
        for mat_id, group in ofs_dict.items():
            ofs_state[mat_id] = group.to_dict("records")

        # Iterate orders
        # Note: orders_to_process is already sorted by GoodsIssueDate because 'orders' was sorted.

        for _, order in orders_to_process.iterrows():
            mat_id = order["MaterialId"]
            qty_needed = order["QtyConfirmed"]

            # Get available OFs for this material
            available_ofs = ofs_state.get(mat_id, [])

            # Iterate through available OFs for this material
            # We must use a while loop or careful management to remove exhausted OFs
            # Accessing list by index to allow modification

            ofs_idx = 0
            while qty_needed > 0 and ofs_idx < len(available_ofs):
                supply_row = available_ofs[ofs_idx]
                supply_qty = supply_row["remain_qty"]

                if supply_qty <= 0:
                    ofs_idx += 1
                    continue

                take_qty = min(qty_needed, supply_qty)

                # Create Result Record
                insert_results.append(
                    {
                        "SalesId": order["SalesId"],
                        "MaterialId": mat_id,
                        "CustId": order["CustId"],
                        "GoodsIssueDate": order["GoodsIssueDate"],
                        "QtyConfirmed": float(
                            take_qty
                        ),  # Portion confirmed against this OF
                        "OfZpa": supply_row["aufnr_zpa"],
                        "OfZsem": supply_row["aufnr_zsem"],
                        "QtyOfZpa": float(
                            supply_row["qty"]
                        ),  # Original Qty of OF? Original code used 'qty' which was calculated as gamng-wemng
                        "ReleaseDate": supply_row["release_date"],
                        "FinishDate": supply_row["finish_date"],
                        "BatchNumber": supply_row["batch"],
                        "InventLocation": supply_row["invent_location"],
                    }
                )

                # Update State
                qty_needed -= take_qty
                supply_row["remain_qty"] -= take_qty

                # If supply exhausted, move to next
                if supply_row["remain_qty"] == 0:
                    ofs_idx += 1

            # Clean up exhausted OFs from the front of the list to speed up next lookup
            if ofs_idx > 0:
                ofs_state[mat_id] = available_ofs[ofs_idx:]

            # If still needed but no OFs found (or ran out)
            if qty_needed > 0:
                insert_results.append(
                    {
                        "SalesId": order["SalesId"],
                        "MaterialId": mat_id,
                        "CustId": order["CustId"],
                        "GoodsIssueDate": order["GoodsIssueDate"],
                        "QtyConfirmed": float(qty_needed),
                        "OfZpa": None,
                        "OfZsem": None,
                        "QtyOfZpa": None,
                        "ReleaseDate": None,
                        "FinishDate": None,
                        "BatchNumber": None,
                        "InventLocation": None,
                    }
                )

        # 6. Define Table and Insert
        # ---------------------------------------------------------------------
        metadata = MetaData()
        availabiliy_table = Table(
            self._config.TABLE_AVAILABILITY_CALCULATION_FACT,
            metadata,
            Column("SalesId", String(10)),
            Column("MaterialId", Integer),
            Column("CustId", Integer),
            Column("GoodsIssueDate", Date),
            Column("QtyConfirmed", DECIMAL(15, 4)),
            Column("OfZpa", String(15)),
            Column("OfZsem", String(15)),
            Column("QtyOfZpa", DECIMAL(15, 4)),
            Column("ReleaseDate", Date),
            Column("FinishDate", Date),
            Column("BatchNumber", String(10)),
            Column("InventLocation", String(10)),
        )

        stmt_insert = insert(availabiliy_table)
        stmt_truncate = text(
            f"TRUNCATE TABLE {self._config.TABLE_AVAILABILITY_CALCULATION_FACT}"
        )

        with self._con_dw.begin() as conn:
            if insert_results:
                Logger().info(f"Inserting {len(insert_results)} allocation records.")
                conn.execute(stmt_truncate)
                conn.execute(stmt_insert, insert_results)
            else:
                Logger().info("No allocation records generated.")

            self._update_etl_info(conn, "process_availability_materials")
