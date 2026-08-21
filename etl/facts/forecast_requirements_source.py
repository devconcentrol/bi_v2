import pandas as pd
from sqlalchemy import (
    DECIMAL,
    Column,
    Date,
    Integer,
    MetaData,
    String,
    Table,
    insert,
    text,
)

from etl.base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class ForecastRequirementsSourceETL(BaseFactETL):
    TERMINAL_MTARTS = {"ROH", "ENV", "LEER"}

    @staticmethod
    def _normalize_key(value: object) -> str:
        if pd.isna(value):
            return ""
        return str(value).strip()

    def _build_top_to_leaf_relations(self, results: pd.DataFrame) -> pd.DataFrame:
        results = results.copy()
        for column in ("matnr", "baugr", "plnum"):
            results[column] = results[column].map(self._normalize_key)

        top_seed_rows = results.loc[results["mtart_baugr"] == "FERT"].copy()
        if top_seed_rows.empty:
            return top_seed_rows

        rows_by_plnum: dict[str, pd.DataFrame] = {
            str(plnum): group.copy()
            for plnum, group in results.groupby("plnum", dropna=False)
        }

        def walk_down(
            plnum_rows: pd.DataFrame,
            current_matnr: str,
            current_bdter: object,
            current_sbter: object,
            path: list[str],
            depth: int,
            visited: set[tuple[str, object, object]],
        ) -> list[dict[str, object]]:
            node_key = (current_matnr, current_bdter, current_sbter)
            if node_key in visited:
                return []

            child_rows = results.loc[
                (results["baugr"] == current_matnr)
                & (
                    (results["bdter"] == current_bdter)
                    | (results["sbter"] == current_bdter)
                )
            ].copy()

            child_rows = child_rows.loc[
                ~(
                    (child_rows["matnr"] == current_matnr)
                    & (child_rows["bdter"] == current_bdter)
                    & (child_rows["sbter"] == current_sbter)
                )
            ]
            child_rows = child_rows.drop_duplicates(
                subset=["matnr", "bdter", "sbter", "baugr"]
            )

            if child_rows.empty:
                return []

            relations: list[dict[str, object]] = []
            next_visited = visited | {node_key}
            for _, child_row in child_rows.iterrows():
                child_matnr = child_row["matnr"]
                child_bdter = child_row["bdter"]
                child_sbter = child_row["sbter"]
                child_path = path + [child_matnr]

                if child_row["mtart"] in self.TERMINAL_MTARTS:
                    relations.append(
                        {
                            "leaf_matnr": child_matnr,
                            "leaf_bdter": child_bdter,
                            "leaf_sbter": child_sbter,
                            "leaf_qty": child_row["bdmng"],
                            "leaf_unit": child_row["meins"],
                            "hierarchy_depth": depth + 1,
                            "hierarchy_path": " -> ".join(child_path),
                        }
                    )
                    continue

                relations.extend(
                    walk_down(
                        plnum_rows=plnum_rows,
                        current_matnr=child_matnr,
                        current_bdter=child_bdter,
                        current_sbter=child_sbter,
                        path=child_path,
                        depth=depth + 1,
                        visited=next_visited,
                    )
                )

            return relations

        relation_rows: list[dict[str, object]] = []
        for _, top_row in top_seed_rows.iterrows():
            plnum = str(top_row["plnum"])
            if plnum == "0000128681":
                print(
                    f"Debug: Processing top row with PLNUM={plnum}, MATNR={top_row['matnr']}"
                )
            plnum_rows = rows_by_plnum.get(plnum)
            if plnum_rows is None or plnum_rows.empty:
                continue

            top_record = {
                "top_matnr": top_row["baugr"],
                "top_plnum": top_row["plnum"],
                "top_bdter": top_row["bdter"],
                "top_sbter": top_row["sbter"],
                "top_qty": top_row["bdmng"],
                "top_unit": top_row["meins"],
            }

            leaf_relations = walk_down(
                plnum_rows=plnum_rows,
                current_matnr=top_row["matnr"],
                current_bdter=top_row["bdter"],
                current_sbter=top_row["sbter"],
                path=[top_row["baugr"], top_row["matnr"]],
                depth=1,
                visited=set(),
            )
            for relation in leaf_relations:
                relation_rows.append(top_record | relation)

        return pd.DataFrame(relation_rows).drop_duplicates().reset_index(drop=True)

    def _log_top_to_leaf_relations(
        self, relations: pd.DataFrame, matnr: str, limit: int = 50
    ) -> None:
        if relations.empty:
            Logger().info("No top-to-leaf relations available.")
            return

        sample = relations.loc[
            relations["top_matnr"].astype(str) == str(matnr),
            [
                "top_matnr",
                "top_plnum",
                "top_bdter",
                "top_qty",
                "top_unit",
                "leaf_matnr",
                "leaf_bdter",
                "leaf_sbter",
                "leaf_qty",
                "leaf_unit",
                "hierarchy_depth",
                "hierarchy_path",
            ],
        ].drop_duplicates()

        if sample.empty:
            Logger().info("No relations found for top MATNR=%s.", matnr)
            return

        Logger().info(
            "Top to leaf relations for MATNR=%s:\n%s",
            matnr,
            sample.head(limit).to_string(index=False),
        )

    @error_handler
    def run(self) -> None:
        Logger().info("Processing Forecast Consumptions Source Fact...")

        sql_get_forecast = """
            SELECT WERKS,
                   MATNR,
                   MEINS,
                   BDTER,
                   SBTER,
                   BDMNG,
                   MTART,
                   PLNUM,
                   BAUGR,
                   MTART_BAUGR
            FROM SAPSR3.ZCON_V_CONSUMPTION_FORECAST
            WHERE PLSCN = '001'
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_forecast,
            con=self._con_sap,
            dtype_backend="numpy_nullable",
        )
        results.columns = results.columns.str.lower()

        top_to_leaf_relations = self._build_top_to_leaf_relations(results)
        material_map = self._lookup.get_material_map()
        top_to_leaf_relations["TopMaterialId"] = (
            top_to_leaf_relations["top_matnr"].astype(str).map(material_map)
        )
        top_to_leaf_relations["LeafMaterialId"] = (
            top_to_leaf_relations["leaf_matnr"].astype(str).map(material_map)
        )

        top_to_leaf_relations = top_to_leaf_relations.dropna(
            subset=["TopMaterialId", "LeafMaterialId"]
        )
        Logger().info(
            "Built %s top-to-leaf forecast relations.",
            len(top_to_leaf_relations),
        )
        self._log_top_to_leaf_relations(
            top_to_leaf_relations,
            matnr="35625+MA225PTV",
            limit=len(top_to_leaf_relations),
        )

        metadata = MetaData()
        target_table = Table(
            self._config.TABLE_FORECAST_REQUIREMENTS_SOURCE_FACT,
            metadata,
            Column("TopMaterialId", Integer),
            Column("TopRequirementDate", Date),
            Column("TopQty", DECIMAL(15, 4)),
            Column("TopUnit", String(10)),
            Column("LeafMaterialId", Integer),
            Column("LeafRequirementDate", Date),
            Column("LeafQty", DECIMAL(15, 4)),
            Column("LeafUnit", String(10)),
            Column("HierarchyDepth", Integer),
            Column("HierarchyPath", String(1000)),
        )

        insert_df = top_to_leaf_relations.rename(
            columns={
                "TopMaterialId": "TopMaterialId",
                "top_plnum": "TopPLNum",
                "top_bdter": "TopRequirementDate",
                "top_qty": "TopQty",
                "top_unit": "TopUnit",
                "LeafMaterialId": "LeafMaterialId",
                "leaf_bdter": "LeafRequirementDate",
                "leaf_qty": "LeafQty",
                "leaf_unit": "LeafUnit",
                "hierarchy_depth": "HierarchyDepth",
                "hierarchy_path": "HierarchyPath",
            }
        )
        if insert_df.empty:
            insert_df = pd.DataFrame(
                columns=[
                    "TopMaterialId",
                    "TopPLNum",
                    "TopRequirementDate",
                    "TopQty",
                    "TopUnit",
                    "LeafMaterialId",
                    "LeafRequirementDate",
                    "LeafQty",
                    "LeafUnit",
                    "HierarchyDepth",
                    "HierarchyPath",
                ]
            )

        for date_col in ("TopStartDate", "LeafDate", "LeafEndDate"):
            if date_col in insert_df.columns:
                insert_df[date_col] = pd.to_datetime(
                    insert_df[date_col].astype(str), format="%Y%m%d", errors="coerce"
                ).dt.date

        final_cols = [
            "TopMaterialId",
            "TopRequirementDate",
            "TopQty",
            "TopUnit",
            "LeafMaterialId",
            "LeafRequirementDate",
            "LeafQty",
            "LeafUnit",
            "HierarchyDepth",
            "HierarchyPath",
        ]
        insert_records = (
            insert_df[final_cols]
            .where(pd.notnull(insert_df[final_cols]), None)
            .to_dict(orient="records")
        )

        stmt_truncate = text(
            f"TRUNCATE TABLE {self._config.TABLE_FORECAST_REQUIREMENTS_SOURCE_FACT}"
        )

        with self._con_dw.begin() as conn:
            conn.execute(stmt_truncate)
            if insert_records:
                Logger().info(
                    "Inserting %s forecast requirements source records.",
                    len(insert_records),
                )
                conn.execute(insert(target_table), insert_records)  # type: ignore

            self._update_etl_info(conn, "forecast_requirements_source_fact")
