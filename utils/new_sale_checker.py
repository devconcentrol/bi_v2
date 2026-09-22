from __future__ import annotations

from datetime import datetime

import pandas as pd
from sqlalchemy import Engine, text

from etl.base_fact_etl import BaseFactETL
from utils.dimension_lookup import DimensionLookup
from utils.error_handler import error_handler
from utils.logger import Logger
from utils.message_sender import MessageSender


class NewSaleChecker(BaseFactETL):
    """Notify sales-order lines that have no matching monthly forecast."""

    ETL_NAME = "process_new_sale_checker"

    def __init__(
        self,
        con_dw: Engine,
        con_sap: Engine,
        lookup: DimensionLookup,
        email_sender: MessageSender,
    ) -> None:
        super().__init__(con_dw, con_sap, lookup)
        self._email_sender = email_sender

    def _get_last_execution(self) -> datetime:
        query = text(
            f"SELECT ProcessDate FROM {self._config.TABLE_ETL_INFO} "
            "WHERE ETL = :etl_name"
        )
        result = pd.read_sql(
            query,
            self._con_dw,
            params={"etl_name": self.ETL_NAME},
        )

        if result.empty or pd.isna(result.iloc[0]["ProcessDate"]):
            raise ValueError(
                f"No execution date found in {self._config.TABLE_ETL_INFO} "
                f"for '{self.ETL_NAME}'."
            )

        return pd.Timestamp(result.iloc[0]["ProcessDate"]).to_pydatetime()

    def _get_new_orders(self, last_execution: datetime) -> pd.DataFrame:
        # ERDAT and ERZET are SAP date/time values in YYYYMMDD and HHMMSS format.
        query = text(
            """
            SELECT *
            FROM SAPSR3.ZCON_V_OPEN_SALES_ORDERS
            WHERE ERDAT > :process_date
               OR (ERDAT = :process_date AND ERZET > :process_time)
            """
        )
        return pd.read_sql(
            query,
            self._con_sap,
            params={
                "process_date": last_execution.strftime("%Y%m%d"),
                "process_time": last_execution.strftime("%H%M%S"),
            },
            dtype_backend="numpy_nullable",
        )

    def _get_forecast(self) -> pd.DataFrame:
        query = text(
            f"SELECT CustId, MaterialId, ForecastDate FROM "
            f"{self._config.TABLE_SALES_FORECAST_FACT}"
        )
        return pd.read_sql(
            query,
            self._con_dw,
            dtype_backend="numpy_nullable",
        )

    @staticmethod
    def _require_columns(data: pd.DataFrame, required: set[str], source: str) -> None:
        missing = required.difference(data.columns)
        if missing:
            raise ValueError(
                f"Missing columns in {source}: {', '.join(sorted(missing))}."
            )

    def _add_dimension_ids(self, orders: pd.DataFrame) -> pd.DataFrame:
        orders = orders.copy()
        orders.columns = orders.columns.str.lower()
        self._require_columns(
            orders,
            {"kunnr", "matnr", "vkorg", "vtweg", "spart", "delivery_date"},
            "ZCON_V_OPEN_SALES_ORDERS",
        )
        orders["delivery_date"] = pd.to_datetime(
            orders["delivery_date"], errors="coerce", format="%Y%m%d"
        )

        for column in ("kunnr", "matnr", "vkorg", "vtweg", "spart"):
            orders[column] = orders[column].fillna("").astype(str)

        orders["MaterialId"] = orders["matnr"].map(self._lookup.get_material_map())

        customer_map = self._lookup.get_customer_map()
        customer_key = (
            orders["vkorg"] + orders["vtweg"] + orders["spart"] + orders["kunnr"]
        )
        fallback_key = orders["vkorg"] + "10" + orders["spart"] + orders["kunnr"]
        orders["CustId"] = customer_key.map(customer_map).fillna(
            fallback_key.map(customer_map)
        )
        delivery_dates = pd.to_datetime(
            orders["delivery_date"].astype(str),
            format="%Y%m%d",
            errors="coerce",
        )
        delivery_dates = delivery_dates.fillna(
            pd.to_datetime(orders["delivery_date"], errors="coerce")
        )
        orders["ForecastPeriod"] = delivery_dates.dt.to_period("M")
        return orders

    def _find_orders_without_forecast(
        self,
        orders: pd.DataFrame,
        forecast: pd.DataFrame,
    ) -> pd.DataFrame:
        if orders.empty:
            return orders.copy()

        prepared_orders = self._add_dimension_ids(orders)
        forecast = forecast.copy()
        self._require_columns(
            forecast,
            {"CustId", "MaterialId", "ForecastDate"},
            self._config.TABLE_SALES_FORECAST_FACT,
        )
        forecast["ForecastPeriod"] = pd.to_datetime(
            forecast["ForecastDate"], errors="coerce"
        ).dt.to_period("M")

        forecast_key_columns = ["CustId", "MaterialId", "ForecastPeriod"]
        forecast_keys = pd.MultiIndex.from_frame(
            forecast[forecast_key_columns].dropna().drop_duplicates()
        )
        order_keys = pd.MultiIndex.from_frame(
            prepared_orders[["CustId", "MaterialId", "ForecastPeriod"]]
        )
        return prepared_orders.loc[~order_keys.isin(forecast_keys)].copy()

    @staticmethod
    def _build_email(orders: pd.DataFrame, last_execution: datetime) -> str:
        display_columns = [
            column
            for column in (
                "vbeln",
                "posnr",
                "kunnr",
                "matnr",
                "delivery_date",
                "qty",
            )
            if column in orders.columns
        ]
        table = orders[display_columns].to_html(index=False, escape=True)
        return (
            "<h2>Nuevas ventas fuera de previsión</h2>"
            f"<p>Se han encontrado <strong>{len(orders)}</strong> líneas "
            "de pedido sin previsión de venta.</p>"
            f"<p>Pedidos creados desde: {last_execution:%d/%m/%Y %H:%M:%S}</p>"
            f"{table}"
        )

    def _mark_execution(self) -> None:
        statement = text(
            f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() "
            "WHERE ETL = :etl_name"
        )
        with self._con_dw.begin() as connection:
            connection.execute(statement, {"etl_name": self.ETL_NAME})

    @error_handler
    def run(self) -> None:
        """Notify order lines without a forecast and mark the execution."""
        last_execution = self._get_last_execution()
        orders = self._get_new_orders(last_execution)

        if orders.empty:
            Logger().info("No new sales orders found.")
            self._mark_execution()
            return

        forecast = self._get_forecast()
        unmatched = self._find_orders_without_forecast(orders, forecast)

        if unmatched.empty:
            Logger().info("All new sales orders have a sales forecast.")
        else:
            self._email_sender.send(self._build_email(unmatched, last_execution))
            Logger().info(
                "Email sent with %s sales order lines outside the forecast.",
                len(unmatched),
            )

        self._mark_execution()
