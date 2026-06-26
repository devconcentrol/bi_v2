from urllib import error, parse, request

import pandas as pd
from sqlalchemy import Engine

from utils.config import Config
from utils.logger import Logger


class ResultSender:
    def __init__(self, con_dw: Engine):
        self.config = Config.get_instance()
        self._con_dw = con_dw

    def send_result(self) -> None:
        if not self.config.API_KEY:
            raise ValueError("API_KEY not set in environment.")
        if not self.config.API_URL:
            raise ValueError("API_URL not set in environment.")

        sql_get_execution_results = """
                            SELECT ETL
                            FROM ETLInfo
                            WHERE CAST(ProcessDate AS DATE) != CAST(GETDATE() AS DATE)
                            AND [NoCheck] IS NULL
                        """
        results: pd.DataFrame = pd.read_sql(
            sql_get_execution_results,
            self._con_dw,
            dtype_backend="numpy_nullable",
        )
        if results.empty:
            Logger().info("No execution results to send")
            message = "Everything running as expected, no errors found"
        else:
            message = "Execution results:\n" + "\n".join(results["ETL"].tolist())

        self._send_message(message)

    def _send_message(self, message: str) -> None:
        query_params = parse.urlencode(
            {
                "text": message,
                "apikey": self.config.API_KEY,
            }
        )
        separator = "&" if "?" in self.config.API_URL else "?"
        api_url = f"{self.config.API_URL}{separator}{query_params}"

        try:
            with request.urlopen(api_url, timeout=30) as response:
                status_code = response.getcode()
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="ignore")
            Logger().error(
                "API request failed with status %s: %s",
                exc.code,
                response_body,
            )
            raise
        except error.URLError as exc:
            Logger().error("Could not connect to API: %s", exc.reason)
            raise

        Logger().info("Message sent successfully. Status code: %s", status_code)
