import pandas as pd
from sqlalchemy import Engine

from utils.config import Config
from utils.logger import Logger
from utils.message_sender import MessageSender


class ProcessExecutionNotifier:
    def __init__(
        self,
        con_dw: Engine,
        message_sender: MessageSender,
    ):
        self._config = Config.get_instance()
        self._con_dw = con_dw
        self._message_sender = message_sender

    def send_result(self) -> None:
        sql_get_execution_results = f"""
                            SELECT ETL
                            FROM {self._config.TABLE_ETL_INFO}
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

        self._message_sender.send(message)
