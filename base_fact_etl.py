from sqlalchemy import text
from utils.config import Config


class BaseFactETL:
    def __init__(self, con_dw, con_sap, lookup):
        self._con_dw = con_dw
        self._con_sap = con_sap
        self._lookup = lookup
        self._config = Config.get_instance()

    def run(self):
        raise NotImplementedError

    def _update_etl_info(self, conn=None, etl_name: str = None):
        if conn:
            stmt = text(
                f"UPDATE {self._config.TABLE_ETL_INFO} SET ProcessDate = GETDATE() WHERE ETL = :etl_name"
            )
            conn.execute(stmt, {"etl_name": etl_name})
        else:
            raise ValueError("No connection provided")
