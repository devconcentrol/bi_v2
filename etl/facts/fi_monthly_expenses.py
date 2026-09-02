from datetime import datetime

import pandas as pd
from sqlalchemy import (
    DECIMAL,
    Column,
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


class FinanceExpensesFactETL(BaseFactETL):
    # Mapping: SAP Column -> Table Column
    COLUMN_MAPPING = {
        "rbukrs": "CompanyCode",
        "racct": "ExpenseAccountCode",
        "gjahr": "FiscalYear",
        "poper": "FiscalMonth",
        "hsl": "Amount",
    }

    @error_handler
    def run(self) -> None:
        Logger().info("Processing expenses account balance fact.")
        # 1. Fiscal year is curent year
        fiscal_year = (
            datetime.now().year if datetime.now().month > 1 else datetime.now().year - 1
        )

        # 2. Extract Data from SAP
        sql_get_expenses = """
            SELECT RBUKRS,
                   RACCT,
                   GJAHR,
                   POPER,
                   HSL
            FROM SAPSR3.ZCON_V_FI_GASTOS       
            WHERE RBUKRS = '1000' 	          
              AND GJAHR = :fiscal_year
              AND POPER <= 12
        """

        results: pd.DataFrame = pd.read_sql(
            sql_get_expenses,
            con=self._con_sap,
            dtype_backend="numpy_nullable",
            params={"fiscal_year": fiscal_year},
        )

        # 3. Handle Deletion in Data Warehouse
        stmt_delete = text(
            f"DELETE FROM {self._config.TABLE_FI_EXPENSES_ACCOUNT_BALANCE_FACT} "
            "WHERE FiscalYear = :fiscal_year"
        )

        if results.empty:
            Logger().info("No expenses found for processing")
            with self._con_dw.begin() as conn:
                self._update_etl_info(conn, "process_monthly_expenses")
            return

        results = results.rename(columns=self.COLUMN_MAPPING)
        insert_records = results.where(pd.notnull(results), None).to_dict(
            orient="records"
        )

        # 5. Load Data to Data Warehouse
        metadata: MetaData = MetaData()
        monthly_expenses_table: Table = Table(
            self._config.TABLE_FI_EXPENSES_ACCOUNT_BALANCE_FACT,
            metadata,
            Column("CompanyCode", String(5)),
            Column("FiscalYear", Integer),
            Column("FiscalMonth", Integer),
            Column("ExpenseAccountCode", String(50)),
            Column("Amount", DECIMAL(18, 2)),
        )

        with self._con_dw.begin() as conn:
            Logger().info(
                f"Deleting and inserting {len(insert_records)} finance expenses records."
            )
            conn.execute(stmt_delete, {"fiscal_year": fiscal_year})
            if insert_records:
                conn.execute(insert(monthly_expenses_table), insert_records)  # type: ignore

            self._update_etl_info(conn, "process_monthly_expenses")
