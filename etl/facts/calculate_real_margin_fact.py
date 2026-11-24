from sqlalchemy import text

from etl.base_fact_etl import BaseFactETL
from utils.error_handler import error_handler
from utils.logger import Logger


class CalculateRealMarginFactETL(BaseFactETL):
    @error_handler
    def run(self) -> None:
        logger = Logger()
        logger.info("Processing Real Margin...")

        prices = self._config.TABLE_MATERIAL_REAL_PRICE_FACT
        sales = self._config.TABLE_SALES_FACT
        materials = self._config.TABLE_MATERIAL_DIM
        sales_match = """
            sale.MaterialId = candidate.MaterialId
            AND sale.Organization = candidate.Organization
            AND sale.InvoiceDate >= candidate.MonthStart
            AND sale.InvoiceDate < DATEADD(month, 1, candidate.MonthStart)
            """

        with self._con_dw.begin() as conn:
            conn.execute(text(f"""
                SELECT price.PlantId, price.MaterialId, price.CurrencyType,
                       price.PriceDate, period.MonthStart, org.Organization,
                       cost.NetRealSalesCost
                INTO #RealMarginCandidates
                FROM {prices} AS price WITH (UPDLOCK, HOLDLOCK)
                INNER JOIN {materials} AS material
                    ON material.MaterialId = price.MaterialId
                CROSS APPLY (
                    VALUES (
                        DATEFROMPARTS(YEAR(price.PriceDate), MONTH(price.PriceDate), 1)
                    )
                ) AS period (MonthStart)
                CROSS APPLY (VALUES (CAST(price.PlantId AS varchar(11))))
                    AS org (Organization)
                CROSS APPLY (
                    VALUES (
                        TRY_CONVERT(decimal(15, 4),
                            price.RealPrice
                            / NULLIF(price.PriceQuantity * material.NetWeight, 0))
                    )
                ) AS cost (NetRealSalesCost)
                WHERE price.CanCalculate = 1
                  AND price.CalculatedDate IS NULL
                  AND price.CurrencyType = '10'
                  AND cost.NetRealSalesCost IS NOT NULL
                """))
            conn.execute(text(f"""
                DELETE candidate
                FROM #RealMarginCandidates AS candidate
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {sales} AS sale WITH (UPDLOCK, HOLDLOCK)
                    WHERE {sales_match}
                )
                """))
            conn.execute(text("""
                IF EXISTS (
                    SELECT 1
                    FROM #RealMarginCandidates
                    GROUP BY Organization, MaterialId, MonthStart
                    HAVING COUNT_BIG(*) > 1
                )
                    THROW 50001, 'Multiple real prices match the same sales period.', 1;

                CREATE UNIQUE CLUSTERED INDEX IX_RealMarginCandidates
                    ON #RealMarginCandidates (Organization, MaterialId, MonthStart)
                """))
            updated_sales = conn.execute(text(f"""
                UPDATE sale
                SET NetRealSalesCost = (candidate.NetRealSalesCost * sale.NetWeight)
                FROM {sales} AS sale
                INNER JOIN #RealMarginCandidates AS candidate ON {sales_match}
                """)).rowcount
            calculated_prices = conn.execute(text(f"""
                UPDATE price
                SET CalculatedDate = GETDATE()
                FROM {prices} AS price
                INNER JOIN #RealMarginCandidates AS candidate
                    ON candidate.PlantId = price.PlantId
                   AND candidate.MaterialId = price.MaterialId
                   AND candidate.CurrencyType = price.CurrencyType
                   AND candidate.PriceDate = price.PriceDate
                WHERE price.CalculatedDate IS NULL
                """)).rowcount
            conn.execute(text("DROP TABLE #RealMarginCandidates"))
            self._update_etl_info(conn, "process_calculate_real_margin")

        logger.info(
            f"Real Margin ETL completed. Updated {updated_sales} sales "
            f"and marked {calculated_prices} prices as calculated."
        )
