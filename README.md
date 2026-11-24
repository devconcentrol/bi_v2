# bi_v2

Proyecto ETL migrado a `uv`.

## Comandos

```bash
uv sync
uv run bi-etl
uv run bi-etl-cli list
uv run python -m unittest discover -s tests
```

## Compatibilidad

`requirements.txt` se mantiene de momento para no romper integraciones existentes, pero la fuente principal de dependencias pasa a ser `pyproject.toml`.

## ETL de margen real

[`CalculateRealMarginFactETL`](etl/facts/calculate_real_margin_fact.py) calcula el coste real de las ventas en el data warehouse de SQL Server. Usa las tablas configuradas en [`utils/config.py`](utils/config.py).

| Configuración | Tabla | Uso |
| --- | --- | --- |
| `TABLE_MATERIAL_REAL_PRICE_FACT` | `MaterialRealPriceFact` | Leer los precios pendientes y registrar su fecha de cálculo |
| `TABLE_SALES_FACT` | `SalesFact` | Seleccionar las ventas y actualizar `NetRealSalesCost` |
| `TABLE_MATERIAL_DIM` | `MaterialsDim` | Obtener `NetWeight` por `MaterialId` |

1. Selecciona los precios con `CanCalculate = 1`, `CalculatedDate IS NULL` y `CurrencyType = '10'`.
2. Busca las ventas con el mismo `MaterialId`, con `SalesFact.Organization` igual a `PlantId` convertido a texto y con `InvoiceDate` en el mismo mes natural que `PriceDate`.
3. Actualiza `SalesFact.NetRealSalesCost` con `RealPrice / (PriceQuantity * MaterialsDim.NetWeight)`. El resultado se guarda como `decimal(15, 4)`.
4. Asigna la fecha de cálculo del servidor a `CalculatedDate` para los precios utilizados. Identifica cada precio por `PlantId`, `MaterialId`, `CurrencyType` y `PriceDate`.

Las actualizaciones se ejecutan por conjunto y en la misma transacción. Los precios sin ventas coincidentes o con un coste que no se puede calcular quedan pendientes. Si varios precios válidos coinciden con la misma organización, material y mes, la transacción se revierte.

La carga de [precios reales](etl/facts/material_real_price_fact.py) protege los registros con `CalculatedDate` informado. El job `calculate_real_margin_fact` está programado todos los días a las 04:50 en [`jobs_config.json`](jobs_config.json), después de la hora prevista para la carga de precios reales (04:10).
