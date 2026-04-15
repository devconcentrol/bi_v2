from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from sqlalchemy import Engine

from etl.facts.availability_calculation_fact import AvailabilityCalculationFactETL
from etl.facts.consumption_ceco_fact import ConsumptionCeCoFactETL
from etl.facts.consumption_fact import ConsumptionFactETL
from etl.facts.costing_fact import CostingFactETL
from etl.facts.customer_price_fact import CustomerPriceFactETL
from etl.dimensions.agent_dim import AgentDim
from etl.dimensions.contact_dim import ContactDim
from etl.dimensions.customer_dim import CustomerDim
from etl.dimensions.material_dim import MaterialDim
from etl.dimensions.vendor_dim import VendorDim
from etl.facts.document_flow_fact import DocumentFlowFactETL
from etl.facts.ewm_locations_fact import EWMLocationsFactETL
from etl.facts.ewm_task_fact import EWMTasksFactETL
from etl.facts.extended_batch_stock_fact import ExtendedBatchStockFactETL
from etl.facts.extended_stock_fact import ExtendedStockFactETL
from etl.facts.forecast_consumptions_fact import ForecastConsumptionsFactETL
from etl.facts.inmobilized_hist_fact import ImmobilizedHistFactETL
from etl.facts.material_real_price_fact import MaterialRealPriceFactETL
from etl.facts.monitor_stock_fact import MonitorStockFactETL
from etl.facts.planned_orders_qty_fact import PlannedOrdersQtyFactETL
from etl.facts.production_data_fact import ProductionDataFactETL
from etl.facts.production_orders_state_change_fact import (
    ProductonOrdersStateChangeFactETL,
)
from etl.facts.purch_average_price_fact import PurchAvgPriceFactETL
from etl.facts.purch_delivery_date_fact import PurchDeliveryDateFactETL
from etl.facts.purchase_movements_fact import PurchaseMovementsFactETL
from etl.facts.purchase_pending_orders_fact import PurchasePendingOrdersFactETL
from etl.facts.qm_adjustment_fact import QMAdjustmentFactETL
from etl.facts.qm_inspection_lot_fact import QMInspectionLotFactETL
from etl.facts.qm_notification_fact import QMNotificationFactETL
from etl.facts.qm_sample_fact import QMSampleFactETL
from etl.facts.recovery_products_fact import RecoveryProductsFactETL
from etl.facts.regularization_fact import RegularizationFactETL
from etl.facts.sales_delivery_date_change_fact import SalesDeliveryDateChangeFactETL
from etl.facts.sales_fact import SalesFactETL
from etl.facts.sales_open_orders_fact import SalesOpenOrdersFactETL
from etl.facts.sales_order_hist_fact import SalesOrderHistFactETL
from etl.facts.sample_delivery_fact import SampleDeliveryFactETL
from etl.facts.sustainability_data_fact import SustainabilityDataFactETL
from utils.config import Config
from utils.dimension_lookup import DimensionLookup
from utils.job_runner import safe_run_job
from utils.logger import Logger
from etl.facts.vendor_assesment_fact import VendorAssesmentFactETL
from etl.facts.customer_dm_fact import CustomerDMFactETL


@dataclass(frozen=True)
class RuntimeContext:
    con_dw: Engine
    con_sap: Engine
    lookup: DimensionLookup


@dataclass(frozen=True)
class JobDefinition:
    name: str
    times: tuple[str, ...]
    runner: Callable[[], None]


@dataclass(frozen=True)
class JobConfigEntry:
    name: str
    enabled: bool
    times: tuple[str, ...]


def _build_job_factories(context: RuntimeContext) -> dict[str, Callable[[], None]]:
    return {
        "agents_dim": AgentDim(context.con_dw, context.con_sap, context.lookup).run,
        "customer_dim": CustomerDim(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "material_dim": MaterialDim(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "vendor_dim": VendorDim(context.con_dw, context.con_sap, context.lookup).run,
        "contact_dim": ContactDim(context.con_dw, context.con_sap, context.lookup).run,
        "invalidate_dimension_caches": context.lookup.invalidate_caches,
        "costing_fact": CostingFactETL(context.con_dw, context.lookup).run,
        "document_flow_fact": DocumentFlowFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "purchase_movements_fact": PurchaseMovementsFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "recovery_products_fact": RecoveryProductsFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "purch_delivery_date_fact": PurchDeliveryDateFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "immobilized_hist_fact": ImmobilizedHistFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "vendor_assesment_fact": VendorAssesmentFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "sustainability_data_fact": SustainabilityDataFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "sales_order_hist_fact": SalesOrderHistFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "purch_average_price_fact": PurchAvgPriceFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "ewm_tasks_fact": EWMTasksFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "ewm_locations_fact": EWMLocationsFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "extended_stock_fact": ExtendedStockFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "sales_fact": SalesFactETL(context.con_dw, context.con_sap, context.lookup).run,
        "extended_batch_stock_fact": ExtendedBatchStockFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "qm_adjustment_fact": QMAdjustmentFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "qm_notification_fact": QMNotificationFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "qm_sample_fact": QMSampleFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "qm_inspection_lot_fact": QMInspectionLotFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "customer_price_fact": CustomerPriceFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "planned_orders_qty_fact": PlannedOrdersQtyFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "sales_open_orders_fact": SalesOpenOrdersFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "availability_calculation_fact": AvailabilityCalculationFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "production_data_fact": ProductionDataFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "prod_orders_state_change_fact": ProductonOrdersStateChangeFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "material_real_price_fact": MaterialRealPriceFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "regularization_fact": RegularizationFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "consumption_fact": ConsumptionFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "consumption_ceco_fact": ConsumptionCeCoFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "sample_delivery_fact": SampleDeliveryFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "sales_delivery_date_change_fact": SalesDeliveryDateChangeFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "purchase_pending_orders_fact": PurchasePendingOrdersFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "forecast_consumptions_fact": ForecastConsumptionsFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "monitor_stock_fact": MonitorStockFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
        "customer_dm_fact": CustomerDMFactETL(
            context.con_dw, context.con_sap, context.lookup
        ).run,
    }


def _validate_time_format(value: str) -> None:
    try:
        datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise ValueError(f"Invalid schedule time '{value}'. Expected HH:MM.") from exc


def load_job_config(
    config_path: str | None = None,
    valid_job_names: set[str] | None = None,
) -> list[JobConfigEntry]:
    resolved_path = Path(config_path or Config.get_instance().JOB_CONFIG_PATH)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Job configuration file not found: {resolved_path}")

    with resolved_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    raw_jobs = payload.get("jobs")
    if not isinstance(raw_jobs, list):
        raise ValueError("Job configuration must contain a top-level 'jobs' list.")

    seen_names: set[str] = set()
    entries: list[JobConfigEntry] = []

    for raw_job in raw_jobs:
        if not isinstance(raw_job, dict):
            raise ValueError("Each job config entry must be an object.")

        name = raw_job.get("name")
        enabled = raw_job.get("enabled", True)
        times = raw_job.get("times", [])

        if not isinstance(name, str) or not name:
            raise ValueError(
                "Each job config entry must define a non-empty string 'name'."
            )
        if name in seen_names:
            raise ValueError(f"Job '{name}' is defined more than once in the config.")
        if valid_job_names is not None and name not in valid_job_names:
            raise ValueError(f"Job '{name}' is not registered in the codebase.")
        if not isinstance(enabled, bool):
            raise ValueError(
                f"Job '{name}' has invalid 'enabled' value; expected boolean."
            )
        if not isinstance(times, list) or not all(
            isinstance(item, str) for item in times
        ):
            raise ValueError(f"Job '{name}' must define 'times' as a list of strings.")
        if enabled and not times:
            raise ValueError(
                f"Enabled job '{name}' must define at least one schedule time."
            )

        for scheduled_time in times:
            _validate_time_format(scheduled_time)

        seen_names.add(name)
        entries.append(JobConfigEntry(name=name, enabled=enabled, times=tuple(times)))

    return entries


def build_job_definitions(
    context: RuntimeContext,
    config_path: str | None = None,
) -> list[JobDefinition]:
    job_factories = _build_job_factories(context)
    job_config = load_job_config(config_path, set(job_factories))

    jobs = [
        JobDefinition(
            name=entry.name,
            times=entry.times,
            runner=job_factories[entry.name],
        )
        for entry in job_config
        if entry.enabled
    ]

    return jobs


def register_jobs(scheduler, jobs: list[JobDefinition]) -> None:
    logger = Logger()
    for job in jobs:
        for scheduled_time in job.times:
            logger.info("Registering job '%s' at %s", job.name, scheduled_time)
            scheduler.every().day.at(scheduled_time).do(
                safe_run_job,
                job_name=job.name,
                job_callable=job.runner,
            )


def get_job_by_name(jobs: list[JobDefinition], job_name: str) -> JobDefinition | None:
    for job in jobs:
        if job.name == job_name:
            return job
    return None
