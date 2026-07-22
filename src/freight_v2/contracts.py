"""Column-level contracts shared by pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Collection


class ContractError(ValueError):
    """Base error for a table that violates its declared contract."""


class UnknownTableError(ContractError):
    """Raised when a pipeline stage requests an undeclared table."""


class MissingColumnsError(ContractError):
    """Raised when required observable columns are missing."""

    def __init__(self, table_name: str, missing: Collection[str]) -> None:
        self.table_name = table_name
        self.missing = frozenset(missing)
        joined = ", ".join(sorted(self.missing))
        super().__init__(f"{table_name} is missing required columns: {joined}")


class ColumnTypeError(ContractError):
    """Raised when a serialized required field uses an incompatible type."""


@dataclass(frozen=True, slots=True)
class TableContract:
    """Required columns and durable filename for one canonical table."""

    name: str
    filename: str
    required_columns: frozenset[str]


def _columns(*names: str) -> frozenset[str]:
    return frozenset(("run_id", "schema_version", *names))


TABLE_CONTRACTS = MappingProxyType(
    {
        "shipments": TableContract(
            "shipments",
            "shipments.parquet",
            _columns(
                "shipment_id",
                "ship_date",
                "lane_id",
                "origin_city",
                "origin_state",
                "destination_city",
                "destination_state",
                "carrier_id",
                "mode",
                "weight_lbs",
                "freight_class",
                "base_cost",
                "fuel_surcharge",
                "total_cost",
                "on_time_flag",
            ),
        ),
        "carrier_rates": TableContract(
            "carrier_rates",
            "carrier_rates.parquet",
            _columns(
                "rate_id",
                "carrier_id",
                "lane_id",
                "mode",
                "effective_start",
                "effective_end",
                "base_rate_per_cwt",
                "minimum_charge",
            ),
        ),
        "fuel_surcharges": TableContract(
            "fuel_surcharges",
            "fuel_surcharges.parquet",
            _columns("week_start", "mode", "fuel_index", "surcharge_rate", "curve_basis"),
        ),
        "anomaly_ground_truth": TableContract(
            "anomaly_ground_truth",
            "anomaly_ground_truth.parquet",
            _columns(
                "shipment_id",
                "is_anomaly",
                "anomaly_type",
                "changed_fields",
                "injected_magnitude",
            ),
        ),
        "anomaly_flags": TableContract(
            "anomaly_flags",
            "anomaly_flags.parquet",
            _columns(
                "flag_id",
                "shipment_id",
                "method",
                "score",
                "threshold",
                "support",
                "reason",
                "is_flagged",
                "evaluated_at",
            ),
        ),
        "lane_week_trends": TableContract(
            "lane_week_trends",
            "lane_week_trends.parquet",
            _columns(
                "lane_id",
                "mode",
                "week_start",
                "shipment_count",
                "average_cost_per_lb",
                "trailing_baseline",
                "deviation_score",
            ),
        ),
        "operational_alerts": TableContract(
            "operational_alerts",
            "operational_alerts.parquet",
            _columns(
                "alert_id",
                "lane_id",
                "mode",
                "carrier_scope",
                "window_start",
                "window_end",
                "primary_signal",
                "reason",
                "severity",
                "affected_shipment_count",
                "estimated_excess_cost",
                "confidence_score",
                "priority_score",
            ),
        ),
    }
)

COLUMN_KINDS = MappingProxyType(
    {
        "shipments": {
            "run_id": "string",
            "schema_version": "string",
            "shipment_id": "string",
            "ship_date": "temporal",
            "lane_id": "string",
            "origin_city": "string",
            "origin_state": "string",
            "destination_city": "string",
            "destination_state": "string",
            "carrier_id": "string",
            "mode": "string",
            "weight_lbs": "numeric",
            "freight_class": "string",
            "base_cost": "numeric",
            "fuel_surcharge": "numeric",
            "total_cost": "numeric",
            "on_time_flag": "integer",
        },
        "carrier_rates": {
            "run_id": "string",
            "schema_version": "string",
            "rate_id": "string",
            "carrier_id": "string",
            "lane_id": "string",
            "mode": "string",
            "effective_start": "temporal",
            "effective_end": "temporal",
            "base_rate_per_cwt": "numeric",
            "minimum_charge": "numeric",
        },
        "fuel_surcharges": {
            "run_id": "string",
            "schema_version": "string",
            "week_start": "temporal",
            "mode": "string",
            "fuel_index": "numeric",
            "surcharge_rate": "numeric",
            "curve_basis": "string",
        },
        "anomaly_ground_truth": {
            "run_id": "string",
            "schema_version": "string",
            "shipment_id": "string",
            "is_anomaly": "integer",
            "anomaly_type": "string",
            "changed_fields": "string",
            "injected_magnitude": "numeric",
        },
        "anomaly_flags": {
            "run_id": "string",
            "schema_version": "string",
            "flag_id": "string",
            "shipment_id": "string",
            "method": "string",
            "score": "numeric",
            "threshold": "numeric",
            "support": "numeric",
            "reason": "string",
            "is_flagged": "integer",
            "evaluated_at": "temporal",
        },
        "lane_week_trends": {
            "run_id": "string",
            "schema_version": "string",
            "lane_id": "string",
            "mode": "string",
            "week_start": "temporal",
            "shipment_count": "integer",
            "average_cost_per_lb": "numeric",
            "trailing_baseline": "numeric",
            "deviation_score": "numeric",
        },
        "operational_alerts": {
            "run_id": "string",
            "schema_version": "string",
            "alert_id": "string",
            "lane_id": "string",
            "mode": "string",
            "carrier_scope": "string",
            "window_start": "temporal",
            "window_end": "temporal",
            "primary_signal": "string",
            "reason": "string",
            "severity": "string",
            "affected_shipment_count": "integer",
            "estimated_excess_cost": "numeric",
            "confidence_score": "numeric",
            "priority_score": "numeric",
        },
    }
)


def get_contract(table_name: str) -> TableContract:
    """Return the declared contract or fail with a useful table name."""
    try:
        return TABLE_CONTRACTS[table_name]
    except KeyError as error:
        raise UnknownTableError(f"Unknown Freight v2 table: {table_name}") from error


def validate_columns(table_name: str, columns: Collection[str]) -> None:
    """Allow additive columns while rejecting absent required fields."""
    contract = get_contract(table_name)
    missing = contract.required_columns.difference(columns)
    if missing:
        raise MissingColumnsError(table_name, missing)


def _matches_kind(data_type: pa.DataType, kind: str) -> bool:
    if kind == "string":
        return pa.types.is_string(data_type) or pa.types.is_large_string(data_type)
    if kind == "numeric":
        return (
            pa.types.is_integer(data_type)
            or pa.types.is_floating(data_type)
            or pa.types.is_decimal(data_type)
        ) and not pa.types.is_boolean(data_type)
    if kind == "integer":
        return pa.types.is_integer(data_type) or pa.types.is_boolean(data_type)
    if kind == "temporal":
        return pa.types.is_date(data_type) or pa.types.is_timestamp(data_type)
    raise RuntimeError(f"Unknown contract kind: {kind}")


def validate_arrow_schema(table_name: str, schema: pa.Schema) -> None:
    """Validate required columns and broad physical types after serialization."""
    validate_columns(table_name, schema.names)
    for column, kind in COLUMN_KINDS[table_name].items():
        data_type = schema.field(column).type
        if not _matches_kind(data_type, kind):
            raise ColumnTypeError(f"{table_name}.{column} uses {data_type}, expected {kind}")
