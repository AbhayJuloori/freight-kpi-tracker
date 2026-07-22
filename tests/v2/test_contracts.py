"""Tests for stable Freight v2 table contracts."""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from freight_v2.config import SCHEMA_VERSION, SeedSource, create_run_id
from freight_v2.contracts import (
    COLUMN_KINDS,
    TABLE_CONTRACTS,
    ColumnTypeError,
    MissingColumnsError,
    UnknownTableError,
    validate_arrow_schema,
    validate_columns,
)


def test_schema_and_supported_seed_sources_are_explicit() -> None:
    assert SCHEMA_VERSION == "2.0.0"
    assert {source.value for source in SeedSource} == {"FAF5", "PRIORS", "TEST"}


def test_fixture_run_ids_are_deterministic_but_normal_runs_are_uuid4() -> None:
    first = create_run_id(SeedSource.TEST, random_seed=41, fixture_name="contract")
    second = create_run_id(SeedSource.TEST, random_seed=41, fixture_name="contract")
    assert first == second
    assert first.startswith("test-")

    normal = create_run_id(SeedSource.PRIORS, random_seed=41)
    assert uuid.UUID(normal).version == 4
    assert normal != create_run_id(SeedSource.PRIORS, random_seed=41)


def test_test_run_requires_named_fixture() -> None:
    with pytest.raises(ValueError, match="fixture_name"):
        create_run_id(SeedSource.TEST, random_seed=41)


def test_every_canonical_table_has_run_identity_and_durable_filename() -> None:
    assert set(TABLE_CONTRACTS) == {
        "shipments",
        "carrier_rates",
        "fuel_surcharges",
        "anomaly_ground_truth",
        "anomaly_flags",
        "lane_week_trends",
        "operational_alerts",
    }
    for name, contract in TABLE_CONTRACTS.items():
        assert contract.name == name
        assert contract.filename.endswith(".parquet")
        assert "run_id" in contract.required_columns
        assert "schema_version" in contract.required_columns


@pytest.mark.parametrize(
    ("table_name", "domain_columns"),
    [
        (
            "shipments",
            {
                "shipment_id",
                "ship_date",
                "lane_id",
                "carrier_id",
                "mode",
                "weight_lbs",
                "base_cost",
                "fuel_surcharge",
                "total_cost",
                "on_time_flag",
            },
        ),
        (
            "carrier_rates",
            {
                "rate_id",
                "carrier_id",
                "lane_id",
                "mode",
                "effective_start",
                "effective_end",
                "base_rate_per_cwt",
                "minimum_charge",
            },
        ),
        (
            "fuel_surcharges",
            {"week_start", "mode", "fuel_index", "surcharge_rate", "curve_basis"},
        ),
        (
            "anomaly_ground_truth",
            {
                "shipment_id",
                "is_anomaly",
                "anomaly_type",
                "changed_fields",
                "injected_magnitude",
            },
        ),
        (
            "anomaly_flags",
            {
                "flag_id",
                "shipment_id",
                "method",
                "score",
                "threshold",
                "support",
                "reason",
                "is_flagged",
                "evaluated_at",
            },
        ),
        (
            "lane_week_trends",
            {
                "lane_id",
                "mode",
                "week_start",
                "shipment_count",
                "average_cost_per_lb",
                "trailing_baseline",
                "deviation_score",
            },
        ),
        (
            "operational_alerts",
            {
                "alert_id",
                "lane_id",
                "mode",
                "carrier_scope",
                "primary_signal",
                "reason",
                "severity",
                "affected_shipment_count",
                "estimated_excess_cost",
                "confidence_score",
                "priority_score",
            },
        ),
    ],
)
def test_each_table_requires_its_named_domain_columns(
    table_name: str, domain_columns: set[str]
) -> None:
    required = TABLE_CONTRACTS[table_name].required_columns
    assert domain_columns <= required
    for omitted in domain_columns:
        with pytest.raises(MissingColumnsError) as error_info:
            validate_columns(table_name, required - {omitted})
        assert error_info.value.missing == {omitted}


def test_contract_allows_additive_columns_and_reports_all_missing_columns() -> None:
    required = TABLE_CONTRACTS["shipments"].required_columns
    validate_columns("shipments", required | {"future_additive_field"})

    with pytest.raises(MissingColumnsError) as error_info:
        validate_columns("shipments", required - {"shipment_id", "total_cost"})
    assert error_info.value.missing == {"shipment_id", "total_cost"}


def test_unknown_table_is_rejected() -> None:
    with pytest.raises(UnknownTableError, match="mystery"):
        validate_columns("mystery", {"run_id"})


def test_fuel_curve_basis_is_a_required_serialized_string() -> None:
    assert COLUMN_KINDS["fuel_surcharges"]["curve_basis"] == "string"
    schema = pa.schema(
        [
            ("run_id", pa.string()),
            ("schema_version", pa.string()),
            ("week_start", pa.timestamp("ns")),
            ("mode", pa.string()),
            ("fuel_index", pa.float64()),
            ("surcharge_rate", pa.float64()),
            ("curve_basis", pa.string()),
        ]
    )
    validate_arrow_schema("fuel_surcharges", schema)

    invalid_schema = schema.set(
        schema.get_field_index("curve_basis"), pa.field("curve_basis", pa.float64())
    )
    with pytest.raises(ColumnTypeError, match=r"fuel_surcharges\.curve_basis.*expected string"):
        validate_arrow_schema("fuel_surcharges", invalid_schema)
