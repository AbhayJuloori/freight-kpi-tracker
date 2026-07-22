"""Tests for transparent, deduplicated operational-alert prioritization."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from freight_v2.config import SCHEMA_VERSION
from freight_v2.contracts import validate_columns
from freight_v2.prioritization import (
    CONFIDENCE_COMPONENT_WEIGHTS,
    PRIORITY_COMPONENT_WEIGHTS,
    prioritize_operational_alerts,
)

RUN_ID = "test-prioritization"


@pytest.fixture
def shipments() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "run_id": RUN_ID,
            "schema_version": SCHEMA_VERSION,
            "shipment_id": ["S1", "S2", "S3"],
            "ship_date": pd.to_datetime(["2024-04-08", "2024-04-09", "2024-04-10"]),
            "lane_id": ["IL-TX", "IL-TX", "IL-TX"],
            "carrier_id": ["C1", "C1", "C1"],
            "mode": ["LTL", "LTL", "PARCEL"],
            "on_time_flag": [1, 0, 0],
            "transit_days": [4, 7, 6],
        }
    )


def _flag(
    flag_id: str,
    shipment_id: str,
    method: str,
    method_family: str,
    *,
    mode: str = "LTL",
    exposure: float | None = 0.0,
    evidence_unit_id: str | None = None,
    is_flagged: int = 1,
    is_evaluable: bool = True,
    support: int = 20,
) -> dict[str, object]:
    week_start = pd.Timestamp("2024-04-08")
    return {
        "run_id": RUN_ID,
        "schema_version": SCHEMA_VERSION,
        "flag_id": flag_id,
        "shipment_id": shipment_id,
        "method": method,
        "method_family": method_family,
        "score": 4.0 if is_evaluable else np.nan,
        "threshold": 3.0,
        "support": support,
        "reason": "detector evidence",
        "is_flagged": is_flagged,
        "is_evaluable": is_evaluable,
        "evaluated_at": week_start + pd.Timedelta(days=6),
        "lane_id": "IL-TX",
        "carrier_id": "C1",
        "mode": mode,
        "week_start": week_start,
        "evidence_unit_id": evidence_unit_id or flag_id,
        "estimated_excess_cost": exposure,
    }


def _flags(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_weights_are_fixed_documented_and_normalized() -> None:
    assert tuple(PRIORITY_COMPONENT_WEIGHTS) == (
        "exposure",
        "persistence",
        "service_impact",
        "method_agreement",
        "data_quality",
        "support",
    )
    assert sum(PRIORITY_COMPONENT_WEIGHTS.values()) == pytest.approx(1.0)
    assert sum(CONFIDENCE_COMPONENT_WEIGHTS.values()) == pytest.approx(1.0)


def test_alerts_deduplicate_shipments_families_exposure_and_group_evidence(shipments) -> None:
    flags = _flags(
        _flag("F1", "S1", "robust_residual", "cost_reconciliation", exposure=100.0),
        _flag("F2", "S1", "iqr", "cost_reconciliation", exposure=100.0),
        _flag("F3", "S2", "robust_residual", "cost_reconciliation", exposure=50.0),
        _flag(
            "F4",
            "S1",
            "lane_week_deviation",
            "lane_cost_trend",
            exposure=100.0,
            evidence_unit_id="LANE-WEEK-1",
        ),
        _flag(
            "F5",
            "S2",
            "lane_week_deviation",
            "lane_cost_trend",
            exposure=50.0,
            evidence_unit_id="LANE-WEEK-1",
        ),
    )

    alerts = prioritize_operational_alerts(shipments, flags)
    validate_columns("operational_alerts", alerts.columns)
    assert len(alerts) == 2
    carrier_alert = alerts.loc[alerts["carrier_scope"].eq("C1")].iloc[0]
    assert carrier_alert["affected_shipment_count"] == 2
    assert carrier_alert["estimated_excess_cost"] == pytest.approx(150.0)
    assert carrier_alert["method_family_count"] == 1
    assert carrier_alert["evidence_unit_count"] == 2

    lane_alert = alerts.loc[alerts["carrier_scope"].eq("ALL")].iloc[0]
    assert lane_alert["affected_shipment_count"] == 2
    assert lane_alert["estimated_excess_cost"] == pytest.approx(150.0)
    assert lane_alert["evidence_unit_count"] == 1
    assert lane_alert["mode"] == "LTL"
    assert alerts["alert_id"].is_unique
    assert not alerts.columns.intersection(
        {"is_anomaly", "anomaly_type", "changed_fields", "injected_magnitude"}
    ).any()


def test_service_evidence_preserves_cross_mode_group_scope(shipments) -> None:
    flags = _flags(
        _flag(
            "SVC1",
            "S2",
            "service_deterioration",
            "carrier_service_trend",
            exposure=None,
            evidence_unit_id="SERVICE-GROUP-1",
        ),
        _flag(
            "SVC2",
            "S3",
            "service_deterioration",
            "carrier_service_trend",
            mode="PARCEL",
            exposure=None,
            evidence_unit_id="SERVICE-GROUP-1",
        ),
    )
    alert = prioritize_operational_alerts(shipments, flags).iloc[0]
    assert alert["mode"] == "ALL"
    assert alert["carrier_scope"] == "C1"
    assert alert["affected_shipment_count"] == 2
    assert alert["affected_service_shipment_count"] == 2
    assert alert["evidence_unit_count"] == 1
    assert alert["service_impact_component"] == pytest.approx(1.0)


def test_only_flagged_and_evaluable_rows_contribute(shipments) -> None:
    flags = _flags(
        _flag("KEEP", "S1", "robust_residual", "cost_reconciliation", exposure=10.0),
        _flag(
            "UNFLAGGED",
            "S2",
            "robust_residual",
            "cost_reconciliation",
            exposure=1_000_000.0,
            is_flagged=0,
        ),
        _flag(
            "UNEVALUABLE",
            "S3",
            "robust_residual",
            "cost_reconciliation",
            mode="PARCEL",
            exposure=1_000_000.0,
            is_evaluable=False,
        ),
    )
    alert = prioritize_operational_alerts(shipments, flags).iloc[0]
    assert alert["affected_shipment_count"] == 1
    assert alert["estimated_excess_cost"] == pytest.approx(10.0)


def test_priority_is_monotone_in_exposure_and_confidence_in_distinct_family_agreement(
    shipments,
) -> None:
    base = _flags(_flag("F1", "S1", "robust_residual", "cost_reconciliation", exposure=10.0))
    higher_exposure = base.copy(deep=True)
    higher_exposure["estimated_excess_cost"] = 10_000.0
    first = prioritize_operational_alerts(shipments, base).iloc[0]
    second = prioritize_operational_alerts(shipments, higher_exposure).iloc[0]
    assert second["priority_score"] > first["priority_score"]
    assert second["confidence_score"] == first["confidence_score"]

    agreement = pd.concat(
        [
            base,
            _flags(
                _flag(
                    "F2",
                    "S1",
                    "independent_confirmation",
                    "model_confirmation",
                    exposure=10.0,
                )
            ),
        ],
        ignore_index=True,
    )
    third = prioritize_operational_alerts(shipments, agreement).iloc[0]
    assert third["method_family_count"] == 2
    assert third["priority_score"] > first["priority_score"]
    assert third["confidence_score"] > first["confidence_score"]


def test_sparse_data_quality_and_empty_cases_are_finite_and_bounded(shipments) -> None:
    dq = _flags(
        _flag(
            "DQ1",
            "S1",
            "data_quality",
            "data_quality",
            exposure=None,
            support=1,
        )
    )
    alert = prioritize_operational_alerts(shipments, dq).iloc[0]
    assert alert["estimated_excess_cost"] == 0.0
    assert alert["data_quality_shipment_count"] == 1
    assert alert["data_quality_component"] == 0.0
    assert np.isfinite(alert["priority_score"])
    assert np.isfinite(alert["confidence_score"])
    assert 0 <= alert["priority_score"] <= 100
    assert 0 <= alert["confidence_score"] <= 1
    assert alert["reason"].endswith(".")

    empty = prioritize_operational_alerts(shipments, dq.assign(is_flagged=0))
    assert empty.empty
    validate_columns("operational_alerts", empty.columns)


def test_output_is_stable_under_input_order(shipments) -> None:
    flags = _flags(
        _flag("F1", "S1", "robust_residual", "cost_reconciliation", exposure=100.0),
        _flag("F2", "S2", "data_quality", "data_quality", exposure=None),
    )
    expected = prioritize_operational_alerts(shipments, flags)
    actual = prioritize_operational_alerts(
        shipments.sample(frac=1, random_state=3),
        flags.sample(frac=1, random_state=4),
    )
    pd.testing.assert_frame_equal(expected, actual)


@pytest.mark.parametrize(
    ("shipment_mutation", "flag_mutation", "message"),
    [
        (lambda frame: frame.assign(run_id="other"), lambda frame: frame, "run_id"),
        (lambda frame: frame, lambda frame: frame.assign(schema_version="1.0.0"), "schema"),
        (
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            lambda frame: frame,
            "duplicate shipment_id",
        ),
        (
            lambda frame: frame,
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "duplicate flag_id",
        ),
        (lambda frame: frame, lambda frame: frame.assign(shipment_id="OUTSIDE"), "outside"),
        (lambda frame: frame.assign(is_anomaly=0), lambda frame: frame, "truth"),
    ],
)
def test_lineage_keys_membership_and_truth_leakage_are_rejected(
    shipments,
    shipment_mutation,
    flag_mutation,
    message: str,
) -> None:
    flags = _flags(_flag("F1", "S1", "robust_residual", "cost_reconciliation", exposure=10.0))
    with pytest.raises(ValueError, match=message):
        prioritize_operational_alerts(shipment_mutation(shipments.copy()), flag_mutation(flags))
