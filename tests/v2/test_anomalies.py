"""Tests for typed anomaly isolation and documented field effects."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from freight_v2.anomalies import (
    ANOMALY_TYPES,
    DEFAULT_ANOMALY_RATES,
    DOCUMENTED_CHANGED_FIELDS,
    INJECTION_REQUIRED_COLUMNS,
    TRUTH_FIELDS,
    inject_anomalies,
)
from freight_v2.config import SeedSource, create_run_id
from freight_v2.generation import EVALUATION_START, generate_normal_run
from freight_v2.sources import fixture_distribution


@pytest.fixture(scope="module")
def normal_shipments() -> pd.DataFrame:
    run_id = create_run_id(SeedSource.TEST, random_seed=31, fixture_name="anomalies")
    return generate_normal_run(
        fixture_distribution(SeedSource.TEST),
        run_id=run_id,
        random_seed=31,
        n_shipments=5_000,
    ).shipments


@pytest.fixture(scope="module")
def injected(normal_shipments: pd.DataFrame):
    return inject_anomalies(normal_shipments, random_seed=73)


def _frame_hash(frame: pd.DataFrame, path: Path) -> str:
    frame.to_parquet(path, index=False)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_ground_truth_is_separate_complete_and_non_overlapping(
    normal_shipments: pd.DataFrame, injected
) -> None:
    assert TRUTH_FIELDS.isdisjoint(injected.shipments.columns)
    assert len(injected.ground_truth) == len(normal_shipments)
    assert injected.ground_truth["shipment_id"].is_unique
    anomalous = injected.ground_truth.query("is_anomaly == 1")
    assert set(anomalous["anomaly_type"]) == set(ANOMALY_TYPES)
    requested_count = sum(
        round(rate * len(normal_shipments)) for rate in DEFAULT_ANOMALY_RATES.values()
    )
    assert 0 < len(anomalous) <= requested_count + len(ANOMALY_TYPES)
    assert anomalous.groupby("shipment_id").size().max() == 1


def test_each_type_changes_only_its_documented_observable_fields(
    normal_shipments: pd.DataFrame, injected
) -> None:
    original = normal_shipments.set_index("shipment_id")
    modified = injected.shipments.set_index("shipment_id")
    truth = injected.ground_truth.query("is_anomaly == 1")
    for anomaly_type in ANOMALY_TYPES:
        ids = truth.loc[truth["anomaly_type"] == anomaly_type, "shipment_id"]
        for shipment_id in ids:
            changed = {
                column
                for column in original.columns
                if original.loc[shipment_id, column] != modified.loc[shipment_id, column]
            }
            assert changed == DOCUMENTED_CHANGED_FIELDS[anomaly_type]
        encoded = truth.loc[truth["anomaly_type"] == anomaly_type, "changed_fields"].unique()
        assert len(encoded) == 1
        assert set(json.loads(encoded[0])) == DOCUMENTED_CHANGED_FIELDS[anomaly_type]


def test_non_anomalous_rows_are_byte_for_byte_unchanged(
    normal_shipments: pd.DataFrame, injected
) -> None:
    normal_ids = injected.ground_truth.loc[injected.ground_truth["is_anomaly"].eq(0), "shipment_id"]
    pd.testing.assert_frame_equal(
        normal_shipments.set_index("shipment_id").loc[normal_ids],
        injected.shipments.set_index("shipment_id").loc[normal_ids],
    )


def test_cost_anomalies_preserve_invoice_arithmetic_without_counterfactual_costs(
    normal_shipments, injected
) -> None:
    cost_types = {
        "carrier_overcharge",
        "duplicate_fuel_surcharge",
        "rate_card_override",
        "persistent_lane_drift",
    }
    ids = injected.ground_truth.loc[
        injected.ground_truth["anomaly_type"].isin(cost_types), "shipment_id"
    ]
    modified = injected.shipments.set_index("shipment_id").loc[ids]
    assert modified["total_cost"].to_numpy() == pytest.approx(
        modified["base_cost"] + modified["fuel_surcharge"], abs=0.001
    )
    expected_columns = {"expected_base_cost", "expected_fuel_surcharge", "expected_total_cost"}
    assert expected_columns.isdisjoint(normal_shipments.columns)
    assert expected_columns.isdisjoint(injected.shipments.columns)


def test_group_anomalies_are_late_window_and_operationally_coherent(
    normal_shipments: pd.DataFrame, injected
) -> None:
    truth = injected.ground_truth.set_index("shipment_id")
    modified = injected.shipments.set_index("shipment_id")
    original = normal_shipments.set_index("shipment_id")

    persistent_ids = truth.index[truth["anomaly_type"].eq("persistent_lane_drift")]
    assert modified.loc[persistent_ids, "ship_date"].ge(EVALUATION_START).all()
    assert truth.loc[persistent_ids, "anomaly_group_id"].ne("").all()

    service_ids = truth.index[truth["anomaly_type"].eq("service_deterioration")]
    assert modified.loc[service_ids, "ship_date"].ge(EVALUATION_START).all()
    assert original.loc[service_ids, "on_time_flag"].eq(1).all()
    assert modified.loc[service_ids, "on_time_flag"].eq(0).all()
    assert (
        modified.loc[service_ids, "transit_days"]
        .gt(original.loc[service_ids, "transit_days"])
        .all()
    )


def test_injection_is_reproducible_and_does_not_mutate_input(
    normal_shipments: pd.DataFrame, tmp_path: Path
) -> None:
    before = normal_shipments.copy(deep=True)
    first = inject_anomalies(normal_shipments, random_seed=73)
    second = inject_anomalies(normal_shipments, random_seed=73)
    pd.testing.assert_frame_equal(normal_shipments, before)
    for name in ("shipments", "ground_truth"):
        assert _frame_hash(getattr(first, name), tmp_path / f"first-{name}.parquet") == _frame_hash(
            getattr(second, name), tmp_path / f"second-{name}.parquet"
        )


def test_invalid_rates_and_preexisting_truth_are_rejected(normal_shipments: pd.DataFrame) -> None:
    invalid_rates = dict(DEFAULT_ANOMALY_RATES)
    invalid_rates["carrier_overcharge"] = float("nan")
    with pytest.raises(ValueError, match="finite"):
        inject_anomalies(normal_shipments, random_seed=1, rates=invalid_rates)

    leaked = normal_shipments.assign(is_anomaly=0)
    with pytest.raises(ValueError, match="ground-truth fields"):
        inject_anomalies(leaked, random_seed=1)


def test_injector_declares_and_enforces_stage_specific_dependencies(
    normal_shipments: pd.DataFrame,
) -> None:
    assert set(normal_shipments.columns) >= INJECTION_REQUIRED_COLUMNS
    missing = normal_shipments.drop(columns="minimum_charge")
    with pytest.raises(ValueError, match="injection dependencies.*minimum_charge"):
        inject_anomalies(missing, random_seed=1)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda frame: frame.assign(weight_lbs=0.0),
        lambda frame: frame.assign(freight_class="UNKNOWN"),
        lambda frame: frame.assign(total_cost=frame["total_cost"] + 1.0),
    ],
)
def test_already_corrupt_or_non_normal_inputs_are_rejected(
    normal_shipments: pd.DataFrame, mutation
) -> None:
    with pytest.raises(ValueError):
        inject_anomalies(mutation(normal_shipments.copy()), random_seed=1)


def test_tiny_custom_rates_that_round_to_zero_are_honored(
    normal_shipments: pd.DataFrame,
) -> None:
    rates = {anomaly_type: 0.0 for anomaly_type in ANOMALY_TYPES}
    rates["carrier_overcharge"] = 1e-12
    result = inject_anomalies(normal_shipments, random_seed=1, rates=rates)
    assert result.ground_truth["is_anomaly"].sum() == 0
    pd.testing.assert_frame_equal(result.shipments, normal_shipments)


@pytest.mark.parametrize(
    "ship_dates",
    [
        lambda values: values.dt.tz_localize("UTC"),
        lambda values: values + pd.Timedelta(hours=12),
    ],
)
def test_timezone_aware_or_nonmidnight_dates_are_rejected(
    normal_shipments: pd.DataFrame, ship_dates
) -> None:
    invalid = normal_shipments.copy()
    invalid["ship_date"] = ship_dates(invalid["ship_date"])
    with pytest.raises(ValueError, match="ship_date"):
        inject_anomalies(invalid, random_seed=1)


def test_invoice_validation_uses_zero_relative_tolerance(normal_shipments: pd.DataFrame) -> None:
    invalid = normal_shipments.copy()
    index = invalid["total_cost"].idxmax()
    invalid.loc[index, "total_cost"] += 0.02
    with pytest.raises(ValueError, match="invoice-total identity"):
        inject_anomalies(invalid, random_seed=1)


def test_numeric_strings_are_rejected_before_arithmetic(normal_shipments: pd.DataFrame) -> None:
    invalid = normal_shipments.copy()
    invalid["total_cost"] = invalid["total_cost"].astype(str)
    with pytest.raises(ValueError, match="numeric dtype"):
        inject_anomalies(invalid, random_seed=1)


def test_zero_fuel_rows_fail_as_ineligible_for_duplicate_surcharge(
    normal_shipments: pd.DataFrame,
) -> None:
    zero_fuel = normal_shipments.copy()
    zero_fuel["fuel_surcharge_rate"] = 0.0
    zero_fuel["fuel_surcharge"] = 0.0
    zero_fuel["total_cost"] = zero_fuel["base_cost"]
    rates = {anomaly_type: 0.0 for anomaly_type in ANOMALY_TYPES}
    rates["duplicate_fuel_surcharge"] = 0.01
    with pytest.raises(ValueError, match="eligible"):
        inject_anomalies(zero_fuel, random_seed=1, rates=rates)


def _assert_rows_equal_by_id(
    first: pd.DataFrame,
    second: pd.DataFrame,
    shipment_ids: pd.Series,
) -> None:
    pd.testing.assert_frame_equal(
        first.set_index("shipment_id").loc[shipment_ids].sort_index(),
        second.set_index("shipment_id").loc[shipment_ids].sort_index(),
    )


@pytest.mark.parametrize("future_change", ["append", "remove", "mutate"])
def test_evaluation_rows_cannot_change_earlier_injection(
    normal_shipments: pd.DataFrame,
    future_change: str,
) -> None:
    original = inject_anomalies(normal_shipments, random_seed=73)
    earlier_ids = normal_shipments.loc[
        normal_shipments["ship_date"].lt(EVALUATION_START), "shipment_id"
    ]
    evaluation = normal_shipments.loc[normal_shipments["ship_date"].ge(EVALUATION_START)]
    changed = normal_shipments.copy()
    if future_change == "append":
        appended = evaluation.iloc[:100].copy()
        appended["shipment_id"] = appended["shipment_id"].map(lambda value: f"APPENDED-{value}")
        changed = pd.concat([changed, appended], ignore_index=True)
    elif future_change == "remove":
        changed = changed.drop(index=evaluation.index[:100])
    else:
        changed.loc[evaluation.index, "on_time_flag"] = (
            1 - changed.loc[evaluation.index, "on_time_flag"]
        )
        changed.loc[evaluation.index, "transit_days"] += 1

    reinjected = inject_anomalies(changed, random_seed=73)
    _assert_rows_equal_by_id(original.ground_truth, reinjected.ground_truth, earlier_ids)
    _assert_rows_equal_by_id(original.shipments, reinjected.shipments, earlier_ids)


def test_injection_is_invariant_to_row_order_and_custom_index(
    normal_shipments: pd.DataFrame,
) -> None:
    original = inject_anomalies(normal_shipments, random_seed=73)
    reordered = normal_shipments.sample(frac=1.0, random_state=901).copy()
    reordered.index = pd.Index([f"custom-{index}" for index in range(len(reordered))])
    reinjected = inject_anomalies(reordered, random_seed=73)
    pd.testing.assert_frame_equal(original.ground_truth, reinjected.ground_truth)
    pd.testing.assert_frame_equal(original.shipments, reinjected.shipments)


@pytest.mark.parametrize("changed_rate", [0.005, 0.025])
def test_one_family_rate_cannot_perturb_other_family_assignments_or_effects(
    normal_shipments: pd.DataFrame,
    changed_rate: float,
) -> None:
    original = inject_anomalies(normal_shipments, random_seed=73)
    changed_rates = dict(DEFAULT_ANOMALY_RATES)
    changed_rates["carrier_overcharge"] = changed_rate
    changed = inject_anomalies(normal_shipments, random_seed=73, rates=changed_rates)

    for anomaly_type in set(ANOMALY_TYPES) - {"carrier_overcharge"}:
        original_ids = original.ground_truth.loc[
            original.ground_truth["anomaly_type"].eq(anomaly_type), "shipment_id"
        ]
        changed_ids = changed.ground_truth.loc[
            changed.ground_truth["anomaly_type"].eq(anomaly_type), "shipment_id"
        ]
        assert set(original_ids) == set(changed_ids)
        _assert_rows_equal_by_id(original.shipments, changed.shipments, original_ids)


def test_family_ownership_is_disjoint_even_when_types_are_injected_separately(
    normal_shipments: pd.DataFrame,
) -> None:
    selected_by_type: dict[str, set[str]] = {}
    for anomaly_type in ANOMALY_TYPES:
        rates = {candidate: 0.0 for candidate in ANOMALY_TYPES}
        rates[anomaly_type] = DEFAULT_ANOMALY_RATES[anomaly_type]
        truth = inject_anomalies(normal_shipments, random_seed=73, rates=rates).ground_truth
        selected_by_type[anomaly_type] = set(
            truth.loc[truth["anomaly_type"].eq(anomaly_type), "shipment_id"]
        )

    for position, anomaly_type in enumerate(ANOMALY_TYPES):
        for other_type in ANOMALY_TYPES[position + 1 :]:
            assert selected_by_type[anomaly_type].isdisjoint(selected_by_type[other_type])


def test_cost_anomaly_assignment_never_changes_service_observables(
    normal_shipments: pd.DataFrame,
) -> None:
    no_anomalies = {anomaly_type: 0.0 for anomaly_type in ANOMALY_TYPES}
    cost_only = dict(no_anomalies)
    cost_only["carrier_overcharge"] = DEFAULT_ANOMALY_RATES["carrier_overcharge"]
    control = inject_anomalies(normal_shipments, random_seed=73, rates=no_anomalies)
    changed = inject_anomalies(normal_shipments, random_seed=73, rates=cost_only)
    service_columns = ["shipment_id", "on_time_flag", "transit_days"]
    pd.testing.assert_frame_equal(
        control.shipments[service_columns], changed.shipments[service_columns]
    )
