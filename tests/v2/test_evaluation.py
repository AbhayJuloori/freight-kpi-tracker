"""Tests for held-out metrics, calibration selection, and aggregation grain."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from freight_v2.anomalies import ANOMALY_TYPES, inject_anomalies
from freight_v2.baselines import fit_and_score_baselines
from freight_v2.config import SCHEMA_VERSION, SeedSource, create_run_id
from freight_v2.detection import DETECTION_METHODS, DetectorConfig
from freight_v2.evaluation import (
    DEFAULT_SENSITIVITY_GRID,
    SELECTION_RULE,
    build_evaluation,
    evaluate_flags,
    summarize_group_evidence,
)
from freight_v2.generation import generate_normal_run
from freight_v2.sources import fixture_distribution

RUN_ID = "test-evaluation-contract"


def _scored() -> pd.DataFrame:
    shipment_ids = list("ABCDEFG")
    return pd.DataFrame(
        {
            "run_id": RUN_ID,
            "schema_version": SCHEMA_VERSION,
            "shipment_id": shipment_ids,
            "time_window": ["evaluation"] * 6 + ["calibration"],
            "total_cost": [120.0, 70.0, 40.0, 30.0, 20.0, 10.0, 90.0],
            "on_time_flag": [1, 1, 1, 1, 0, 1, 1],
            "estimated_excess_cost": [100.0, 50.0, 0.0, 0.0, 0.0, 0.0, 80.0],
        }
    )


def _truth() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "run_id": RUN_ID,
            "schema_version": SCHEMA_VERSION,
            "shipment_id": list("ABCDEFG"),
            "is_anomaly": [1, 1, 0, 0, 1, 0, 1],
            "anomaly_type": [
                "carrier_overcharge",
                "duplicate_fuel_surcharge",
                "NONE",
                "NONE",
                "service_deterioration",
                "NONE",
                "carrier_overcharge",
            ],
        }
    )


def _flags() -> pd.DataFrame:
    contracts = {
        "robust_residual": ("cost_reconciliation", "baseline_shipments"),
        "iqr": ("cost_reconciliation", "baseline_shipments"),
        "lane_week_deviation": ("lane_cost_trend", "prior_observed_weeks"),
        "service_deterioration": ("carrier_service_trend", "prior_observed_weeks"),
        "data_quality": ("data_quality", "rules_evaluated"),
    }
    triggered = {
        ("A", "robust_residual"),
        ("A", "iqr"),
        ("A", "data_quality"),
        ("C", "data_quality"),
        ("E", "service_deterioration"),
        ("G", "robust_residual"),
    }
    rows = []
    for shipment_id in list("ABCDEFG"):
        for method in DETECTION_METHODS:
            family, support_unit = contracts[method]
            is_service_group = shipment_id == "E" and method == "service_deterioration"
            rows.append(
                {
                    "run_id": RUN_ID,
                    "schema_version": SCHEMA_VERSION,
                    "shipment_id": shipment_id,
                    "method": method,
                    "method_family": family,
                    "is_flagged": int((shipment_id, method) in triggered),
                    "evidence_unit_id": "SERVICE-GROUP-1"
                    if is_service_group
                    else f"FLAG-{shipment_id}-{method}",
                    "support_unit": support_unit,
                    "current_support": 2 if is_service_group else 1,
                }
            )
    return pd.DataFrame(rows)


def test_exact_held_out_hand_calculation_and_type_metrics() -> None:
    result = evaluate_flags(_scored(), _flags(), _truth(), window="evaluation")
    overall = result.overall
    assert overall == {
        "shipment_count": 6,
        "positive_count": 3,
        "negative_count": 3,
        "true_positives": 2,
        "false_positives": 1,
        "true_negatives": 2,
        "false_negatives": 1,
        "precision": pytest.approx(2 / 3),
        "recall": pytest.approx(2 / 3),
        "f1": pytest.approx(2 / 3),
        "false_positive_rate": pytest.approx(1 / 3),
        "review_volume": 3,
        "review_rate": pytest.approx(0.5),
        "excess_cost_coverage": pytest.approx(2 / 3),
    }
    carrier = result.by_anomaly_type.set_index("anomaly_type").loc["carrier_overcharge"]
    assert carrier["true_positives"] == 1
    assert carrier["false_positives"] == 2
    assert carrier["false_negatives"] == 0
    assert carrier["precision"] == pytest.approx(1 / 3)
    assert carrier["recall"] == 1.0
    assert carrier["false_positive_rate"] == pytest.approx(2 / 5)
    assert carrier["excess_cost_coverage"] == 1.0
    assert set(result.by_anomaly_type["anomaly_type"]) == set(ANOMALY_TYPES)


def test_union_and_method_agreement_use_shipment_and_distinct_family_grain() -> None:
    result = evaluate_flags(_scored(), _flags(), _truth(), window="evaluation")
    agreement = result.method_agreement.set_index("shipment_id")
    assert result.overall["review_volume"] == 3
    assert agreement.loc["A", "flagged_method_count"] == 3
    assert agreement.loc["A", "method_family_agreement"] == 2
    assert agreement.loc["B", "method_family_agreement"] == 0


@pytest.mark.parametrize("all_positive", [False, True])
def test_all_zero_and_all_positive_truth_have_finite_bounded_metrics(all_positive: bool) -> None:
    truth = _truth()
    evaluation = truth["shipment_id"].isin(list("ABCDEF"))
    truth.loc[evaluation, "is_anomaly"] = int(all_positive)
    truth.loc[evaluation, "anomaly_type"] = "carrier_overcharge" if all_positive else "NONE"
    result = evaluate_flags(_scored(), _flags(), truth, window="evaluation")
    for key in (
        "precision",
        "recall",
        "f1",
        "false_positive_rate",
        "review_rate",
        "excess_cost_coverage",
    ):
        assert math.isfinite(result.overall[key])
        assert 0.0 <= result.overall[key] <= 1.0
    if all_positive:
        assert result.overall["false_positive_rate"] == 0.0
    else:
        assert result.overall["recall"] == 0.0
        assert result.overall["false_negatives"] == 0


@pytest.mark.parametrize("prediction", [0, 1])
def test_all_zero_and_all_positive_predictions_are_well_defined(prediction: int) -> None:
    flags = _flags().assign(is_flagged=prediction)
    result = evaluate_flags(_scored(), flags, _truth(), window="evaluation")
    assert result.overall["review_volume"] == prediction * 6
    for key in ("precision", "recall", "f1", "false_positive_rate"):
        assert math.isfinite(result.overall[key])
        assert 0.0 <= result.overall[key] <= 1.0
    if prediction == 0:
        assert result.overall["precision"] == 0.0
        assert result.overall["recall"] == 0.0
    else:
        assert result.overall["recall"] == 1.0
        assert result.overall["false_positive_rate"] == 1.0


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda scored, flags, truth: (scored, flags, pd.concat([truth, truth.iloc[[0]]])),
            "truth",
        ),
        (lambda scored, flags, truth: (scored, pd.concat([flags, flags.iloc[[0]]]), truth), "flag"),
        (
            lambda scored, flags, truth: (
                scored,
                flags.assign(run_id="another-run"),
                truth,
            ),
            "run_id",
        ),
        (
            lambda scored, flags, truth: (
                scored,
                flags.assign(schema_version="0.0.0"),
                truth,
            ),
            "schema",
        ),
        (
            lambda scored, flags, truth: (
                scored,
                pd.concat(
                    [flags, flags.iloc[[0]].assign(shipment_id="OUTSIDE", method="outside")],
                    ignore_index=True,
                ),
                truth,
            ),
            "outside",
        ),
    ],
)
def test_key_run_and_schema_contracts_are_strict(mutation, message: str) -> None:
    scored, flags, truth = mutation(_scored(), _flags(), _truth())
    with pytest.raises(ValueError, match=message):
        evaluate_flags(scored, flags, truth, window="evaluation")


def test_group_evidence_deduplicates_historical_fanout_style_fixture() -> None:
    shipment_count = 75_000
    affected_count = 3_814
    # The historical defect repeated a 75k group support value across 3,814 affected rows.
    # Materializing the affected shipment grain is sufficient to prove that neither value
    # is multiplied; the normalized matrix still contains all five detector rows per shipment.
    shipment_ids = np.array([f"SHP-{index:08d}" for index in range(affected_count)])
    scored = pd.DataFrame(
        {
            "run_id": RUN_ID,
            "schema_version": SCHEMA_VERSION,
            "shipment_id": shipment_ids,
            "time_window": "evaluation",
            "total_cost": 1.0,
            "on_time_flag": 1,
            "estimated_excess_cost": 0.25,
        }
    )
    method_contracts = {
        "robust_residual": ("cost_reconciliation", "baseline_shipments"),
        "iqr": ("cost_reconciliation", "baseline_shipments"),
        "lane_week_deviation": ("lane_cost_trend", "prior_observed_weeks"),
        "service_deterioration": ("carrier_service_trend", "prior_observed_weeks"),
        "data_quality": ("data_quality", "rules_evaluated"),
    }
    repeated_ids = np.repeat(shipment_ids, len(DETECTION_METHODS))
    tiled_methods = np.tile(np.array(DETECTION_METHODS), affected_count)
    flagged_lane = tiled_methods == "lane_week_deviation"
    flags = pd.DataFrame(
        {
            "run_id": RUN_ID,
            "schema_version": SCHEMA_VERSION,
            "shipment_id": pd.Categorical(repeated_ids, categories=shipment_ids),
            "method": pd.Categorical(tiled_methods, categories=DETECTION_METHODS),
            "method_family": pd.Categorical(
                [method_contracts[method][0] for method in tiled_methods]
            ),
            "is_flagged": flagged_lane.astype(int),
            "evidence_unit_id": pd.Categorical(
                np.where(flagged_lane, "ONE-LANE-WEEK", "UNFLAGGED")
            ),
            "support_unit": pd.Categorical(
                [method_contracts[method][1] for method in tiled_methods]
            ),
            "current_support": np.where(flagged_lane, shipment_count, 1),
        }
    )
    evidence = summarize_group_evidence(scored, flags, window="evaluation")
    assert len(evidence) == 1
    row = evidence.iloc[0]
    assert row["affected_shipment_count"] == affected_count
    assert row["current_support"] == shipment_count
    assert row["observed_spend"] == affected_count
    assert row["on_time_shipment_count"] == affected_count
    assert row["estimated_excess_cost"] == pytest.approx(affected_count * 0.25)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda flags: flags.loc[~flags["shipment_id"].eq("A")],
            "Cartesian matrix",
        ),
        (
            lambda flags: flags.loc[
                ~(flags["shipment_id"].eq("A") & flags["method"].eq("robust_residual"))
            ],
            "Cartesian matrix",
        ),
        (
            lambda flags: flags.assign(
                method=np.where(flags.index == 0, "invented_detector", flags["method"])
            ),
            "unknown detector",
        ),
        (
            lambda flags: flags.assign(
                method_family=np.where(flags.index == 0, "wrong_family", flags["method_family"])
            ),
            "method_family",
        ),
        (
            lambda flags: flags.assign(
                support_unit=np.where(flags.index == 0, "wrong_support", flags["support_unit"])
            ),
            "support_unit",
        ),
    ],
)
def test_normalized_flag_matrix_and_method_metadata_fail_closed(mutation, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        evaluate_flags(_scored(), mutation(_flags()), _truth(), window="evaluation")


@pytest.fixture(scope="module")
def pipeline_inputs():
    run_id = create_run_id(
        SeedSource.TEST,
        random_seed=411,
        fixture_name="evaluation",
        rows=2_000,
        anomaly_seed=412,
    )
    normal = generate_normal_run(
        fixture_distribution(SeedSource.TEST),
        run_id=run_id,
        random_seed=411,
        n_shipments=2_000,
    )
    injected = inject_anomalies(normal.shipments, random_seed=412)
    baseline = fit_and_score_baselines(
        injected.shipments,
        normal.carrier_rates,
        normal.fuel_surcharges,
    )
    return baseline, injected.ground_truth


def test_bounded_grid_is_deterministic_and_selection_uses_calibration_only(
    pipeline_inputs,
) -> None:
    baseline, truth = pipeline_inputs
    first = build_evaluation(baseline, truth, configs=DEFAULT_SENSITIVITY_GRID)
    second = build_evaluation(baseline, truth, configs=tuple(reversed(DEFAULT_SENSITIVITY_GRID)))
    assert first.selected_config == second.selected_config
    assert first.selection_rule == SELECTION_RULE
    assert len(first.sensitivity_grid) == len(DEFAULT_SENSITIVITY_GRID)
    assert first.sensitivity_grid["config_id"].is_unique
    assert first.sensitivity_grid["selected"].sum() == 1
    assert np.isfinite(first.sensitivity_grid.select_dtypes(include="number")).all().all()

    mutated = truth.copy()
    evaluation_ids = set(
        baseline.scored.loc[baseline.scored["time_window"].eq("evaluation"), "shipment_id"]
    )
    future = mutated["shipment_id"].isin(evaluation_ids)
    mutated.loc[future, "is_anomaly"] = 0
    mutated.loc[future, "anomaly_type"] = "NONE"
    changed = build_evaluation(baseline, mutated, configs=DEFAULT_SENSITIVITY_GRID)
    assert changed.selected_config == first.selected_config
    calibration_columns = [
        column for column in first.sensitivity_grid if column.startswith("calibration_")
    ]
    pd.testing.assert_frame_equal(
        first.sensitivity_grid.sort_values("config_id")[calibration_columns].reset_index(drop=True),
        changed.sensitivity_grid.sort_values("config_id")[calibration_columns].reset_index(
            drop=True
        ),
    )


def test_grid_rejects_duplicates_unbounded_values_and_excess_size(pipeline_inputs) -> None:
    baseline, truth = pipeline_inputs
    duplicate = (DetectorConfig(), DetectorConfig())
    with pytest.raises(ValueError, match="unique"):
        build_evaluation(baseline, truth, configs=duplicate)
    with pytest.raises(ValueError, match="bounded"):
        build_evaluation(
            baseline,
            truth,
            configs=(DetectorConfig(robust_threshold=100.0),),
        )
    with pytest.raises(ValueError, match="at most"):
        build_evaluation(
            baseline,
            truth,
            configs=tuple(
                DetectorConfig(robust_threshold=2.0 + index / 100) for index in range(26)
            ),
        )
