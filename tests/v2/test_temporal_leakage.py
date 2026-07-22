"""Mutation tests for Freight v2 temporal leakage boundaries."""

from __future__ import annotations

import pandas as pd

from freight_v2.anomalies import inject_anomalies
from freight_v2.baselines import fit_and_score_baselines
from freight_v2.config import SeedSource, create_run_id
from freight_v2.generation import (
    BASELINE_END,
    CALIBRATION_END,
    CALIBRATION_START,
    EVALUATION_START,
    generate_normal_run,
)
from freight_v2.sources import fixture_distribution


def _tables():
    run_id = create_run_id(
        SeedSource.TEST,
        random_seed=211,
        fixture_name="temporal-leakage",
        rows=4_000,
        anomaly_seed=212,
    )
    normal = generate_normal_run(
        fixture_distribution(SeedSource.TEST),
        run_id=run_id,
        random_seed=211,
        n_shipments=4_000,
    )
    injected = inject_anomalies(normal.shipments, random_seed=212)
    return injected.shipments, normal.carrier_rates, normal.fuel_surcharges


def test_evaluation_mutation_cannot_change_fitted_statistics_or_earlier_scores() -> None:
    shipments, rates, fuel = _tables()
    original = fit_and_score_baselines(shipments, rates, fuel)

    mutated = shipments.copy()
    future = mutated["ship_date"].ge(EVALUATION_START)
    mutated.loc[future, "total_cost"] = mutated.loc[future, "total_cost"] * 25 + 10_000
    mutated.loc[future, "on_time_flag"] = 1 - mutated.loc[future, "on_time_flag"]
    changed = fit_and_score_baselines(mutated, rates, fuel)

    pd.testing.assert_frame_equal(original.model.statistics, changed.model.statistics)
    earlier_ids = original.scored.loc[
        original.scored["ship_date"].le(CALIBRATION_END), "shipment_id"
    ]
    score_columns = [
        "shipment_id",
        "expected_base_cost",
        "expected_fuel_surcharge",
        "expected_total_cost",
        "cost_residual",
        "baseline_source",
        "baseline_support",
        "residual_median",
        "residual_mad",
        "residual_mad_scale",
        "residual_q1",
        "residual_q3",
        "residual_iqr",
        "residual_iqr_lower",
        "residual_iqr_upper",
        "residual_from_median",
    ]
    left = original.scored.set_index("shipment_id").loc[earlier_ids, score_columns[1:]]
    right = changed.scored.set_index("shipment_id").loc[earlier_ids, score_columns[1:]]
    pd.testing.assert_frame_equal(left, right)


def test_calibration_mutation_cannot_change_model_or_baseline_scores() -> None:
    shipments, rates, fuel = _tables()
    original = fit_and_score_baselines(shipments, rates, fuel)

    mutated = shipments.copy()
    calibration = mutated["ship_date"].between(CALIBRATION_START, CALIBRATION_END)
    mutated.loc[calibration, "total_cost"] = mutated.loc[calibration, "total_cost"] * 40
    changed = fit_and_score_baselines(mutated, rates, fuel)

    pd.testing.assert_frame_equal(original.model.statistics, changed.model.statistics)
    baseline_ids = original.scored.loc[original.scored["ship_date"].le(BASELINE_END), "shipment_id"]
    left = original.scored.set_index("shipment_id").loc[baseline_ids]
    right = changed.scored.set_index("shipment_id").loc[baseline_ids]
    pd.testing.assert_frame_equal(left, right)


def test_fitted_support_uses_baseline_rows_only() -> None:
    shipments, rates, fuel = _tables()
    result = fit_and_score_baselines(shipments, rates, fuel)
    global_row = result.model.statistics.query("baseline_source == 'global'").iloc[0]
    trusted_baseline = (
        result.scored["ship_date"].le(BASELINE_END) & result.scored["monetary_values_trusted"]
    )
    assert global_row["baseline_support"] == trusted_baseline.sum()


def test_future_fuel_schedule_mutation_cannot_change_model_or_earlier_scores() -> None:
    shipments, rates, fuel = _tables()
    original = fit_and_score_baselines(shipments, rates, fuel)

    mutated_fuel = fuel.copy()
    future_fuel = mutated_fuel["week_start"].ge(CALIBRATION_START)
    mutated_fuel.loc[future_fuel, "surcharge_rate"] *= 1.75
    changed = fit_and_score_baselines(shipments, rates, mutated_fuel)

    pd.testing.assert_frame_equal(original.model.statistics, changed.model.statistics)
    baseline_ids = original.scored.loc[original.scored["ship_date"].le(BASELINE_END), "shipment_id"]
    left = original.scored.set_index("shipment_id").loc[baseline_ids]
    right = changed.scored.set_index("shipment_id").loc[baseline_ids]
    pd.testing.assert_frame_equal(left, right)
