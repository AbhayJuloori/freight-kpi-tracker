"""Tests for authoritative expected costs and robust fallback baselines."""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from freight_v2.anomalies import TRUTH_FIELDS, inject_anomalies
from freight_v2.baselines import (
    BASELINE_HIERARCHY,
    MINIMUM_SUPPORTS,
    RESERVED_SCRATCH_COLUMNS,
    BaselineModel,
    calculate_expected_costs,
    fit_and_score_baselines,
    fit_baselines,
    score_with_baselines,
)
from freight_v2.config import SCHEMA_VERSION, SeedSource, create_run_id
from freight_v2.generation import CALIBRATION_END, generate_normal_run
from freight_v2.sources import fixture_distribution


@pytest.fixture(scope="module")
def tables():
    run_id = create_run_id(
        SeedSource.TEST,
        random_seed=101,
        fixture_name="baselines",
        rows=3_000,
        anomaly_seed=102,
    )
    normal = generate_normal_run(
        fixture_distribution(SeedSource.TEST),
        run_id=run_id,
        random_seed=101,
        n_shipments=3_000,
    )
    injected = inject_anomalies(normal.shipments, random_seed=102)
    return injected, normal.carrier_rates, normal.fuel_surcharges


def test_expected_cost_uses_authoritative_rate_and_fuel_tables(tables) -> None:
    injected, rates, fuel = tables
    original = calculate_expected_costs(injected.shipments, rates, fuel)

    mutable_duplicates = injected.shipments.copy()
    mutable_duplicates["expected_base_cost"] = -999_999.0
    mutable_duplicates["expected_fuel_surcharge"] = -999_999.0
    mutable_duplicates["expected_total_cost"] = -999_999.0
    mutable_duplicates["base_rate_per_cwt"] = 999_999.0
    mutable_duplicates["minimum_charge"] = 999_999.0
    mutable_duplicates["fuel_surcharge_rate"] = 0.999999
    recalculated = calculate_expected_costs(mutable_duplicates, rates, fuel)

    authoritative = [
        "authoritative_base_rate_per_cwt",
        "authoritative_minimum_charge",
        "authoritative_fuel_surcharge_rate",
        "expected_base_cost",
        "expected_fuel_surcharge",
        "expected_total_cost",
        "cost_residual",
    ]
    pd.testing.assert_frame_equal(original[authoritative], recalculated[authoritative])

    expected_base = np.maximum(
        original["authoritative_base_rate_per_cwt"] * original["weight_lbs"] / 100.0,
        original["authoritative_minimum_charge"],
    ).round(2)
    expected_fuel = (expected_base * original["authoritative_fuel_surcharge_rate"]).round(2)
    trusted = original["monetary_values_trusted"]
    assert original.loc[trusted, "expected_base_cost"].to_numpy() == pytest.approx(
        expected_base[trusted], abs=0.001
    )
    assert original.loc[trusted, "expected_fuel_surcharge"].to_numpy() == pytest.approx(
        expected_fuel[trusted], abs=0.001
    )
    assert original.loc[trusted, "expected_total_cost"].to_numpy() == pytest.approx(
        expected_base[trusted] + expected_fuel[trusted], abs=0.001
    )


def test_fit_records_robust_statistics_and_explicit_fallback(tables) -> None:
    injected, rates, fuel = tables
    result = fit_and_score_baselines(injected.shipments, rates, fuel)

    assert BASELINE_HIERARCHY == (
        ("carrier_lane_mode", ("carrier_id", "lane_id", "mode")),
        ("lane_mode", ("lane_id", "mode")),
        ("mode", ("mode",)),
        ("global", ()),
    )
    assert MINIMUM_SUPPORTS == {
        "carrier_lane_mode": 20,
        "lane_mode": 30,
        "mode": 50,
        "global": 1,
    }
    assert set(result.model.statistics["baseline_source"]) == {
        "carrier_lane_mode",
        "lane_mode",
        "mode",
        "global",
    }
    required = {
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
        "residual_scale",
        "residual_scale_source",
        "residual_scale_degenerate",
    }
    assert required <= set(result.scored.columns)
    assert result.scored["baseline_source"].notna().all()
    assert result.scored["baseline_support"].gt(0).all()
    assert result.scored["baseline_source"].isin(dict(BASELINE_HIERARCHY)).all()
    assert (result.scored["residual_iqr"] >= 0).all()
    assert (result.scored["residual_mad"] >= 0).all()
    assert result.scored["residual_mad_scale"].to_numpy() == pytest.approx(
        result.scored["residual_mad"] * 1.4826
    )
    assert result.scored["residual_iqr_lower"].to_numpy() == pytest.approx(
        result.scored["residual_q1"] - 1.5 * result.scored["residual_iqr"]
    )
    assert result.scored["residual_iqr_upper"].to_numpy() == pytest.approx(
        result.scored["residual_q3"] + 1.5 * result.scored["residual_iqr"]
    )
    assert np.isfinite(result.scored["residual_scale"]).all()
    assert result.scored["residual_scale"].gt(0).all()
    assert result.scored["residual_scale_source"].isin({"mad", "iqr", "floor"}).all()
    floor = result.scored["residual_scale_source"].eq("floor")
    assert result.scored.loc[floor, "residual_scale"].eq(0.01).all()
    assert result.scored.loc[floor, "residual_scale_degenerate"].all()


def test_degenerate_exact_segment_falls_back_to_supported_lane_mode(tables) -> None:
    injected, rates, fuel = tables
    expected = calculate_expected_costs(injected.shipments, rates, fuel)
    baseline = expected["time_window"].eq("baseline") & expected["monetary_values_trusted"]
    target = expected.loc[baseline].iloc[0]
    exact = (
        expected["carrier_id"].eq(target["carrier_id"])
        & expected["lane_id"].eq(target["lane_id"])
        & expected["mode"].eq(target["mode"])
    )
    broader = (
        baseline
        & expected["lane_id"].eq(target["lane_id"])
        & expected["mode"].eq(target["mode"])
        & ~expected["carrier_id"].eq(target["carrier_id"])
    )
    expected.loc[baseline, "cost_residual"] = 0.0
    expected.loc[broader, "cost_residual"] = np.resize(
        np.array([-3.0, -1.0, 1.0, 3.0]), broader.sum()
    )

    model = fit_baselines(expected)
    scored = score_with_baselines(expected, model)
    exact_stat = model.statistics.loc[
        model.statistics["baseline_source"].eq("carrier_lane_mode")
        & model.statistics["carrier_id"].eq(target["carrier_id"])
        & model.statistics["lane_id"].eq(target["lane_id"])
        & model.statistics["mode"].eq(target["mode"])
    ].iloc[0]
    lane_stat = model.statistics.loc[
        model.statistics["baseline_source"].eq("lane_mode")
        & model.statistics["lane_id"].eq(target["lane_id"])
        & model.statistics["mode"].eq(target["mode"])
    ].iloc[0]
    assert not exact_stat["baseline_eligible"]
    assert lane_stat["baseline_eligible"]
    assert scored.loc[exact, "baseline_source"].eq("lane_mode").all()


def test_zero_mad_uses_positive_iqr_scale_before_floor(tables) -> None:
    injected, rates, fuel = tables
    expected = calculate_expected_costs(injected.shipments, rates, fuel)
    baseline = expected["time_window"].eq("baseline") & expected["monetary_values_trusted"]
    expected.loc[baseline, "cost_residual"] = np.resize(
        np.array([0.0, 0.0, 0.0, 1.0, 2.0]), baseline.sum()
    )
    model = fit_baselines(expected)
    global_row = model.statistics.loc[model.statistics["baseline_source"].eq("global")].iloc[0]
    assert global_row["residual_mad"] == 0.0
    assert global_row["residual_iqr"] > 0.0
    assert global_row["residual_scale_source"] == "iqr"
    assert not global_row["residual_scale_degenerate"]
    assert global_row["residual_scale"] == pytest.approx(global_row["residual_iqr"] / 1.349)


@pytest.mark.parametrize(
    ("table_name", "mutation", "message"),
    [
        ("shipments", lambda frame: frame.assign(is_anomaly=0), "truth"),
        ("shipments", lambda frame: frame.assign(run_id="other-run"), "run_id"),
        ("fuel", lambda frame: frame.assign(schema_version="0.0.0"), "schema"),
        (
            "shipments",
            lambda frame: frame.assign(ship_date=frame["ship_date"] + pd.Timedelta(hours=1)),
            "normalized",
        ),
        (
            "rates",
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "duplicate",
        ),
        (
            "fuel",
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "duplicate",
        ),
        ("shipments", lambda frame: frame.assign(rate_id="missing-rate"), "rate"),
        ("shipments", lambda frame: frame.assign(carrier_id="wrong-carrier"), "rate"),
        (
            "rates",
            lambda frame: frame.assign(effective_end=pd.Timestamp("2022-12-31")),
            "effective",
        ),
    ],
)
def test_corrupt_lineage_dates_and_keys_are_rejected(
    tables, table_name: str, mutation, message: str
) -> None:
    injected, carrier_rates, fuel_surcharges = tables
    shipments = injected.shipments.copy()
    rates = carrier_rates.copy()
    fuel = fuel_surcharges.copy()
    if table_name == "shipments":
        shipments = mutation(shipments)
    elif table_name == "rates":
        rates = mutation(rates)
    else:
        fuel = mutation(fuel)
    with pytest.raises(ValueError, match=message):
        calculate_expected_costs(shipments, rates, fuel)


def test_expected_cost_rejects_nonfinite_authoritative_inputs(tables) -> None:
    injected, rates, fuel = tables
    invalid = rates.copy()
    invalid.loc[invalid.index[0], "base_rate_per_cwt"] = np.inf
    with pytest.raises(ValueError, match="finite"):
        calculate_expected_costs(injected.shipments, invalid, fuel)


def test_absurd_weight_is_untrusted_and_overflowing_authority_is_rejected(tables) -> None:
    injected, rates, fuel = tables
    absurd = injected.shipments.copy()
    absurd.loc[absurd.index[0], "weight_lbs"] = np.finfo(float).max
    result = calculate_expected_costs(absurd, rates, fuel)
    assert not result.loc[0, "monetary_values_trusted"]
    assert pd.isna(result.loc[0, "expected_total_cost"])

    overflow_rate = rates.copy()
    overflow_rate.loc[overflow_rate.index[0], "base_rate_per_cwt"] = np.finfo(float).max
    with pytest.raises(ValueError, match="at most"):
        calculate_expected_costs(injected.shipments, overflow_rate, fuel)


def test_fit_rejects_nonfinite_trusted_residual(tables) -> None:
    injected, rates, fuel = tables
    expected = calculate_expected_costs(injected.shipments, rates, fuel)
    index = expected.index[
        expected["time_window"].eq("baseline") & expected["monetary_values_trusted"]
    ][0]
    expected.loc[index, "cost_residual"] = np.inf
    with pytest.raises(ValueError, match="trusted cost_residual.*finite"):
        fit_baselines(expected)


def test_nonpositive_billed_weight_has_nullable_untrusted_monetary_values(tables) -> None:
    injected, rates, fuel = tables
    corrupted = injected.shipments.copy()
    corrupted.loc[corrupted.index[:2], "weight_lbs"] = [0.0, -1.0]
    result = calculate_expected_costs(corrupted, rates, fuel)
    affected = result.iloc[:2]
    assert affected["monetary_values_trusted"].eq(False).all()  # noqa: E712
    assert (
        affected[
            [
                "expected_base_cost",
                "expected_fuel_surcharge",
                "expected_total_cost",
                "cost_residual",
                "estimated_excess_cost",
            ]
        ]
        .isna()
        .all()
        .all()
    )


def test_schema_version_is_current_on_scored_rows(tables) -> None:
    injected, rates, fuel = tables
    result = fit_and_score_baselines(injected.shipments, rates, fuel)
    assert result.scored["schema_version"].eq(SCHEMA_VERSION).all()


@pytest.mark.parametrize("truth_field", sorted(TRUTH_FIELDS))
def test_every_truth_field_is_rejected(tables, truth_field: str) -> None:
    injected, rates, fuel = tables
    leaked = injected.shipments.assign(**{truth_field: "leaked"})
    with pytest.raises(ValueError, match="truth/cause"):
        calculate_expected_costs(leaked, rates, fuel)


@pytest.mark.parametrize("reserved", sorted(RESERVED_SCRATCH_COLUMNS))
def test_reserved_scratch_columns_are_rejected_cleanly(tables, reserved: str) -> None:
    injected, rates, fuel = tables
    collided = injected.shipments.assign(**{reserved: 0})
    with pytest.raises(ValueError, match="reserved scratch"):
        calculate_expected_costs(collided, rates, fuel)


def test_model_snapshot_detects_tampering_and_invalid_fit_metadata(tables) -> None:
    injected, rates, fuel = tables
    result = fit_and_score_baselines(injected.shipments, rates, fuel)
    expected = calculate_expected_costs(injected.shipments, rates, fuel)

    result.model.statistics.loc[0, "residual_scale"] = 999.0
    with pytest.raises(ValueError, match="fingerprint"):
        score_with_baselines(expected, result.model)

    clean = fit_baselines(expected)
    with pytest.raises(ValueError, match="fit bounds"):
        replace(clean, fit_end=CALIBRATION_END)
    with pytest.raises(ValueError, match="statistic schema"):
        BaselineModel(
            run_id=clean.run_id,
            schema_version=clean.schema_version,
            fit_start=clean.fit_start,
            fit_end=clean.fit_end,
            statistics=clean.statistics.drop(columns="residual_scale"),
        )

    impossible = clean.statistics.copy(deep=True)
    impossible.loc[0, "residual_median"] = impossible.loc[0, "residual_q3"] + 1.0
    with pytest.raises(ValueError, match="median must lie within"):
        BaselineModel(
            run_id=clean.run_id,
            schema_version=clean.schema_version,
            fit_start=clean.fit_start,
            fit_end=clean.fit_end,
            statistics=impossible,
        )
