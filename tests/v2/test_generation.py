"""Tests for deterministic, internally consistent normal freight generation."""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from freight_v2.config import SeedSource, create_run_id
from freight_v2.contracts import TABLE_CONTRACTS
from freight_v2.generation import (
    BASELINE_END,
    BASELINE_START,
    CALIBRATION_END,
    CALIBRATION_START,
    EVALUATION_END,
    EVALUATION_START,
    FUEL_CURVE_BASIS,
    MODE_WEIGHT,
    RATE_PERIODS,
    generate_normal_run,
    generate_shipments,
)
from freight_v2.sources import fixture_distribution


@pytest.fixture
def normal_run():
    run_id = create_run_id(SeedSource.TEST, random_seed=19, fixture_name="generation")
    return generate_normal_run(
        fixture_distribution(SeedSource.TEST),
        run_id=run_id,
        random_seed=19,
        n_shipments=2_000,
    )


def _frame_hash(frame: pd.DataFrame, path: Path) -> str:
    frame.to_parquet(path, index=False)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_generated_tables_satisfy_contracts_and_unique_keys(normal_run) -> None:
    tables = {
        "carrier_rates": normal_run.carrier_rates,
        "fuel_surcharges": normal_run.fuel_surcharges,
        "shipments": normal_run.shipments,
    }
    for name, frame in tables.items():
        assert TABLE_CONTRACTS[name].required_columns <= set(frame.columns)
        assert frame["run_id"].nunique() == 1
        assert frame["schema_version"].nunique() == 1
    assert not normal_run.carrier_rates.duplicated(
        ["carrier_id", "lane_id", "mode", "effective_start"]
    ).any()
    assert normal_run.carrier_rates["rate_id"].is_unique
    assert not normal_run.fuel_surcharges.duplicated(["week_start", "mode"]).any()
    assert normal_run.shipments["shipment_id"].is_unique


def test_every_shipment_resolves_one_rate_and_one_fuel_record(normal_run) -> None:
    shipments = normal_run.shipments
    rates = normal_run.carrier_rates
    rate_join = shipments.merge(
        rates[
            [
                "rate_id",
                "carrier_id",
                "lane_id",
                "mode",
                "effective_start",
                "effective_end",
            ]
        ],
        on=["rate_id", "carrier_id", "lane_id", "mode"],
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    assert (rate_join["_merge"] == "both").all()
    assert (
        rate_join["ship_date"]
        .between(rate_join["effective_start"], rate_join["effective_end"])
        .all()
    )

    weeks = shipments["ship_date"] - pd.to_timedelta(shipments["ship_date"].dt.dayofweek, unit="D")
    fuel_keys = pd.MultiIndex.from_frame(normal_run.fuel_surcharges[["week_start", "mode"]])
    shipment_keys = pd.MultiIndex.from_arrays([weeks, shipments["mode"]])
    assert shipment_keys.isin(fuel_keys).all()


def test_rate_cards_are_time_versioned_contiguous_and_repriced(normal_run) -> None:
    rates = normal_run.carrier_rates
    expected_starts = [start for _, start, _ in RATE_PERIODS]
    expected_ends = [end for _, _, end in RATE_PERIODS]
    assert len(RATE_PERIODS) == 6
    for _, versions in rates.groupby(["carrier_id", "lane_id", "mode"], sort=True):
        ordered = versions.sort_values("effective_start")
        assert ordered["effective_start"].tolist() == expected_starts
        assert ordered["effective_end"].tolist() == expected_ends
        assert ordered["base_rate_per_cwt"].nunique() > 1
        changes = ordered["base_rate_per_cwt"].pct_change().dropna().abs()
        assert changes.max() <= 0.08
    assert rates["rate_id"].is_unique
    assert rates["effective_start"].nunique() == len(RATE_PERIODS)
    assert rates["base_rate_per_cwt"].groupby(rates["effective_start"]).mean().nunique() > 1


def test_weekly_fuel_curve_is_complete_nonflat_bounded_and_disclosed(normal_run) -> None:
    fuel = normal_run.fuel_surcharges
    expected_weeks = pd.date_range(BASELINE_START, EVALUATION_END, freq="W-MON")
    assert "synthetic" in FUEL_CURVE_BASIS.lower()
    assert "not observed EIA" in FUEL_CURVE_BASIS
    assert fuel["curve_basis"].eq(FUEL_CURVE_BASIS).all()
    for mode, curve in fuel.groupby("mode", sort=True):
        ordered = curve.sort_values("week_start")
        assert ordered["week_start"].tolist() == expected_weeks.tolist(), mode
        assert ordered["fuel_index"].nunique() > 10
        assert ordered["surcharge_rate"].nunique() > 10
        assert ordered["fuel_index"].diff().abs().max() <= 0.06
        assert ordered["surcharge_rate"].diff().abs().max() <= 0.01


def test_normal_costs_are_decomposable_and_truth_free(normal_run) -> None:
    shipments = normal_run.shipments
    expected_base = (shipments["base_rate_per_cwt"] * shipments["weight_lbs"] / 100).clip(
        lower=shipments["minimum_charge"]
    )
    assert shipments["base_cost"].to_numpy() == pytest.approx(expected_base, abs=0.0051)
    assert shipments["fuel_surcharge"].to_numpy() == pytest.approx(
        shipments["base_cost"] * shipments["fuel_surcharge_rate"], abs=0.011
    )
    assert shipments["total_cost"].to_numpy() == pytest.approx(
        shipments["base_cost"] + shipments["fuel_surcharge"], abs=0.001
    )
    forbidden = {
        "expected_base_cost",
        "expected_fuel_surcharge",
        "expected_total_cost",
        "is_anomaly",
        "anomaly_type",
        "changed_fields",
        "injected_magnitude",
    }
    assert forbidden.isdisjoint(shipments.columns)


def test_weights_service_and_dates_stay_in_supported_ranges(normal_run) -> None:
    shipments = normal_run.shipments
    for mode, (_, _, lower, upper) in MODE_WEIGHT.items():
        values = shipments.loc[shipments["mode"] == mode, "weight_lbs"]
        assert not values.empty
        assert values.between(lower, upper).all()
    assert set(shipments["on_time_flag"].unique()) <= {0, 1}
    assert (shipments["transit_days"] >= 2).all()
    assert shipments["total_cost"].gt(0).all()
    assert shipments["ship_date"].between(BASELINE_START, EVALUATION_END).all()
    assert shipments["ship_date"].between(BASELINE_START, BASELINE_END).any()
    assert shipments["ship_date"].between(CALIBRATION_START, CALIBRATION_END).any()
    assert shipments["ship_date"].between(EVALUATION_START, EVALUATION_END).any()

    counts = [
        shipments["ship_date"].between(start, end).sum()
        for start, end in (
            (BASELINE_START, BASELINE_END),
            (CALIBRATION_START, CALIBRATION_END),
            (EVALUATION_START, EVALUATION_END),
        )
    ]
    daily_rates = [counts[0] / 364, counts[1] / 91, counts[2] / 91]
    assert max(daily_rates) / min(daily_rates) < 1.02


def test_identical_seed_produces_identical_parquet_hashes(tmp_path: Path) -> None:
    distribution = fixture_distribution(SeedSource.TEST)
    run_id = create_run_id(SeedSource.TEST, random_seed=23, fixture_name="hash")
    first = generate_normal_run(distribution, run_id=run_id, random_seed=23, n_shipments=500)
    second = generate_normal_run(distribution, run_id=run_id, random_seed=23, n_shipments=500)
    for name in ("carrier_rates", "fuel_surcharges", "shipments"):
        assert _frame_hash(getattr(first, name), tmp_path / f"first-{name}.parquet") == _frame_hash(
            getattr(second, name), tmp_path / f"second-{name}.parquet"
        )


def test_generation_rejects_tiny_or_invalid_requests() -> None:
    distribution = fixture_distribution(SeedSource.TEST)
    with pytest.raises(ValueError, match="non-negative"):
        generate_normal_run(distribution, run_id="test", random_seed=-1, n_shipments=100)
    with pytest.raises(ValueError, match="at least 3"):
        generate_normal_run(distribution, run_id="test", random_seed=1, n_shipments=2)


@pytest.mark.parametrize(
    ("table_name", "mutation", "message"),
    [
        ("carrier_rates", lambda frame: frame.assign(run_id=None), "different run"),
        ("carrier_rates", lambda frame: frame.assign(schema_version="0.0.0"), "schema"),
        (
            "carrier_rates",
            lambda frame: frame.assign(effective_end=pd.Timestamp("2022-12-31")),
            "full generation window",
        ),
        ("fuel_surcharges", lambda frame: frame.assign(run_id=None), "different run"),
        ("fuel_surcharges", lambda frame: frame.assign(schema_version="0.0.0"), "schema"),
        (
            "fuel_surcharges",
            lambda frame: frame.assign(curve_basis="Observed EIA diesel prices"),
            "curve basis",
        ),
    ],
)
def test_generation_rejects_invalid_input_lineage(
    normal_run, table_name: str, mutation, message: str
) -> None:
    rates = normal_run.carrier_rates.copy()
    fuel = normal_run.fuel_surcharges.copy()
    if table_name == "carrier_rates":
        rates = mutation(rates)
    else:
        fuel = mutation(fuel)
    with pytest.raises(ValueError, match=message):
        generate_shipments(
            rates,
            fuel,
            fixture_distribution(SeedSource.TEST),
            run_id=normal_run.shipments["run_id"].iloc[0],
            n_shipments=100,
            rng=np.random.default_rng(1),
        )


def test_generation_rejects_duplicate_rate_and_fuel_keys(normal_run) -> None:
    run_id = normal_run.shipments["run_id"].iloc[0]
    distribution = fixture_distribution(SeedSource.TEST)
    duplicated_rates = pd.concat(
        [normal_run.carrier_rates, normal_run.carrier_rates.iloc[[0]]], ignore_index=True
    )
    with pytest.raises(ValueError, match="duplicate keys"):
        generate_shipments(
            duplicated_rates,
            normal_run.fuel_surcharges,
            distribution,
            run_id=run_id,
            n_shipments=100,
            rng=np.random.default_rng(1),
        )

    duplicated_fuel = pd.concat(
        [normal_run.fuel_surcharges, normal_run.fuel_surcharges.iloc[[0]]], ignore_index=True
    )
    with pytest.raises(ValueError, match="duplicate week/mode"):
        generate_shipments(
            normal_run.carrier_rates,
            duplicated_fuel,
            distribution,
            run_id=run_id,
            n_shipments=100,
            rng=np.random.default_rng(1),
        )


@pytest.mark.parametrize("boundary_delta", [-1, 1])
def test_generation_rejects_rate_period_gaps_and_overlaps(normal_run, boundary_delta: int) -> None:
    rates = normal_run.carrier_rates.copy()
    key = rates.iloc[0][["carrier_id", "lane_id", "mode"]]
    group = rates.index[
        rates["carrier_id"].eq(key["carrier_id"])
        & rates["lane_id"].eq(key["lane_id"])
        & rates["mode"].eq(key["mode"])
    ]
    first = rates.loc[group].sort_values("effective_start").index[0]
    rates.loc[first, "effective_end"] += pd.Timedelta(days=boundary_delta)
    with pytest.raises(ValueError, match="gap|overlap"):
        generate_shipments(
            rates,
            normal_run.fuel_surcharges,
            fixture_distribution(SeedSource.TEST),
            run_id=normal_run.shipments["run_id"].iloc[0],
            n_shipments=100,
            rng=np.random.default_rng(1),
        )


def test_generation_requires_exact_weekly_fuel_lookup_without_fallback(normal_run) -> None:
    missing = normal_run.fuel_surcharges.drop(normal_run.fuel_surcharges.index[0])
    with pytest.raises(ValueError, match="exactly cover"):
        generate_shipments(
            normal_run.carrier_rates,
            missing,
            fixture_distribution(SeedSource.TEST),
            run_id=normal_run.shipments["run_id"].iloc[0],
            n_shipments=100,
            rng=np.random.default_rng(1),
        )


@pytest.mark.parametrize(
    ("table_name", "column", "value"),
    [
        ("carrier_rates", "base_rate_per_cwt", float("nan")),
        ("carrier_rates", "minimum_charge", -1.0),
        ("carrier_rates", "service_level_target", 1.1),
        ("carrier_rates", "contract_transit_days", -10),
        ("fuel_surcharges", "fuel_index", float("inf")),
        ("fuel_surcharges", "surcharge_rate", float("nan")),
    ],
)
def test_generation_rejects_invalid_numeric_domains(
    normal_run, table_name: str, column: str, value: float
) -> None:
    rates = normal_run.carrier_rates.copy()
    fuel = normal_run.fuel_surcharges.copy()
    target = rates if table_name == "carrier_rates" else fuel
    target[column] = value
    with pytest.raises(ValueError, match=column):
        generate_shipments(
            rates,
            fuel,
            fixture_distribution(SeedSource.TEST),
            run_id=normal_run.shipments["run_id"].iloc[0],
            n_shipments=100,
            rng=np.random.default_rng(1),
        )


def test_generation_normalizes_valid_string_fuel_dates(normal_run) -> None:
    fuel = normal_run.fuel_surcharges.copy()
    fuel["week_start"] = fuel["week_start"].dt.strftime("%Y-%m-%d")
    shipments = generate_shipments(
        normal_run.carrier_rates,
        fuel,
        fixture_distribution(SeedSource.TEST),
        run_id=normal_run.shipments["run_id"].iloc[0],
        n_shipments=100,
        rng=np.random.default_rng(1),
    )
    assert len(shipments) == 100
    assert shipments["total_cost"].notna().all()


def test_generation_rejects_duplicate_fuel_keys_after_date_normalization(normal_run) -> None:
    fuel = normal_run.fuel_surcharges.copy()
    duplicate = fuel.iloc[[0]].copy()
    duplicate["week_start"] = duplicate["week_start"].dt.strftime("%Y-%m-%d")
    mixed_dates = pd.concat([fuel, duplicate], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate week/mode"):
        generate_shipments(
            normal_run.carrier_rates,
            mixed_dates,
            fixture_distribution(SeedSource.TEST),
            run_id=normal_run.shipments["run_id"].iloc[0],
            n_shipments=100,
            rng=np.random.default_rng(1),
        )


def test_uneven_carrier_counts_do_not_distort_lane_mode_distribution(normal_run) -> None:
    distribution = fixture_distribution(SeedSource.TEST)
    rates = normal_run.carrier_rates
    target = rates[(rates["lane_id"] == "IL-TX") & (rates["mode"] == "PARCEL")]
    keep_carrier = target["carrier_id"].iloc[0]
    target_rows = target.index[target["carrier_id"].ne(keep_carrier)]
    uneven_rates = rates.drop(target_rows).reset_index(drop=True)
    shipments = generate_shipments(
        uneven_rates,
        normal_run.fuel_surcharges,
        distribution,
        run_id=normal_run.shipments["run_id"].iloc[0],
        n_shipments=30_000,
        rng=np.random.default_rng(7),
    )
    observed = ((shipments["lane_id"] == "IL-TX") & (shipments["mode"] == "PARCEL")).mean()
    expected = distribution.lane_weights["IL-TX"] * distribution.mode_probabilities["PARCEL"]
    assert observed == pytest.approx(expected, abs=0.01)


@pytest.mark.parametrize(("column", "value"), [("rate_id", None), ("carrier_id", "")])
def test_generation_rejects_null_or_empty_rate_business_keys(
    normal_run, column: str, value: object
) -> None:
    rates = normal_run.carrier_rates.copy()
    rates.loc[rates.index[0], column] = value
    with pytest.raises(ValueError, match=column):
        generate_shipments(
            rates,
            normal_run.fuel_surcharges,
            fixture_distribution(SeedSource.TEST),
            run_id=normal_run.shipments["run_id"].iloc[0],
            n_shipments=100,
            rng=np.random.default_rng(1),
        )
