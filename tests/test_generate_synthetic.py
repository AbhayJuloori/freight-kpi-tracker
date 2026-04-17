"""Unit tests for generate_synthetic.py"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from generate_synthetic import (
    MODES,
    generate_carrier_rates,
    generate_fuel_surcharges,
    generate_shipments,
)

SEED = 42


def _make_rng() -> np.random.Generator:
    return np.random.default_rng(SEED)


def test_fuel_surcharges_shape():
    df = generate_fuel_surcharges()
    assert len(df) > 100
    assert set(df.columns) == {"week_start", "fuel_price_per_gallon", "surcharge_pct"}


def test_fuel_surcharges_range():
    df = generate_fuel_surcharges()
    assert df["fuel_price_per_gallon"].between(3.0, 7.0).all()
    assert df["surcharge_pct"].between(0.0, 1.0).all()


def test_carrier_rates_no_duplicate_pks():
    rates = generate_carrier_rates(_make_rng())
    dupes = rates.duplicated(subset=["carrier_id", "mode", "lane_id"]).sum()
    assert dupes == 0, f"CARRIER_RATES has {dupes} duplicate (carrier_id, mode, lane_id) PKs"


def test_carrier_rates_columns():
    rates = generate_carrier_rates(_make_rng())
    assert {"carrier_id", "mode", "lane_id", "base_rate_per_cwt", "effective_date"}.issubset(
        set(rates.columns)
    )


def test_shipments_row_count():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 500, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert len(ships) == 500


def test_shipments_columns():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    expected = {
        "shipment_id", "ship_date", "origin_city", "dest_city", "mode",
        "carrier_id", "weight_lbs", "base_rate_per_cwt", "base_cost",
        "fuel_surcharge_pct", "total_cost", "on_time_flag", "lane_id", "run_id",
    }
    assert expected.issubset(set(ships.columns))


def test_base_cost_equals_rate_times_weight():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    # For non-anomaly rows: base_cost ≈ base_rate_per_cwt * weight_lbs / 100
    # Anomalies inflate total_cost but not base_cost, so check base_cost directly
    computed = (ships["base_rate_per_cwt"] * ships["weight_lbs"] / 100).clip(lower=1.0).round(2)
    assert (ships["base_cost"] - computed).abs().max() < 0.02


def test_join_coverage_100_percent():
    """Every shipment (carrier_id, mode, lane_id) must exist in CARRIER_RATES."""
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 500, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    rate_keys = set(zip(rates["carrier_id"], rates["mode"], rates["lane_id"]))
    ship_keys = set(zip(ships["carrier_id"], ships["mode"], ships["lane_id"]))
    missing = ship_keys - rate_keys
    assert len(missing) == 0, f"{len(missing)} shipment keys not found in CARRIER_RATES"


def test_modes_valid():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert set(ships["mode"].unique()).issubset(set(MODES))


def test_on_time_flag_binary():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert set(ships["on_time_flag"].unique()).issubset({0, 1})


def test_anomaly_injection_rate():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    _, gt = generate_shipments(rates, fuel, 2000, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    rate = gt["is_anomaly"].mean()
    assert 0.04 <= rate <= 0.10, f"Anomaly rate {rate:.2%} outside expected 4-10%"


def test_total_cost_positive():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert (ships["total_cost"] > 0).all()


def test_lane_id_format():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    sample = ships["lane_id"].iloc[0]
    parts = sample.split("-")
    assert len(parts) == 2
    assert all(len(p) == 2 for p in parts)


def test_run_id_propagated():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, gt = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "my-run-123")
    assert (ships["run_id"] == "my-run-123").all()
    assert (gt["run_id"] == "my-run-123").all()
