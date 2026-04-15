"""Unit tests for generate_synthetic.py"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from generate_synthetic import (
    CARRIER_POOL,
    CITY_POOL,
    MODES,
    N_SHIPMENTS,
    generate_fuel_surcharges,
    generate_shipments,
)


def test_fuel_surcharges_shape():
    df = generate_fuel_surcharges()
    assert len(df) > 100
    assert set(df.columns) == {"week_start", "fuel_price_per_gallon", "surcharge_pct"}


def test_fuel_surcharges_range():
    df = generate_fuel_surcharges()
    assert df["fuel_price_per_gallon"].between(3.0, 7.0).all()
    assert df["surcharge_pct"].between(0.0, 1.0).all()


def test_shipments_row_count():
    fuel = generate_fuel_surcharges()
    ships, _ = generate_shipments(fuel)
    assert len(ships) == N_SHIPMENTS


def test_shipments_columns():
    fuel = generate_fuel_surcharges()
    ships, _ = generate_shipments(fuel)
    expected = {
        "shipment_id", "ship_date", "origin_city", "dest_city", "mode",
        "carrier_id", "weight_lbs", "base_rate", "fuel_surcharge_pct",
        "total_cost", "on_time_flag", "lane_id",
    }
    assert expected.issubset(set(ships.columns))


def test_modes_valid():
    fuel = generate_fuel_surcharges()
    ships, _ = generate_shipments(fuel)
    assert set(ships["mode"].unique()).issubset(set(MODES))


def test_on_time_flag_binary():
    fuel = generate_fuel_surcharges()
    ships, _ = generate_shipments(fuel)
    assert set(ships["on_time_flag"].unique()).issubset({0, 1})


def test_anomaly_injection_rate():
    fuel = generate_fuel_surcharges()
    _, gt = generate_shipments(fuel)
    rate = gt["is_anomaly"].mean()
    assert 0.04 <= rate <= 0.10, f"Anomaly rate {rate:.2%} outside expected 4-10%"


def test_total_cost_positive():
    fuel = generate_fuel_surcharges()
    ships, _ = generate_shipments(fuel)
    assert (ships["total_cost"] > 0).all()


def test_lane_id_format():
    fuel = generate_fuel_surcharges()
    ships, _ = generate_shipments(fuel)
    # lane_id should be STATE-STATE format, e.g. IL-CA
    sample = ships["lane_id"].iloc[0]
    parts = sample.split("-")
    assert len(parts) == 2
    assert all(len(p) == 2 for p in parts)
