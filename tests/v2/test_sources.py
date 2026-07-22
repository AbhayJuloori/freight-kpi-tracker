"""Tests for explicit distribution-source adapters."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from freight_v2.config import SeedSource
from freight_v2.sources import (
    MODES,
    fixture_distribution,
    load_distribution,
    load_faf5_distribution,
    zone_to_state,
)


def _write_faf5_fixture(path: Path) -> None:
    pd.DataFrame(
        {
            "dms_orig": [171, 171, 481, 61, 999, 171],
            "dms_dest": [481, 481, 131, 531, 171, 481],
            "dms_mode": [1, 1, 4, 5, 1, 2],
            "tons_2024": [10.0, 5.0, 2.0, 3.0, 100.0, 100.0],
        }
    ).to_csv(path, index=False)


def test_test_and_priors_distributions_are_normalized_and_immutable() -> None:
    for source in (SeedSource.TEST, SeedSource.PRIORS):
        distribution = fixture_distribution(source)
        assert distribution.seed_source is source
        assert sum(distribution.lane_weights.values()) == pytest.approx(1.0)
        assert sum(distribution.mode_probabilities.values()) == pytest.approx(1.0)
        assert set(distribution.mode_probabilities) == set(MODES)
        with pytest.raises(TypeError):
            distribution.lane_weights["IL-CA"] = 1.0  # type: ignore[index]


def test_faf5_adapter_streams_and_derives_supported_rows(tmp_path: Path) -> None:
    path = tmp_path / "faf5.csv"
    _write_faf5_fixture(path)
    distribution = load_faf5_distribution(path, chunksize=2)
    assert distribution.seed_source is SeedSource.FAF5
    assert set(distribution.lane_weights) == {"IL-TX", "TX-GA", "CA-WA"}
    assert distribution.lane_weights["IL-TX"] == pytest.approx(0.75)
    assert distribution.mode_probabilities == pytest.approx(
        {"PARCEL": 0.40, "LTL": 0.35, "FTL": 0.25}
    )
    assert distribution.source_mode_shares == pytest.approx(
        {
            "TRUCK": 15 / 120,
            "AIR_TRUCK_AIR": 2 / 120,
            "MULTIPLE_MODES_MAIL": 3 / 120,
            "RAIL": 100 / 120,
        }
    )
    assert "positive-tonnage interstate flows" in distribution.mode_basis
    assert distribution.source_file == "faf5.csv"
    assert distribution.source_sha256 is not None and len(distribution.source_sha256) == 64


def test_source_selection_is_explicit(tmp_path: Path) -> None:
    path = tmp_path / "faf5.csv"
    _write_faf5_fixture(path)
    assert load_distribution(SeedSource.FAF5, faf5_path=path, chunksize=2).source_file
    with pytest.raises(ValueError, match="requires faf5_path"):
        load_distribution(SeedSource.FAF5)
    with pytest.raises(ValueError, match="cannot accept"):
        load_distribution(SeedSource.TEST, faf5_path=path)


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1.0, True])
def test_distribution_rejects_nonfinite_or_invalid_weights(invalid: float) -> None:
    with pytest.raises(ValueError, match="lane_weights"):
        from freight_v2.sources import SourceDistribution

        SourceDistribution(
            seed_source=SeedSource.TEST,
            lane_weights={"IL-TX": invalid},
            mode_probabilities={"PARCEL": 0.4, "LTL": 0.35, "FTL": 0.25},
            mode_basis="test",
        )


def test_faf5_adapter_rejects_missing_or_unusable_schema(tmp_path: Path) -> None:
    missing = tmp_path / "missing.csv"
    with pytest.raises(FileNotFoundError):
        load_faf5_distribution(missing)

    unusable = tmp_path / "unusable.csv"
    pd.DataFrame({"wrong": [1]}).to_csv(unusable, index=False)
    with pytest.raises(ValueError, match="missing required columns"):
        load_faf5_distribution(unusable, chunksize=1)


@pytest.mark.parametrize("zone", [float("nan"), float("inf"), 171.9, -171, "bad"])
def test_zone_mapping_rejects_nonfinite_fractional_or_invalid_values(zone: object) -> None:
    assert zone_to_state(zone) is None
