"""Explicit adapters for FAF5 and deterministic distribution fixtures."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from math import isfinite
from pathlib import Path
from types import MappingProxyType

import pandas as pd

from freight_v2.config import SeedSource
from freight_v2.provenance import sha256_file

MODES = ("PARCEL", "LTL", "FTL")
# Official FAF5 modes are not Parcel/LTL/FTL. Preserve their real meaning instead of
# repeating the v1 prototype's invalid one-to-one relabeling.
FAF5_MODE_NAMES = {
    1: "TRUCK",
    2: "RAIL",
    3: "WATER",
    4: "AIR_TRUCK_AIR",
    5: "MULTIPLE_MODES_MAIL",
    6: "PIPELINE",
    7: "OTHER_UNKNOWN",
    8: "NO_DOMESTIC_MODE",
}
ROAD_RELEVANT_FAF5_MODES = {1, 4, 5}

FIPS_TO_STATE = {
    4: "AZ",
    6: "CA",
    8: "CO",
    12: "FL",
    13: "GA",
    17: "IL",
    18: "IN",
    21: "KY",
    24: "MD",
    25: "MA",
    26: "MI",
    27: "MN",
    29: "MO",
    36: "NY",
    37: "NC",
    39: "OH",
    41: "OR",
    42: "PA",
    47: "TN",
    48: "TX",
    49: "UT",
    53: "WA",
}

STATE_CITIES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "AZ": ("Phoenix",),
        "CA": ("Los Angeles", "San Jose"),
        "CO": ("Denver",),
        "FL": ("Miami",),
        "GA": ("Atlanta",),
        "IL": ("Chicago",),
        "IN": ("Indianapolis",),
        "KY": ("Louisville",),
        "MD": ("Baltimore",),
        "MA": ("Boston",),
        "MI": ("Detroit",),
        "MN": ("Minneapolis",),
        "MO": ("Kansas City", "St. Louis"),
        "NY": ("New York",),
        "NC": ("Charlotte",),
        "OH": ("Cincinnati", "Cleveland", "Columbus"),
        "OR": ("Portland",),
        "PA": ("Philadelphia", "Pittsburgh"),
        "TN": ("Memphis", "Nashville"),
        "TX": ("Dallas", "Houston", "San Antonio"),
        "UT": ("Salt Lake City",),
        "WA": ("Seattle",),
    }
)

PRIORS_LANE_WEIGHTS = {
    "CA-TX": 12.0,
    "TX-GA": 10.0,
    "IL-TX": 9.0,
    "GA-NC": 7.0,
    "PA-OH": 7.0,
    "TN-FL": 6.0,
    "CA-WA": 6.0,
    "NY-PA": 5.0,
    "MI-IL": 5.0,
    "TX-CA": 4.0,
    "OH-TN": 4.0,
    "MO-CO": 3.0,
}


@dataclass(frozen=True, slots=True)
class SourceDistribution:
    """Validated lane and mode support derived from one declared source."""

    seed_source: SeedSource
    lane_weights: Mapping[str, float]
    mode_probabilities: Mapping[str, float]
    mode_basis: str
    source_mode_shares: Mapping[str, float] = field(default_factory=dict)
    source_file: str | None = None
    source_sha256: str | None = None

    def __post_init__(self) -> None:
        lanes = dict(self.lane_weights)
        modes = dict(self.mode_probabilities)
        source_modes = dict(self.source_mode_shares)
        if not isinstance(self.seed_source, SeedSource):
            raise ValueError("seed_source must be a supported SeedSource")
        if not isinstance(self.mode_basis, str) or not self.mode_basis:
            raise ValueError("mode_basis must be a non-empty disclosure")
        if not lanes or any(
            not _valid_lane(lane)
            or not isinstance(weight, (int, float))
            or isinstance(weight, bool)
            or not isfinite(weight)
            or weight <= 0
            for lane, weight in lanes.items()
        ):
            raise ValueError(
                "lane_weights must contain positive weights for supported interstate lanes"
            )
        if set(modes) != set(MODES) or any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isfinite(value)
            or value < 0
            for value in modes.values()
        ):
            raise ValueError(f"mode_probabilities must define exactly {MODES}")
        total = sum(modes.values())
        if total <= 0:
            raise ValueError("mode probabilities must have positive support")
        normalized_lanes = {
            lane: weight / sum(lanes.values()) for lane, weight in sorted(lanes.items())
        }
        normalized_modes = {mode: modes[mode] / total for mode in MODES}
        invalid_source_modes = any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isfinite(value)
            or value < 0
            for value in source_modes.values()
        )
        if invalid_source_modes:
            raise ValueError("source_mode_shares must contain non-negative supported values")
        source_mode_total = sum(source_modes.values())
        if source_modes and source_mode_total <= 0:
            raise ValueError("source_mode_shares must have positive total support")
        if any(mode not in FAF5_MODE_NAMES.values() for mode in source_modes):
            raise ValueError("source_mode_shares contains an unknown official FAF5 mode")
        if self.source_file is not None and (
            not isinstance(self.source_file, str)
            or not self.source_file
            or Path(self.source_file).name != self.source_file
        ):
            raise ValueError("source_file must be a plain filename or null")
        if self.source_sha256 is not None and (
            not isinstance(self.source_sha256, str)
            or len(self.source_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.source_sha256)
        ):
            raise ValueError("source_sha256 must be a lowercase SHA-256 digest or null")
        if self.seed_source is SeedSource.FAF5 and (
            not source_modes or self.source_file is None or self.source_sha256 is None
        ):
            raise ValueError("FAF5 distributions require source evidence and broad mode shares")
        normalized_source_modes = (
            {mode: value / source_mode_total for mode, value in sorted(source_modes.items())}
            if source_modes
            else {}
        )
        object.__setattr__(self, "lane_weights", MappingProxyType(normalized_lanes))
        object.__setattr__(self, "mode_probabilities", MappingProxyType(normalized_modes))
        object.__setattr__(self, "source_mode_shares", MappingProxyType(normalized_source_modes))


def _valid_lane(lane_id: str) -> bool:
    if not isinstance(lane_id, str) or lane_id.count("-") != 1:
        return False
    origin, destination = lane_id.split("-")
    return origin != destination and origin in STATE_CITIES and destination in STATE_CITIES


def zone_to_state(zone: object) -> str | None:
    """Map a three-digit FAF zone to its two-digit state FIPS prefix."""
    try:
        numeric = float(zone)
    except (TypeError, ValueError):
        return None
    if not isfinite(numeric) or not numeric.is_integer() or numeric < 0:
        return None
    digits = str(int(numeric)).zfill(3)
    return FIPS_TO_STATE.get(int(digits[:2]))


def fixture_distribution(seed_source: SeedSource) -> SourceDistribution:
    """Return deterministic priors for tests or offline public reproduction."""
    if seed_source is SeedSource.FAF5:
        raise ValueError("FAF5 distributions require an explicit source file")
    lane_weights = (
        {"IL-TX": 4.0, "TX-GA": 3.0, "CA-WA": 2.0, "PA-OH": 1.0}
        if seed_source is SeedSource.TEST
        else PRIORS_LANE_WEIGHTS
    )
    mode_probabilities = (
        {"PARCEL": 0.25, "LTL": 0.45, "FTL": 0.30}
        if seed_source is SeedSource.TEST
        else {"PARCEL": 0.40, "LTL": 0.35, "FTL": 0.25}
    )
    return SourceDistribution(
        seed_source,
        lane_weights,
        mode_probabilities,
        mode_basis="Declared operational priors; TEST fixture"
        if seed_source is SeedSource.TEST
        else "Declared operational priors",
    )


def load_faf5_distribution(path: Path, *, chunksize: int = 25_000) -> SourceDistribution:
    """Stream FAF5 rows and derive supported state-pair and mode distributions."""
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"FAF5 source does not exist: {path}")
    if not isinstance(chunksize, int) or isinstance(chunksize, bool) or chunksize <= 0:
        raise ValueError("chunksize must be a positive integer")

    lane_tonnage: Counter[str] = Counter()
    source_mode_tonnage: Counter[str] = Counter()
    try:
        chunks = pd.read_csv(
            path,
            usecols=["dms_orig", "dms_dest", "dms_mode", "tons_2024"],
            chunksize=chunksize,
            low_memory=False,
        )
        for chunk in chunks:
            origin = chunk["dms_orig"].map(zone_to_state)
            destination = chunk["dms_dest"].map(zone_to_state)
            mode_codes = pd.to_numeric(chunk["dms_mode"], errors="coerce")
            tonnage = pd.to_numeric(chunk["tons_2024"], errors="coerce").fillna(0.0).clip(lower=0.0)
            broad_mode_valid = (
                origin.notna()
                & destination.notna()
                & mode_codes.isin(FAF5_MODE_NAMES)
                & origin.ne(destination)
                & tonnage.gt(0)
            )
            road_lane_valid = broad_mode_valid & mode_codes.isin(ROAD_RELEVANT_FAF5_MODES)
            for origin_state, destination_state, tons in zip(
                origin[road_lane_valid],
                destination[road_lane_valid],
                tonnage[road_lane_valid],
                strict=True,
            ):
                lane_tonnage[f"{origin_state}-{destination_state}"] += float(tons)
            for mode_code, tons in zip(
                mode_codes[broad_mode_valid].astype(int),
                tonnage[broad_mode_valid],
                strict=True,
            ):
                source_mode_tonnage[FAF5_MODE_NAMES[int(mode_code)]] += float(tons)
    except ValueError as error:
        raise ValueError(f"FAF5 source is missing required columns: {path}") from error

    if not lane_tonnage or not source_mode_tonnage:
        raise ValueError("FAF5 source produced no supported interstate lane/mode rows")
    return SourceDistribution(
        seed_source=SeedSource.FAF5,
        lane_weights=lane_tonnage,
        # FAF5 cannot distinguish LTL from FTL, and tonnage is not an invoice-count
        # distribution for parcel. Keep product modes as declared operational priors.
        mode_probabilities={"PARCEL": 0.40, "LTL": 0.35, "FTL": 0.25},
        mode_basis=(
            "Declared operational priors; FAF5 broad shares cover positive-tonnage interstate "
            "flows between supported states and are retained separately"
        ),
        source_mode_shares=source_mode_tonnage,
        source_file=path.name,
        source_sha256=sha256_file(path),
    )


def load_distribution(
    seed_source: SeedSource,
    *,
    faf5_path: Path | None = None,
    chunksize: int = 25_000,
) -> SourceDistribution:
    """Resolve exactly one explicit distribution adapter."""
    if seed_source is SeedSource.FAF5:
        if faf5_path is None:
            raise ValueError("FAF5 seed source requires faf5_path")
        return load_faf5_distribution(faf5_path, chunksize=chunksize)
    if faf5_path is not None:
        raise ValueError(f"{seed_source.value} seed source cannot accept faf5_path")
    return fixture_distribution(seed_source)
