"""Deterministic generation of normal freight operations."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from freight_v2.config import SCHEMA_VERSION
from freight_v2.contracts import validate_columns
from freight_v2.sources import MODES, STATE_CITIES, SourceDistribution

BASELINE_START = pd.Timestamp("2023-01-02")
BASELINE_END = pd.Timestamp("2023-12-31")
CALIBRATION_START = pd.Timestamp("2024-01-01")
CALIBRATION_END = pd.Timestamp("2024-03-31")
EVALUATION_START = pd.Timestamp("2024-04-01")
EVALUATION_END = pd.Timestamp("2024-06-30")

RATE_PERIODS = (
    ("2023Q1", BASELINE_START, pd.Timestamp("2023-03-31")),
    ("2023Q2", pd.Timestamp("2023-04-01"), pd.Timestamp("2023-06-30")),
    ("2023Q3", pd.Timestamp("2023-07-01"), pd.Timestamp("2023-09-30")),
    ("2023Q4", pd.Timestamp("2023-10-01"), BASELINE_END),
    ("2024Q1", CALIBRATION_START, CALIBRATION_END),
    ("2024Q2", EVALUATION_START, EVALUATION_END),
)
RATE_PERIOD_MARKET_DRIFT = (0.000, 0.018, 0.006, 0.031, 0.024, 0.042)
FUEL_CURVE_BASIS = (
    "Synthetic deterministic diesel-index curve with smooth seasonal and trend components; "
    "not observed EIA data."
)

CARRIERS = tuple(f"CARRIER_{number:03d}" for number in range(1, 13))
MODE_COST = {
    "PARCEL": (42.0, 14.0),
    "LTL": (18.5, 125.0),
    "FTL": (5.8, 850.0),
}
MODE_WEIGHT = {
    "PARCEL": (3.6, 0.9, 1.0, 150.0),
    "LTL": (7.2, 0.9, 150.0, 15_000.0),
    "FTL": (10.1, 0.45, 8_000.0, 45_000.0),
}
MODE_TRANSIT_DAYS = {"PARCEL": 3, "LTL": 4, "FTL": 2}
WINDOWS = (
    ("baseline", BASELINE_START, BASELINE_END, 2 / 3),
    ("calibration", CALIBRATION_START, CALIBRATION_END, 1 / 6),
    ("evaluation", EVALUATION_START, EVALUATION_END, 1 / 6),
)


@dataclass(frozen=True, slots=True)
class NormalRun:
    """The three observable tables produced before anomaly injection."""

    carrier_rates: pd.DataFrame
    fuel_surcharges: pd.DataFrame
    shipments: pd.DataFrame


def _require_run_id(run_id: str) -> None:
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("run_id must be a non-empty string")


def generate_fuel_surcharges(run_id: str) -> pd.DataFrame:
    """Generate a disclosed synthetic weekly fuel curve for every supported mode."""
    _require_run_id(run_id)
    weeks = pd.date_range(BASELINE_START, EVALUATION_END, freq="W-MON")
    progress = np.linspace(0.0, 1.0, len(weeks))
    diesel = 4.05 + 0.42 * np.sin(progress * np.pi * 3.0) - 0.18 * progress
    mode_factor = {"PARCEL": 0.78, "LTL": 1.0, "FTL": 1.12}
    rows = []
    for week_index, week_start in enumerate(weeks):
        base_rate = max(0.0, (diesel[week_index] - 2.50) / 2.50 * 0.30)
        for mode in MODES:
            rows.append(
                {
                    "run_id": run_id,
                    "schema_version": SCHEMA_VERSION,
                    "week_start": week_start,
                    "mode": mode,
                    "fuel_index": round(float(diesel[week_index]), 4),
                    "surcharge_rate": round(float(base_rate * mode_factor[mode]), 6),
                    "curve_basis": FUEL_CURVE_BASIS,
                }
            )
    frame = pd.DataFrame(rows)
    validate_columns("fuel_surcharges", frame.columns)
    return frame


def generate_carrier_rates(
    distribution: SourceDistribution,
    *,
    run_id: str,
    rng: np.random.Generator,
    carriers_per_lane_mode: int = 4,
) -> pd.DataFrame:
    """Generate contiguous quarterly rate-card versions before sampling shipments."""
    _require_run_id(run_id)
    if not 1 <= carriers_per_lane_mode <= len(CARRIERS):
        raise ValueError("carriers_per_lane_mode is outside supported range")
    rows = []
    for lane_index, lane_id in enumerate(distribution.lane_weights):
        lane_factor = 0.88 + (lane_index % 9) * 0.035
        for mode_index, mode in enumerate(MODES):
            start = (lane_index * 3 + mode_index * 2) % len(CARRIERS)
            carriers = tuple(
                CARRIERS[(start + offset) % len(CARRIERS)]
                for offset in range(carriers_per_lane_mode)
            )
            for carrier_index, carrier_id in enumerate(carriers):
                mean_rate, minimum = MODE_COST[mode]
                carrier_factor = 0.90 + carrier_index * 0.055 + rng.uniform(-0.015, 0.015)
                service_target = {
                    "PARCEL": 0.94,
                    "LTL": 0.89,
                    "FTL": 0.93,
                }[mode] + rng.uniform(-0.025, 0.02)
                stable_rate = mean_rate * lane_factor * carrier_factor
                stable_minimum = minimum * lane_factor
                for period_index, (period_label, effective_start, effective_end) in enumerate(
                    RATE_PERIODS
                ):
                    market_drift = RATE_PERIOD_MARKET_DRIFT[period_index]
                    repricing_step = (
                        (lane_index * 7 + mode_index * 5 + carrier_index * 3 + period_index * 11)
                        % 9
                        - 4
                    ) * 0.0025
                    rate_factor = 1.0 + market_drift + repricing_step
                    minimum_factor = 1.0 + market_drift * 0.55 + repricing_step * 0.50
                    rows.append(
                        {
                            "run_id": run_id,
                            "schema_version": SCHEMA_VERSION,
                            "rate_id": (
                                f"RATE-{lane_index:04d}-{mode_index}-{carrier_index}-{period_label}"
                            ),
                            "carrier_id": carrier_id,
                            "lane_id": lane_id,
                            "mode": mode,
                            "effective_start": effective_start,
                            "effective_end": effective_end,
                            "base_rate_per_cwt": round(stable_rate * rate_factor, 4),
                            "minimum_charge": round(stable_minimum * minimum_factor, 2),
                            "service_level_target": round(
                                float(np.clip(service_target, 0.80, 0.98)), 4
                            ),
                            "contract_transit_days": MODE_TRANSIT_DAYS[mode],
                        }
                    )
    frame = pd.DataFrame(rows).sort_values("rate_id", ignore_index=True)
    validate_columns("carrier_rates", frame.columns)
    if frame.duplicated(["carrier_id", "lane_id", "mode", "effective_start"]).any():
        raise RuntimeError("generated carrier rates contain duplicate business keys")
    return frame


def _window_counts(n_shipments: int) -> list[int]:
    if n_shipments < len(WINDOWS):
        raise ValueError(f"n_shipments must be at least {len(WINDOWS)}")
    raw = np.array([window[3] for window in WINDOWS]) * n_shipments
    counts = np.floor(raw).astype(int)
    for index in np.argsort(-(raw - counts))[: n_shipments - int(counts.sum())]:
        counts[index] += 1
    for index in np.flatnonzero(counts == 0):
        donor = int(np.argmax(counts))
        counts[donor] -= 1
        counts[index] = 1
    return counts.tolist()


def _sample_dates(n_shipments: int, rng: np.random.Generator) -> pd.DatetimeIndex:
    sampled: list[pd.Timestamp] = []
    for count, (_, start, end, _) in zip(_window_counts(n_shipments), WINDOWS, strict=True):
        dates = pd.date_range(start, end, freq="D")
        seasonal = np.where(dates.month.isin([10, 11, 12]), 1.32, 1.0)
        weekday = np.where(dates.dayofweek < 5, 1.15, 0.55)
        probabilities = seasonal * weekday
        probabilities = probabilities / probabilities.sum()
        sampled.extend(rng.choice(dates, size=count, replace=True, p=probabilities).tolist())
    order = rng.permutation(n_shipments)
    return pd.DatetimeIndex(sampled).take(order)


def _weights_for_modes(modes: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    weights = np.empty(len(modes), dtype=float)
    for mode in MODES:
        indices = np.flatnonzero(modes == mode)
        mean, sigma, lower, upper = MODE_WEIGHT[mode]
        values = rng.lognormal(mean=mean, sigma=sigma, size=len(indices))
        weights[indices] = np.clip(values, lower, upper)
    return weights


def _validated_numeric_column(
    frame: pd.DataFrame,
    column: str,
    *,
    lower: float,
    upper: float,
    integer: bool = False,
) -> pd.Series:
    if pd.api.types.is_bool_dtype(frame[column]):
        raise ValueError(f"{column} must be numeric, not boolean")
    try:
        values = pd.to_numeric(frame[column], errors="raise")
    except (TypeError, ValueError) as error:
        raise ValueError(f"{column} must be numeric") from error
    numeric = values.to_numpy(dtype=float)
    if not np.isfinite(numeric).all() or not values.between(lower, upper).all():
        raise ValueError(f"{column} must be finite and between {lower} and {upper}")
    if integer and not np.equal(numeric, np.floor(numeric)).all():
        raise ValueError(f"{column} must contain whole numbers")
    return values.astype(int if integer else float)


def _normalized_dates(values: pd.Series, *, label: str) -> pd.Series:
    try:
        parsed = pd.to_datetime(values, errors="raise", utc=True)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must contain valid dates") from error
    if parsed.isna().any():
        raise ValueError(f"{label} cannot contain null dates")
    return parsed.dt.tz_convert(None).dt.normalize()


def _validate_string_keys(frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    for column in columns:
        valid = frame[column].map(lambda value: isinstance(value, str) and bool(value.strip()))
        if not valid.all():
            raise ValueError(f"{column} must contain non-empty string keys")


def _validate_rate_calendars(rates: pd.DataFrame) -> None:
    business_key = ["carrier_id", "lane_id", "mode"]
    for key, versions in rates.groupby(business_key, observed=True, sort=True):
        ordered = versions.sort_values(["effective_start", "effective_end"], kind="stable")
        if (
            ordered["effective_start"].iloc[0] != BASELINE_START
            or ordered["effective_end"].iloc[-1] != EVALUATION_END
        ):
            raise ValueError(f"carrier rate {key} does not cover the full generation window")
        if ordered["effective_start"].gt(ordered["effective_end"]).any():
            raise ValueError(f"carrier rate {key} has reversed effective dates")
        starts = ordered["effective_start"].iloc[1:].reset_index(drop=True)
        expected_starts = ordered["effective_end"].iloc[:-1].reset_index(drop=True) + pd.Timedelta(
            days=1
        )
        if not starts.equals(expected_starts):
            relation = np.where(starts > expected_starts, "gap", "overlap")
            kind = "gap" if "gap" in relation else "overlap"
            raise ValueError(f"carrier rate {key} contains an effective-date {kind}")


def generate_shipments(
    rates: pd.DataFrame,
    fuel: pd.DataFrame,
    distribution: SourceDistribution,
    *,
    run_id: str,
    n_shipments: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Sample normal shipments only from declared rate-card support."""
    _require_run_id(run_id)
    validate_columns("carrier_rates", rates.columns)
    validate_columns("fuel_surcharges", fuel.columns)
    if not isinstance(n_shipments, int) or isinstance(n_shipments, bool) or n_shipments < 3:
        raise ValueError("n_shipments must be an integer of at least 3")
    rates = rates.copy()
    fuel = fuel.copy()
    if rates.empty or fuel.empty:
        raise ValueError("carrier rates and fuel surcharges must be non-empty")
    if not rates["run_id"].eq(run_id).all():
        raise ValueError("carrier rates belong to a different run")
    if not fuel["run_id"].eq(run_id).all():
        raise ValueError("fuel surcharges belong to a different run")
    if not rates["schema_version"].eq(SCHEMA_VERSION).all():
        raise ValueError("carrier rates use an incompatible schema version")
    if not fuel["schema_version"].eq(SCHEMA_VERSION).all():
        raise ValueError("fuel surcharges use an incompatible schema version")
    if not fuel["curve_basis"].eq(FUEL_CURVE_BASIS).all():
        raise ValueError("fuel surcharges use an incompatible curve basis")
    _validate_string_keys(rates, ("rate_id", "carrier_id", "lane_id", "mode"))
    _validate_string_keys(fuel, ("mode",))
    if (
        rates["rate_id"].duplicated().any()
        or rates.duplicated(["carrier_id", "lane_id", "mode", "effective_start"]).any()
    ):
        raise ValueError("carrier rates contain duplicate keys")
    rates["base_rate_per_cwt"] = _validated_numeric_column(
        rates, "base_rate_per_cwt", lower=0.01, upper=500.0
    )
    rates["minimum_charge"] = _validated_numeric_column(
        rates, "minimum_charge", lower=0.0, upper=50_000.0
    )
    rates["service_level_target"] = _validated_numeric_column(
        rates, "service_level_target", lower=0.0, upper=1.0
    )
    rates["contract_transit_days"] = _validated_numeric_column(
        rates, "contract_transit_days", lower=1, upper=30, integer=True
    )
    fuel["fuel_index"] = _validated_numeric_column(fuel, "fuel_index", lower=0.01, upper=20.0)
    fuel["surcharge_rate"] = _validated_numeric_column(fuel, "surcharge_rate", lower=0.0, upper=1.0)

    rates["effective_start"] = _normalized_dates(rates["effective_start"], label="effective_start")
    rates["effective_end"] = _normalized_dates(rates["effective_end"], label="effective_end")
    fuel["week_start"] = _normalized_dates(fuel["week_start"], label="week_start")
    if fuel.duplicated(["week_start", "mode"]).any():
        raise ValueError("fuel surcharges contain duplicate week/mode keys")
    _validate_rate_calendars(rates)
    if not rates["mode"].isin(MODES).all() or not fuel["mode"].isin(MODES).all():
        raise ValueError("rate or fuel input contains an unsupported mode")
    expected_lane_modes = {
        (lane_id, mode) for lane_id in distribution.lane_weights for mode in MODES
    }
    actual_lane_modes = set(zip(rates["lane_id"], rates["mode"], strict=True))
    if actual_lane_modes != expected_lane_modes:
        raise ValueError("carrier rates do not exactly cover distribution lane/mode support")
    expected_fuel_keys = {
        (week_start, mode)
        for week_start in pd.date_range(BASELINE_START, EVALUATION_END, freq="W-MON")
        for mode in MODES
    }
    actual_fuel_keys = set(zip(fuel["week_start"], fuel["mode"], strict=True))
    if actual_fuel_keys != expected_fuel_keys:
        raise ValueError("fuel surcharges do not exactly cover the generation window")

    ship_dates = _sample_dates(n_shipments, rng)
    support = (
        rates[["carrier_id", "lane_id", "mode"]]
        .drop_duplicates()
        .sort_values(["lane_id", "mode", "carrier_id"], kind="stable", ignore_index=True)
    )
    lane_probability = support["lane_id"].map(distribution.lane_weights).to_numpy(dtype=float)
    mode_probability = support["mode"].map(distribution.mode_probabilities).to_numpy(dtype=float)
    carrier_count = (
        support.groupby(["lane_id", "mode"], observed=True)["carrier_id"]
        .transform("size")
        .to_numpy(dtype=float)
    )
    sample_probability = lane_probability * mode_probability / carrier_count
    sample_probability /= sample_probability.sum()
    sampled_support = support.iloc[
        rng.choice(len(support), size=n_shipments, replace=True, p=sample_probability)
    ].reset_index(drop=True)
    sampled_support["_shipment_position"] = np.arange(n_shipments)
    sampled_support["ship_date"] = ship_dates
    candidates = sampled_support.merge(
        rates,
        on=["carrier_id", "lane_id", "mode"],
        how="left",
        validate="many_to_many",
        sort=False,
    )
    active = candidates["ship_date"].between(
        candidates["effective_start"], candidates["effective_end"]
    )
    sampled = candidates.loc[active].sort_values("_shipment_position", kind="stable")
    matches = sampled["_shipment_position"].value_counts()
    if len(sampled) != n_shipments or not matches.eq(1).all():
        raise ValueError("every shipment date must resolve exactly one active carrier rate")
    sampled = sampled.reset_index(drop=True)
    ship_dates = pd.DatetimeIndex(sampled["ship_date"])
    modes = sampled["mode"].to_numpy()
    weights = np.round(_weights_for_modes(modes, rng), 1)

    origins: list[str] = []
    destinations: list[str] = []
    origin_states: list[str] = []
    destination_states: list[str] = []
    for lane_id in sampled["lane_id"]:
        origin_state, destination_state = lane_id.split("-")
        origin_states.append(origin_state)
        destination_states.append(destination_state)
        origins.append(str(rng.choice(STATE_CITIES[origin_state])))
        destinations.append(str(rng.choice(STATE_CITIES[destination_state])))

    fuel_lookup = fuel.set_index(["week_start", "mode"])["surcharge_rate"]
    week_starts = ship_dates - pd.to_timedelta(ship_dates.dayofweek, unit="D")
    surcharge_rates = np.array(
        [fuel_lookup.loc[(week, mode)] for week, mode in zip(week_starts, modes, strict=True)]
    )
    unrounded_base = np.maximum(
        sampled["base_rate_per_cwt"].to_numpy(dtype=float) * weights / 100.0,
        sampled["minimum_charge"].to_numpy(dtype=float),
    )
    base_cost = np.round(unrounded_base, 2)
    fuel_surcharge = np.round(base_cost * surcharge_rates, 2)
    total_cost = np.round(base_cost + fuel_surcharge, 2)

    peak_penalty = np.where(ship_dates.month.isin([11, 12]), 0.035, 0.0)
    service_probability = np.clip(
        sampled["service_level_target"].to_numpy(dtype=float) - peak_penalty,
        0.70,
        0.99,
    )
    on_time = (rng.random(n_shipments) < service_probability).astype(int)
    transit_days = sampled["contract_transit_days"].to_numpy(dtype=int) + (1 - on_time)
    freight_class = np.where(
        modes == "PARCEL",
        "PARCEL",
        np.where(modes == "FTL", "TRUCKLOAD", np.where(weights < 500, "125", "70")),
    )

    frame = pd.DataFrame(
        {
            "run_id": run_id,
            "schema_version": SCHEMA_VERSION,
            "shipment_id": [f"SHP-{index:08d}" for index in range(n_shipments)],
            "ship_date": ship_dates,
            "lane_id": sampled["lane_id"].to_numpy(),
            "origin_city": origins,
            "origin_state": origin_states,
            "destination_city": destinations,
            "destination_state": destination_states,
            "carrier_id": sampled["carrier_id"].to_numpy(),
            "mode": modes,
            "weight_lbs": weights,
            "freight_class": freight_class,
            "rate_id": sampled["rate_id"].to_numpy(),
            "base_rate_per_cwt": sampled["base_rate_per_cwt"].to_numpy(),
            "minimum_charge": sampled["minimum_charge"].to_numpy(),
            "fuel_surcharge_rate": np.round(surcharge_rates, 6),
            "base_cost": base_cost,
            "fuel_surcharge": fuel_surcharge,
            "total_cost": total_cost,
            "on_time_flag": on_time,
            "transit_days": transit_days,
        }
    )
    validate_columns("shipments", frame.columns)
    return frame.sort_values("shipment_id", ignore_index=True)


def generate_normal_run(
    distribution: SourceDistribution,
    *,
    run_id: str,
    random_seed: int,
    n_shipments: int,
) -> NormalRun:
    """Generate all normal-operation tables from one deterministic random stream."""
    if not isinstance(random_seed, int) or isinstance(random_seed, bool) or random_seed < 0:
        raise ValueError("random_seed must be a non-negative integer")
    rng = np.random.default_rng(random_seed)
    fuel = generate_fuel_surcharges(run_id)
    rates = generate_carrier_rates(distribution, run_id=run_id, rng=rng)
    shipments = generate_shipments(
        rates,
        fuel,
        distribution,
        run_id=run_id,
        n_shipments=n_shipments,
        rng=rng,
    )
    return NormalRun(carrier_rates=rates, fuel_surcharges=fuel, shipments=shipments)
