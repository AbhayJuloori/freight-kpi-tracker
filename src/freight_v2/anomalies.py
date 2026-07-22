"""Typed, reproducible anomaly injection with isolated ground truth."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite

import numpy as np
import pandas as pd

from freight_v2.config import SCHEMA_VERSION
from freight_v2.contracts import validate_columns
from freight_v2.generation import (
    BASELINE_END,
    BASELINE_START,
    CALIBRATION_END,
    CALIBRATION_START,
    EVALUATION_END,
    EVALUATION_START,
)

ANOMALY_TYPES = (
    "carrier_overcharge",
    "duplicate_fuel_surcharge",
    "rate_card_override",
    "weight_class_mismatch",
    "persistent_lane_drift",
    "service_deterioration",
    "data_quality_corruption",
)

DEFAULT_ANOMALY_RATES: Mapping[str, float] = {
    "carrier_overcharge": 0.015,
    "duplicate_fuel_surcharge": 0.010,
    "rate_card_override": 0.010,
    "weight_class_mismatch": 0.010,
    "persistent_lane_drift": 0.012,
    "service_deterioration": 0.008,
    "data_quality_corruption": 0.005,
}

DOCUMENTED_CHANGED_FIELDS: Mapping[str, frozenset[str]] = {
    "carrier_overcharge": frozenset({"base_cost", "total_cost"}),
    "duplicate_fuel_surcharge": frozenset({"fuel_surcharge", "total_cost"}),
    "rate_card_override": frozenset(
        {"base_rate_per_cwt", "base_cost", "fuel_surcharge", "total_cost"}
    ),
    "weight_class_mismatch": frozenset({"weight_lbs", "freight_class"}),
    "persistent_lane_drift": frozenset({"base_cost", "total_cost"}),
    "service_deterioration": frozenset({"on_time_flag", "transit_days"}),
    "data_quality_corruption": frozenset({"weight_lbs", "freight_class"}),
}

TRUTH_FIELDS = frozenset(
    {"is_anomaly", "anomaly_type", "changed_fields", "injected_magnitude", "anomaly_group_id"}
)

INJECTION_REQUIRED_COLUMNS = frozenset(
    {
        "rate_id",
        "base_rate_per_cwt",
        "minimum_charge",
        "fuel_surcharge_rate",
        "transit_days",
    }
)

DIRECT_ANOMALY_TYPES = (
    "carrier_overcharge",
    "duplicate_fuel_surcharge",
    "rate_card_override",
    "weight_class_mismatch",
    "data_quality_corruption",
)

TEMPORAL_WINDOWS = (
    ("baseline", BASELINE_START, BASELINE_END),
    ("calibration", CALIBRATION_START, CALIBRATION_END),
    ("evaluation", EVALUATION_START, EVALUATION_END),
)


@dataclass(frozen=True, slots=True)
class InjectionResult:
    """Operational observations and their separately stored evaluation truth."""

    shipments: pd.DataFrame
    ground_truth: pd.DataFrame


def _validated_rates(rates: Mapping[str, float] | None) -> dict[str, float]:
    selected = dict(DEFAULT_ANOMALY_RATES if rates is None else rates)
    if set(selected) != set(ANOMALY_TYPES):
        raise ValueError(f"anomaly rates must define exactly {ANOMALY_TYPES}")
    if any(
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not isfinite(value)
        or value < 0
        for value in selected.values()
    ):
        raise ValueError("anomaly rates must be finite and non-negative")
    if sum(selected.values()) > 0.25:
        raise ValueError("total anomaly rate cannot exceed 25%")
    return selected


def _stable_hash(random_seed: int, namespace: str, value: str) -> int:
    payload = f"{random_seed}|{namespace}|{value}".encode()
    return int.from_bytes(hashlib.blake2b(payload, digest_size=8).digest(), "big")


def _owned_indices(
    shipments: pd.DataFrame,
    anomaly_type: str,
    *,
    random_seed: int,
) -> pd.Index:
    """Return a fixed disjoint capacity partition for one anomaly family.

    Ownership never depends on requested rates. This deliberately makes extreme rates
    deterministic upper bounds rather than promises when a family's eligible partition
    is exhausted, while guaranteeing that changing one family cannot move another.
    """
    owner_number = ANOMALY_TYPES.index(anomaly_type)
    owners = shipments["shipment_id"].map(
        lambda shipment_id: (
            _stable_hash(random_seed, "anomaly-owner", shipment_id) % len(ANOMALY_TYPES)
        )
    )
    return shipments.index[owners.eq(owner_number)]


def _stable_selection(
    shipments: pd.DataFrame,
    eligible: pd.Index,
    *,
    count: int,
    anomaly_type: str,
    random_seed: int,
) -> pd.Index:
    if count == 0:
        return pd.Index([], dtype=int)
    if len(eligible) == 0:
        raise ValueError(f"no eligible rows for requested {anomaly_type} anomalies")
    ranked = sorted(
        (int(index) for index in eligible),
        key=lambda index: (
            _stable_hash(
                random_seed,
                f"{anomaly_type}:selection",
                str(shipments.at[index, "shipment_id"]),
            ),
            str(shipments.at[index, "shipment_id"]),
        ),
    )
    return pd.Index(ranked[: min(count, len(ranked))])


def _grouped_tail_selection(
    shipments: pd.DataFrame,
    eligible: pd.Index,
    *,
    count: int,
    group_columns: list[str],
    anomaly_type: str,
    random_seed: int,
) -> tuple[pd.Index, dict[int, str]]:
    if count == 0:
        return pd.Index([], dtype=int), {}
    candidates = shipments.loc[eligible].copy()
    groups = list(candidates.groupby(group_columns, sort=True, observed=True).groups.items())
    if not groups:
        raise ValueError(f"no eligible rows for grouped anomaly on {group_columns}")

    def _group_label(item: tuple[object, pd.Index]) -> str:
        group_key = item[0] if isinstance(item[0], tuple) else (item[0],)
        return "|".join(str(value) for value in group_key)

    ordered_groups = sorted(
        groups,
        key=lambda item: _stable_hash(
            random_seed,
            f"{anomaly_type}:group",
            _group_label(item),
        ),
    )
    selected: list[int] = []
    group_ids: dict[int, str] = {}
    for group_key, group_index in ordered_groups:
        ordered = shipments.loc[group_index].sort_values(["ship_date", "shipment_id"])
        remaining = count - len(selected)
        chosen = ordered.tail(remaining).index.tolist()
        group_values = group_key if isinstance(group_key, tuple) else (group_key,)
        stable_group_id = "|".join(str(value) for value in group_values)
        selected.extend(chosen)
        group_ids.update({int(index): stable_group_id for index in chosen})
        if len(selected) == count:
            break
    return pd.Index(selected), group_ids


def _stable_uniform(
    shipments: pd.DataFrame,
    indices: pd.Index,
    *,
    random_seed: int,
    namespace: str,
    lower: float,
    upper: float,
) -> np.ndarray:
    span = upper - lower
    denominator = float(2**64)
    return np.array(
        [
            lower
            + span
            * _stable_hash(
                random_seed,
                namespace,
                str(shipments.at[int(index), "shipment_id"]),
            )
            / denominator
            for index in indices
        ]
    )


def _change_fields_json(anomaly_type: str) -> str:
    return json.dumps(sorted(DOCUMENTED_CHANGED_FIELDS[anomaly_type]), separators=(",", ":"))


def _validate_shipments(shipments: pd.DataFrame) -> str:
    validate_columns("shipments", shipments.columns)
    missing = INJECTION_REQUIRED_COLUMNS.difference(shipments.columns)
    if missing:
        raise ValueError(f"shipments are missing injection dependencies: {sorted(missing)}")
    leaked = TRUTH_FIELDS.intersection(shipments.columns)
    if leaked:
        raise ValueError(
            f"operational shipments already contain ground-truth fields: {sorted(leaked)}"
        )
    if shipments.empty or len(shipments) < 200:
        raise ValueError("at least 200 normal shipments are required for typed injection")
    if (
        not shipments["shipment_id"]
        .map(lambda value: isinstance(value, str) and bool(value.strip()))
        .all()
        or not shipments["shipment_id"].is_unique
    ):
        raise ValueError("shipment_id values must be unique non-empty strings")
    run_ids = shipments["run_id"].drop_duplicates()
    if len(run_ids) != 1 or not isinstance(run_ids.iloc[0], str) or not run_ids.iloc[0]:
        raise ValueError("shipments must belong to exactly one non-empty run_id")
    if not shipments["schema_version"].eq(SCHEMA_VERSION).all():
        raise ValueError("shipments use an incompatible schema version")
    if not pd.api.types.is_datetime64_any_dtype(shipments["ship_date"]):
        raise ValueError("ship_date must use a normalized datetime dtype")
    if shipments["ship_date"].isna().any():
        raise ValueError("shipments contain null ship_date values")
    if shipments["ship_date"].dt.tz is not None:
        raise ValueError("ship_date must be timezone-naive")
    if not shipments["ship_date"].dt.normalize().equals(shipments["ship_date"]):
        raise ValueError("ship_date must contain normalized midnight timestamps")
    for column in ("shipment_id", "rate_id", "carrier_id", "lane_id", "mode", "freight_class"):
        if (
            not shipments[column]
            .map(lambda value: isinstance(value, str) and bool(value.strip()))
            .all()
        ):
            raise ValueError(f"{column} must contain non-empty strings")
    if shipments["freight_class"].isin({"UNKNOWN", "MISMATCHED", "UNCLASSIFIED"}).any():
        raise ValueError("shipments already contain reserved anomaly freight classes")

    numeric_ranges = {
        "weight_lbs": (0.1, 45_000.0),
        "base_rate_per_cwt": (0.01, 500.0),
        "minimum_charge": (0.0, 50_000.0),
        "fuel_surcharge_rate": (0.0, 1.0),
        "base_cost": (0.01, 1_000_000.0),
        "fuel_surcharge": (0.0, 1_000_000.0),
        "total_cost": (0.01, 2_000_000.0),
        "transit_days": (1.0, 30.0),
    }
    for column, (lower, upper) in numeric_ranges.items():
        if pd.api.types.is_bool_dtype(shipments[column]) or not pd.api.types.is_numeric_dtype(
            shipments[column]
        ):
            raise ValueError(f"{column} must use a numeric dtype")
        try:
            values = pd.to_numeric(shipments[column], errors="raise").to_numpy(dtype=float)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{column} must be numeric") from error
        if not np.isfinite(values).all() or ((values < lower) | (values > upper)).any():
            raise ValueError(f"{column} must be finite and between {lower} and {upper}")
    if not shipments["on_time_flag"].isin({0, 1}).all():
        raise ValueError("on_time_flag must contain only 0 or 1")
    transit = shipments["transit_days"].to_numpy(dtype=float)
    if not np.equal(transit, np.floor(transit)).all():
        raise ValueError("transit_days must contain whole numbers")

    expected_base = np.round(
        np.maximum(
            shipments["base_rate_per_cwt"].to_numpy(dtype=float)
            * shipments["weight_lbs"].to_numpy(dtype=float)
            / 100.0,
            shipments["minimum_charge"].to_numpy(dtype=float),
        ),
        2,
    )
    expected_fuel = np.round(
        shipments["base_cost"].to_numpy(dtype=float)
        * shipments["fuel_surcharge_rate"].to_numpy(dtype=float),
        2,
    )
    expected_total = np.round(
        shipments["base_cost"].to_numpy(dtype=float)
        + shipments["fuel_surcharge"].to_numpy(dtype=float),
        2,
    )
    if not np.allclose(shipments["base_cost"], expected_base, atol=0.001, rtol=0):
        raise ValueError("normal shipments violate the rate-card base-cost identity")
    if not np.allclose(shipments["fuel_surcharge"], expected_fuel, atol=0.001, rtol=0):
        raise ValueError("normal shipments violate the fuel-surcharge identity")
    if not np.allclose(shipments["total_cost"], expected_total, atol=0.001, rtol=0):
        raise ValueError("normal shipments violate the invoice-total identity")
    if not shipments["ship_date"].between(BASELINE_START, EVALUATION_END).all():
        raise ValueError("ship_date falls outside the supported injection window")
    return str(run_ids.iloc[0])


def _equal_scalar(left: object, right: object) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    return bool(left == right)


def inject_anomalies(
    shipments: pd.DataFrame,
    *,
    random_seed: int,
    rates: Mapping[str, float] | None = None,
) -> InjectionResult:
    """Inject reproducible, non-overlapping anomalies after normal cost construction.

    Each shipment has a stable anomaly-family owner derived from the seed and shipment ID.
    Direct-anomaly targets are rounded independently within each temporal window; grouped
    targets use only evaluation-owned rows. Realized counts never exceed those targets and
    may be lower when the family's eligible ownership partition lacks capacity. This bounded
    approximation makes assignments invariant to row order, other-family rates, and changes
    to later temporal windows.
    """
    if not isinstance(random_seed, int) or isinstance(random_seed, bool) or random_seed < 0:
        raise ValueError("random_seed must be a non-negative integer")
    run_id = _validate_shipments(shipments)
    anomaly_rates = _validated_rates(rates)
    original_columns = shipments.columns.tolist()
    normal = shipments.copy(deep=True).reset_index(drop=True)
    observed = normal.copy(deep=True)

    assignments: dict[str, pd.Index] = {}
    group_ids: dict[int, str] = {}
    evaluation = observed.index[observed["ship_date"].ge(EVALUATION_START)]
    persistent_owned = _owned_indices(observed, "persistent_lane_drift", random_seed=random_seed)
    persistent_eligible = evaluation.intersection(persistent_owned)
    persistent_eligible = persistent_eligible[
        observed.loc[persistent_eligible, "base_cost"].ge(0.07)
    ]
    persistent_count = int(round(anomaly_rates["persistent_lane_drift"] * len(observed)))
    persistent, persistent_groups = _grouped_tail_selection(
        observed,
        persistent_eligible,
        count=min(persistent_count, len(persistent_eligible)),
        group_columns=["lane_id", "mode"],
        anomaly_type="persistent_lane_drift",
        random_seed=random_seed,
    )
    assignments["persistent_lane_drift"] = persistent
    group_ids.update(persistent_groups)

    service_owned = _owned_indices(observed, "service_deterioration", random_seed=random_seed)
    service_eligible = evaluation.intersection(service_owned)
    service_eligible = service_eligible[observed.loc[service_eligible, "on_time_flag"].eq(1)]
    service_count = int(round(anomaly_rates["service_deterioration"] * len(observed)))
    service, service_groups = _grouped_tail_selection(
        observed,
        service_eligible,
        count=min(service_count, len(service_eligible)),
        group_columns=["lane_id", "carrier_id"],
        anomaly_type="service_deterioration",
        random_seed=random_seed,
    )
    assignments["service_deterioration"] = service
    group_ids.update(service_groups)

    for anomaly_type in DIRECT_ANOMALY_TYPES:
        owned = _owned_indices(observed, anomaly_type, random_seed=random_seed)
        type_assignments: list[int] = []
        for _, start, end in TEMPORAL_WINDOWS:
            window = observed.index[observed["ship_date"].between(start, end)]
            eligible = window.intersection(owned)
            if anomaly_type == "carrier_overcharge":
                eligible = eligible[observed.loc[eligible, "base_cost"].ge(0.03)]
            elif anomaly_type == "duplicate_fuel_surcharge":
                eligible = eligible[observed.loc[eligible, "fuel_surcharge"].ge(0.01)]
            elif anomaly_type == "rate_card_override":
                eligible = eligible[
                    observed.loc[eligible, "base_cost"].ge(0.10)
                    & observed.loc[eligible, "base_cost"].gt(
                        observed.loc[eligible, "minimum_charge"] + 0.01
                    )
                    & observed.loc[eligible, "fuel_surcharge"].ge(0.05)
                    & observed.loc[eligible, "fuel_surcharge_rate"].gt(0)
                ]
            target = int(round(anomaly_rates[anomaly_type] * len(window)))
            selected = _stable_selection(
                observed,
                eligible,
                count=target,
                anomaly_type=anomaly_type,
                random_seed=random_seed,
            )
            type_assignments.extend(int(index) for index in selected)
        assignments[anomaly_type] = pd.Index(type_assignments)

    magnitudes: dict[int, float] = {}

    indices = assignments["carrier_overcharge"]
    factors = _stable_uniform(
        observed,
        indices,
        random_seed=random_seed,
        namespace="carrier_overcharge:magnitude",
        lower=1.18,
        upper=1.65,
    )
    observed.loc[indices, "base_cost"] = np.round(
        observed.loc[indices, "base_cost"].to_numpy(dtype=float) * factors, 2
    )
    observed.loc[indices, "total_cost"] = np.round(
        observed.loc[indices, "base_cost"] + observed.loc[indices, "fuel_surcharge"], 2
    )
    magnitudes.update(
        {int(index): float(factor - 1.0) for index, factor in zip(indices, factors, strict=True)}
    )

    indices = assignments["duplicate_fuel_surcharge"]
    observed.loc[indices, "fuel_surcharge"] = np.round(
        observed.loc[indices, "fuel_surcharge"].to_numpy(dtype=float) * 2.0, 2
    )
    observed.loc[indices, "total_cost"] = np.round(
        observed.loc[indices, "base_cost"] + observed.loc[indices, "fuel_surcharge"], 2
    )
    magnitudes.update({int(index): 1.0 for index in indices})

    indices = assignments["rate_card_override"]
    factors = _stable_uniform(
        observed,
        indices,
        random_seed=random_seed,
        namespace="rate_card_override:magnitude",
        lower=1.12,
        upper=1.35,
    )
    observed.loc[indices, "base_rate_per_cwt"] = np.round(
        observed.loc[indices, "base_rate_per_cwt"].to_numpy(dtype=float) * factors, 4
    )
    recomputed_base = np.maximum(
        observed.loc[indices, "base_rate_per_cwt"].to_numpy(dtype=float)
        * observed.loc[indices, "weight_lbs"].to_numpy(dtype=float)
        / 100.0,
        observed.loc[indices, "minimum_charge"].to_numpy(dtype=float),
    )
    observed.loc[indices, "base_cost"] = np.round(recomputed_base, 2)
    observed.loc[indices, "fuel_surcharge"] = np.round(
        observed.loc[indices, "base_cost"].to_numpy(dtype=float)
        * observed.loc[indices, "fuel_surcharge_rate"].to_numpy(dtype=float),
        2,
    )
    observed.loc[indices, "total_cost"] = np.round(
        observed.loc[indices, "base_cost"] + observed.loc[indices, "fuel_surcharge"], 2
    )
    magnitudes.update(
        {int(index): float(factor - 1.0) for index, factor in zip(indices, factors, strict=True)}
    )

    indices = assignments["weight_class_mismatch"]
    factors = np.where(
        _stable_uniform(
            observed,
            indices,
            random_seed=random_seed,
            namespace="weight_class_mismatch:magnitude",
            lower=0.0,
            upper=1.0,
        )
        < 0.5,
        0.45,
        1.80,
    )
    observed.loc[indices, "weight_lbs"] = np.round(
        observed.loc[indices, "weight_lbs"].to_numpy(dtype=float) * factors, 1
    )
    original_classes = observed.loc[indices, "freight_class"].astype(str)
    observed.loc[indices, "freight_class"] = np.where(
        original_classes.eq("MISMATCHED"), "UNCLASSIFIED", "MISMATCHED"
    )
    magnitudes.update(
        {int(index): float(factor - 1.0) for index, factor in zip(indices, factors, strict=True)}
    )

    indices = assignments["persistent_lane_drift"]
    ordered = observed.loc[indices].sort_values(["ship_date", "shipment_id"]).index
    factors = np.linspace(1.08, 1.25, len(ordered))
    observed.loc[ordered, "base_cost"] = np.round(
        observed.loc[ordered, "base_cost"].to_numpy(dtype=float) * factors, 2
    )
    observed.loc[ordered, "total_cost"] = np.round(
        observed.loc[ordered, "base_cost"] + observed.loc[ordered, "fuel_surcharge"], 2
    )
    magnitudes.update(
        {int(index): float(factor - 1.0) for index, factor in zip(ordered, factors, strict=True)}
    )

    indices = assignments["service_deterioration"]
    delays = np.floor(
        _stable_uniform(
            observed,
            indices,
            random_seed=random_seed,
            namespace="service_deterioration:magnitude",
            lower=2.0,
            upper=6.0,
        )
    ).astype(int)
    observed.loc[indices, "on_time_flag"] = 0
    observed.loc[indices, "transit_days"] = (
        observed.loc[indices, "transit_days"].to_numpy(dtype=int) + delays
    )
    magnitudes.update(
        {int(index): float(delay) for index, delay in zip(indices, delays, strict=True)}
    )

    indices = assignments["data_quality_corruption"]
    observed.loc[indices, "weight_lbs"] = 0.0
    observed.loc[indices, "freight_class"] = "UNKNOWN"
    magnitudes.update({int(index): -1.0 for index in indices})

    for anomaly_type, indices in assignments.items():
        expected_fields = DOCUMENTED_CHANGED_FIELDS[anomaly_type]
        for index in indices:
            actual_fields = {
                column
                for column in original_columns
                if not _equal_scalar(normal.at[index, column], observed.at[index, column])
            }
            if actual_fields != expected_fields:
                raise RuntimeError(
                    f"{anomaly_type} changed {sorted(actual_fields)}, "
                    f"expected {sorted(expected_fields)}"
                )

    truth = pd.DataFrame(
        {
            "run_id": run_id,
            "schema_version": SCHEMA_VERSION,
            "shipment_id": observed["shipment_id"],
            "is_anomaly": 0,
            "anomaly_type": "NONE",
            "changed_fields": "[]",
            "injected_magnitude": 0.0,
            "anomaly_group_id": "",
        }
    )
    for anomaly_type, indices in assignments.items():
        truth.loc[indices, "is_anomaly"] = 1
        truth.loc[indices, "anomaly_type"] = anomaly_type
        truth.loc[indices, "changed_fields"] = _change_fields_json(anomaly_type)
        truth.loc[indices, "injected_magnitude"] = [magnitudes[int(index)] for index in indices]
        truth.loc[indices, "anomaly_group_id"] = [
            group_ids.get(int(index), "") for index in indices
        ]

    validate_columns("shipments", observed.columns)
    validate_columns("anomaly_ground_truth", truth.columns)
    if TRUTH_FIELDS.intersection(observed.columns):
        raise RuntimeError("ground-truth fields leaked into operational shipments")
    if observed.columns.tolist() != original_columns:
        raise RuntimeError("anomaly injection changed the operational schema")
    return InjectionResult(
        shipments=observed.sort_values("shipment_id", ignore_index=True),
        ground_truth=truth.sort_values("shipment_id", ignore_index=True),
    )
