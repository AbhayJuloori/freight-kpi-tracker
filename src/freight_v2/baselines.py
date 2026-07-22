"""Authoritative expected costs and leakage-safe robust baseline fitting."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from types import MappingProxyType

import numpy as np
import pandas as pd

from freight_v2.anomalies import TRUTH_FIELDS
from freight_v2.config import SCHEMA_VERSION
from freight_v2.generation import (
    BASELINE_END,
    BASELINE_START,
    CALIBRATION_END,
    CALIBRATION_START,
    EVALUATION_END,
    EVALUATION_START,
)

BASELINE_HIERARCHY = (
    ("carrier_lane_mode", ("carrier_id", "lane_id", "mode")),
    ("lane_mode", ("lane_id", "mode")),
    ("mode", ("mode",)),
    ("global", ()),
)

MINIMUM_SUPPORTS = MappingProxyType(
    {
        "carrier_lane_mode": 20,
        "lane_mode": 30,
        "mode": 50,
        "global": 1,
    }
)

SAFE_SCALE_FLOOR = 0.01
MAX_TRUSTED_WEIGHT_LBS = 1_000_000.0
MAX_TRUSTED_MONETARY_VALUE = 20_000_000.0

RESERVED_SCRATCH_COLUMNS = frozenset(
    {
        "_input_order",
        "_score_order",
        "_rate_join",
        "_fuel_join",
        "_rate_carrier_id",
        "_rate_lane_id",
        "_rate_mode",
    }
)

FORBIDDEN_SCORING_COLUMNS = frozenset(TRUTH_FIELDS) | frozenset({"injected_cause", "ground_truth"})

SHIPMENT_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "shipment_id",
        "ship_date",
        "rate_id",
        "carrier_id",
        "lane_id",
        "mode",
        "weight_lbs",
        "total_cost",
    }
)
RATE_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "rate_id",
        "carrier_id",
        "lane_id",
        "mode",
        "effective_start",
        "effective_end",
        "base_rate_per_cwt",
        "minimum_charge",
    }
)
FUEL_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "week_start",
        "mode",
        "surcharge_rate",
    }
)

DERIVED_COLUMNS = frozenset(
    {
        "authoritative_base_rate_per_cwt",
        "authoritative_minimum_charge",
        "authoritative_fuel_surcharge_rate",
        "authoritative_rate_effective_start",
        "authoritative_rate_effective_end",
        "authoritative_fuel_week_start",
        "monetary_values_trusted",
        "expected_base_cost",
        "expected_fuel_surcharge",
        "expected_total_cost",
        "cost_residual",
        "estimated_excess_cost",
        "time_window",
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
        "residual_from_median",
    }
)

STATISTIC_COLUMNS = (
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
)

MODEL_STATISTIC_COLUMNS = (
    "baseline_source",
    "carrier_id",
    "lane_id",
    "mode",
    *STATISTIC_COLUMNS[1:],
    "baseline_eligible",
)

FINITE_STATISTIC_COLUMNS = (
    "residual_median",
    "residual_mad",
    "residual_mad_scale",
    "residual_q1",
    "residual_q3",
    "residual_iqr",
    "residual_iqr_lower",
    "residual_iqr_upper",
    "residual_scale",
)


@dataclass(frozen=True, slots=True)
class BaselineModel:
    """Frozen fit metadata and hierarchical robust residual statistics."""

    run_id: str
    schema_version: str
    fit_start: pd.Timestamp
    fit_end: pd.Timestamp
    statistics: pd.DataFrame
    _statistics_fingerprint: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        _validate_model_metadata(self)
        snapshot = self.statistics.copy(deep=True)
        _validate_model_statistics(snapshot)
        object.__setattr__(self, "statistics", snapshot)
        object.__setattr__(self, "_statistics_fingerprint", _frame_fingerprint(snapshot))


@dataclass(frozen=True, slots=True)
class BaselineResult:
    """Authoritatively costed rows scored against one baseline-only model."""

    scored: pd.DataFrame
    model: BaselineModel


def _require_columns(name: str, frame: pd.DataFrame, required: frozenset[str]) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise ValueError(f"{name} must be a pandas DataFrame")
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"{name} is missing required columns: {sorted(missing)}")


def _reject_reserved_columns(name: str, frame: pd.DataFrame) -> None:
    collisions = RESERVED_SCRATCH_COLUMNS.intersection(frame.columns)
    if collisions:
        raise ValueError(f"{name} contains reserved scratch columns: {sorted(collisions)}")


def _frame_fingerprint(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    digest.update(repr(tuple(frame.columns)).encode("utf-8"))
    digest.update(repr(tuple(str(dtype) for dtype in frame.dtypes)).encode("utf-8"))
    digest.update(pd.util.hash_pandas_object(frame, index=True).to_numpy().tobytes())
    return digest.hexdigest()


def _valid_key(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _missing_key(value: object) -> bool:
    return bool(pd.isna(value))


def _expected_scale(row: pd.Series) -> tuple[float, str, bool]:
    mad_scale = float(row["residual_mad_scale"])
    iqr_scale = float(row["residual_iqr"]) / 1.349
    if mad_scale > 0:
        return mad_scale, "mad", False
    if iqr_scale > 0:
        return iqr_scale, "iqr", False
    return SAFE_SCALE_FLOOR, "floor", True


def _validate_model_metadata(model: BaselineModel) -> None:
    if not isinstance(model.run_id, str) or not model.run_id.strip():
        raise ValueError("baseline model run_id must be a non-empty string")
    if model.schema_version != SCHEMA_VERSION:
        raise ValueError("baseline model schema_version is incompatible")
    if not isinstance(model.fit_start, pd.Timestamp) or not isinstance(model.fit_end, pd.Timestamp):
        raise ValueError("baseline model fit bounds must be pandas timestamps")
    if model.fit_start != BASELINE_START or model.fit_end != BASELINE_END:
        raise ValueError("baseline model fit bounds must exactly match the baseline window")
    if model.fit_start.tz is not None or model.fit_end.tz is not None:
        raise ValueError("baseline model fit bounds must be timezone-naive")


def _validate_model_statistics(statistics: pd.DataFrame) -> None:
    if not isinstance(statistics, pd.DataFrame) or statistics.empty:
        raise ValueError("baseline model statistics must be a non-empty DataFrame")
    if tuple(statistics.columns) != MODEL_STATISTIC_COLUMNS:
        raise ValueError("baseline model statistic schema is missing, reordered, or additive")
    sources = {source for source, _ in BASELINE_HIERARCHY}
    if set(statistics["baseline_source"]) != sources:
        raise ValueError("baseline model statistics must contain every fallback source")
    if not pd.api.types.is_integer_dtype(statistics["baseline_support"]):
        raise ValueError("baseline model support must use an integer dtype")
    if statistics["baseline_support"].le(0).any():
        raise ValueError("baseline model support must be positive")
    if not pd.api.types.is_bool_dtype(statistics["baseline_eligible"]):
        raise ValueError("baseline model eligibility must use a boolean dtype")
    if not pd.api.types.is_bool_dtype(statistics["residual_scale_degenerate"]):
        raise ValueError("baseline model scale degeneracy must use a boolean dtype")
    numeric = statistics[list(FINITE_STATISTIC_COLUMNS)].to_numpy(dtype=float)
    if not np.isfinite(numeric).all():
        raise ValueError("baseline model statistics must be finite")
    if statistics[["residual_mad", "residual_mad_scale", "residual_iqr"]].lt(0).any().any():
        raise ValueError("baseline model MAD and IQR statistics must be non-negative")
    if statistics["residual_scale"].le(0).any():
        raise ValueError("baseline model residual_scale must be finite and positive")
    if not np.allclose(
        statistics["residual_mad_scale"],
        statistics["residual_mad"] * 1.4826,
        atol=1e-12,
        rtol=0,
    ):
        raise ValueError("baseline model MAD scale is inconsistent")
    if not np.allclose(
        statistics["residual_iqr"],
        statistics["residual_q3"] - statistics["residual_q1"],
        atol=1e-12,
        rtol=0,
    ):
        raise ValueError("baseline model IQR is inconsistent")
    if not (
        statistics["residual_q1"].le(statistics["residual_median"])
        & statistics["residual_median"].le(statistics["residual_q3"])
    ).all():
        raise ValueError("baseline model median must lie within its quartiles")
    if not np.allclose(
        statistics["residual_iqr_lower"],
        statistics["residual_q1"] - 1.5 * statistics["residual_iqr"],
        atol=1e-12,
        rtol=0,
    ) or not np.allclose(
        statistics["residual_iqr_upper"],
        statistics["residual_q3"] + 1.5 * statistics["residual_iqr"],
        atol=1e-12,
        rtol=0,
    ):
        raise ValueError("baseline model IQR fences are inconsistent")

    for source, keys in BASELINE_HIERARCHY:
        rows = statistics.loc[statistics["baseline_source"].eq(source)]
        if source == "global" and len(rows) != 1:
            raise ValueError("baseline model must contain exactly one global fallback")
        if keys and rows.duplicated(list(keys)).any():
            raise ValueError(f"baseline model contains duplicate {source} keys")
        for _, row in rows.iterrows():
            for key in ("carrier_id", "lane_id", "mode"):
                if key in keys:
                    if not _valid_key(row[key]):
                        raise ValueError(f"baseline model {source} has an invalid {key}")
                elif not _missing_key(row[key]):
                    raise ValueError(f"baseline model {source} must not populate {key}")
            expected_scale, expected_source, expected_degenerate = _expected_scale(row)
            if row["residual_scale_source"] != expected_source or not np.isclose(
                row["residual_scale"], expected_scale, atol=1e-12, rtol=0
            ):
                raise ValueError("baseline model residual scale provenance is inconsistent")
            if bool(row["residual_scale_degenerate"]) != expected_degenerate:
                raise ValueError("baseline model residual scale degeneracy is inconsistent")
            expected_eligible = source == "global" or (
                int(row["baseline_support"]) >= MINIMUM_SUPPORTS[source] and not expected_degenerate
            )
            if bool(row["baseline_eligible"]) != expected_eligible:
                raise ValueError("baseline model eligibility is inconsistent")


def _validate_model(model: BaselineModel) -> None:
    if not isinstance(model, BaselineModel):
        raise ValueError("model must be a BaselineModel")
    _validate_model_metadata(model)
    if _frame_fingerprint(model.statistics) != model._statistics_fingerprint:
        raise ValueError("baseline model statistics fingerprint mismatch after mutation")
    _validate_model_statistics(model.statistics)


def _validate_identity(name: str, frame: pd.DataFrame) -> str:
    if frame.empty:
        raise ValueError(f"{name} must be non-empty")
    run_ids = frame["run_id"].drop_duplicates()
    if len(run_ids) != 1 or not isinstance(run_ids.iloc[0], str) or not run_ids.iloc[0].strip():
        raise ValueError(f"{name} must contain exactly one non-empty run_id")
    if not frame["schema_version"].eq(SCHEMA_VERSION).all():
        raise ValueError(f"{name} contains an incompatible schema_version")
    return str(run_ids.iloc[0])


def _validate_string_columns(name: str, frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    for column in columns:
        valid = frame[column].map(lambda value: isinstance(value, str) and bool(value.strip()))
        if not valid.all():
            raise ValueError(f"{name}.{column} must contain non-empty strings")


def _validate_dates(
    name: str,
    frame: pd.DataFrame,
    columns: tuple[str, ...],
    *,
    monday: bool = False,
) -> None:
    for column in columns:
        values = frame[column]
        if not pd.api.types.is_datetime64_any_dtype(values) or values.isna().any():
            raise ValueError(f"{name}.{column} must use a non-null datetime dtype")
        if values.dt.tz is not None:
            raise ValueError(f"{name}.{column} must be timezone-naive")
        if not values.dt.normalize().equals(values):
            raise ValueError(f"{name}.{column} must contain normalized midnight dates")
        if monday and not values.dt.dayofweek.eq(0).all():
            raise ValueError(f"{name}.{column} must contain Monday week starts")


def _finite_numeric(
    name: str,
    frame: pd.DataFrame,
    column: str,
    *,
    lower: float | None = None,
    upper: float | None = None,
) -> np.ndarray:
    values = frame[column]
    if pd.api.types.is_bool_dtype(values) or not pd.api.types.is_numeric_dtype(values):
        raise ValueError(f"{name}.{column} must use a numeric dtype")
    array = values.to_numpy(dtype=float)
    if not np.isfinite(array).all():
        raise ValueError(f"{name}.{column} must contain finite values")
    if lower is not None and (array < lower).any():
        raise ValueError(f"{name}.{column} must be at least {lower}")
    if upper is not None and (array > upper).any():
        raise ValueError(f"{name}.{column} must be at most {upper}")
    return array


def _validate_inputs(
    shipments: pd.DataFrame,
    carrier_rates: pd.DataFrame,
    fuel_surcharges: pd.DataFrame,
) -> str:
    _require_columns("shipments", shipments, SHIPMENT_COLUMNS)
    _require_columns("carrier_rates", carrier_rates, RATE_COLUMNS)
    _require_columns("fuel_surcharges", fuel_surcharges, FUEL_COLUMNS)
    for name, frame in (
        ("shipments", shipments),
        ("carrier_rates", carrier_rates),
        ("fuel_surcharges", fuel_surcharges),
    ):
        _reject_reserved_columns(name, frame)
    leaked = FORBIDDEN_SCORING_COLUMNS.intersection(shipments.columns)
    if leaked:
        raise ValueError(f"shipments contain forbidden truth/cause fields: {sorted(leaked)}")
    if shipments.empty or carrier_rates.empty or fuel_surcharges.empty:
        raise ValueError("scoring inputs must be non-empty")

    run_id = _validate_identity("shipments", shipments)
    for name, frame in (
        ("carrier_rates", carrier_rates),
        ("fuel_surcharges", fuel_surcharges),
    ):
        if _validate_identity(name, frame) != run_id:
            raise ValueError(f"{name} run_id does not match shipments run_id")

    _validate_string_columns(
        "shipments", shipments, ("shipment_id", "rate_id", "carrier_id", "lane_id", "mode")
    )
    _validate_string_columns(
        "carrier_rates", carrier_rates, ("rate_id", "carrier_id", "lane_id", "mode")
    )
    _validate_string_columns("fuel_surcharges", fuel_surcharges, ("mode",))
    if shipments["shipment_id"].duplicated().any():
        raise ValueError("shipments contain duplicate shipment_id values")
    if carrier_rates.duplicated(["run_id", "rate_id"]).any():
        raise ValueError("carrier_rates contain duplicate run_id/rate_id keys")
    if fuel_surcharges.duplicated(["run_id", "week_start", "mode"]).any():
        raise ValueError("fuel_surcharges contain duplicate run_id/week_start/mode keys")

    _validate_dates("shipments", shipments, ("ship_date",))
    _validate_dates("carrier_rates", carrier_rates, ("effective_start", "effective_end"))
    _validate_dates("fuel_surcharges", fuel_surcharges, ("week_start",), monday=True)
    if not shipments["ship_date"].between(BASELINE_START, EVALUATION_END).all():
        raise ValueError("shipments.ship_date falls outside the declared analysis windows")
    if carrier_rates["effective_start"].gt(carrier_rates["effective_end"]).any():
        raise ValueError("carrier rate effective dates are reversed")

    _finite_numeric("shipments", shipments, "weight_lbs")
    _finite_numeric(
        "shipments", shipments, "total_cost", lower=0.0, upper=MAX_TRUSTED_MONETARY_VALUE
    )
    _finite_numeric("carrier_rates", carrier_rates, "base_rate_per_cwt", lower=0.01, upper=500.0)
    _finite_numeric("carrier_rates", carrier_rates, "minimum_charge", lower=0.0, upper=50_000.0)
    _finite_numeric("fuel_surcharges", fuel_surcharges, "surcharge_rate", lower=0.0, upper=1.0)
    return run_id


def calculate_expected_costs(
    shipments: pd.DataFrame,
    carrier_rates: pd.DataFrame,
    fuel_surcharges: pd.DataFrame,
) -> pd.DataFrame:
    """Rebuild expected cost solely from authoritative rate and fuel tables."""
    _validate_inputs(shipments, carrier_rates, fuel_surcharges)
    observed = shipments.drop(columns=DERIVED_COLUMNS.intersection(shipments.columns)).copy(
        deep=True
    )
    observed["_input_order"] = np.arange(len(observed))

    rate_lookup = carrier_rates[
        [
            "run_id",
            "rate_id",
            "carrier_id",
            "lane_id",
            "mode",
            "effective_start",
            "effective_end",
            "base_rate_per_cwt",
            "minimum_charge",
        ]
    ].rename(
        columns={
            "carrier_id": "_rate_carrier_id",
            "lane_id": "_rate_lane_id",
            "mode": "_rate_mode",
            "effective_start": "authoritative_rate_effective_start",
            "effective_end": "authoritative_rate_effective_end",
            "base_rate_per_cwt": "authoritative_base_rate_per_cwt",
            "minimum_charge": "authoritative_minimum_charge",
        }
    )
    costed = observed.merge(
        rate_lookup,
        on=["run_id", "rate_id"],
        how="left",
        validate="many_to_one",
        indicator="_rate_join",
        sort=False,
    )
    if not costed["_rate_join"].eq("both").all():
        raise ValueError("one or more shipments do not resolve to an authoritative rate")
    for shipment_column, rate_column in (
        ("carrier_id", "_rate_carrier_id"),
        ("lane_id", "_rate_lane_id"),
        ("mode", "_rate_mode"),
    ):
        if not costed[shipment_column].eq(costed[rate_column]).all():
            raise ValueError("shipment carrier/lane/mode does not match its authoritative rate key")
    if not (
        costed["ship_date"].ge(costed["authoritative_rate_effective_start"])
        & costed["ship_date"].le(costed["authoritative_rate_effective_end"])
    ).all():
        raise ValueError("shipment date falls outside its authoritative rate effective window")

    costed["authoritative_fuel_week_start"] = costed["ship_date"] - pd.to_timedelta(
        costed["ship_date"].dt.dayofweek, unit="D"
    )
    fuel_lookup = fuel_surcharges[["run_id", "week_start", "mode", "surcharge_rate"]].rename(
        columns={
            "week_start": "authoritative_fuel_week_start",
            "surcharge_rate": "authoritative_fuel_surcharge_rate",
        }
    )
    costed = costed.merge(
        fuel_lookup,
        on=["run_id", "authoritative_fuel_week_start", "mode"],
        how="left",
        validate="many_to_one",
        indicator="_fuel_join",
        sort=False,
    )
    if not costed["_fuel_join"].eq("both").all():
        raise ValueError("one or more shipments do not resolve to an authoritative fuel schedule")

    weights = costed["weight_lbs"].to_numpy(dtype=float)
    trusted = (weights > 0) & (weights <= MAX_TRUSTED_WEIGHT_LBS)
    expected_base = np.full(len(costed), np.nan)
    expected_fuel = np.full(len(costed), np.nan)
    expected_total = np.full(len(costed), np.nan)
    expected_base[trusted] = np.round(
        np.maximum(
            costed.loc[trusted, "authoritative_base_rate_per_cwt"].to_numpy(dtype=float)
            * costed.loc[trusted, "weight_lbs"].to_numpy(dtype=float)
            / 100.0,
            costed.loc[trusted, "authoritative_minimum_charge"].to_numpy(dtype=float),
        ),
        2,
    )
    expected_fuel[trusted] = np.round(
        expected_base[trusted]
        * costed.loc[trusted, "authoritative_fuel_surcharge_rate"].to_numpy(dtype=float),
        2,
    )
    expected_total[trusted] = np.round(expected_base[trusted] + expected_fuel[trusted], 2)
    residual = np.round(costed["total_cost"].to_numpy(dtype=float) - expected_total, 2)
    excess = np.round(np.maximum(residual, 0.0), 2)
    trusted_derived = np.column_stack(
        (expected_base[trusted], expected_fuel[trusted], expected_total[trusted], residual[trusted])
    )
    if (
        not np.isfinite(trusted_derived).all()
        or (np.abs(trusted_derived) > MAX_TRUSTED_MONETARY_VALUE).any()
    ):
        raise ValueError(
            "trusted expected cost or residual is nonfinite, overflowing, or unbounded"
        )

    costed["monetary_values_trusted"] = trusted
    costed["expected_base_cost"] = expected_base
    costed["expected_fuel_surcharge"] = expected_fuel
    costed["expected_total_cost"] = expected_total
    costed["cost_residual"] = residual
    costed["estimated_excess_cost"] = excess
    costed["time_window"] = np.select(
        [
            costed["ship_date"].le(BASELINE_END),
            costed["ship_date"].between(CALIBRATION_START, CALIBRATION_END),
            costed["ship_date"].ge(EVALUATION_START),
        ],
        ["baseline", "calibration", "evaluation"],
        default="invalid",
    )
    costed = costed.sort_values("_input_order", kind="stable")
    return costed.drop(
        columns=[
            "_input_order",
            "_rate_join",
            "_fuel_join",
            "_rate_carrier_id",
            "_rate_lane_id",
            "_rate_mode",
        ]
    ).reset_index(drop=True)


def _robust_statistics(values: pd.Series) -> dict[str, float | int | str | bool]:
    array = values.to_numpy(dtype=float)
    median = float(np.median(array))
    q1, q3 = np.quantile(array, [0.25, 0.75])
    mad = float(np.median(np.abs(array - median)))
    iqr = float(q3 - q1)
    statistics: dict[str, float | int | str | bool] = {
        "baseline_support": int(len(array)),
        "residual_median": median,
        "residual_mad": mad,
        "residual_mad_scale": float(1.4826 * mad),
        "residual_q1": float(q1),
        "residual_q3": float(q3),
        "residual_iqr": iqr,
        "residual_iqr_lower": float(q1 - 1.5 * iqr),
        "residual_iqr_upper": float(q3 + 1.5 * iqr),
    }
    scale, source, degenerate = _expected_scale(pd.Series(statistics))
    statistics["residual_scale"] = scale
    statistics["residual_scale_source"] = source
    statistics["residual_scale_degenerate"] = degenerate
    return statistics


def fit_baselines(expected_costs: pd.DataFrame) -> BaselineModel:
    """Fit hierarchical robust residual distributions on baseline rows only."""
    _require_columns(
        "expected_costs",
        expected_costs,
        SHIPMENT_COLUMNS | frozenset({"cost_residual", "monetary_values_trusted", "time_window"}),
    )
    leaked = FORBIDDEN_SCORING_COLUMNS.intersection(expected_costs.columns)
    if leaked:
        raise ValueError(f"expected_costs contain forbidden truth/cause fields: {sorted(leaked)}")
    _reject_reserved_columns("expected_costs", expected_costs)
    run_id = _validate_identity("expected_costs", expected_costs)
    if not pd.api.types.is_bool_dtype(expected_costs["monetary_values_trusted"]):
        raise ValueError("monetary_values_trusted must use a boolean dtype")
    if not pd.api.types.is_numeric_dtype(expected_costs["cost_residual"]):
        raise ValueError("cost_residual must use a numeric dtype")
    trusted_residuals = expected_costs.loc[
        expected_costs["monetary_values_trusted"], "cost_residual"
    ].to_numpy(dtype=float)
    if (
        not np.isfinite(trusted_residuals).all()
        or (np.abs(trusted_residuals) > MAX_TRUSTED_MONETARY_VALUE).any()
    ):
        raise ValueError("trusted cost_residual values must be finite and bounded")
    baseline = expected_costs.loc[
        expected_costs["ship_date"].between(BASELINE_START, BASELINE_END)
        & expected_costs["monetary_values_trusted"].eq(True)
        & expected_costs["cost_residual"].notna()
    ]
    if baseline.empty:
        raise ValueError("no trusted baseline rows are available for fitting")

    rows: list[dict[str, object]] = []
    for source, keys in BASELINE_HIERARCHY:
        if keys:
            grouped = baseline.groupby(list(keys), observed=True, sort=True, dropna=False)
            for group_key, group in grouped:
                key_values = group_key if isinstance(group_key, tuple) else (group_key,)
                row: dict[str, object] = dict(zip(keys, key_values, strict=True))
                row.update(_robust_statistics(group["cost_residual"]))
                row["baseline_source"] = source
                rows.append(row)
        else:
            row = _robust_statistics(baseline["cost_residual"])
            row["baseline_source"] = source
            rows.append(row)

    statistics = pd.DataFrame(rows)
    for key in ("carrier_id", "lane_id", "mode"):
        if key not in statistics:
            statistics[key] = pd.NA
    statistics["baseline_eligible"] = statistics.apply(
        lambda row: bool(
            row["baseline_source"] == "global"
            or (
                row["baseline_support"] >= MINIMUM_SUPPORTS[row["baseline_source"]]
                and not row["residual_scale_degenerate"]
            )
        ),
        axis=1,
    )
    source_order = {name: index for index, (name, _) in enumerate(BASELINE_HIERARCHY)}
    statistics["_source_order"] = statistics["baseline_source"].map(source_order)
    statistics = statistics.sort_values(
        ["_source_order", "carrier_id", "lane_id", "mode"],
        kind="stable",
        na_position="last",
        ignore_index=True,
    ).drop(columns="_source_order")
    statistics = statistics.loc[:, MODEL_STATISTIC_COLUMNS]
    return BaselineModel(
        run_id=run_id,
        schema_version=SCHEMA_VERSION,
        fit_start=BASELINE_START,
        fit_end=BASELINE_END,
        statistics=statistics,
    )


def score_with_baselines(expected_costs: pd.DataFrame, model: BaselineModel) -> pd.DataFrame:
    """Attach the narrowest supported, non-degenerate baseline to every row."""
    _require_columns(
        "expected_costs",
        expected_costs,
        SHIPMENT_COLUMNS | frozenset({"cost_residual", "monetary_values_trusted", "time_window"}),
    )
    leaked = FORBIDDEN_SCORING_COLUMNS.intersection(expected_costs.columns)
    if leaked:
        raise ValueError(f"expected_costs contain forbidden truth/cause fields: {sorted(leaked)}")
    _reject_reserved_columns("expected_costs", expected_costs)
    _validate_model(model)
    run_id = _validate_identity("expected_costs", expected_costs)
    if run_id != model.run_id or model.schema_version != SCHEMA_VERSION:
        raise ValueError("baseline model run_id or schema_version does not match scoring rows")
    scored = (
        expected_costs.drop(
            columns=set(STATISTIC_COLUMNS).union({"residual_from_median"}), errors="ignore"
        )
        .copy(deep=True)
        .reset_index(drop=True)
    )
    scored["_score_order"] = np.arange(len(scored))

    global_rows = model.statistics.loc[model.statistics["baseline_source"].eq("global")]
    if len(global_rows) != 1:
        raise ValueError("baseline model must contain exactly one global fallback")
    global_stats = global_rows.iloc[0]
    for column in STATISTIC_COLUMNS:
        scored[column] = global_stats[column]

    for source, keys in reversed(BASELINE_HIERARCHY[:-1]):
        candidates = model.statistics.loc[
            model.statistics["baseline_source"].eq(source) & model.statistics["baseline_eligible"]
        ]
        if candidates.empty:
            continue
        lookup_columns = [*keys, *STATISTIC_COLUMNS[1:]]
        lookup = candidates[lookup_columns]
        matches = scored[["_score_order", *keys]].merge(
            lookup,
            on=list(keys),
            how="left",
            validate="many_to_one",
            sort=False,
        )
        matched = matches["baseline_support"].notna().to_numpy()
        if not matched.any():
            continue
        row_indices = matches.loc[matched, "_score_order"].to_numpy(dtype=int)
        scored.loc[row_indices, "baseline_source"] = source
        for column in STATISTIC_COLUMNS[1:]:
            values = matches.loc[matched, column]
            if column == "residual_scale_degenerate":
                values = values.astype(bool)
            elif column == "baseline_support":
                values = values.astype(int)
            scored.loc[row_indices, column] = values.to_numpy()

    scored["baseline_support"] = scored["baseline_support"].astype(int)
    scored["residual_from_median"] = np.round(
        scored["cost_residual"] - scored["residual_median"], 6
    )
    return (
        scored.sort_values("_score_order", kind="stable")
        .drop(columns="_score_order")
        .reset_index(drop=True)
    )


def fit_and_score_baselines(
    shipments: pd.DataFrame,
    carrier_rates: pd.DataFrame,
    fuel_surcharges: pd.DataFrame,
) -> BaselineResult:
    """Cost, baseline-fit, and score one run without using future observations."""
    expected = calculate_expected_costs(shipments, carrier_rates, fuel_surcharges)
    model = fit_baselines(expected)
    return BaselineResult(scored=score_with_baselines(expected, model), model=model)
