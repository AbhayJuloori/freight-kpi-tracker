"""Leakage-safe shipment and lane-week exception detectors."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from math import isfinite

import numpy as np
import pandas as pd

from freight_v2.anomalies import TRUTH_FIELDS
from freight_v2.baselines import FORBIDDEN_SCORING_COLUMNS, BaselineResult, score_with_baselines
from freight_v2.config import SCHEMA_VERSION
from freight_v2.contracts import validate_columns
from freight_v2.generation import (
    BASELINE_END,
    BASELINE_START,
    CALIBRATION_END,
    CALIBRATION_START,
    EVALUATION_END,
    EVALUATION_START,
    MODE_TRANSIT_DAYS,
)

DETECTION_METHODS = (
    "robust_residual",
    "iqr",
    "lane_week_deviation",
    "service_deterioration",
    "service_sla_breach",
    "data_quality",
)

SERVICE_SLA_EXCESS_DAYS = 2

REQUIRED_SCORED_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "shipment_id",
        "ship_date",
        "carrier_id",
        "lane_id",
        "mode",
        "freight_class",
        "weight_lbs",
        "total_cost",
        "on_time_flag",
        "transit_days",
        "monetary_values_trusted",
        "cost_residual",
        "estimated_excess_cost",
        "time_window",
        "baseline_source",
        "baseline_support",
        "residual_median",
        "residual_q3",
        "residual_iqr",
        "residual_scale",
        "residual_from_median",
    }
)

_ALWAYS_FINITE = (
    "weight_lbs",
    "total_cost",
    "transit_days",
    "baseline_support",
    "residual_median",
    "residual_q3",
    "residual_iqr",
    "residual_scale",
)
_TRUSTED_FINITE = ("cost_residual", "estimated_excess_cost", "residual_from_median")


@dataclass(frozen=True, slots=True)
class DetectorConfig:
    """Immutable detector parameters suitable for a bounded sensitivity grid."""

    robust_threshold: float = 3.5
    iqr_multiplier: float = 1.5
    lane_week_threshold: float = 3.0
    service_drop_threshold: float = 0.15
    trailing_observed_weeks: int = 8
    minimum_trailing_weeks: int = 4
    service_current_observed_weeks: int = 3
    minimum_service_current_shipments: int = 6
    trend_scale_floor: float = 0.01
    maximum_trusted_weight_lbs: float = 1_000_000.0

    def __post_init__(self) -> None:
        positive = {
            "robust_threshold": self.robust_threshold,
            "iqr_multiplier": self.iqr_multiplier,
            "lane_week_threshold": self.lane_week_threshold,
            "trend_scale_floor": self.trend_scale_floor,
            "maximum_trusted_weight_lbs": self.maximum_trusted_weight_lbs,
        }
        for name, value in positive.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a finite positive number")
            if not isfinite(float(value)) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
        if (
            isinstance(self.service_drop_threshold, bool)
            or not isinstance(self.service_drop_threshold, (int, float))
            or not isfinite(float(self.service_drop_threshold))
            or not 0 < self.service_drop_threshold <= 1
        ):
            raise ValueError("service_drop_threshold must be in (0, 1]")
        for name, value in (
            ("trailing_observed_weeks", self.trailing_observed_weeks),
            ("minimum_trailing_weeks", self.minimum_trailing_weeks),
            ("service_current_observed_weeks", self.service_current_observed_weeks),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 2:
                raise ValueError(f"{name} must be an integer of at least 2")
        if self.minimum_trailing_weeks > self.trailing_observed_weeks:
            raise ValueError("minimum_trailing_weeks cannot exceed trailing_observed_weeks")
        if (
            isinstance(self.minimum_service_current_shipments, bool)
            or not isinstance(self.minimum_service_current_shipments, int)
            or self.minimum_service_current_shipments < 2
        ):
            raise ValueError("minimum_service_current_shipments must be an integer of at least 2")


@dataclass(frozen=True, slots=True)
class DetectionResult:
    """Normalized shipment flags and their strictly trailing weekly evidence."""

    flags: pd.DataFrame
    lane_week_trends: pd.DataFrame


def _valid_key(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_scored_baseline_lineage(frame: pd.DataFrame, result: BaselineResult) -> None:
    reconstructed = score_with_baselines(frame, result.model)
    exact_columns = ("baseline_source", "baseline_support")
    for column in exact_columns:
        if not frame[column].reset_index(drop=True).equals(reconstructed[column]):
            raise ValueError(f"scored.{column} does not match the baseline model")
    numeric_columns = (
        "residual_median",
        "residual_q3",
        "residual_iqr",
        "residual_scale",
        "residual_from_median",
    )
    for column in numeric_columns:
        actual = frame[column].to_numpy(dtype=float)
        expected = reconstructed[column].to_numpy(dtype=float)
        if not np.allclose(actual, expected, atol=1e-9, rtol=0, equal_nan=True):
            raise ValueError(f"scored.{column} does not match the baseline model")

    trusted = frame["monetary_values_trusted"].to_numpy(dtype=bool)
    expected_excess = np.full(len(frame), np.nan)
    expected_excess[trusted] = np.round(
        np.maximum(frame.loc[trusted, "cost_residual"].to_numpy(dtype=float), 0.0),
        2,
    )
    if not np.allclose(
        frame["estimated_excess_cost"].to_numpy(dtype=float),
        expected_excess,
        atol=1e-9,
        rtol=0,
        equal_nan=True,
    ):
        raise ValueError("scored.estimated_excess_cost violates the positive-residual identity")


def _validate_scored(result: BaselineResult) -> tuple[pd.DataFrame, str]:
    if not isinstance(result, BaselineResult):
        raise ValueError("baseline_result must be a BaselineResult")
    frame = result.scored
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise ValueError("baseline_result.scored must be a non-empty DataFrame")
    missing = REQUIRED_SCORED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"scored rows are missing required columns: {sorted(missing)}")
    leaked = (frozenset(TRUTH_FIELDS) | FORBIDDEN_SCORING_COLUMNS).intersection(frame.columns)
    if leaked:
        raise ValueError(f"scored rows contain forbidden truth/cause fields: {sorted(leaked)}")

    run_ids = frame["run_id"].drop_duplicates()
    if len(run_ids) != 1 or not _valid_key(run_ids.iloc[0]):
        raise ValueError("scored rows must contain exactly one non-empty run_id")
    run_id = str(run_ids.iloc[0])
    if run_id != result.model.run_id:
        raise ValueError("scored run_id does not match baseline model run_id")
    if (
        result.model.schema_version != SCHEMA_VERSION
        or not frame["schema_version"].eq(SCHEMA_VERSION).all()
    ):
        raise ValueError("scored rows or baseline model use an incompatible schema_version")

    for column in (
        "shipment_id",
        "carrier_id",
        "lane_id",
        "mode",
        "freight_class",
        "baseline_source",
    ):
        if not frame[column].map(_valid_key).all():
            raise ValueError(f"scored.{column} must contain non-empty strings")
    if frame["shipment_id"].duplicated().any():
        raise ValueError("scored rows contain duplicate shipment_id values")

    dates = frame["ship_date"]
    if not pd.api.types.is_datetime64_any_dtype(dates) or dates.isna().any():
        raise ValueError("scored.ship_date must use a non-null datetime dtype")
    if dates.dt.tz is not None or not dates.dt.normalize().equals(dates):
        raise ValueError("scored.ship_date must contain normalized timezone-naive dates")
    if not dates.between(BASELINE_START, EVALUATION_END).all():
        raise ValueError("scored.ship_date falls outside declared analysis windows")
    expected_window = np.select(
        [
            dates.le(BASELINE_END),
            dates.between(CALIBRATION_START, CALIBRATION_END),
            dates.ge(EVALUATION_START),
        ],
        ["baseline", "calibration", "evaluation"],
        default="invalid",
    )
    if not np.array_equal(frame["time_window"].to_numpy(), expected_window):
        raise ValueError("scored.time_window does not match ship_date lineage")

    for column in _ALWAYS_FINITE:
        values = frame[column]
        if pd.api.types.is_bool_dtype(values) or not pd.api.types.is_numeric_dtype(values):
            raise ValueError(f"scored.{column} must be numeric")
        if not np.isfinite(values.to_numpy(dtype=float)).all():
            raise ValueError(f"scored.{column} must contain finite values")
    if frame["residual_scale"].le(0).any():
        raise ValueError("scored.residual_scale must be finite and positive")
    if (
        frame["baseline_support"].le(0).any()
        or not np.equal(frame["baseline_support"], np.floor(frame["baseline_support"])).all()
    ):
        raise ValueError("scored.baseline_support must contain positive whole numbers")
    if not pd.api.types.is_bool_dtype(frame["monetary_values_trusted"]):
        raise ValueError("scored.monetary_values_trusted must use a boolean dtype")
    if not frame["on_time_flag"].isin([0, 1]).all():
        raise ValueError("scored.on_time_flag must contain only 0 or 1")

    trusted = frame["monetary_values_trusted"]
    for column in _TRUSTED_FINITE:
        if not pd.api.types.is_numeric_dtype(frame[column]):
            raise ValueError(f"scored.{column} must be numeric")
        if not np.isfinite(frame.loc[trusted, column].to_numpy(dtype=float)).all():
            raise ValueError(f"trusted scored.{column} values must be finite")
        if frame.loc[~trusted, column].notna().any():
            raise ValueError(f"untrusted scored.{column} values must be null")
    if frame.loc[trusted, "estimated_excess_cost"].lt(0).any():
        raise ValueError("scored.estimated_excess_cost must be non-negative")
    _validate_scored_baseline_lineage(frame, result)
    return frame.copy(deep=True), run_id


def _safe_trailing_scale(values: np.ndarray, floor: float) -> float:
    if len(values) == 0:
        return floor
    median = float(np.median(values))
    mad_scale = 1.4826 * float(np.median(np.abs(values - median)))
    if isfinite(mad_scale) and mad_scale > 0:
        return mad_scale
    if len(values) > 1:
        standard = float(np.std(values, ddof=1))
        if isfinite(standard) and standard > 0:
            return standard
    return floor


def _build_weekly_trends(scored: pd.DataFrame, config: DetectorConfig) -> pd.DataFrame:
    working = scored.sort_values(["ship_date", "shipment_id"], kind="stable").copy()
    working["week_start"] = working["ship_date"] - pd.to_timedelta(
        working["ship_date"].dt.dayofweek, unit="D"
    )
    trusted = working["monetary_values_trusted"] & working["weight_lbs"].gt(0)
    working["_cost_per_lb"] = np.where(
        trusted,
        working["total_cost"] / working["weight_lbs"],
        np.nan,
    )
    working["_trusted_residual"] = working["cost_residual"].where(trusted)
    working["_trusted_cost"] = working["total_cost"].where(trusted)
    working["_trusted_weight"] = working["weight_lbs"].where(trusted)

    weekly = (
        working.groupby(["run_id", "schema_version", "lane_id", "mode", "week_start"], sort=True)
        .agg(
            shipment_count=("shipment_id", "size"),
            trusted_shipment_count=("_trusted_residual", "count"),
            trusted_cost_sum=("_trusted_cost", "sum"),
            trusted_weight_sum=("_trusted_weight", "sum"),
            average_cost_residual=("_trusted_residual", "mean"),
            on_time_rate=("on_time_flag", "mean"),
        )
        .reset_index()
        .sort_values(["lane_id", "mode", "week_start"], kind="stable", ignore_index=True)
    )
    weekly["average_cost_per_lb"] = np.where(
        weekly["trusted_weight_sum"].gt(0),
        weekly["trusted_cost_sum"] / weekly["trusted_weight_sum"],
        np.nan,
    )
    weekly["trailing_baseline"] = np.nan
    weekly["trailing_scale"] = np.nan
    weekly["trailing_support"] = 0
    weekly["deviation_score"] = np.nan
    weekly["monetary_signal_available"] = False
    weekly["trailing_on_time_rate"] = np.nan
    weekly["service_trailing_support"] = 0
    weekly["service_drop_score"] = np.nan
    weekly["service_signal_available"] = False

    for _, positions in weekly.groupby(["lane_id", "mode"], sort=True).groups.items():
        ordered_positions = list(positions)
        for offset, position in enumerate(ordered_positions):
            prior_positions = ordered_positions[
                max(0, offset - config.trailing_observed_weeks) : offset
            ]
            trailing_support = len(prior_positions)
            weekly.at[position, "trailing_support"] = trailing_support
            weekly.at[position, "service_trailing_support"] = trailing_support
            if not prior_positions:
                continue

            prior_residuals = weekly.loc[prior_positions, "average_cost_residual"].dropna()
            current_residual = weekly.at[position, "average_cost_residual"]
            if len(prior_residuals) >= config.minimum_trailing_weeks and pd.notna(current_residual):
                values = prior_residuals.to_numpy(dtype=float)
                baseline = float(np.median(values))
                scale = _safe_trailing_scale(values, config.trend_scale_floor)
                weekly.at[position, "trailing_baseline"] = baseline
                weekly.at[position, "trailing_scale"] = scale
                weekly.at[position, "deviation_score"] = float(
                    (float(current_residual) - baseline) / scale
                )
                weekly.at[position, "monetary_signal_available"] = True

            prior_service = weekly.loc[prior_positions, "on_time_rate"].to_numpy(dtype=float)
            if len(prior_service) >= config.minimum_trailing_weeks:
                service_baseline = float(np.median(prior_service))
                weekly.at[position, "trailing_on_time_rate"] = service_baseline
                weekly.at[position, "service_drop_score"] = max(
                    0.0,
                    service_baseline - float(weekly.at[position, "on_time_rate"]),
                )
                weekly.at[position, "service_signal_available"] = True

    numeric = [
        "average_cost_per_lb",
        "average_cost_residual",
        "on_time_rate",
        "trailing_baseline",
        "trailing_scale",
        "deviation_score",
        "trailing_on_time_rate",
        "service_drop_score",
    ]
    for column in numeric:
        weekly[column] = weekly[column].astype(float).round(6)
    validate_columns("lane_week_trends", weekly.columns)
    return weekly


def _build_service_trends(scored: pd.DataFrame, config: DetectorConfig) -> pd.DataFrame:
    working = scored.sort_values(["ship_date", "shipment_id"], kind="stable").copy()
    working["week_start"] = working["ship_date"] - pd.to_timedelta(
        working["ship_date"].dt.dayofweek, unit="D"
    )
    working["_contract_transit_days"] = working["mode"].map(MODE_TRANSIT_DAYS)
    working["_excess_transit_days"] = working["transit_days"] - working["_contract_transit_days"]
    weekly = (
        working.groupby(
            ["lane_id", "carrier_id", "week_start"],
            observed=True,
            sort=True,
        )
        .agg(
            shipment_count=("shipment_id", "size"),
            on_time_count=("on_time_flag", "sum"),
            contract_transit_days_sum=("_contract_transit_days", "sum"),
            excess_transit_days_sum=("_excess_transit_days", "sum"),
            on_time_rate=("on_time_flag", "mean"),
            average_excess_transit_days=("_excess_transit_days", "mean"),
        )
        .reset_index()
        .sort_values(
            ["lane_id", "carrier_id", "week_start"],
            kind="stable",
            ignore_index=True,
        )
    )
    weekly["service_trailing_support"] = 0
    weekly["service_current_observed_weeks"] = 0
    weekly["service_current_shipment_support"] = 0
    weekly["service_degraded_observed_weeks"] = 0
    weekly["trailing_on_time_rate"] = np.nan
    weekly["trailing_excess_transit_days"] = np.nan
    weekly["current_on_time_rate"] = np.nan
    weekly["current_excess_transit_days"] = np.nan
    weekly["on_time_drop_score"] = np.nan
    weekly["transit_increase_score"] = np.nan
    weekly["service_drop_score"] = np.nan
    weekly["service_signal_available"] = False
    for _, positions in weekly.groupby(
        ["lane_id", "carrier_id"], observed=True, sort=True
    ).groups.items():
        ordered_positions = list(positions)
        for offset, position in enumerate(ordered_positions):
            current_start = max(0, offset - config.service_current_observed_weeks + 1)
            current_positions = ordered_positions[current_start : offset + 1]
            prior_positions = ordered_positions[
                max(0, current_start - config.trailing_observed_weeks) : current_start
            ]
            trailing_support = len(prior_positions)
            current_observed_weeks = len(current_positions)
            current_shipment_support = int(weekly.loc[current_positions, "shipment_count"].sum())
            weekly.at[position, "service_trailing_support"] = trailing_support
            weekly.at[position, "service_current_observed_weeks"] = current_observed_weeks
            weekly.at[position, "service_current_shipment_support"] = current_shipment_support
            if (
                trailing_support < config.minimum_trailing_weeks
                or current_observed_weeks < config.service_current_observed_weeks
                or current_shipment_support < config.minimum_service_current_shipments
            ):
                continue
            prior_shipments = float(weekly.loc[prior_positions, "shipment_count"].sum())
            current_shipments = float(current_shipment_support)
            on_time_baseline = float(
                weekly.loc[prior_positions, "on_time_count"].sum() / prior_shipments
            )
            excess_transit_baseline = float(
                weekly.loc[prior_positions, "excess_transit_days_sum"].sum() / prior_shipments
            )
            contract_transit_baseline = float(
                weekly.loc[prior_positions, "contract_transit_days_sum"].sum() / prior_shipments
            )
            current_on_time = float(
                weekly.loc[current_positions, "on_time_count"].sum() / current_shipments
            )
            current_excess_transit = float(
                weekly.loc[current_positions, "excess_transit_days_sum"].sum() / current_shipments
            )
            degraded_weeks = int(
                (
                    weekly.loc[current_positions, "on_time_rate"].lt(on_time_baseline)
                    | weekly.loc[current_positions, "average_excess_transit_days"].gt(
                        excess_transit_baseline
                    )
                ).sum()
            )
            on_time_drop = max(0.0, on_time_baseline - current_on_time)
            transit_increase = max(
                0.0,
                (current_excess_transit - excess_transit_baseline)
                / max(contract_transit_baseline, 1.0),
            )
            weekly.at[position, "trailing_on_time_rate"] = on_time_baseline
            weekly.at[position, "trailing_excess_transit_days"] = excess_transit_baseline
            weekly.at[position, "current_on_time_rate"] = current_on_time
            weekly.at[position, "current_excess_transit_days"] = current_excess_transit
            weekly.at[position, "service_degraded_observed_weeks"] = degraded_weeks
            sustained_on_time_drop = on_time_drop if degraded_weeks >= 2 else 0.0
            sustained_transit_increase = transit_increase if degraded_weeks >= 2 else 0.0
            weekly.at[position, "on_time_drop_score"] = sustained_on_time_drop
            weekly.at[position, "transit_increase_score"] = sustained_transit_increase
            weekly.at[position, "service_drop_score"] = max(
                sustained_on_time_drop,
                sustained_transit_increase,
            )
            weekly.at[position, "service_signal_available"] = True
    return weekly


def _flag_id(run_id: str, shipment_id: str, method: str) -> str:
    payload = f"{run_id}|{shipment_id}|{method}".encode()
    return f"FLAG-{hashlib.sha256(payload).hexdigest()[:20].upper()}"


def _group_evidence_id(run_id: str, method: str, *parts: object) -> str:
    payload = "|".join((run_id, method, *(str(part) for part in parts))).encode()
    return f"EVID-{hashlib.sha256(payload).hexdigest()[:20].upper()}"


def _freight_class_matches_mode(mode: str, freight_class: str) -> bool:
    if mode == "PARCEL":
        return freight_class == "PARCEL"
    if mode == "FTL":
        return freight_class == "TRUCKLOAD"
    if mode == "LTL":
        try:
            return float(freight_class) > 0
        except (TypeError, ValueError):
            return False
    return False


def _reason(method: str, flagged: bool, available: bool) -> str:
    if not available:
        if method in {"robust_residual", "iqr", "lane_week_deviation"}:
            return "Monetary signal unavailable or lacks strictly trailing support."
        if method == "service_deterioration":
            return "Service signal lacks strictly trailing observed-week support."
    labels = {
        "robust_residual": "Robust residual threshold",
        "iqr": "IQR upper fence",
        "lane_week_deviation": "Trailing lane-week deviation threshold",
        "service_deterioration": "Trailing service deterioration threshold",
        "service_sla_breach": "Shipment service-level threshold",
        "data_quality": "Explicit data-quality rules",
    }
    return f"{labels[method]} {'triggered.' if flagged else 'not triggered.'}"


def _normalized_flags(
    scored: pd.DataFrame,
    weekly: pd.DataFrame,
    service_weekly: pd.DataFrame,
    config: DetectorConfig,
    run_id: str,
) -> pd.DataFrame:
    rows = scored.sort_values(["ship_date", "shipment_id"], kind="stable").copy()
    rows["week_start"] = rows["ship_date"] - pd.to_timedelta(
        rows["ship_date"].dt.dayofweek, unit="D"
    )
    weekly_lookup = weekly[
        [
            "lane_id",
            "mode",
            "week_start",
            "deviation_score",
            "trailing_support",
            "shipment_count",
            "monetary_signal_available",
        ]
    ]
    rows = rows.merge(
        weekly_lookup,
        on=["lane_id", "mode", "week_start"],
        how="left",
        validate="many_to_one",
        sort=False,
    )
    rows = rows.merge(
        service_weekly[
            [
                "lane_id",
                "carrier_id",
                "week_start",
                "service_drop_score",
                "on_time_drop_score",
                "transit_increase_score",
                "service_trailing_support",
                "service_current_shipment_support",
                "service_signal_available",
            ]
        ],
        on=["lane_id", "carrier_id", "week_start"],
        how="left",
        validate="many_to_one",
        sort=False,
    )

    output: list[dict[str, object]] = []
    for row in rows.itertuples(index=False):
        trusted = bool(row.monetary_values_trusted)
        exposure = float(row.estimated_excess_cost) if trusted else np.nan
        robust_score = float(row.residual_from_median / row.residual_scale) if trusted else np.nan

        iqr_scale = max(float(row.residual_iqr), float(row.residual_scale) * 1.349)
        iqr_score = float((row.cost_residual - row.residual_q3) / iqr_scale) if trusted else np.nan

        lane_available = trusted and bool(row.monetary_signal_available)
        lane_score = float(row.deviation_score) if lane_available else np.nan

        service_available = bool(row.service_signal_available)
        service_group_score = float(row.service_drop_score) if service_available else np.nan
        excess_transit_days = row.transit_days - MODE_TRANSIT_DAYS[row.mode]
        serialized_service_threshold = round(float(config.service_drop_threshold), 6)
        on_time_branch_breached = (
            service_available
            and round(float(row.on_time_drop_score), 6) > serialized_service_threshold
        )
        transit_branch_breached = (
            service_available
            and round(float(row.transit_increase_score), 6) > serialized_service_threshold
        )
        service_affected = (on_time_branch_breached and row.on_time_flag == 0) or (
            transit_branch_breached and excess_transit_days >= 2
        )
        service_score = (
            service_group_score if service_affected else (0.0 if service_available else np.nan)
        )
        sla_score = float(excess_transit_days) if row.on_time_flag == 0 else 0.0

        dq_reasons: list[str] = []
        if row.weight_lbs <= 0:
            dq_reasons.append("nonpositive billed weight")
        if row.weight_lbs > config.maximum_trusted_weight_lbs:
            dq_reasons.append("billed weight exceeds trusted range")
        if not trusted:
            dq_reasons.append("monetary values are untrusted")
        if row.total_cost <= 0:
            dq_reasons.append("nonpositive observed total cost")
        if row.transit_days <= 0 or row.transit_days > 30:
            dq_reasons.append("transit days fall outside supported range")
        invalid_classes = {"UNKNOWN", "MISMATCHED", "UNCLASSIFIED"}
        if row.freight_class in invalid_classes:
            dq_reasons.append("freight class is unknown or inconsistent")
        elif not _freight_class_matches_mode(row.mode, row.freight_class):
            dq_reasons.append("freight class is incompatible with shipment mode")
        dq_reasons = list(dict.fromkeys(dq_reasons))

        methods = (
            (
                "robust_residual",
                "cost_reconciliation",
                robust_score,
                config.robust_threshold,
                int(row.baseline_support),
                "baseline_shipments",
                trusted,
                row.ship_date,
                exposure,
                np.nan,
                int(row.baseline_support),
            ),
            (
                "iqr",
                "cost_reconciliation",
                iqr_score,
                config.iqr_multiplier,
                int(row.baseline_support),
                "baseline_shipments",
                trusted,
                row.ship_date,
                exposure,
                np.nan,
                int(row.baseline_support),
            ),
            (
                "lane_week_deviation",
                "lane_cost_trend",
                lane_score,
                config.lane_week_threshold,
                int(row.trailing_support),
                "prior_observed_weeks",
                lane_available,
                row.week_start + pd.Timedelta(days=6),
                exposure if lane_available else np.nan,
                lane_score,
                int(row.shipment_count),
            ),
            (
                "service_deterioration",
                "carrier_service_trend",
                service_score,
                config.service_drop_threshold,
                int(row.service_trailing_support),
                "prior_observed_weeks",
                service_available,
                row.week_start + pd.Timedelta(days=6),
                np.nan,
                service_group_score,
                int(row.service_current_shipment_support),
            ),
            (
                "service_sla_breach",
                "service_reconciliation",
                sla_score,
                float(SERVICE_SLA_EXCESS_DAYS - 1),
                1,
                "rules_evaluated",
                True,
                row.ship_date,
                np.nan,
                np.nan,
                1,
            ),
            (
                "data_quality",
                "data_quality",
                float(len(dq_reasons)),
                0.0,
                1,
                "rules_evaluated",
                True,
                row.ship_date,
                np.nan,
                np.nan,
                1,
            ),
        )
        for (
            method,
            method_family,
            score,
            threshold,
            support,
            support_unit,
            available,
            evaluated_at,
            amount,
            group_signal_score,
            current_support,
        ) in methods:
            serialized_score = round(float(score), 6) if available else np.nan
            serialized_threshold = round(float(threshold), 6)
            flagged = bool(available and serialized_score > serialized_threshold)
            flag_id = _flag_id(run_id, row.shipment_id, method)
            if method == "lane_week_deviation":
                evidence_unit_id = _group_evidence_id(
                    run_id, method, row.lane_id, row.mode, row.week_start.date()
                )
            elif method == "service_deterioration":
                evidence_unit_id = _group_evidence_id(
                    run_id, method, row.lane_id, row.carrier_id, row.week_start.date()
                )
            else:
                evidence_unit_id = flag_id
            reason = (
                "; ".join(dq_reasons) + "."
                if method == "data_quality" and dq_reasons
                else _reason(method, bool(flagged), bool(available))
            )
            output.append(
                {
                    "run_id": run_id,
                    "schema_version": SCHEMA_VERSION,
                    "flag_id": flag_id,
                    "evidence_unit_id": evidence_unit_id,
                    "shipment_id": row.shipment_id,
                    "method": method,
                    "method_family": method_family,
                    "score": serialized_score,
                    "threshold": serialized_threshold,
                    "support": int(support),
                    "support_unit": support_unit,
                    "current_support": int(current_support),
                    "group_signal_score": round(float(group_signal_score), 6)
                    if pd.notna(group_signal_score)
                    else np.nan,
                    "is_evaluable": bool(available),
                    "reason": reason,
                    "is_flagged": int(bool(flagged)),
                    "evaluated_at": pd.Timestamp(evaluated_at),
                    "lane_id": row.lane_id,
                    "carrier_id": row.carrier_id,
                    "mode": row.mode,
                    "week_start": row.week_start,
                    "baseline_source": row.baseline_source,
                    "monetary_signal_available": bool(available)
                    if method in {"robust_residual", "iqr", "lane_week_deviation"}
                    else False,
                    "estimated_excess_cost": amount,
                }
            )

    flags = pd.DataFrame(output)
    method_order = {method: position for position, method in enumerate(DETECTION_METHODS)}
    flags["_method_order"] = flags["method"].map(method_order)
    flags = flags.sort_values(
        ["evaluated_at", "shipment_id", "_method_order"],
        kind="stable",
        ignore_index=True,
    ).drop(columns="_method_order")
    validate_columns("anomaly_flags", flags.columns)
    return flags


def detect_exceptions(
    baseline_result: BaselineResult,
    config: DetectorConfig | None = None,
) -> DetectionResult:
    """Run normalized shipment, lane-week, service, and data-quality detectors."""
    selected = DetectorConfig() if config is None else config
    if not isinstance(selected, DetectorConfig):
        raise ValueError("config must be a DetectorConfig")
    scored, run_id = _validate_scored(baseline_result)
    weekly = _build_weekly_trends(scored, selected)
    service_weekly = _build_service_trends(scored, selected)
    flags = _normalized_flags(scored, weekly, service_weekly, selected, run_id)
    return DetectionResult(flags=flags, lane_week_trends=weekly)
