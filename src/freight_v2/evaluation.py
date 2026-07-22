"""Held-out evaluation with strict key grain and calibration-only model selection."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from math import isfinite
from typing import Any

import numpy as np
import pandas as pd

from freight_v2.anomalies import ANOMALY_TYPES
from freight_v2.baselines import BaselineResult
from freight_v2.config import SCHEMA_VERSION
from freight_v2.detection import (
    DETECTION_METHODS,
    DetectionResult,
    DetectorConfig,
    detect_exceptions,
)

EVALUATION_WINDOWS = frozenset({"calibration", "evaluation"})
MAX_GRID_SIZE = 25
SELECTION_RULE = (
    "Maximize calibration utility = 0.60 excess-cost coverage + 0.25 recall "
    "- 0.10 false-positive rate - 0.05 review rate; break ties by coverage, recall, "
    "lower false-positive rate, lower review volume, then config ID."
)

DEFAULT_SENSITIVITY_GRID = (
    DetectorConfig(
        robust_threshold=3.0,
        iqr_multiplier=1.25,
        lane_week_threshold=2.5,
        service_drop_threshold=0.10,
    ),
    DetectorConfig(),
    DetectorConfig(
        robust_threshold=4.0,
        iqr_multiplier=2.0,
        lane_week_threshold=3.5,
        service_drop_threshold=0.20,
    ),
)

_SCORED_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "shipment_id",
        "time_window",
        "total_cost",
        "on_time_flag",
        "estimated_excess_cost",
    }
)
_TRUTH_COLUMNS = frozenset(
    {"run_id", "schema_version", "shipment_id", "is_anomaly", "anomaly_type"}
)
_FLAG_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "shipment_id",
        "method",
        "method_family",
        "is_flagged",
        "evidence_unit_id",
        "support_unit",
        "current_support",
    }
)

_METHOD_CONTRACTS = {
    "robust_residual": ("cost_reconciliation", "baseline_shipments"),
    "iqr": ("cost_reconciliation", "baseline_shipments"),
    "lane_week_deviation": ("lane_cost_trend", "prior_observed_weeks"),
    "service_deterioration": ("carrier_service_trend", "prior_observed_weeks"),
    "data_quality": ("data_quality", "rules_evaluated"),
}


@dataclass(frozen=True, slots=True)
class WindowEvaluation:
    """Metrics and grain-safe supporting tables for one declared time window."""

    window: str
    overall: dict[str, int | float]
    by_anomaly_type: pd.DataFrame
    method_agreement: pd.DataFrame
    group_evidence: pd.DataFrame


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    """Selected held-out result and the finite sensitivity grid that produced it."""

    overall: dict[str, int | float]
    by_anomaly_type: pd.DataFrame
    method_agreement: pd.DataFrame
    group_evidence: pd.DataFrame
    sensitivity_grid: pd.DataFrame
    selected_config: DetectorConfig
    selection_rule: str
    selected_flags: pd.DataFrame
    selected_lane_week_trends: pd.DataFrame


def evaluation_payload(run_id: str, result: EvaluationResult) -> dict[str, Any]:
    """Convert one evaluation result into finite canonical JSON primitives."""
    if not _valid_key(run_id) or not isinstance(result, EvaluationResult):
        raise ValueError("run_id and EvaluationResult are required")
    by_type = json.loads(result.by_anomaly_type.to_json(orient="records"))
    sensitivity = json.loads(result.sensitivity_grid.to_json(orient="records"))
    return {
        "run_id": run_id,
        "schema_version": SCHEMA_VERSION,
        "row_count": len(sensitivity),
        "selected_config": asdict(result.selected_config),
        "selection_rule": result.selection_rule,
        "overall": dict(result.overall),
        "by_anomaly_type": by_type,
        "sensitivity_grid": sensitivity,
    }


def _valid_key(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _require_columns(frame: pd.DataFrame, required: frozenset[str], label: str) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise ValueError(f"{label} must be a DataFrame")
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"{label} is missing required columns: {sorted(missing)}")


def _run_and_schema(frame: pd.DataFrame, label: str) -> tuple[str, str]:
    if frame.empty:
        raise ValueError(f"{label} must not be empty")
    run_ids = frame["run_id"].drop_duplicates()
    schemas = frame["schema_version"].drop_duplicates()
    if len(run_ids) != 1 or not _valid_key(run_ids.iloc[0]):
        raise ValueError(f"{label} must contain exactly one non-empty run_id")
    if len(schemas) != 1 or schemas.iloc[0] != SCHEMA_VERSION:
        raise ValueError(f"{label} uses an incompatible schema_version")
    return str(run_ids.iloc[0]), str(schemas.iloc[0])


def _validate_scored(scored: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    _require_columns(scored, _SCORED_COLUMNS, "scored shipments")
    run_id, _ = _run_and_schema(scored, "scored shipments")
    frame = scored.copy(deep=True)
    if (
        not frame["shipment_id"].map(_valid_key).all()
        or frame.duplicated(["run_id", "shipment_id"]).any()
    ):
        raise ValueError("scored shipment keys must be unique non-empty values")
    if not frame["time_window"].isin({"baseline", *EVALUATION_WINDOWS}).all():
        raise ValueError("scored shipments contain an invalid time_window")
    for column in ("total_cost", "estimated_excess_cost"):
        if not pd.api.types.is_numeric_dtype(frame[column]):
            raise ValueError(f"scored shipments {column} must be numeric")
    total_cost = frame["total_cost"].to_numpy(dtype=float)
    if not np.isfinite(total_cost).all() or (total_cost < 0).any():
        raise ValueError("scored shipment total_cost must be finite and non-negative")
    excess = frame["estimated_excess_cost"].dropna().to_numpy(dtype=float)
    if not np.isfinite(excess).all() or (excess < 0).any():
        raise ValueError("scored estimated_excess_cost must be finite and non-negative when set")
    if not frame["on_time_flag"].isin({0, 1}).all():
        raise ValueError("scored on_time_flag must contain only 0 or 1")
    return frame, run_id


def _validate_truth(truth: pd.DataFrame, scored: pd.DataFrame, run_id: str) -> pd.DataFrame:
    _require_columns(truth, _TRUTH_COLUMNS, "ground truth")
    truth_run_id, _ = _run_and_schema(truth, "ground truth")
    if truth_run_id != run_id:
        raise ValueError("ground truth run_id does not match scored shipments")
    frame = truth.copy(deep=True)
    if (
        not frame["shipment_id"].map(_valid_key).all()
        or frame.duplicated(["run_id", "shipment_id"]).any()
    ):
        raise ValueError("ground truth keys must be unique non-empty values")
    if not frame["is_anomaly"].isin({0, 1}).all():
        raise ValueError("ground truth is_anomaly must contain only 0 or 1")
    valid_types = {"NONE", *ANOMALY_TYPES}
    if not frame["anomaly_type"].isin(valid_types).all():
        raise ValueError("ground truth contains an unsupported anomaly_type")
    consistent = np.where(
        frame["is_anomaly"].eq(1),
        frame["anomaly_type"].ne("NONE"),
        frame["anomaly_type"].eq("NONE"),
    )
    if not consistent.all():
        raise ValueError("ground truth anomaly labels are inconsistent")
    scored_keys = set(zip(scored["run_id"], scored["shipment_id"], strict=True))
    truth_keys = set(zip(frame["run_id"], frame["shipment_id"], strict=True))
    if scored_keys != truth_keys:
        raise ValueError("ground truth and scored shipment key sets must match exactly")
    return frame


def _validate_flags(flags: pd.DataFrame, truth: pd.DataFrame, run_id: str) -> pd.DataFrame:
    _require_columns(flags, _FLAG_COLUMNS, "flags")
    flag_run_id, _ = _run_and_schema(flags, "flags")
    if flag_run_id != run_id:
        raise ValueError("flags run_id does not match scored shipments")
    frame = flags.copy(deep=True)
    for column in ("shipment_id", "method", "method_family", "evidence_unit_id", "support_unit"):
        if not frame[column].map(_valid_key).all():
            raise ValueError(f"flags {column} must contain non-empty strings")
    if frame.duplicated(["run_id", "shipment_id", "method"]).any():
        raise ValueError("flag keys must be unique by run_id, shipment_id, and method")
    if not frame["is_flagged"].isin({0, 1}).all():
        raise ValueError("flags is_flagged must contain only 0 or 1")
    if not pd.api.types.is_numeric_dtype(frame["current_support"]):
        raise ValueError("flags current_support must be numeric")
    support = frame["current_support"].to_numpy(dtype=float)
    if (
        not np.isfinite(support).all()
        or (support < 0).any()
        or not np.equal(support, np.floor(support)).all()
    ):
        raise ValueError("flags current_support must contain finite non-negative whole numbers")
    truth_keys = set(zip(truth["run_id"], truth["shipment_id"], strict=True))
    flag_keys = set(zip(frame["run_id"], frame["shipment_id"], strict=True))
    if not flag_keys.issubset(truth_keys):
        raise ValueError("flagged shipment IDs fall outside ground truth")
    unknown_methods = set(frame["method"]).difference(DETECTION_METHODS)
    if unknown_methods:
        raise ValueError(f"flags contain unknown detector methods: {sorted(unknown_methods)}")
    expected_family = frame["method"].map(
        {method: metadata[0] for method, metadata in _METHOD_CONTRACTS.items()}
    )
    if not frame["method_family"].eq(expected_family).all():
        raise ValueError("flags method_family does not match the normalized detector contract")
    expected_support_unit = frame["method"].map(
        {method: metadata[1] for method, metadata in _METHOD_CONTRACTS.items()}
    )
    if not frame["support_unit"].eq(expected_support_unit).all():
        raise ValueError("flags support_unit does not match the normalized detector contract")
    method_coverage = frame.groupby(["run_id", "shipment_id"], observed=True)["method"].nunique()
    expected_rows = len(truth_keys) * len(DETECTION_METHODS)
    if (
        len(frame) != expected_rows
        or len(method_coverage) != len(truth_keys)
        or not method_coverage.eq(len(DETECTION_METHODS)).all()
    ):
        raise ValueError(
            "flags must contain the exact normalized Cartesian matrix of every shipment "
            "and detector method"
        )
    return frame


def _validated_inputs(
    scored: pd.DataFrame,
    flags: pd.DataFrame,
    truth: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    scored_frame, run_id = _validate_scored(scored)
    truth_frame = _validate_truth(truth, scored_frame, run_id)
    flag_frame = _validate_flags(flags, truth_frame, run_id)
    return scored_frame, flag_frame, truth_frame, run_id


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator / denominator)


def _metric_row(details: pd.DataFrame, positive: pd.Series) -> dict[str, int | float]:
    predicted = details["is_predicted"].astype(bool)
    positive = positive.astype(bool)
    true_positives = int((predicted & positive).sum())
    false_positives = int((predicted & ~positive).sum())
    true_negatives = int((~predicted & ~positive).sum())
    false_negatives = int((~predicted & positive).sum())
    precision = _safe_ratio(true_positives, true_positives + false_positives)
    recall = _safe_ratio(true_positives, true_positives + false_negatives)
    f1 = _safe_ratio(2 * precision * recall, precision + recall)
    excess = details["estimated_excess_cost"].fillna(0.0).clip(lower=0.0)
    available_excess = float(excess[positive].sum())
    captured_excess = float(excess[positive & predicted].sum())
    shipment_count = int(len(details))
    review_volume = int(predicted.sum())
    return {
        "shipment_count": shipment_count,
        "positive_count": int(positive.sum()),
        "negative_count": int((~positive).sum()),
        "true_positives": true_positives,
        "false_positives": false_positives,
        "true_negatives": true_negatives,
        "false_negatives": false_negatives,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": _safe_ratio(false_positives, false_positives + true_negatives),
        "review_volume": review_volume,
        "review_rate": _safe_ratio(review_volume, shipment_count),
        "excess_cost_coverage": _safe_ratio(captured_excess, available_excess),
    }


def _agreement(scored: pd.DataFrame, flags: pd.DataFrame, window: str) -> pd.DataFrame:
    scoped = scored.loc[scored["time_window"].eq(window), ["run_id", "shipment_id"]]
    flagged = flags.loc[flags["is_flagged"].eq(1)]
    counts = (
        flagged.groupby(["run_id", "shipment_id"], sort=True)
        .agg(
            flagged_method_count=("method", "nunique"),
            method_family_agreement=("method_family", "nunique"),
        )
        .reset_index()
    )
    agreement = scoped.merge(
        counts,
        on=["run_id", "shipment_id"],
        how="left",
        validate="one_to_one",
        sort=False,
    ).fillna({"flagged_method_count": 0, "method_family_agreement": 0})
    for column in ("flagged_method_count", "method_family_agreement"):
        agreement[column] = agreement[column].astype(int)
    return agreement.sort_values("shipment_id", kind="stable", ignore_index=True)


def summarize_group_evidence(
    scored: pd.DataFrame,
    flags: pd.DataFrame,
    *,
    window: str,
) -> pd.DataFrame:
    """Return one row per group evidence unit without repeated shipment measures."""
    if window not in EVALUATION_WINDOWS:
        raise ValueError(f"window must be one of {sorted(EVALUATION_WINDOWS)}")
    scored_frame, run_id = _validate_scored(scored)
    _require_columns(flags, _FLAG_COLUMNS, "flags")
    flag_run_id, _ = _run_and_schema(flags, "flags")
    if flag_run_id != run_id:
        raise ValueError("flags run_id does not match scored shipments")
    placeholder_truth = scored_frame[["run_id", "schema_version", "shipment_id"]].assign(
        is_anomaly=0,
        anomaly_type="NONE",
    )
    flag_frame = _validate_flags(flags, placeholder_truth, run_id)
    scoped = scored_frame.loc[scored_frame["time_window"].eq(window)]
    scoped_ids = set(scoped["shipment_id"])
    group_flags = flag_frame.loc[
        flag_frame["is_flagged"].eq(1)
        & flag_frame["support_unit"].eq("prior_observed_weeks")
        & flag_frame["shipment_id"].isin(scoped_ids)
    ]
    columns = [
        "run_id",
        "schema_version",
        "evidence_unit_id",
        "method",
        "method_family",
        "affected_shipment_count",
        "current_support",
        "observed_spend",
        "on_time_shipment_count",
        "estimated_excess_cost",
    ]
    if group_flags.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, Any]] = []
    for evidence_unit_id, evidence_flags in group_flags.groupby(
        "evidence_unit_id", sort=True, observed=True
    ):
        if (
            evidence_flags["method"].nunique() != 1
            or evidence_flags["method_family"].nunique() != 1
            or evidence_flags["current_support"].nunique() != 1
        ):
            raise ValueError("group evidence metadata must be consistent within evidence_unit_id")
        shipment_ids = evidence_flags["shipment_id"].drop_duplicates()
        shipments = pd.DataFrame({"shipment_id": shipment_ids}).merge(
            scoped,
            on="shipment_id",
            how="left",
            validate="one_to_one",
        )
        rows.append(
            {
                "run_id": run_id,
                "schema_version": SCHEMA_VERSION,
                "evidence_unit_id": evidence_unit_id,
                "method": evidence_flags["method"].iloc[0],
                "method_family": evidence_flags["method_family"].iloc[0],
                "affected_shipment_count": int(shipments["shipment_id"].nunique()),
                "current_support": int(evidence_flags["current_support"].iloc[0]),
                "observed_spend": float(shipments["total_cost"].sum()),
                "on_time_shipment_count": int(shipments["on_time_flag"].sum()),
                "estimated_excess_cost": float(
                    shipments["estimated_excess_cost"].fillna(0.0).sum()
                ),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def evaluate_flags(
    scored: pd.DataFrame,
    flags: pd.DataFrame,
    truth: pd.DataFrame,
    *,
    window: str = "evaluation",
) -> WindowEvaluation:
    """Evaluate unioned shipment flags in calibration or the held-out evaluation window."""
    if window not in EVALUATION_WINDOWS:
        raise ValueError(f"window must be one of {sorted(EVALUATION_WINDOWS)}")
    scored_frame, flag_frame, truth_frame, _ = _validated_inputs(scored, flags, truth)
    scoped = scored_frame.loc[scored_frame["time_window"].eq(window)].copy()
    if scoped.empty:
        raise ValueError(f"scored shipments contain no rows for {window}")
    scoped = scoped.merge(
        truth_frame[["run_id", "shipment_id", "is_anomaly", "anomaly_type"]],
        on=["run_id", "shipment_id"],
        how="left",
        validate="one_to_one",
    )
    predicted_ids = set(flag_frame.loc[flag_frame["is_flagged"].eq(1), "shipment_id"])
    scoped["is_predicted"] = scoped["shipment_id"].isin(predicted_ids)
    overall = _metric_row(scoped, scoped["is_anomaly"].eq(1))
    type_rows = []
    for anomaly_type in ANOMALY_TYPES:
        metrics = _metric_row(scoped, scoped["anomaly_type"].eq(anomaly_type))
        type_rows.append({"anomaly_type": anomaly_type, **metrics})
    return WindowEvaluation(
        window=window,
        overall=overall,
        by_anomaly_type=pd.DataFrame(type_rows),
        method_agreement=_agreement(scored_frame, flag_frame, window),
        group_evidence=summarize_group_evidence(scored_frame, flag_frame, window=window),
    )


def _config_id(config: DetectorConfig) -> str:
    payload = json.dumps(asdict(config), sort_keys=True, separators=(",", ":"))
    return f"CFG-{hashlib.sha256(payload.encode()).hexdigest()[:12].upper()}"


def _validate_grid(configs: tuple[DetectorConfig, ...]) -> tuple[DetectorConfig, ...]:
    if not isinstance(configs, tuple) or not configs:
        raise ValueError("configs must be a non-empty tuple")
    if len(configs) > MAX_GRID_SIZE:
        raise ValueError(f"sensitivity grid must contain at most {MAX_GRID_SIZE} configurations")
    if not all(isinstance(config, DetectorConfig) for config in configs):
        raise ValueError("every sensitivity configuration must be a DetectorConfig")
    config_ids = [_config_id(config) for config in configs]
    if len(config_ids) != len(set(config_ids)):
        raise ValueError("sensitivity configurations must be unique")
    bounds = {
        "robust_threshold": (2.0, 6.0),
        "iqr_multiplier": (1.0, 3.0),
        "lane_week_threshold": (2.0, 5.0),
        "service_drop_threshold": (0.05, 0.30),
        "trailing_observed_weeks": (4, 16),
        "minimum_trailing_weeks": (2, 12),
        "service_current_observed_weeks": (2, 8),
        "minimum_service_current_shipments": (2, 100),
        "trend_scale_floor": (0.001, 1.0),
        "maximum_trusted_weight_lbs": (45_000.0, 2_000_000.0),
    }
    for config in configs:
        for field, (lower, upper) in bounds.items():
            value = getattr(config, field)
            if not isinstance(value, (int, float)) or not isfinite(float(value)):
                raise ValueError("sensitivity grid values must be finite and bounded")
            if value < lower or value > upper:
                raise ValueError(f"sensitivity grid {field} must be bounded in [{lower}, {upper}]")
    return configs


def _selection_score(metrics: dict[str, int | float]) -> float:
    return float(
        0.60 * metrics["excess_cost_coverage"]
        + 0.25 * metrics["recall"]
        - 0.10 * metrics["false_positive_rate"]
        - 0.05 * metrics["review_rate"]
    )


def build_evaluation(
    baseline_result: BaselineResult,
    ground_truth: pd.DataFrame,
    *,
    configs: tuple[DetectorConfig, ...] = DEFAULT_SENSITIVITY_GRID,
) -> EvaluationResult:
    """Select on calibration truth, then report held-out and finite-grid evaluation."""
    if not isinstance(baseline_result, BaselineResult):
        raise ValueError("baseline_result must be a BaselineResult")
    selected_grid = _validate_grid(configs)
    detections: dict[str, DetectionResult] = {}
    calibration_rows: list[dict[str, Any]] = []
    for config in selected_grid:
        config_id = _config_id(config)
        detection = detect_exceptions(baseline_result, config)
        detections[config_id] = detection
        calibration = evaluate_flags(
            baseline_result.scored,
            detection.flags,
            ground_truth,
            window="calibration",
        )
        calibration_rows.append(
            {
                "config_id": config_id,
                "config": config,
                "selection_score": _selection_score(calibration.overall),
                **calibration.overall,
            }
        )

    ranked = sorted(
        calibration_rows,
        key=lambda row: (
            -row["selection_score"],
            -row["excess_cost_coverage"],
            -row["recall"],
            row["false_positive_rate"],
            row["review_volume"],
            row["config_id"],
        ),
    )
    selected_id = str(ranked[0]["config_id"])
    selected_config = ranked[0]["config"]

    calibration_by_id = {str(row["config_id"]): row for row in calibration_rows}
    sensitivity_rows: list[dict[str, Any]] = []
    evaluation_by_id: dict[str, WindowEvaluation] = {}
    for config in selected_grid:
        config_id = _config_id(config)
        held_out = evaluate_flags(
            baseline_result.scored,
            detections[config_id].flags,
            ground_truth,
            window="evaluation",
        )
        evaluation_by_id[config_id] = held_out
        calibration = calibration_by_id[config_id]
        sensitivity_rows.append(
            {
                "config_id": config_id,
                **asdict(config),
                "calibration_selection_score": float(calibration["selection_score"]),
                "calibration_recall": float(calibration["recall"]),
                "calibration_false_positive_rate": float(calibration["false_positive_rate"]),
                "calibration_review_volume": int(calibration["review_volume"]),
                "calibration_review_rate": float(calibration["review_rate"]),
                "calibration_excess_cost_coverage": float(calibration["excess_cost_coverage"]),
                "evaluation_precision": float(held_out.overall["precision"]),
                "evaluation_recall": float(held_out.overall["recall"]),
                "evaluation_f1": float(held_out.overall["f1"]),
                "evaluation_false_positive_rate": float(held_out.overall["false_positive_rate"]),
                "evaluation_review_volume": int(held_out.overall["review_volume"]),
                "evaluation_review_rate": float(held_out.overall["review_rate"]),
                "evaluation_excess_cost_coverage": float(held_out.overall["excess_cost_coverage"]),
                "selected": config_id == selected_id,
            }
        )
    sensitivity = pd.DataFrame(sensitivity_rows).sort_values(
        "config_id", kind="stable", ignore_index=True
    )
    selected = evaluation_by_id[selected_id]
    return EvaluationResult(
        overall=selected.overall,
        by_anomaly_type=selected.by_anomaly_type,
        method_agreement=selected.method_agreement,
        group_evidence=selected.group_evidence,
        sensitivity_grid=sensitivity,
        selected_config=selected_config,
        selection_rule=SELECTION_RULE,
        selected_flags=detections[selected_id].flags,
        selected_lane_week_trends=detections[selected_id].lane_week_trends,
    )
