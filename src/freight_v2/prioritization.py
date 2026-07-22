"""Transparent operational-alert aggregation and fixed-weight prioritization.

Priority is a 0-100 weighted sum of six bounded components. Confidence is a
0-1 weighted sum of evidence agreement, persistence, data quality, and support.
The public weights below are deliberately fixed so an alert can be reconstructed
without hidden model state.
"""

from __future__ import annotations

import hashlib
from types import MappingProxyType

import numpy as np
import pandas as pd

from freight_v2.anomalies import TRUTH_FIELDS
from freight_v2.config import SCHEMA_VERSION
from freight_v2.contracts import validate_columns

PRIORITY_COMPONENT_WEIGHTS = MappingProxyType(
    {
        "exposure": 0.35,
        "persistence": 0.15,
        "service_impact": 0.15,
        "method_agreement": 0.15,
        "data_quality": 0.10,
        "support": 0.10,
    }
)
CONFIDENCE_COMPONENT_WEIGHTS = MappingProxyType(
    {
        "method_agreement": 0.40,
        "persistence": 0.20,
        "data_quality": 0.20,
        "support": 0.20,
    }
)

EXPOSURE_REFERENCE_DOLLARS = 10_000.0
PERSISTENCE_REFERENCE_WEEKS = 4
METHOD_FAMILY_REFERENCE_COUNT = 4
SUPPORT_REFERENCE_COUNT = 20.0

SHIPMENT_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "shipment_id",
        "ship_date",
        "lane_id",
        "carrier_id",
        "mode",
        "on_time_flag",
        "transit_days",
    }
)
FLAG_COLUMNS = frozenset(
    {
        "run_id",
        "schema_version",
        "flag_id",
        "shipment_id",
        "method",
        "method_family",
        "score",
        "threshold",
        "support",
        "reason",
        "is_flagged",
        "is_evaluable",
        "evaluated_at",
        "lane_id",
        "carrier_id",
        "mode",
        "week_start",
        "evidence_unit_id",
        "estimated_excess_cost",
    }
)
GROUP_METHOD_FAMILIES = frozenset({"lane_cost_trend", "carrier_service_trend"})

OUTPUT_COLUMNS = (
    "run_id",
    "schema_version",
    "alert_id",
    "lane_id",
    "mode",
    "carrier_scope",
    "window_start",
    "window_end",
    "primary_signal",
    "reason",
    "severity",
    "affected_shipment_count",
    "affected_service_shipment_count",
    "data_quality_shipment_count",
    "estimated_excess_cost",
    "method_family_count",
    "evidence_unit_count",
    "observed_week_count",
    "evidence_support",
    "exposure_component",
    "persistence_component",
    "service_impact_component",
    "method_agreement_component",
    "data_quality_component",
    "support_component",
    "confidence_score",
    "priority_score",
)


def _require_columns(name: str, frame: pd.DataFrame, required: frozenset[str]) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise ValueError(f"{name} must be a pandas DataFrame")
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"{name} is missing required columns: {sorted(missing)}")


def _valid_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_strings(name: str, frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    for column in columns:
        if not frame[column].map(_valid_string).all():
            raise ValueError(f"{name}.{column} must contain non-empty strings")


def _validate_dates(name: str, frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    for column in columns:
        values = frame[column]
        if not pd.api.types.is_datetime64_any_dtype(values) or values.isna().any():
            raise ValueError(f"{name}.{column} must use a non-null datetime dtype")
        if values.dt.tz is not None or not values.dt.normalize().equals(values):
            raise ValueError(f"{name}.{column} must contain normalized timezone-naive dates")


def _single_run_id(name: str, frame: pd.DataFrame, *, allow_empty: bool = False) -> str | None:
    if frame.empty and allow_empty:
        return None
    run_ids = frame["run_id"].drop_duplicates()
    if len(run_ids) != 1 or not _valid_string(run_ids.iloc[0]):
        raise ValueError(f"{name} must contain exactly one non-empty run_id")
    return str(run_ids.iloc[0])


def _validate_inputs(
    shipments: pd.DataFrame, flags: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    _require_columns("shipments", shipments, SHIPMENT_COLUMNS)
    _require_columns("flags", flags, FLAG_COLUMNS)
    leaked = TRUTH_FIELDS.intersection(shipments.columns.union(flags.columns))
    if leaked:
        raise ValueError(f"prioritization inputs contain forbidden truth fields: {sorted(leaked)}")
    if shipments.empty:
        raise ValueError("shipments must be non-empty")

    shipment_rows = shipments.copy(deep=True)
    flag_rows = flags.copy(deep=True)
    run_id = _single_run_id("shipments", shipment_rows)
    flag_run_id = _single_run_id("flags", flag_rows, allow_empty=True)
    if flag_run_id is not None and flag_run_id != run_id:
        raise ValueError("flags run_id does not match shipments run_id")
    if (
        not shipment_rows["schema_version"].eq(SCHEMA_VERSION).all()
        or not flag_rows["schema_version"].eq(SCHEMA_VERSION).all()
    ):
        raise ValueError("shipments or flags use an incompatible schema_version")

    _validate_strings("shipments", shipment_rows, ("shipment_id", "lane_id", "carrier_id", "mode"))
    _validate_dates("shipments", shipment_rows, ("ship_date",))
    if shipment_rows["shipment_id"].duplicated().any():
        raise ValueError("shipments contain duplicate shipment_id values")
    if flag_rows.empty:
        return shipment_rows, flag_rows, str(run_id)

    _validate_strings(
        "flags",
        flag_rows,
        (
            "flag_id",
            "shipment_id",
            "method",
            "method_family",
            "reason",
            "lane_id",
            "carrier_id",
            "mode",
            "evidence_unit_id",
        ),
    )
    _validate_dates("flags", flag_rows, ("evaluated_at", "week_start"))
    if flag_rows["flag_id"].duplicated().any():
        raise ValueError("flags contain duplicate flag_id values")
    if flag_rows.duplicated(["shipment_id", "method"]).any():
        raise ValueError("flags contain duplicate shipment_id/method keys")
    if not pd.api.types.is_bool_dtype(flag_rows["is_evaluable"]):
        raise ValueError("flags.is_evaluable must use a boolean dtype")
    if not flag_rows["is_flagged"].isin([0, 1]).all():
        raise ValueError("flags.is_flagged must contain only 0 or 1")

    support = pd.to_numeric(flag_rows["support"], errors="coerce").to_numpy(dtype=float)
    if (
        not np.isfinite(support).all()
        or (support < 0).any()
        or not np.equal(support, np.floor(support)).all()
    ):
        raise ValueError("flags.support must contain non-negative whole numbers")
    flag_rows["support"] = support.astype(int)
    thresholds = pd.to_numeric(flag_rows["threshold"], errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(thresholds).all():
        raise ValueError("flags.threshold must contain finite values")
    evaluable = flag_rows["is_evaluable"].to_numpy(dtype=bool)
    scores = pd.to_numeric(flag_rows["score"], errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(scores[evaluable]).all():
        raise ValueError("evaluable flags.score values must be finite")

    exposure = pd.to_numeric(flag_rows["estimated_excess_cost"], errors="coerce")
    present_exposure = exposure.notna()
    if (
        not np.isfinite(exposure.loc[present_exposure].to_numpy(dtype=float)).all()
        or exposure.loc[present_exposure].lt(0).any()
    ):
        raise ValueError("flags.estimated_excess_cost must be null or finite and non-negative")
    flag_rows["estimated_excess_cost"] = exposure.astype(float)

    shipment_ids = set(shipment_rows["shipment_id"])
    if not set(flag_rows["shipment_id"]).issubset(shipment_ids):
        raise ValueError("one or more flags reference shipments outside the supplied run")
    authoritative = shipment_rows.set_index("shipment_id")[["lane_id", "carrier_id", "mode"]]
    joined = flag_rows[["shipment_id", "lane_id", "carrier_id", "mode"]].join(
        authoritative,
        on="shipment_id",
        rsuffix="_shipment",
        validate="many_to_one",
    )
    for column in ("lane_id", "carrier_id", "mode"):
        if not joined[column].eq(joined[f"{column}_shipment"]).all():
            raise ValueError(f"flags.{column} does not match its authoritative shipment")
    return shipment_rows, flag_rows, str(run_id)


def _empty_alerts() -> pd.DataFrame:
    string_columns = {
        "run_id",
        "schema_version",
        "alert_id",
        "lane_id",
        "mode",
        "carrier_scope",
        "primary_signal",
        "reason",
        "severity",
    }
    date_columns = {"window_start", "window_end"}
    integer_columns = {
        "affected_shipment_count",
        "affected_service_shipment_count",
        "data_quality_shipment_count",
        "method_family_count",
        "evidence_unit_count",
        "observed_week_count",
        "evidence_support",
    }
    return pd.DataFrame(
        {
            column: pd.Series(
                dtype=(
                    "string"
                    if column in string_columns
                    else "datetime64[ns]"
                    if column in date_columns
                    else "int64"
                    if column in integer_columns
                    else "float64"
                )
            )
            for column in OUTPUT_COLUMNS
        }
    )


def _alert_id(
    run_id: str,
    lane_id: str,
    mode: str,
    carrier_scope: str,
    window_start: pd.Timestamp,
) -> str:
    payload = f"{run_id}|{lane_id}|{mode}|{carrier_scope}|{window_start.date()}".encode()
    return f"ALERT-{hashlib.sha256(payload).hexdigest()[:20].upper()}"


def _component(value: float) -> float:
    return float(np.clip(value, 0.0, 1.0))


def _severity(priority_score: float) -> str:
    if priority_score >= 75:
        return "critical"
    if priority_score >= 50:
        return "high"
    if priority_score >= 25:
        return "medium"
    return "low"


def _signal_label(method_family: str) -> str:
    labels = {
        "cost_reconciliation": "Cost reconciliation",
        "lane_cost_trend": "Persistent lane cost",
        "carrier_service_trend": "Carrier service deterioration",
        "data_quality": "Data quality",
    }
    return labels.get(method_family, method_family.replace("_", " ").title())


def _priority_reason(
    primary_signal: str,
    affected_count: int,
    observed_weeks: int,
    exposure: float,
    family_count: int,
    service_count: int,
    data_quality_count: int,
) -> str:
    reason = (
        f"{primary_signal}: {affected_count} distinct shipment(s) across "
        f"{observed_weeks} observed week(s), ${exposure:,.2f} estimated excess, "
        f"and {family_count} distinct method family/families"
    )
    if service_count:
        reason += f"; {service_count} service-affected shipment(s)"
    if data_quality_count:
        reason += f"; {data_quality_count} shipment(s) require data-quality review"
    return reason + "."


def prioritize_operational_alerts(shipments: pd.DataFrame, flags: pd.DataFrame) -> pd.DataFrame:
    """Aggregate flagged, evaluable evidence into deterministic monthly alert units."""
    shipment_rows, flag_rows, run_id = _validate_inputs(shipments, flags)
    relevant = flag_rows.loc[flag_rows["is_flagged"].eq(1) & flag_rows["is_evaluable"]].copy()
    if relevant.empty:
        empty = _empty_alerts()
        validate_columns("operational_alerts", empty.columns)
        return empty

    ship_dates = shipment_rows.set_index("shipment_id")["ship_date"]
    relevant["ship_date"] = relevant["shipment_id"].map(ship_dates)
    relevant["window_start"] = relevant["week_start"].dt.to_period("M").dt.to_timestamp()
    relevant["window_end"] = relevant["window_start"] + pd.offsets.MonthEnd(0)
    relevant["alert_mode"] = np.where(
        relevant["method_family"].eq("carrier_service_trend"), "ALL", relevant["mode"]
    )
    relevant["carrier_scope"] = np.where(
        relevant["method_family"].eq("lane_cost_trend"), "ALL", relevant["carrier_id"]
    )
    relevant["semantic_evidence_key"] = np.where(
        relevant["method_family"].isin(GROUP_METHOD_FAMILIES),
        relevant["evidence_unit_id"],
        relevant["shipment_id"] + "|" + relevant["method_family"],
    )

    alerts: list[dict[str, object]] = []
    group_columns = ["lane_id", "alert_mode", "carrier_scope", "window_start", "window_end"]
    for key, group in relevant.groupby(group_columns, observed=True, sort=True):
        lane_id, mode, carrier_scope, window_start, window_end = key
        affected_count = int(group["shipment_id"].nunique())
        service_ids = group.loc[
            group["method_family"].eq("carrier_service_trend"), "shipment_id"
        ].unique()
        data_quality_ids = group.loc[
            group["method_family"].eq("data_quality"), "shipment_id"
        ].unique()
        service_count = int(len(service_ids))
        data_quality_count = int(len(data_quality_ids))
        family_count = int(group["method_family"].nunique())
        evidence_unit_count = int(group["semantic_evidence_key"].nunique())
        observed_weeks = int(group["week_start"].nunique())

        shipment_exposure = group.groupby("shipment_id", sort=True)["estimated_excess_cost"].max()
        exposure = float(shipment_exposure.fillna(0.0).sum())
        semantic_support = group.groupby("semantic_evidence_key", sort=True)["support"].max()
        evidence_support = int(round(float(semantic_support.median())))

        exposure_component = _component(exposure / (exposure + EXPOSURE_REFERENCE_DOLLARS))
        persistence_component = _component(observed_weeks / PERSISTENCE_REFERENCE_WEEKS)
        service_component = _component(service_count / affected_count)
        agreement_component = _component(family_count / METHOD_FAMILY_REFERENCE_COUNT)
        data_quality_component = _component(1.0 - data_quality_count / affected_count)
        support_component = _component(
            evidence_support / (evidence_support + SUPPORT_REFERENCE_COUNT)
        )
        components = {
            "exposure": exposure_component,
            "persistence": persistence_component,
            "service_impact": service_component,
            "method_agreement": agreement_component,
            "data_quality": data_quality_component,
            "support": support_component,
        }
        priority_score = round(
            100.0
            * sum(components[name] * weight for name, weight in PRIORITY_COMPONENT_WEIGHTS.items()),
            6,
        )
        confidence_score = round(
            sum(components[name] * weight for name, weight in CONFIDENCE_COMPONENT_WEIGHTS.items()),
            6,
        )

        family_evidence_counts = (
            group.groupby("method_family", sort=True)["semantic_evidence_key"]
            .nunique()
            .sort_values(ascending=False, kind="stable")
        )
        primary_family = sorted(
            family_evidence_counts.index,
            key=lambda family: (-int(family_evidence_counts[family]), str(family)),
        )[0]
        primary_signal = _signal_label(str(primary_family))
        alerts.append(
            {
                "run_id": run_id,
                "schema_version": SCHEMA_VERSION,
                "alert_id": _alert_id(
                    run_id, str(lane_id), str(mode), str(carrier_scope), window_start
                ),
                "lane_id": str(lane_id),
                "mode": str(mode),
                "carrier_scope": str(carrier_scope),
                "window_start": pd.Timestamp(window_start),
                "window_end": pd.Timestamp(window_end),
                "primary_signal": primary_signal,
                "reason": _priority_reason(
                    primary_signal,
                    affected_count,
                    observed_weeks,
                    exposure,
                    family_count,
                    service_count,
                    data_quality_count,
                ),
                "severity": _severity(priority_score),
                "affected_shipment_count": affected_count,
                "affected_service_shipment_count": service_count,
                "data_quality_shipment_count": data_quality_count,
                "estimated_excess_cost": round(exposure, 2),
                "method_family_count": family_count,
                "evidence_unit_count": evidence_unit_count,
                "observed_week_count": observed_weeks,
                "evidence_support": evidence_support,
                "exposure_component": round(exposure_component, 6),
                "persistence_component": round(persistence_component, 6),
                "service_impact_component": round(service_component, 6),
                "method_agreement_component": round(agreement_component, 6),
                "data_quality_component": round(data_quality_component, 6),
                "support_component": round(support_component, 6),
                "confidence_score": confidence_score,
                "priority_score": priority_score,
            }
        )

    result = pd.DataFrame(alerts, columns=OUTPUT_COLUMNS)
    result = result.sort_values(
        ["priority_score", "lane_id", "mode", "carrier_scope", "window_start", "alert_id"],
        ascending=[False, True, True, True, True, True],
        kind="stable",
        ignore_index=True,
    )
    if result["alert_id"].duplicated().any():
        raise RuntimeError("operational alert keys are not unique")
    numeric = result[
        [
            "estimated_excess_cost",
            "confidence_score",
            "priority_score",
            "exposure_component",
            "persistence_component",
            "service_impact_component",
            "method_agreement_component",
            "data_quality_component",
            "support_component",
        ]
    ].to_numpy(dtype=float)
    if not np.isfinite(numeric).all():
        raise RuntimeError("operational alert scores must be finite")
    if (
        not result["priority_score"].between(0.0, 100.0).all()
        or not result["confidence_score"].between(0.0, 1.0).all()
    ):
        raise RuntimeError("operational alert priority or confidence is outside its bounds")
    validate_columns("operational_alerts", result.columns)
    return result
