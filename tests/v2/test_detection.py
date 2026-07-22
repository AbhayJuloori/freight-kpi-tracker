"""Tests for normalized, leakage-safe Freight v2 detectors."""

from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError, replace

import numpy as np
import pandas as pd
import pytest

from freight_v2.anomalies import inject_anomalies
from freight_v2.baselines import BaselineResult, fit_and_score_baselines
from freight_v2.config import SCHEMA_VERSION, SeedSource, create_run_id
from freight_v2.contracts import validate_columns
from freight_v2.detection import (
    DETECTION_METHODS,
    DetectorConfig,
    detect_exceptions,
)
from freight_v2.generation import EVALUATION_START, MODE_TRANSIT_DAYS, generate_normal_run
from freight_v2.sources import fixture_distribution


@pytest.fixture(scope="module")
def baseline_result() -> BaselineResult:
    run_id = create_run_id(
        SeedSource.TEST,
        random_seed=301,
        fixture_name="detection",
        rows=4_000,
        anomaly_seed=302,
    )
    normal = generate_normal_run(
        fixture_distribution(SeedSource.TEST),
        run_id=run_id,
        random_seed=301,
        n_shipments=4_000,
    )
    injected = inject_anomalies(normal.shipments, random_seed=302)
    return fit_and_score_baselines(
        injected.shipments,
        normal.carrier_rates,
        normal.fuel_surcharges,
    )


def test_detector_config_is_typed_frozen_and_validated() -> None:
    config = DetectorConfig()
    with pytest.raises(FrozenInstanceError):
        config.robust_threshold = 9.0  # type: ignore[misc]
    with pytest.raises(ValueError, match="robust_threshold"):
        replace(config, robust_threshold=0.0)
    with pytest.raises(ValueError, match="trailing_observed_weeks"):
        replace(config, trailing_observed_weeks=1)
    with pytest.raises(ValueError, match="service_current_observed_weeks"):
        replace(config, service_current_observed_weeks=1)
    with pytest.raises(ValueError, match="minimum_service_current_shipments"):
        replace(config, minimum_service_current_shipments=1)


def test_normalized_flags_and_trends_are_complete_and_unique(baseline_result) -> None:
    result = detect_exceptions(baseline_result)

    validate_columns("anomaly_flags", result.flags.columns)
    validate_columns("lane_week_trends", result.lane_week_trends.columns)
    assert set(result.flags["method"]) == set(DETECTION_METHODS)
    assert len(result.flags) == len(baseline_result.scored) * len(DETECTION_METHODS)
    assert not result.flags.duplicated(["run_id", "shipment_id", "method"]).any()
    assert result.flags["flag_id"].is_unique
    assert result.flags["schema_version"].eq(SCHEMA_VERSION).all()
    assert result.flags.loc[result.flags["is_evaluable"], "score"].map(np.isfinite).all()
    assert result.flags.loc[~result.flags["is_evaluable"], "score"].isna().all()
    assert result.flags["threshold"].map(np.isfinite).all()
    assert result.flags["support"].ge(0).all()
    assert result.flags["is_flagged"].isin([0, 1]).all()
    assert result.flags["reason"].str.len().gt(0).all()
    assert not result.flags.columns.intersection(
        {"is_anomaly", "anomaly_type", "changed_fields", "injected_magnitude"}
    ).any()

    agreement = result.flags.groupby("shipment_id")["is_flagged"].sum()
    assert agreement.index.is_unique
    assert agreement.between(0, len(DETECTION_METHODS)).all()
    cost_methods = result.flags[result.flags["method"].isin(["robust_residual", "iqr"])]
    assert cost_methods["method_family"].eq("cost_reconciliation").all()
    assert result.flags["support_unit"].str.len().gt(0).all()
    assert result.flags["evidence_unit_id"].map(lambda value: isinstance(value, str)).all()
    evaluable = result.flags["is_evaluable"]
    expected_flag = (
        evaluable
        & result.flags["score"].notna()
        & result.flags["score"].gt(result.flags["threshold"])
    ).astype(int)
    assert result.flags["is_flagged"].equals(expected_flag)
    assert not (
        result.flags["is_flagged"].eq(1) & result.flags["score"].eq(result.flags["threshold"])
    ).any()
    lane_evidence = result.flags[result.flags["method"].eq("lane_week_deviation")]
    assert (
        lane_evidence.groupby(["lane_id", "mode", "week_start"])["evidence_unit_id"]
        .nunique()
        .eq(1)
        .all()
    )
    service_evidence = result.flags[result.flags["method"].eq("service_deterioration")]
    assert (
        service_evidence.groupby(["lane_id", "carrier_id", "week_start"])["evidence_unit_id"]
        .nunique()
        .eq(1)
        .all()
    )


def test_safe_scale_keeps_zero_mad_and_iqr_scores_finite(baseline_result) -> None:
    scored = baseline_result.scored.copy(deep=True)
    trusted = scored["monetary_values_trusted"]
    scored.loc[trusted, "cost_residual"] = 0.0
    scored.loc[trusted, "estimated_excess_cost"] = 0.0
    scored.loc[trusted, "residual_from_median"] = np.round(
        -scored.loc[trusted, "residual_median"], 6
    )

    result = detect_exceptions(BaselineResult(scored=scored, model=baseline_result.model))
    residual_flags = result.flags["method"].isin(["robust_residual", "iqr"])
    evaluable = residual_flags & result.flags["is_evaluable"]
    assert result.flags.loc[evaluable, "score"].map(np.isfinite).all()


def test_missing_weeks_use_preceding_observed_weeks_only(baseline_result) -> None:
    scored = baseline_result.scored
    target = scored.groupby(["lane_id", "mode"]).size().idxmax()
    in_segment = scored["lane_id"].eq(target[0]) & scored["mode"].eq(target[1])
    segment_weeks = (
        scored.loc[in_segment, "ship_date"]
        .sub(pd.to_timedelta(scored.loc[in_segment, "ship_date"].dt.dayofweek, unit="D"))
        .drop_duplicates()
        .sort_values()
    )
    removed = set(segment_weeks.iloc[1::2])
    week_start = scored["ship_date"] - pd.to_timedelta(scored["ship_date"].dt.dayofweek, unit="D")
    kept = ~(in_segment & week_start.isin(removed))
    reduced = BaselineResult(scored=scored.loc[kept].copy(), model=baseline_result.model)

    trends = detect_exceptions(
        reduced,
        DetectorConfig(trailing_observed_weeks=4, minimum_trailing_weeks=2),
    ).lane_week_trends
    lane = trends[trends["lane_id"].eq(target[0]) & trends["mode"].eq(target[1])]

    assert not lane["week_start"].isin(removed).any()
    assert lane["trailing_support"].tolist()[:6] == [0, 1, 2, 3, 4, 4]


def test_future_mutation_cannot_change_earlier_flags_or_trends(baseline_result) -> None:
    config = DetectorConfig(trailing_observed_weeks=6, minimum_trailing_weeks=3)
    original = detect_exceptions(baseline_result, config)
    scored = baseline_result.scored.copy(deep=True)
    future = scored["ship_date"].ge(EVALUATION_START + pd.Timedelta(days=28))
    scored.loc[future, "cost_residual"] += 100_000
    scored.loc[future, "residual_from_median"] += 100_000
    scored.loc[future, "total_cost"] += 100_000
    scored.loc[future, "estimated_excess_cost"] = np.round(
        scored.loc[future, "cost_residual"].clip(lower=0), 2
    )
    scored.loc[future, "on_time_flag"] = 0
    changed = detect_exceptions(BaselineResult(scored=scored, model=baseline_result.model), config)
    cutoff = EVALUATION_START + pd.Timedelta(days=21)

    left_flags = original.flags[original.flags["evaluated_at"].le(cutoff)].reset_index(drop=True)
    right_flags = changed.flags[changed.flags["evaluated_at"].le(cutoff)].reset_index(drop=True)
    pd.testing.assert_frame_equal(left_flags, right_flags)
    left_trends = original.lane_week_trends[
        original.lane_week_trends["week_start"].le(cutoff)
    ].reset_index(drop=True)
    right_trends = changed.lane_week_trends[
        changed.lane_week_trends["week_start"].le(cutoff)
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(left_trends, right_trends)


def test_isolated_late_shipment_does_not_create_group_service_alert(baseline_result) -> None:
    scored = baseline_result.scored.copy(deep=True)
    target = scored.groupby(["lane_id", "carrier_id"]).size().idxmax()
    in_segment = scored["lane_id"].eq(target[0]) & scored["carrier_id"].eq(target[1])
    scored.loc[in_segment, "on_time_flag"] = 1
    scored.loc[in_segment, "transit_days"] = scored.loc[in_segment, "mode"].map(MODE_TRANSIT_DAYS)
    late_index = scored.loc[in_segment].sort_values(["ship_date", "shipment_id"]).index[-1]
    scored.loc[late_index, "on_time_flag"] = 0
    scored.loc[late_index, "transit_days"] += 1
    result = detect_exceptions(
        BaselineResult(scored=scored, model=baseline_result.model),
        DetectorConfig(
            minimum_trailing_weeks=3,
            service_current_observed_weeks=3,
            minimum_service_current_shipments=6,
            service_drop_threshold=0.10,
        ),
    )
    flags = result.flags[
        result.flags["method"].eq("service_deterioration")
        & result.flags["lane_id"].eq(target[0])
        & result.flags["carrier_id"].eq(target[1])
    ]
    assert not flags.empty
    assert flags["is_flagged"].eq(0).all()


@pytest.mark.parametrize("signal", ["combined", "on_time", "transit"])
def test_sustained_service_deterioration_flags_only_affected_shipments(
    baseline_result, signal: str
) -> None:
    scored = baseline_result.scored.copy(deep=True)
    scored["_week"] = scored["ship_date"] - pd.to_timedelta(
        scored["ship_date"].dt.dayofweek, unit="D"
    )
    weekly_counts = (
        scored.groupby(["lane_id", "carrier_id", "_week"], sort=True)
        .size()
        .rename("count")
        .reset_index()
    )
    candidates = []
    for key, group in weekly_counts.groupby(["lane_id", "carrier_id"], sort=True):
        ordered = group.sort_values("_week").reset_index(drop=True)
        for end in range(2, len(ordered)):
            window = ordered.iloc[end - 2 : end + 1]
            candidates.append((int(window["count"].sum()), key, tuple(window["_week"])))
    _, target, current_weeks = max(candidates)
    in_group = scored["lane_id"].eq(target[0]) & scored["carrier_id"].eq(target[1])
    scored.loc[in_group, "on_time_flag"] = 1
    scored.loc[in_group, "transit_days"] = scored.loc[in_group, "mode"].map(MODE_TRANSIT_DAYS)
    if signal == "on_time":
        scored.loc[in_group, "transit_days"] += 1
    affected = in_group & scored["_week"].isin(current_weeks)
    attributed = affected.copy()
    if signal == "on_time":
        attributed[:] = False
        for week in current_weeks:
            week_indices = (
                scored.loc[in_group & scored["_week"].eq(week)].sort_values("shipment_id").index
            )
            late_count = max(1, int(round(len(week_indices) * 0.5)))
            attributed.loc[week_indices[:late_count]] = True
        scored.loc[attributed, "on_time_flag"] = 0
        late_share = float(scored.loc[affected, "on_time_flag"].eq(0).mean())
        assert 0.4 <= late_share <= 0.6
    elif signal == "combined":
        scored.loc[affected, "on_time_flag"] = 0
    if signal in {"combined", "transit"}:
        scored.loc[affected, "transit_days"] += 3
    scored = scored.drop(columns="_week")

    flags = detect_exceptions(
        BaselineResult(scored=scored, model=baseline_result.model),
        DetectorConfig(
            minimum_trailing_weeks=3,
            service_current_observed_weeks=3,
            minimum_service_current_shipments=6,
            service_drop_threshold=0.10,
        ),
    ).flags
    service = flags[
        flags["method"].eq("service_deterioration")
        & flags["lane_id"].eq(target[0])
        & flags["carrier_id"].eq(target[1])
    ]
    affected_ids = set(scored.loc[attributed, "shipment_id"])
    flagged_ids = set(service.loc[service["is_flagged"].eq(1), "shipment_id"])
    assert flagged_ids
    assert flagged_ids <= affected_ids
    flagged_evidence = service.loc[service["is_flagged"].eq(1), "evidence_unit_id"]
    assert flagged_evidence.nunique() <= len(current_weeks)


def test_zero_weight_is_dq_only_and_has_no_monetary_score(baseline_result) -> None:
    scored = baseline_result.scored.copy(deep=True)
    index = scored.index[0]
    scored.loc[index, "weight_lbs"] = 0.0
    scored.loc[index, "monetary_values_trusted"] = False
    for column in (
        "expected_base_cost",
        "expected_fuel_surcharge",
        "expected_total_cost",
        "cost_residual",
        "estimated_excess_cost",
        "residual_from_median",
    ):
        scored.loc[index, column] = np.nan
    flags = detect_exceptions(BaselineResult(scored=scored, model=baseline_result.model)).flags
    flags = flags.loc[flags["shipment_id"].eq(scored.loc[index, "shipment_id"])]
    monetary = flags[flags["method"].isin(["robust_residual", "iqr", "lane_week_deviation"])]
    assert monetary["is_flagged"].eq(0).all()
    assert monetary["score"].isna().all()
    assert monetary["is_evaluable"].eq(False).all()  # noqa: E712
    assert monetary["estimated_excess_cost"].isna().all()
    dq = flags[flags["method"].eq("data_quality")].iloc[0]
    assert dq["is_flagged"] == 1
    assert dq["score"] >= 1
    assert pd.isna(dq["estimated_excess_cost"])


def test_data_quality_checks_mode_class_coherence_and_deduplicates_reasons(
    baseline_result,
) -> None:
    scored = baseline_result.scored.copy(deep=True)
    index = scored.index[scored["mode"].eq("FTL")][0]
    scored.loc[index, "freight_class"] = "70"
    shipment_id = scored.loc[index, "shipment_id"]
    dq = detect_exceptions(BaselineResult(scored=scored, model=baseline_result.model)).flags
    dq = dq.loc[dq["shipment_id"].eq(shipment_id) & dq["method"].eq("data_quality")].iloc[0]
    assert dq["is_flagged"] == 1
    assert dq["reason"] == "freight class is incompatible with shipment mode."
    reasons = dq["reason"].removesuffix(".").split("; ")
    assert reasons == list(dict.fromkeys(reasons))


def test_baseline_model_and_scored_lineage_tampering_is_rejected(baseline_result) -> None:
    mutated_model = copy.deepcopy(baseline_result.model)
    mutated_model.statistics.loc[0, "residual_median"] += 1.0
    with pytest.raises(ValueError, match="fingerprint"):
        detect_exceptions(BaselineResult(scored=baseline_result.scored, model=mutated_model))

    mutations = {
        "baseline_source": lambda frame: frame.__setitem__("baseline_source", "mode"),
        "baseline_support": lambda frame: frame.__setitem__(
            "baseline_support", frame["baseline_support"] + 1
        ),
        "residual_median": lambda frame: frame.__setitem__(
            "residual_median", frame["residual_median"] + 1.0
        ),
        "residual_q3": lambda frame: frame.__setitem__("residual_q3", frame["residual_q3"] + 1.0),
        "residual_iqr": lambda frame: frame.__setitem__(
            "residual_iqr", frame["residual_iqr"] + 1.0
        ),
        "residual_scale": lambda frame: frame.__setitem__(
            "residual_scale", frame["residual_scale"] + 1.0
        ),
        "residual_from_median": lambda frame: frame.__setitem__(
            "residual_from_median", frame["residual_from_median"] + 1.0
        ),
        "estimated_excess_cost": lambda frame: frame.__setitem__(
            "estimated_excess_cost", frame["estimated_excess_cost"] + 1.0
        ),
    }
    for field, mutate in mutations.items():
        scored = baseline_result.scored.copy(deep=True)
        mutate(scored)
        with pytest.raises(ValueError, match=field):
            detect_exceptions(BaselineResult(scored=scored, model=baseline_result.model))


def test_rounded_equal_score_and_threshold_is_not_flagged(baseline_result) -> None:
    scored = baseline_result.scored
    raw_scores = scored["residual_from_median"] / scored["residual_scale"]
    index = raw_scores[raw_scores.gt(1.0)].index[0]
    threshold = float(raw_scores.loc[index] - 0.0000004)
    flags = detect_exceptions(
        baseline_result,
        DetectorConfig(robust_threshold=threshold),
    ).flags
    flag = flags.loc[
        flags["shipment_id"].eq(scored.loc[index, "shipment_id"])
        & flags["method"].eq("robust_residual")
    ].iloc[0]
    assert flag["score"] == flag["threshold"]
    assert flag["is_flagged"] == 0


def test_unavailable_trend_signals_serialize_null_numerics(baseline_result) -> None:
    trends = detect_exceptions(baseline_result).lane_week_trends
    unavailable_money = ~trends["monetary_signal_available"]
    assert (
        trends.loc[unavailable_money, ["trailing_baseline", "trailing_scale", "deviation_score"]]
        .isna()
        .all()
        .all()
    )
    unavailable_service = ~trends["service_signal_available"]
    assert (
        trends.loc[unavailable_service, ["trailing_on_time_rate", "service_drop_score"]]
        .isna()
        .all()
        .all()
    )


def test_custom_index_and_input_order_produce_stable_output(baseline_result) -> None:
    original = detect_exceptions(baseline_result)
    shuffled = baseline_result.scored.sample(frac=1, random_state=9).copy()
    shuffled.index = np.arange(10_000, 10_000 + len(shuffled))
    changed = detect_exceptions(BaselineResult(scored=shuffled, model=baseline_result.model))

    pd.testing.assert_frame_equal(original.flags, changed.flags)
    pd.testing.assert_frame_equal(original.lane_week_trends, changed.lane_week_trends)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda frame: frame.assign(is_anomaly=0), "truth"),
        (lambda frame: frame.assign(run_id="other-run"), "run_id"),
        (lambda frame: frame.assign(schema_version="1.0.0"), "schema"),
        (lambda frame: frame.assign(time_window="evaluation"), "time_window"),
        (
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "duplicate",
        ),
        (
            lambda frame: frame.assign(ship_date=frame["ship_date"] + pd.Timedelta(hours=1)),
            "normalized",
        ),
        (lambda frame: frame.assign(residual_scale=np.inf), "finite"),
    ],
)
def test_bad_truth_lineage_dates_duplicates_and_nonfinite_values_are_rejected(
    baseline_result,
    mutation,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        detect_exceptions(
            BaselineResult(
                scored=mutation(baseline_result.scored.copy()),
                model=baseline_result.model,
            )
        )
