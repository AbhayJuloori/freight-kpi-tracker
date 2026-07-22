"""Atomic, bounded portfolio evidence export from one immutable Freight run."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from freight_v2.config import SCHEMA_VERSION
from freight_v2.generation import EVALUATION_START
from freight_v2.provenance import RunManifest, sha256_file
from freight_v2.run_builder import REQUIRED_ARTIFACTS, _validate_canonical_run

PUBLIC_ROOT_FILES = frozenset({"manifest.json", "network.json", "alerts.json", "evaluation.json"})
MAX_PUBLIC_ALERTS = 50
MAX_LANE_SHIPMENTS = 100
MAX_LANE_FLAGS = 200
FORBIDDEN_LANE_FIELDS = frozenset(
    {
        "is_anomaly",
        "anomaly_type",
        "changed_fields",
        "injected_magnitude",
        "anomaly_group_id",
        "injected_cause",
        "ground_truth",
    }
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON constant: {value}")

    payload = json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant)
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return payload


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _artifact_frame(run_path: Path, manifest: RunManifest, name: str) -> pd.DataFrame:
    record = manifest.artifacts[name]
    return pq.read_table(run_path / record.filename).to_pandas()


def _identity(run_id: str) -> dict[str, str]:
    return {"run_id": run_id, "schema_version": SCHEMA_VERSION}


def _network_payload(run_id: str, shipments: pd.DataFrame, flags: pd.DataFrame) -> dict[str, Any]:
    evaluation = shipments.loc[shipments["ship_date"].ge(EVALUATION_START)].copy()
    flagged_ids = set(flags.loc[flags["is_flagged"].eq(1), "shipment_id"])
    evaluation["is_flagged"] = evaluation["shipment_id"].isin(flagged_ids).astype(int)
    lanes = (
        evaluation.groupby(["lane_id", "mode"], observed=True, sort=True)
        .agg(
            shipment_count=("shipment_id", "nunique"),
            total_spend=("total_cost", "sum"),
            on_time_rate=("on_time_flag", "mean"),
            flagged_shipment_count=("is_flagged", "sum"),
        )
        .reset_index()
        .sort_values(
            ["flagged_shipment_count", "total_spend", "lane_id", "mode"],
            ascending=[False, False, True, True],
            kind="stable",
        )
    )
    for column in ("total_spend", "on_time_rate"):
        lanes[column] = lanes[column].astype(float).round(6)
    return {**_identity(run_id), "row_count": len(lanes), "lanes": _records(lanes)}


def _public_alerts(alerts: pd.DataFrame) -> pd.DataFrame:
    return alerts.sort_values(
        ["priority_score", "confidence_score", "estimated_excess_cost", "alert_id"],
        ascending=[False, False, False, True],
        kind="stable",
        ignore_index=True,
    ).head(MAX_PUBLIC_ALERTS)


def _representatives(alerts: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    if alerts.empty:
        return []
    available = alerts.drop_duplicates(["lane_id", "mode"], keep="first")
    chosen: list[tuple[str, pd.Series]] = []
    used: set[str] = set()

    high = available.iloc[0]
    chosen.append(("high", high))
    used.add(str(high["alert_id"]))

    remaining = available.loc[~available["alert_id"].astype(str).isin(used)]
    if not remaining.empty:
        median = float(available["priority_score"].median())
        medium = (
            remaining.assign(_distance=(remaining["priority_score"].astype(float) - median).abs())
            .sort_values(["_distance", "alert_id"], kind="stable")
            .iloc[0]
        )
        chosen.append(("medium", medium))
        used.add(str(medium["alert_id"]))

    remaining = available.loc[~available["alert_id"].astype(str).isin(used)]
    if not remaining.empty:
        data_quality = remaining.sort_values(
            ["data_quality_shipment_count", "data_quality_component", "priority_score", "alert_id"],
            ascending=[False, False, False, True],
            kind="stable",
        ).iloc[0]
        chosen.append(("data_quality", data_quality))
    return chosen


def _slug(value: object) -> str:
    return "".join(
        character.lower() if character.isalnum() else "-" for character in str(value)
    ).strip("-")


def _lane_payload(
    run_id: str,
    role: str,
    alert: pd.Series,
    shipments: pd.DataFrame,
    flags: pd.DataFrame,
    alerts: pd.DataFrame,
) -> dict[str, Any]:
    lane_id = str(alert["lane_id"])
    mode = str(alert["mode"])
    lane_shipments = shipments.loc[
        shipments["ship_date"].ge(EVALUATION_START) & shipments["lane_id"].eq(lane_id)
    ].copy()
    if mode != "ALL":
        lane_shipments = lane_shipments.loc[lane_shipments["mode"].eq(mode)]
    lane_flags = flags.loc[
        flags["shipment_id"].isin(lane_shipments["shipment_id"]) & flags["is_flagged"].eq(1)
    ].copy()
    flagged_ids = set(lane_flags["shipment_id"])
    lane_shipments["flagged"] = lane_shipments["shipment_id"].isin(flagged_ids)
    lane_shipments = lane_shipments.sort_values(
        ["flagged", "ship_date", "shipment_id"],
        ascending=[False, False, True],
        kind="stable",
    ).head(MAX_LANE_SHIPMENTS)
    shipment_fields = [
        "shipment_id",
        "ship_date",
        "carrier_id",
        "mode",
        "total_cost",
        "on_time_flag",
        "transit_days",
        "flagged",
    ]
    flag_fields = [
        "shipment_id",
        "method",
        "method_family",
        "score",
        "threshold",
        "reason",
        "evidence_unit_id",
    ]
    lane_alerts = alerts.loc[alerts["lane_id"].eq(lane_id)]
    if mode != "ALL":
        lane_alerts = lane_alerts.loc[lane_alerts["mode"].isin({mode, "ALL"})]
    return {
        **_identity(run_id),
        "representative_role": role,
        "lane_id": lane_id,
        "mode": mode,
        "alert_ids": sorted(lane_alerts["alert_id"].astype(str).unique()),
        "shipment_count": int(len(lane_shipments)),
        "shipments": _records(lane_shipments[shipment_fields]),
        "flag_evidence": _records(
            lane_flags.sort_values(["shipment_id", "method"], kind="stable")[flag_fields].head(
                MAX_LANE_FLAGS
            )
        ),
    }


def _validate_identity(payload: dict[str, Any], path: Path, run_id: str) -> None:
    if payload.get("run_id") != run_id:
        raise ValueError(f"{path.name} contains a mismatched run_id")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"{path.name} contains a mismatched schema_version")


def validate_public_bundle(root: Path) -> dict[str, Any]:
    """Validate exact files, identities, hashes, and public cross-references."""
    root = root.expanduser().resolve()
    if not root.is_dir() or root.is_symlink():
        raise ValueError("public bundle must be a real directory")
    names = {path.name for path in root.iterdir()}
    if names != {"manifest.json", "network.json", "alerts.json", "evaluation.json", "lanes"}:
        raise ValueError("public bundle contains missing or unexpected root entries")
    if not (root / "lanes").is_dir() or (root / "lanes").is_symlink():
        raise ValueError("public lane evidence must be a real directory")
    manifest = _read_json(root / "manifest.json")
    run_id = manifest.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("public manifest requires a non-empty run_id")
    _validate_identity(manifest, root / "manifest.json", run_id)

    expected_paths = set(manifest.get("artifact_hashes", {}))
    actual_paths = {
        str(path.relative_to(root)) for path in root.rglob("*.json") if path.name != "manifest.json"
    }
    if expected_paths != actual_paths:
        raise ValueError("public manifest artifact list does not match bundle files")
    for relative, expected_hash in manifest["artifact_hashes"].items():
        path = root / relative
        if path.is_symlink() or sha256_file(path) != expected_hash:
            raise ValueError(f"public artifact hash mismatch: {relative}")
        _validate_identity(_read_json(path), path, run_id)

    alerts_payload = _read_json(root / "alerts.json")
    alert_ids = {str(alert["alert_id"]) for alert in alerts_payload.get("alerts", [])}
    lane_paths = set()
    for representative in manifest.get("representative_lanes", []):
        lane_path = str(representative.get("path", ""))
        lane_paths.add(lane_path)
        if representative.get("alert_id") not in alert_ids:
            raise ValueError("representative lane references an unknown alert")
        lane_payload = _read_json(root / lane_path)
        if representative["alert_id"] not in lane_payload.get("alert_ids", []):
            raise ValueError("lane evidence does not reference its representative alert")
        serialized_lane = json.dumps(lane_payload).lower()
        if any(f'"{field}"' in serialized_lane for field in FORBIDDEN_LANE_FIELDS):
            raise ValueError("operator lane evidence contains forbidden truth fields")
    actual_lane_paths = {str(path.relative_to(root)) for path in (root / "lanes").glob("*.json")}
    if lane_paths != actual_lane_paths:
        raise ValueError("representative lane list does not exactly match lane files")

    evaluation = _read_json(root / "evaluation.json")
    states = evaluation.get("sensitivity_grid")
    if not isinstance(states, list) or not 0 < len(states) <= 25:
        raise ValueError("evaluation must contain a finite bounded sensitivity grid")
    if sum(state.get("selected") is True for state in states) != 1:
        raise ValueError("evaluation sensitivity grid must contain exactly one selected state")
    return manifest


def _replace_directory(staging: Path, target: Path) -> None:
    if target.is_symlink() or (target.exists() and not target.is_dir()):
        raise ValueError("public export target must be a real directory path")
    if not target.exists():
        os.replace(staging, target)
        return
    backup = Path(tempfile.mkdtemp(prefix=f".{target.name}-backup-", dir=target.parent))
    backup.rmdir()
    os.replace(target, backup)
    try:
        os.replace(staging, target)
    except BaseException:
        os.replace(backup, target)
        raise
    shutil.rmtree(backup)


def export_portfolio_bundle(run_path: Path, output: Path) -> Path:
    """Stage, validate, and replace the exact requested portfolio evidence directory."""
    run_path = run_path.expanduser().resolve()
    manifest = _validate_canonical_run(run_path)
    if not REQUIRED_ARTIFACTS.issubset(manifest.artifacts):
        raise ValueError("run is missing required derived artifacts")
    output = output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_parent = Path(tempfile.mkdtemp(prefix=f".{output.name}-export-", dir=output.parent))
    staging = temporary_parent / output.name
    try:
        staging.mkdir()
        (staging / "lanes").mkdir()
        shipments = _artifact_frame(run_path, manifest, "shipments")
        flags = _artifact_frame(run_path, manifest, "anomaly_flags")
        alerts = _public_alerts(_artifact_frame(run_path, manifest, "operational_alerts"))
        evaluation = _read_json(run_path / manifest.artifacts["evaluation"].filename)

        _write_json(staging / "network.json", _network_payload(manifest.run_id, shipments, flags))
        _write_json(
            staging / "alerts.json",
            {
                **_identity(manifest.run_id),
                "row_count": len(alerts),
                "alerts": _records(alerts),
            },
        )
        _write_json(staging / "evaluation.json", evaluation)

        representative_rows = []
        for role, alert in _representatives(alerts):
            filename = f"{role}-{_slug(alert['lane_id'])}-{_slug(alert['mode'])}.json"
            relative = f"lanes/{filename}"
            lane_payload = _lane_payload(manifest.run_id, role, alert, shipments, flags, alerts)
            _write_json(staging / relative, lane_payload)
            representative_rows.append(
                {"role": role, "path": relative, "alert_id": str(alert["alert_id"])}
            )

        artifact_hashes = {
            str(path.relative_to(staging)): sha256_file(path)
            for path in sorted(staging.rglob("*.json"))
            if path.name != "manifest.json"
        }
        _write_json(
            staging / "manifest.json",
            {
                **_identity(manifest.run_id),
                "source_manifest_sha256": sha256_file(run_path / "manifest.json"),
                "artifact_hashes": artifact_hashes,
                "representative_lanes": representative_rows,
            },
        )
        validate_public_bundle(staging)
        _replace_directory(staging, output)
        return output
    finally:
        shutil.rmtree(temporary_parent, ignore_errors=True)
