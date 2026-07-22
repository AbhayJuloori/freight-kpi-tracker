"""Build and atomically promote immutable Freight v2 data runs."""

from __future__ import annotations

import ctypes
import errno
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from freight_v2 import __version__
from freight_v2.anomalies import ANOMALY_TYPES, inject_anomalies
from freight_v2.baselines import fit_and_score_baselines
from freight_v2.config import SCHEMA_VERSION, SeedSource, create_run_id
from freight_v2.contracts import validate_arrow_schema
from freight_v2.detection import DETECTION_METHODS
from freight_v2.evaluation import build_evaluation, evaluation_payload
from freight_v2.generation import (
    BASELINE_END,
    BASELINE_START,
    CALIBRATION_END,
    CALIBRATION_START,
    EVALUATION_END,
    EVALUATION_START,
    FUEL_CURVE_BASIS,
    generate_normal_run,
)
from freight_v2.prioritization import prioritize_operational_alerts
from freight_v2.provenance import (
    ProvenanceError,
    RunManifest,
    create_manifest,
    register_artifact,
    sha256_file,
    validate_manifest,
    write_manifest,
)
from freight_v2.sources import MODES, load_distribution

RAW_ARTIFACTS = frozenset({"shipments", "carrier_rates", "fuel_surcharges", "anomaly_ground_truth"})
DERIVED_ARTIFACTS = frozenset(
    {
        "anomaly_flags",
        "lane_week_trends",
        "operational_alerts",
        "evaluation",
        "data_quality",
    }
)
REQUIRED_ARTIFACTS = RAW_ARTIFACTS | DERIVED_ARTIFACTS
PARQUET_ARTIFACTS = REQUIRED_ARTIFACTS.difference({"evaluation", "data_quality"})
RUN_PARAMETER_KEYS = frozenset({"rows", "random_seed", "anomaly_seed", "fixture_name"})


def _window_metadata() -> dict[str, str]:
    return {
        "baseline_start": BASELINE_START.date().isoformat(),
        "baseline_end": BASELINE_END.date().isoformat(),
        "calibration_start": CALIBRATION_START.date().isoformat(),
        "calibration_end": CALIBRATION_END.date().isoformat(),
        "evaluation_start": EVALUATION_START.date().isoformat(),
        "evaluation_end": EVALUATION_END.date().isoformat(),
    }


def _write_parquet_tables(staging: Path, tables: dict[str, object]) -> None:
    for name, frame in tables.items():
        frame.to_parquet(staging / f"{name}.parquet", index=False)


def _write_json_artifact(staging: Path, name: str, payload: dict[str, object]) -> None:
    content = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    (staging / f"{name}.json").write_text(content, encoding="utf-8")


def _validate_staged_tables(staging: Path, table_names: object) -> None:
    for name in table_names:
        validate_arrow_schema(name, pq.read_schema(staging / f"{name}.parquet"))


def _atomic_promote(source: Path, target: Path) -> None:
    """Atomically rename a directory while refusing an existing destination."""
    source_bytes = os.fsencode(source)
    target_bytes = os.fsencode(target)
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        rename = library.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(source_bytes, target_bytes, 0x00000004)  # RENAME_EXCL
    elif sys.platform.startswith("linux") and hasattr(library, "renameat2"):
        rename = library.renameat2
        rename.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        result = rename(-100, source_bytes, -100, target_bytes, 1)  # RENAME_NOREPLACE
    elif os.name == "nt":
        os.rename(source, target)
        return
    else:
        raise RuntimeError("atomic no-replace directory promotion is unsupported on this platform")
    if result != 0:
        error_number = ctypes.get_errno()
        if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
            raise FileExistsError(
                error_number,
                f"Run already exists and will not be overwritten: {target}",
                str(target),
            )
        raise OSError(error_number, os.strerror(error_number), str(target))


def _artifact_frame(
    run_directory: Path,
    manifest: RunManifest,
    artifact_name: str,
    columns: list[str],
) -> pd.DataFrame:
    record = manifest.artifacts[artifact_name]
    return pq.read_table(run_directory / record.filename, columns=columns).to_pandas()


def _contains_only_non_empty_strings(values: pd.Series) -> bool:
    return bool(values.map(lambda value: isinstance(value, str) and bool(value.strip())).all())


def _validate_shipment_truth_ids(run_directory: Path, manifest: RunManifest) -> None:
    shipments = _artifact_frame(
        run_directory,
        manifest,
        "shipments",
        ["shipment_id"],
    )
    truth = _artifact_frame(
        run_directory,
        manifest,
        "anomaly_ground_truth",
        ["shipment_id"],
    )
    shipment_ids = shipments["shipment_id"]
    truth_ids = truth["shipment_id"]
    if not _contains_only_non_empty_strings(shipment_ids) or not _contains_only_non_empty_strings(
        truth_ids
    ):
        raise ProvenanceError("shipments and ground truth require non-empty string shipment IDs")
    if shipment_ids.duplicated().any():
        raise ProvenanceError("shipments contain duplicate shipment IDs")
    if truth_ids.duplicated().any():
        raise ProvenanceError("anomaly ground truth contains duplicate ground-truth shipment IDs")
    if set(shipment_ids) != set(truth_ids):
        raise ProvenanceError("shipment and ground-truth ID sets disagree")


def _validate_rate_calendars(run_directory: Path, manifest: RunManifest) -> None:
    business_key = ["run_id", "carrier_id", "lane_id", "mode"]
    rates = _artifact_frame(
        run_directory,
        manifest,
        "carrier_rates",
        ["rate_id", *business_key, "effective_start", "effective_end"],
    )
    string_keys = ["rate_id", "carrier_id", "lane_id", "mode"]
    if any(not _contains_only_non_empty_strings(rates[column]) for column in string_keys):
        raise ProvenanceError("carrier rates require non-empty string keys")
    if rates["rate_id"].duplicated().any():
        raise ProvenanceError("carrier rates contain duplicate rate IDs")
    for column in ("effective_start", "effective_end"):
        rates[column] = pd.to_datetime(rates[column], errors="coerce")
    if rates[["effective_start", "effective_end"]].isna().any(axis=None):
        raise ProvenanceError("carrier rates contain invalid effective dates")
    if rates.duplicated([*business_key, "effective_start"]).any():
        raise ProvenanceError("carrier rates contain duplicate carrier-rate business keys")

    for key, versions in rates.groupby(business_key, dropna=False, sort=True):
        ordered = versions.sort_values(["effective_start", "effective_end"], kind="stable")
        if ordered["effective_start"].gt(ordered["effective_end"]).any():
            raise ProvenanceError(f"carrier rate {key} contains reversed effective dates")
        if (
            ordered["effective_start"].iloc[0] != BASELINE_START
            or ordered["effective_end"].iloc[-1] != EVALUATION_END
        ):
            raise ProvenanceError(
                f"carrier rate {key} has an effective-date gap at the generation boundary"
            )
        starts = ordered["effective_start"].iloc[1:].reset_index(drop=True)
        expected_starts = ordered["effective_end"].iloc[:-1].reset_index(drop=True) + pd.Timedelta(
            days=1
        )
        if not starts.equals(expected_starts):
            relation = "gap" if starts.gt(expected_starts).any() else "overlap"
            raise ProvenanceError(f"carrier rate {key} contains an effective-date {relation}")


def _validate_fuel_calendar(run_directory: Path, manifest: RunManifest) -> None:
    business_key = ["run_id", "week_start", "mode"]
    fuel = _artifact_frame(
        run_directory,
        manifest,
        "fuel_surcharges",
        [*business_key, "curve_basis"],
    )
    if not _contains_only_non_empty_strings(fuel["mode"]) or not fuel["mode"].isin(MODES).all():
        raise ProvenanceError("fuel surcharges require non-empty supported modes")
    fuel["week_start"] = pd.to_datetime(fuel["week_start"], errors="coerce")
    if fuel["week_start"].isna().any():
        raise ProvenanceError("fuel surcharges contain invalid week_start values")
    if fuel.duplicated(business_key).any():
        raise ProvenanceError("fuel surcharges contain duplicate fuel-surcharge business keys")
    if not fuel["curve_basis"].eq(FUEL_CURVE_BASIS).all():
        raise ProvenanceError("fuel surcharges contain an inconsistent curve_basis")

    expected_keys = {
        (manifest.run_id, week_start, mode)
        for week_start in pd.date_range(BASELINE_START, EVALUATION_END, freq="W-MON")
        for mode in MODES
    }
    actual_keys = set(zip(*(fuel[column] for column in business_key), strict=True))
    if actual_keys != expected_keys:
        raise ProvenanceError("fuel surcharges do not provide exact weekly fuel coverage")


def _validate_canonical_run(run_directory: Path) -> RunManifest:
    """Cross-check a complete Freight raw run, not merely a generic manifest."""
    manifest = validate_manifest(run_directory)
    missing = REQUIRED_ARTIFACTS.difference(manifest.artifacts)
    if missing:
        raise ProvenanceError(f"Run is incomplete; missing artifacts: {sorted(missing)}")
    if set(manifest.run_parameters) != RUN_PARAMETER_KEYS:
        raise ProvenanceError("Run parameters are missing or contain unknown inputs")

    parameters = manifest.run_parameters
    integer_parameters = ("rows", "random_seed", "anomaly_seed")
    if any(
        not isinstance(parameters[name], int)
        or isinstance(parameters[name], bool)
        or parameters[name] < 0
        for name in integer_parameters
    ):
        raise ProvenanceError("rows and generation seeds must be non-negative integers")
    if parameters["rows"] < 3:
        raise ProvenanceError("rows must be at least three")
    if not isinstance(parameters["fixture_name"], str) or not parameters["fixture_name"]:
        raise ProvenanceError("fixture_name must be a non-empty string")
    if parameters["random_seed"] != manifest.random_seed:
        raise ProvenanceError("run_parameters.random_seed disagrees with the manifest seed")
    if manifest.time_windows != _window_metadata():
        raise ProvenanceError("manifest time windows do not match the engine contract")
    if manifest.anomaly_taxonomy != ANOMALY_TYPES:
        raise ProvenanceError("manifest anomaly taxonomy does not match the engine contract")
    if manifest.seed_source == SeedSource.FAF5.value and (
        manifest.source_file is None or manifest.source_sha256 is None
    ):
        raise ProvenanceError("FAF5 runs require source filename and SHA-256 evidence")

    for name in PARQUET_ARTIFACTS:
        record = manifest.artifacts[name]
        validate_arrow_schema(name, pq.read_schema(run_directory / record.filename))
    shipment_rows = manifest.artifacts["shipments"].row_count
    truth_rows = manifest.artifacts["anomaly_ground_truth"].row_count
    if shipment_rows != parameters["rows"] or truth_rows != parameters["rows"]:
        raise ProvenanceError("declared rows disagree with shipment or ground-truth rows")
    if manifest.artifacts["anomaly_flags"].row_count != shipment_rows * len(DETECTION_METHODS):
        raise ProvenanceError("anomaly flags do not contain the normalized detector matrix")

    _validate_shipment_truth_ids(run_directory, manifest)
    _validate_rate_calendars(run_directory, manifest)
    _validate_fuel_calendar(run_directory, manifest)

    truth_record = manifest.artifacts["anomaly_ground_truth"]
    truth = pq.read_table(
        run_directory / truth_record.filename,
        columns=["is_anomaly", "anomaly_type"],
    ).to_pydict()
    actual_counts = {anomaly_type: 0 for anomaly_type in ANOMALY_TYPES}
    for flag, anomaly_type in zip(truth["is_anomaly"], truth["anomaly_type"], strict=True):
        if flag == 0 and anomaly_type == "NONE":
            continue
        if flag != 1 or anomaly_type not in actual_counts:
            raise ProvenanceError("ground truth contains an invalid anomaly label")
        actual_counts[anomaly_type] += 1
    if dict(manifest.anomaly_counts) != actual_counts:
        raise ProvenanceError("manifest anomaly counts disagree with ground truth")

    if manifest.seed_source == SeedSource.TEST.value:
        expected_id = create_run_id(
            SeedSource.TEST,
            random_seed=parameters["random_seed"],
            fixture_name=parameters["fixture_name"],
            rows=parameters["rows"],
            anomaly_seed=parameters["anomaly_seed"],
        )
        if manifest.run_id != expected_id:
            raise ProvenanceError("TEST run ID disagrees with its generation parameters")
    return manifest


def build_run(
    *,
    artifact_root: Path,
    seed_source: SeedSource,
    rows: int,
    random_seed: int,
    anomaly_seed: int,
    faf5_path: Path | None = None,
    fixture_name: str = "cli",
) -> Path:
    """Generate, validate, and promote one run without exposing partial files."""
    artifact_root = artifact_root.expanduser().resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    run_id = create_run_id(
        seed_source,
        random_seed=random_seed,
        fixture_name=fixture_name if seed_source is SeedSource.TEST else None,
        rows=rows if seed_source is SeedSource.TEST else None,
        anomaly_seed=anomaly_seed if seed_source is SeedSource.TEST else None,
    )
    target = artifact_root / run_id
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"Run already exists and will not be overwritten: {target}")

    distribution = load_distribution(seed_source, faf5_path=faf5_path)
    normal = generate_normal_run(
        distribution,
        run_id=run_id,
        random_seed=random_seed,
        n_shipments=rows,
    )
    injected = inject_anomalies(normal.shipments, random_seed=anomaly_seed)
    baseline = fit_and_score_baselines(
        injected.shipments,
        normal.carrier_rates,
        normal.fuel_surcharges,
    )
    evaluation = build_evaluation(baseline, injected.ground_truth)
    alerts = prioritize_operational_alerts(baseline.scored, evaluation.selected_flags)
    anomaly_counts = (
        injected.ground_truth.query("is_anomaly == 1")["anomaly_type"]
        .value_counts()
        .reindex(ANOMALY_TYPES, fill_value=0)
        .astype(int)
        .to_dict()
    )

    temporary_parent = Path(tempfile.mkdtemp(prefix=".freight-v2-", dir=artifact_root))
    staging = temporary_parent / run_id
    try:
        staging.mkdir()
        tables = {
            "shipments": injected.shipments,
            "carrier_rates": normal.carrier_rates,
            "fuel_surcharges": normal.fuel_surcharges,
            "anomaly_ground_truth": injected.ground_truth,
            "anomaly_flags": evaluation.selected_flags,
            "lane_week_trends": evaluation.selected_lane_week_trends,
            "operational_alerts": alerts,
        }
        _write_parquet_tables(staging, tables)
        _validate_staged_tables(staging, tables)
        _write_json_artifact(staging, "evaluation", evaluation_payload(run_id, evaluation))
        data_quality_flags = evaluation.selected_flags.loc[
            evaluation.selected_flags["method"].eq("data_quality")
        ]
        _write_json_artifact(
            staging,
            "data_quality",
            {
                "run_id": run_id,
                "schema_version": SCHEMA_VERSION,
                "row_count": int(len(data_quality_flags)),
                "flagged_shipment_count": int(data_quality_flags["is_flagged"].sum()),
                "evaluation_flagged_shipment_count": int(
                    data_quality_flags.loc[
                        data_quality_flags["evaluated_at"].ge(EVALUATION_START), "is_flagged"
                    ].sum()
                ),
            },
        )

        manifest: RunManifest = create_manifest(
            run_id=run_id,
            seed_source=seed_source,
            random_seed=random_seed,
            source_file=distribution.source_file,
            source_sha256=distribution.source_sha256,
            time_windows=_window_metadata(),
            anomaly_taxonomy=ANOMALY_TYPES,
            anomaly_counts=anomaly_counts,
            run_parameters={
                "rows": rows,
                "random_seed": random_seed,
                "anomaly_seed": anomaly_seed,
                "fixture_name": fixture_name,
            },
            code_version=__version__,
        )
        for name in tables:
            manifest = register_artifact(
                manifest,
                name=name,
                path=staging / f"{name}.parquet",
                run_directory=staging,
            )
        for name in ("evaluation", "data_quality"):
            manifest = register_artifact(
                manifest,
                name=name,
                path=staging / f"{name}.json",
                run_directory=staging,
            )
        write_manifest(manifest, staging)
        _validate_canonical_run(staging)

        if target.exists() or target.is_symlink():
            raise FileExistsError(f"Run already exists and will not be overwritten: {target}")
        _atomic_promote(staging, target)
        return target
    finally:
        shutil.rmtree(temporary_parent, ignore_errors=True)


def latest_run(artifact_root: Path) -> Path:
    """Resolve the latest valid run by manifest generation timestamp."""
    artifact_root = artifact_root.expanduser().resolve()
    candidates: list[tuple[datetime, Path]] = []
    if not artifact_root.is_dir():
        raise FileNotFoundError(f"Artifact root does not exist: {artifact_root}")
    for manifest_path in artifact_root.glob("*/manifest.json"):
        if manifest_path.parent.is_symlink():
            continue
        try:
            manifest = _validate_canonical_run(manifest_path.parent)
        except (OSError, ValueError):
            continue
        candidates.append((datetime.fromisoformat(manifest.generated_at), manifest_path.parent))
    if not candidates:
        raise FileNotFoundError(f"No Freight v2 runs found beneath {artifact_root}")
    return max(candidates, key=lambda item: (item[0], item[1].name))[1]


def resolve_run(artifact_root: Path, *, run_id: str | None = None, latest: bool = False) -> Path:
    """Resolve and validate one explicit or latest run."""
    if (run_id is None) == (not latest):
        raise ValueError("choose exactly one of run_id or latest")
    root = artifact_root.expanduser().resolve()
    if latest:
        path = latest_run(root)
    else:
        requested = str(run_id)
        if requested == "accepted":
            pointer = root.parent / "accepted-run.json"
            if pointer.is_symlink() or not pointer.is_file():
                raise FileNotFoundError(f"Accepted-run pointer does not exist: {pointer}")
            try:
                payload = json.loads(pointer.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise ProvenanceError("accepted-run.json is not valid JSON") from error
            if set(payload) != {"run_id", "manifest_sha256"}:
                raise ProvenanceError("accepted-run.json has an invalid contract")
            requested = payload["run_id"]
            if not isinstance(requested, str) or not requested:
                raise ProvenanceError("accepted-run.json has an invalid run_id")
            expected_manifest_hash = payload["manifest_sha256"]
            if not isinstance(expected_manifest_hash, str) or len(expected_manifest_hash) != 64:
                raise ProvenanceError("accepted-run.json has an invalid manifest_sha256")
        else:
            expected_manifest_hash = None
        if Path(requested).is_absolute() or Path(requested).name != requested:
            raise ValueError("run_id must be a plain basename")
        unresolved = root / requested
        if unresolved.is_symlink():
            raise ValueError("run directory cannot be a symbolic link")
        path = unresolved.resolve()
        if path.parent != root:
            raise ValueError("run directory must be directly beneath artifact_root")
    _validate_canonical_run(path)
    if not latest and expected_manifest_hash is not None:
        actual_manifest_hash = sha256_file(path / "manifest.json")
        if actual_manifest_hash != expected_manifest_hash:
            raise ProvenanceError("accepted-run manifest hash does not match the selected run")
    return path


def accept_run(artifact_root: Path, *, run_id: str | None = None, latest: bool = False) -> Path:
    """Atomically point `artifacts/accepted-run.json` at one validated immutable run."""
    root = artifact_root.expanduser().resolve()
    path = resolve_run(root, run_id=run_id, latest=latest)
    pointer = root.parent / "accepted-run.json"
    pointer.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": path.name,
        "manifest_sha256": sha256_file(path / "manifest.json"),
    }
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{pointer.name}.", suffix=".tmp", dir=pointer.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, pointer)
    finally:
        temporary.unlink(missing_ok=True)
    resolve_run(root, run_id="accepted")
    return pointer
