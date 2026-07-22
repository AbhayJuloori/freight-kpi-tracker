"""Tests for staged immutable run construction and CLI resolution."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

import freight_v2.run_builder as run_builder
from freight_v2.anomalies import ANOMALY_TYPES, DEFAULT_ANOMALY_RATES
from freight_v2.cli import main
from freight_v2.config import SeedSource
from freight_v2.contracts import ColumnTypeError, MissingColumnsError
from freight_v2.provenance import ArtifactHashMismatchError, ProvenanceError, validate_manifest
from freight_v2.run_builder import accept_run, build_run, latest_run, resolve_run


def _build_fixture(root: Path, *, fixture_name: str = "builder") -> Path:
    return build_run(
        artifact_root=root,
        seed_source=SeedSource.TEST,
        rows=500,
        random_seed=17,
        anomaly_seed=18,
        fixture_name=fixture_name,
    )


def test_builder_promotes_one_complete_validated_run(tmp_path: Path) -> None:
    artifact_root = tmp_path / "runs"
    run_path = _build_fixture(artifact_root)
    manifest = validate_manifest(run_path)
    assert run_path.parent == artifact_root
    assert set(manifest.artifacts) == {
        "shipments",
        "carrier_rates",
        "fuel_surcharges",
        "anomaly_ground_truth",
        "anomaly_flags",
        "lane_week_trends",
        "operational_alerts",
        "evaluation",
        "data_quality",
    }
    assert manifest.artifacts["shipments"].row_count == 500
    requested = sum(round(rate * 500) for rate in DEFAULT_ANOMALY_RATES.values())
    assert set(manifest.anomaly_counts) == set(ANOMALY_TYPES)
    assert 0 < sum(manifest.anomaly_counts.values()) <= requested + len(ANOMALY_TYPES)
    assert not list(artifact_root.glob(".freight-v2-*"))
    assert not list(artifact_root.glob(".*.lock"))


def test_builder_refuses_to_overwrite_deterministic_fixture(tmp_path: Path) -> None:
    artifact_root = tmp_path / "runs"
    first = _build_fixture(artifact_root)
    with pytest.raises(FileExistsError, match="will not be overwritten"):
        _build_fixture(artifact_root)
    assert validate_manifest(first).run_id == first.name


def test_fixture_identity_and_manifest_include_all_generation_inputs(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    first = _build_fixture(root)
    second = build_run(
        artifact_root=root,
        seed_source=SeedSource.TEST,
        rows=600,
        random_seed=17,
        anomaly_seed=999,
        fixture_name="builder",
    )
    assert first.name != second.name
    manifest = validate_manifest(second)
    assert dict(manifest.run_parameters) == {
        "rows": 600,
        "random_seed": 17,
        "anomaly_seed": 999,
        "fixture_name": "builder",
    }


def test_latest_and_explicit_resolution_validate_before_returning(tmp_path: Path) -> None:
    artifact_root = tmp_path / "runs"
    first = _build_fixture(artifact_root, fixture_name="first")
    second = _build_fixture(artifact_root, fixture_name="second")
    assert latest_run(artifact_root) in {first, second}
    assert resolve_run(artifact_root, run_id=first.name) == first
    assert resolve_run(artifact_root, latest=True) in {first, second}
    with pytest.raises(ValueError, match="exactly one"):
        resolve_run(artifact_root)


def test_accept_pointer_is_atomic_hash_bound_and_cli_resolvable(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    artifact_root = tmp_path / "artifacts" / "runs"
    run_path = _build_fixture(artifact_root, fixture_name="accepted")
    pointer = accept_run(artifact_root, run_id=run_path.name)
    assert pointer == artifact_root.parent / "accepted-run.json"
    assert resolve_run(artifact_root, run_id="accepted") == run_path

    payload = json.loads(pointer.read_text(encoding="utf-8"))
    pointer.write_text(json.dumps({**payload, "manifest_sha256": "0" * 64}), encoding="utf-8")
    with pytest.raises(ProvenanceError, match="manifest hash"):
        resolve_run(artifact_root, run_id="accepted")

    pointer = accept_run(artifact_root, latest=True)
    assert main(["validate", "--artifact-root", str(artifact_root), "--run", "accepted"]) == 0
    assert Path(capsys.readouterr().out.strip()) == run_path
    assert pointer.is_file()


def test_explicit_resolution_rejects_traversal_and_symlinked_runs(tmp_path: Path) -> None:
    artifact_root = tmp_path / "runs"
    outside = _build_fixture(tmp_path / "outside")
    with pytest.raises(ValueError, match="plain basename"):
        resolve_run(artifact_root, run_id=f"../outside/{outside.name}")
    artifact_root.mkdir(exist_ok=True)
    (artifact_root / outside.name).symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        resolve_run(artifact_root, run_id=outside.name)


def test_latest_skips_invalid_future_dated_candidate(tmp_path: Path) -> None:
    artifact_root = tmp_path / "runs"
    valid = _build_fixture(artifact_root)
    invalid = artifact_root / "test-invalid"
    invalid.mkdir()
    payload = json.loads((valid / "manifest.json").read_text(encoding="utf-8"))
    payload["generated_at"] = "2999-01-01T00:00:00+00:00"
    (invalid / "manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    assert latest_run(artifact_root) == valid


def test_latest_skips_complete_looking_manifest_without_canonical_artifacts(
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "runs"
    valid = _build_fixture(artifact_root)
    invalid = artifact_root / "test-empty"
    invalid.mkdir()
    payload = json.loads((valid / "manifest.json").read_text(encoding="utf-8"))
    payload.update(run_id=invalid.name, generated_at="2999-01-01T00:00:00+00:00", artifacts={})
    (invalid / "manifest.json").write_text(json.dumps(payload), encoding="utf-8")
    assert latest_run(artifact_root) == valid


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("rows", 499, "rows disagree"),
        ("random_seed", 999, "random_seed disagrees"),
        ("anomaly_seed", 999, "run ID disagrees"),
    ],
)
def test_resolution_cross_checks_manifest_parameters(
    tmp_path: Path, field: str, value: int, message: str
) -> None:
    run_path = _build_fixture(tmp_path / "runs")
    manifest_path = run_path / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["run_parameters"][field] = value
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProvenanceError, match=message):
        resolve_run(run_path.parent, run_id=run_path.name)


def test_resolution_cross_checks_anomaly_counts(tmp_path: Path) -> None:
    run_path = _build_fixture(tmp_path / "runs")
    manifest_path = run_path / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["anomaly_counts"]["carrier_overcharge"] += 1
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ProvenanceError, match="counts disagree"):
        resolve_run(run_path.parent, run_id=run_path.name)


def test_atomic_promotion_refuses_an_existing_empty_directory(tmp_path: Path) -> None:
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()
    (source / "payload").write_text("owned by source", encoding="utf-8")
    with pytest.raises(FileExistsError, match="will not be overwritten"):
        run_builder._atomic_promote(source, target)
    assert source.is_dir()
    assert target.is_dir()
    assert not list(target.iterdir())


def test_promoted_artifact_mutation_is_detected(tmp_path: Path) -> None:
    run_path = _build_fixture(tmp_path / "runs")
    shipments = run_path / "shipments.parquet"
    shipments.write_bytes(shipments.read_bytes() + b"x")
    with pytest.raises(ArtifactHashMismatchError):
        validate_manifest(run_path)


def test_cli_build_and_validate_latest(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    artifact_root = tmp_path / "cli-runs"
    assert (
        main(
            [
                "build",
                "--seed-source",
                "TEST",
                "--rows",
                "500",
                "--output",
                str(artifact_root),
                "--fixture-name",
                "cli-test",
            ]
        )
        == 0
    )
    built_path = Path(capsys.readouterr().out.strip())
    assert built_path.is_dir()
    assert main(["validate", "--artifact-root", str(artifact_root), "--latest"]) == 0
    assert Path(capsys.readouterr().out.strip()) == built_path


def test_cli_requires_a_subcommand() -> None:
    with pytest.raises(SystemExit) as error:
        main([])
    assert error.value.code == 2


@pytest.mark.parametrize(
    ("corruption", "error_type"),
    [
        (lambda frame: frame.drop(columns="total_cost"), MissingColumnsError),
        (lambda frame: frame.assign(total_cost=frame["total_cost"].astype(str)), ColumnTypeError),
    ],
)
def test_staged_parquet_schema_is_validated_before_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corruption,
    error_type: type[Exception],
) -> None:
    artifact_root = tmp_path / "runs"
    original_writer = run_builder._write_parquet_tables

    def corrupting_writer(staging: Path, tables: dict[str, object]) -> None:
        corrupted = dict(tables)
        corrupted["shipments"] = corruption(corrupted["shipments"])
        original_writer(staging, corrupted)

    monkeypatch.setattr(run_builder, "_write_parquet_tables", corrupting_writer)
    with pytest.raises(error_type):
        _build_fixture(artifact_root, fixture_name="corrupt-schema")
    assert not list(artifact_root.glob("*/manifest.json"))
    assert not list(artifact_root.glob(".freight-v2-*"))


def _duplicate_values(
    frame: pd.DataFrame,
    columns: str | list[str],
) -> pd.DataFrame:
    corrupted = frame.copy()
    selected = [columns] if isinstance(columns, str) else columns
    corrupted.loc[corrupted.index[1], selected] = corrupted.loc[
        corrupted.index[0], selected
    ].to_numpy()
    return corrupted


def _replace_first_value(frame: pd.DataFrame, column: str, value: object) -> pd.DataFrame:
    corrupted = frame.copy()
    corrupted.loc[corrupted.index[0], column] = value
    return corrupted


def _shift_first_rate_boundary(frame: pd.DataFrame, days: int) -> pd.DataFrame:
    corrupted = frame.copy()
    key_columns = ["run_id", "carrier_id", "lane_id", "mode"]
    first_key = corrupted.loc[corrupted.index[0], key_columns]
    group = corrupted.loc[(corrupted[key_columns] == first_key).all(axis=1)].sort_values(
        "effective_start", kind="stable"
    )
    first_index = group.index[0]
    corrupted.loc[first_index, "effective_end"] += pd.Timedelta(days=days)
    return corrupted


def _assert_staged_semantic_corruption_rejected(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    table_name: str,
    mutation: Callable[[pd.DataFrame], pd.DataFrame],
    message: str,
) -> None:
    artifact_root = tmp_path / "runs"
    original_writer = run_builder._write_parquet_tables
    promotion_attempted = False

    def corrupting_writer(staging: Path, tables: dict[str, object]) -> None:
        corrupted = dict(tables)
        corrupted[table_name] = mutation(corrupted[table_name])
        original_writer(staging, corrupted)

    def record_promotion_attempt(*_args: object, **_kwargs: object) -> None:
        nonlocal promotion_attempted
        promotion_attempted = True
        pytest.fail("semantic corruption reached atomic promotion")

    monkeypatch.setattr(run_builder, "_write_parquet_tables", corrupting_writer)
    monkeypatch.setattr(run_builder, "_atomic_promote", record_promotion_attempt)
    with pytest.raises(ProvenanceError, match=message):
        _build_fixture(artifact_root, fixture_name=f"corrupt-{table_name}")
    assert promotion_attempted is False
    assert not list(artifact_root.glob("*/manifest.json"))
    assert not list(artifact_root.glob(".freight-v2-*"))


@pytest.mark.parametrize(
    ("table_name", "mutation", "message"),
    [
        (
            "shipments",
            lambda frame: _duplicate_values(frame, "shipment_id"),
            "duplicate shipment IDs",
        ),
        (
            "shipments",
            lambda frame: _replace_first_value(frame, "shipment_id", ""),
            "non-empty string shipment IDs",
        ),
        (
            "anomaly_ground_truth",
            lambda frame: _duplicate_values(frame, "shipment_id"),
            "duplicate ground-truth shipment IDs",
        ),
        (
            "anomaly_ground_truth",
            lambda frame: _replace_first_value(frame, "shipment_id", None),
            "non-empty string shipment IDs",
        ),
        (
            "anomaly_ground_truth",
            lambda frame: _replace_first_value(frame, "shipment_id", "SHP-NOT-IN-SHIPMENTS"),
            "shipment and ground-truth ID sets disagree",
        ),
        (
            "carrier_rates",
            lambda frame: _duplicate_values(
                frame,
                ["run_id", "carrier_id", "lane_id", "mode", "effective_start"],
            ),
            "duplicate carrier-rate business keys",
        ),
        (
            "carrier_rates",
            lambda frame: _duplicate_values(frame, "rate_id"),
            "duplicate rate IDs",
        ),
        (
            "carrier_rates",
            lambda frame: _shift_first_rate_boundary(frame, -1),
            "effective-date gap",
        ),
        (
            "carrier_rates",
            lambda frame: _shift_first_rate_boundary(frame, 1),
            "effective-date overlap",
        ),
        (
            "fuel_surcharges",
            lambda frame: _duplicate_values(frame, ["run_id", "week_start", "mode"]),
            "duplicate fuel-surcharge business keys",
        ),
        (
            "fuel_surcharges",
            lambda frame: _replace_first_value(frame, "mode", ""),
            "non-empty supported modes",
        ),
        (
            "fuel_surcharges",
            lambda frame: _replace_first_value(frame, "mode", "OCEAN"),
            "non-empty supported modes",
        ),
        (
            "fuel_surcharges",
            lambda frame: frame.iloc[1:].reset_index(drop=True),
            "weekly fuel coverage",
        ),
        (
            "fuel_surcharges",
            lambda frame: _replace_first_value(
                frame,
                "week_start",
                frame["week_start"].iloc[0] + pd.Timedelta(days=1),
            ),
            "weekly fuel coverage",
        ),
        (
            "fuel_surcharges",
            lambda frame: _replace_first_value(frame, "curve_basis", "mixed provenance"),
            "inconsistent curve_basis",
        ),
    ],
)
def test_canonical_semantic_corruption_is_rejected_before_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    table_name: str,
    mutation: Callable[[pd.DataFrame], pd.DataFrame],
    message: str,
) -> None:
    _assert_staged_semantic_corruption_rejected(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        table_name=table_name,
        mutation=mutation,
        message=message,
    )


def test_shipment_and_truth_id_validation_is_order_independent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "runs"
    original_writer = run_builder._write_parquet_tables

    def reversing_writer(staging: Path, tables: dict[str, object]) -> None:
        reordered = dict(tables)
        reordered["anomaly_ground_truth"] = reordered["anomaly_ground_truth"].iloc[::-1]
        original_writer(staging, reordered)

    monkeypatch.setattr(run_builder, "_write_parquet_tables", reversing_writer)
    run_path = _build_fixture(artifact_root, fixture_name="truth-order-independent")
    assert run_path.is_dir()
