"""Tests that run artifacts cannot be silently mixed or mutated."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from freight_v2.config import SCHEMA_VERSION, SeedSource, create_run_id
from freight_v2.provenance import (
    ArtifactHashMismatchError,
    ArtifactMissingError,
    ArtifactRecord,
    ArtifactRowCountMismatchError,
    ManifestFormatError,
    ProvenanceError,
    RunIdMismatchError,
    SchemaVersionMismatchError,
    create_manifest,
    manifest_payload,
    read_manifest,
    register_artifact,
    sha256_file,
    validate_manifest,
    write_manifest,
)


def _write_parquet(path: Path, run_id: str, *, rows: int = 2, schema: str = SCHEMA_VERSION) -> None:
    pq.write_table(
        pa.table(
            {
                "run_id": [run_id] * rows,
                "schema_version": [schema] * rows,
                "value": list(range(rows)),
            }
        ),
        path,
    )


def _registered_run(tmp_path: Path, *, rows: int = 2) -> tuple[Path, Path]:
    run_id = create_run_id(SeedSource.TEST, random_seed=7, fixture_name="provenance")
    run_directory = tmp_path / run_id
    run_directory.mkdir()
    artifact = run_directory / "shipments.parquet"
    _write_parquet(artifact, run_id, rows=rows)
    manifest = create_manifest(run_id=run_id, seed_source=SeedSource.TEST, random_seed=7)
    manifest = register_artifact(
        manifest,
        name="shipments",
        path=artifact,
        run_directory=run_directory,
    )
    write_manifest(manifest, run_directory)
    return run_directory, artifact


def test_sha256_is_stable_and_sensitive_to_one_byte(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    artifact.write_bytes(b"abc")
    first = sha256_file(artifact)
    assert first == sha256_file(artifact)
    artifact.write_bytes(b"abd")
    assert sha256_file(artifact) != first
    with pytest.raises(ValueError, match="positive integer"):
        sha256_file(artifact, chunk_size=0)


def test_manifest_round_trip_and_validation(tmp_path: Path) -> None:
    run_directory, _ = _registered_run(tmp_path)
    manifest = validate_manifest(run_directory)
    assert read_manifest(run_directory / "manifest.json") == manifest
    assert manifest.artifacts["shipments"].row_count == 2


def test_one_byte_mutation_fails_hash_validation(tmp_path: Path) -> None:
    run_directory, artifact = _registered_run(tmp_path)
    artifact.write_bytes(artifact.read_bytes() + b"x")
    with pytest.raises(ArtifactHashMismatchError, match="hash mismatch"):
        validate_manifest(run_directory)


def test_missing_registered_file_is_rejected(tmp_path: Path) -> None:
    run_directory, artifact = _registered_run(tmp_path)
    artifact.unlink()
    with pytest.raises(ArtifactMissingError, match="missing"):
        validate_manifest(run_directory)


def test_cross_run_artifact_is_rejected_at_registration(tmp_path: Path) -> None:
    expected = create_run_id(SeedSource.TEST, random_seed=1, fixture_name="expected")
    other = create_run_id(SeedSource.TEST, random_seed=2, fixture_name="other")
    run_directory = tmp_path / expected
    run_directory.mkdir()
    artifact = run_directory / "shipments.parquet"
    _write_parquet(artifact, other)
    manifest = create_manifest(run_id=expected, seed_source=SeedSource.TEST, random_seed=1)
    with pytest.raises(RunIdMismatchError, match="belongs to run"):
        register_artifact(
            manifest,
            name="shipments",
            path=artifact,
            run_directory=run_directory,
        )


def test_schema_mismatch_is_rejected_at_registration(tmp_path: Path) -> None:
    run_id = create_run_id(SeedSource.TEST, random_seed=1, fixture_name="schema")
    run_directory = tmp_path / run_id
    run_directory.mkdir()
    artifact = run_directory / "shipments.parquet"
    _write_parquet(artifact, run_id, schema="1.0.0")
    manifest = create_manifest(run_id=run_id, seed_source=SeedSource.TEST, random_seed=1)
    with pytest.raises(SchemaVersionMismatchError, match="uses schema"):
        register_artifact(
            manifest,
            name="shipments",
            path=artifact,
            run_directory=run_directory,
        )


def test_row_count_drift_fails_even_with_updated_hash(tmp_path: Path) -> None:
    run_directory, artifact = _registered_run(tmp_path, rows=2)
    manifest_path = run_directory / "manifest.json"
    manifest = read_manifest(manifest_path)
    _write_parquet(artifact, manifest.run_id, rows=3)

    payload = manifest_payload(manifest)
    payload["artifacts"]["shipments"]["sha256"] = sha256_file(artifact)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ArtifactRowCountMismatchError, match="row count"):
        validate_manifest(run_directory)


def test_unknown_seed_source_in_manifest_is_rejected(tmp_path: Path) -> None:
    run_directory, _ = _registered_run(tmp_path)
    manifest_path = run_directory / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["seed_source"] = "UNKNOWN"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ProvenanceError, match="seed source.*unsupported"):
        validate_manifest(run_directory)


def test_manifest_nested_mappings_are_read_only() -> None:
    taxonomy = ["carrier_overcharge"]
    manifest = create_manifest(
        run_id="test-immutable",
        seed_source=SeedSource.TEST,
        random_seed=3,
        time_windows={"baseline_start": "2025-01-01"},
        anomaly_taxonomy=taxonomy,
        anomaly_counts={"carrier_overcharge": 2},
    )
    taxonomy.append("rate_override")
    assert manifest.anomaly_taxonomy == ("carrier_overcharge",)
    with pytest.raises(TypeError):
        manifest.time_windows["baseline_end"] = "2025-02-01"  # type: ignore[index]
    with pytest.raises(TypeError):
        manifest.anomaly_counts["carrier_overcharge"] = 3  # type: ignore[index]
    with pytest.raises(TypeError):
        manifest.artifacts["injected"] = object()  # type: ignore[index]

    with pytest.raises(ValueError, match="time_windows"):
        create_manifest(
            run_id="test-mutable-value",
            seed_source=SeedSource.TEST,
            random_seed=3,
            time_windows={"baseline": []},
        )


@pytest.mark.parametrize("filename", ["../outside.json", "/tmp/outside.json"])
def test_manifest_rejects_artifact_paths_outside_run(filename: str, tmp_path: Path) -> None:
    run_directory, _ = _registered_run(tmp_path)
    manifest_path = run_directory / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["artifacts"]["shipments"]["filename"] = filename
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ManifestFormatError, match="plain basename"):
        validate_manifest(run_directory)


def test_manifest_write_refuses_dangling_symlink_and_second_write(tmp_path: Path) -> None:
    run_id = "test-exclusive"
    run_directory = tmp_path / run_id
    run_directory.mkdir()
    outside = tmp_path / "outside.json"
    (run_directory / "manifest.json").symlink_to(outside)
    manifest = create_manifest(run_id=run_id, seed_source=SeedSource.TEST, random_seed=3)
    with pytest.raises(FileExistsError, match="already exists"):
        write_manifest(manifest, run_directory)
    assert not outside.exists()

    (run_directory / "manifest.json").unlink()
    write_manifest(manifest, run_directory)
    with pytest.raises(FileExistsError, match="already exists"):
        write_manifest(manifest, run_directory)


def test_validation_rejects_symlinked_artifact(tmp_path: Path) -> None:
    run_directory, artifact = _registered_run(tmp_path)
    target = run_directory / "real.parquet"
    artifact.rename(target)
    artifact.symlink_to(target)
    with pytest.raises(ProvenanceError, match="symbolic link"):
        validate_manifest(run_directory)


@pytest.mark.parametrize(
    ("run_ids", "schema_versions", "error_type", "message"),
    [
        ([None, None], [SCHEMA_VERSION, SCHEMA_VERSION], RunIdMismatchError, "null run_id"),
        (["", ""], [SCHEMA_VERSION, SCHEMA_VERSION], RunIdMismatchError, "empty run_id"),
        (
            ["test-a", None],
            [SCHEMA_VERSION, SCHEMA_VERSION],
            RunIdMismatchError,
            "null run_id",
        ),
        (["test-a", "test-a"], [None, None], SchemaVersionMismatchError, "null schema"),
    ],
)
def test_parquet_identity_rejects_null_or_empty_values(
    run_ids: list[str | None],
    schema_versions: list[str | None],
    error_type: type[ProvenanceError],
    message: str,
    tmp_path: Path,
) -> None:
    run_directory = tmp_path / "test-a"
    run_directory.mkdir()
    artifact = run_directory / "shipments.parquet"
    pq.write_table(
        pa.table({"run_id": run_ids, "schema_version": schema_versions}),
        artifact,
    )
    manifest = create_manifest(run_id="test-a", seed_source=SeedSource.TEST, random_seed=1)
    with pytest.raises(error_type, match=message):
        register_artifact(
            manifest,
            name="shipments",
            path=artifact,
            run_directory=run_directory,
        )


def test_empty_parquet_and_malformed_manifest_are_precise(tmp_path: Path) -> None:
    run_directory = tmp_path / "test-empty"
    run_directory.mkdir()
    artifact = run_directory / "shipments.parquet"
    pq.write_table(
        pa.table(
            {
                "run_id": pa.array([], type=pa.string()),
                "schema_version": pa.array([], type=pa.string()),
            }
        ),
        artifact,
    )
    manifest = create_manifest(run_id="test-empty", seed_source=SeedSource.TEST, random_seed=1)
    with pytest.raises(ProvenanceError, match="no rows"):
        register_artifact(
            manifest,
            name="shipments",
            path=artifact,
            run_directory=run_directory,
        )

    (run_directory / "manifest.json").write_text('{"artifacts": []}', encoding="utf-8")
    with pytest.raises(ManifestFormatError, match="fields are invalid"):
        validate_manifest(run_directory)


def test_registration_rejects_duplicate_logical_name_and_filename(tmp_path: Path) -> None:
    run_directory, artifact = _registered_run(tmp_path)
    manifest = read_manifest(run_directory / "manifest.json")
    with pytest.raises(ProvenanceError, match="already registered"):
        register_artifact(
            manifest,
            name="shipments",
            path=artifact,
            run_directory=run_directory,
        )

    duplicate = run_directory / "duplicate.parquet"
    os.link(artifact, duplicate)
    renamed_manifest = read_manifest(run_directory / "manifest.json")
    with pytest.raises(ProvenanceError, match="already registered"):
        register_artifact(
            renamed_manifest,
            name="duplicate_logical_name",
            path=artifact,
            run_directory=run_directory,
        )


@pytest.mark.parametrize(
    "content",
    [
        '{"run_id":"first","run_id":"second"}',
        '{"artifact":{"filename":"first","filename":"second"}}',
    ],
)
def test_json_parser_rejects_duplicate_keys_recursively(content: str, tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(content, encoding="utf-8")
    with pytest.raises(ManifestFormatError, match="duplicate key"):
        read_manifest(manifest_path)


@pytest.mark.parametrize(
    ("metadata", "message"),
    [
        ({"random_seed": -1}, "random_seed"),
        ({"random_seed": True}, "random_seed"),
        ({"generated_at": "not-a-date"}, "generated_at"),
        ({"generated_at": "2025-01-01T00:00:00"}, "timezone"),
        ({"source_sha256": "not-a-digest"}, "source_sha256"),
        ({"code_version": 42}, "code_version"),
    ],
)
def test_invalid_scalar_metadata_cannot_construct_manifest(
    metadata: dict[str, object], message: str
) -> None:
    arguments: dict[str, object] = {
        "run_id": "test-invalid",
        "seed_source": SeedSource.TEST,
        "random_seed": 1,
    }
    arguments.update(metadata)
    with pytest.raises(ValueError, match=message):
        create_manifest(**arguments)  # type: ignore[arg-type]


def test_exactly_once_writer_rejects_noncurrent_schema_before_creating_file(
    tmp_path: Path,
) -> None:
    manifest = create_manifest(
        run_id="test-old-schema",
        seed_source=SeedSource.TEST,
        random_seed=1,
        schema_version="1.0.0",
    )
    run_directory = tmp_path / manifest.run_id
    with pytest.raises(SchemaVersionMismatchError, match="Cannot write schema"):
        write_manifest(manifest, run_directory)
    assert not (run_directory / "manifest.json").exists()


def test_artifact_record_validates_all_scalar_fields() -> None:
    with pytest.raises(ValueError, match="plain basename"):
        ArtifactRecord(
            filename="../outside.parquet",
            sha256="0" * 64,
            run_id="test-a",
            schema_version=SCHEMA_VERSION,
            row_count=1,
        )
    with pytest.raises(ValueError, match="sha256"):
        ArtifactRecord(
            filename="inside.parquet",
            sha256="invalid",
            run_id="test-a",
            schema_version=SCHEMA_VERSION,
            row_count=1,
        )
