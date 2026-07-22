"""Immutable run manifests and artifact-integrity validation."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from freight_v2.config import SCHEMA_VERSION, SeedSource


class ProvenanceError(ValueError):
    """Base error for invalid run provenance."""


class ArtifactMissingError(ProvenanceError):
    """Raised when a manifest artifact no longer exists."""


class ArtifactHashMismatchError(ProvenanceError):
    """Raised when artifact bytes differ from the registered bytes."""


class ArtifactRowCountMismatchError(ProvenanceError):
    """Raised when a tabular artifact's row count has drifted."""


class RunIdMismatchError(ProvenanceError):
    """Raised when artifacts from separate runs are combined."""


class SchemaVersionMismatchError(ProvenanceError):
    """Raised when an incompatible artifact schema is loaded."""


class ManifestFormatError(ProvenanceError):
    """Raised when manifest JSON is malformed or weakly typed."""


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    """Integrity metadata for one file within a run directory."""

    filename: str
    sha256: str
    run_id: str
    schema_version: str
    row_count: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.filename, str) or not self.filename:
            raise ValueError("artifact filename must be a non-empty string")
        filename = Path(self.filename)
        if filename.is_absolute() or filename.name != self.filename:
            raise ValueError("artifact filename must be a plain basename")
        if not isinstance(self.sha256, str) or not _is_sha256(self.sha256):
            raise ValueError("artifact sha256 must be a lowercase SHA-256 digest")
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("artifact run_id must be a non-empty string")
        if not isinstance(self.schema_version, str) or not self.schema_version:
            raise ValueError("artifact schema_version must be a non-empty string")
        if self.row_count is not None and (
            not isinstance(self.row_count, int)
            or isinstance(self.row_count, bool)
            or self.row_count < 0
        ):
            raise ValueError("artifact row_count must be a non-negative integer or null")


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Complete provenance required to reproduce and validate one run."""

    run_id: str
    seed_source: str
    random_seed: int
    generated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    schema_version: str = SCHEMA_VERSION
    source_file: str | None = None
    source_sha256: str | None = None
    time_windows: Mapping[str, str] = field(default_factory=dict)
    anomaly_taxonomy: tuple[str, ...] = ()
    anomaly_counts: Mapping[str, int] = field(default_factory=dict)
    run_parameters: Mapping[str, int | str] = field(default_factory=dict)
    code_version: str | None = None
    artifacts: Mapping[str, ArtifactRecord] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("run_id must be a non-empty string")
        try:
            SeedSource(self.seed_source)
        except (TypeError, ValueError) as error:
            raise ValueError(f"seed source {self.seed_source!r} is unsupported") from error
        if (
            not isinstance(self.random_seed, int)
            or isinstance(self.random_seed, bool)
            or self.random_seed < 0
        ):
            raise ValueError("random_seed must be a non-negative integer")
        if not isinstance(self.schema_version, str) or not self.schema_version:
            raise ValueError("schema_version must be a non-empty string")
        if not isinstance(self.generated_at, str) or not self.generated_at:
            raise ValueError("generated_at must be a non-empty ISO-8601 timestamp")
        try:
            generated_at = datetime.fromisoformat(self.generated_at)
        except ValueError as error:
            raise ValueError("generated_at must be an ISO-8601 timestamp") from error
        if generated_at.utcoffset() is None:
            raise ValueError("generated_at must include a timezone offset")
        for field_name in ("source_file", "code_version"):
            value = getattr(self, field_name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"{field_name} must be a non-empty string or null")
        if self.source_sha256 is not None and (
            not isinstance(self.source_sha256, str) or not _is_sha256(self.source_sha256)
        ):
            raise ValueError("source_sha256 must be a lowercase SHA-256 digest or null")
        if not all(
            isinstance(key, str) and key and isinstance(value, str) and value
            for key, value in self.time_windows.items()
        ):
            raise ValueError("time_windows must map non-empty strings to non-empty strings")
        if not all(
            isinstance(key, str)
            and key
            and isinstance(value, int)
            and not isinstance(value, bool)
            and value >= 0
            for key, value in self.anomaly_counts.items()
        ):
            raise ValueError("anomaly_counts must map strings to non-negative integers")
        if not all(
            isinstance(key, str)
            and key
            and isinstance(value, (int, str))
            and not isinstance(value, bool)
            and (not isinstance(value, str) or bool(value))
            for key, value in self.run_parameters.items()
        ):
            raise ValueError("run_parameters must map strings to integers or non-empty strings")
        if not all(isinstance(value, str) and value for value in self.anomaly_taxonomy):
            raise ValueError("anomaly_taxonomy must contain non-empty strings")
        if not all(
            isinstance(key, str) and key and isinstance(value, ArtifactRecord)
            for key, value in self.artifacts.items()
        ):
            raise ValueError("artifacts must map non-empty names to ArtifactRecord values")
        filenames = [record.filename for record in self.artifacts.values()]
        if len(filenames) != len(set(filenames)):
            raise ValueError("artifact filenames must be unique")
        for name, record in self.artifacts.items():
            if record.run_id != self.run_id:
                raise ValueError(f"artifact {name!r} has a mismatched run_id")
            if record.schema_version != self.schema_version:
                raise ValueError(f"artifact {name!r} has a mismatched schema_version")
        object.__setattr__(self, "time_windows", MappingProxyType(dict(self.time_windows)))
        object.__setattr__(self, "anomaly_taxonomy", tuple(self.anomaly_taxonomy))
        object.__setattr__(self, "anomaly_counts", MappingProxyType(dict(self.anomaly_counts)))
        object.__setattr__(self, "run_parameters", MappingProxyType(dict(self.run_parameters)))
        object.__setattr__(self, "artifacts", MappingProxyType(dict(self.artifacts)))


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Hash a file without loading it fully into memory."""
    if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in pairs:
        if key in payload:
            raise ManifestFormatError(f"JSON object contains duplicate key: {key!r}")
        payload[key] = value
    return payload


def create_manifest(
    *,
    run_id: str,
    seed_source: SeedSource,
    random_seed: int,
    **metadata: Any,
) -> RunManifest:
    """Create an empty, typed manifest before artifact registration."""
    return RunManifest(
        run_id=run_id,
        seed_source=seed_source.value,
        random_seed=random_seed,
        **metadata,
    )


def _parquet_identity(path: Path) -> tuple[int, str, str]:
    metadata = pq.read_metadata(path)
    columns = set(metadata.schema.names)
    required = {"run_id", "schema_version"}
    missing = required.difference(columns)
    if missing:
        joined = ", ".join(sorted(missing))
        raise ProvenanceError(f"{path.name} lacks provenance columns: {joined}")

    if metadata.num_rows == 0:
        raise ProvenanceError(f"{path.name} contains no rows to establish provenance")
    identity = pq.read_table(path, columns=["run_id", "schema_version"])
    for field_name in ("run_id", "schema_version"):
        column = identity.column(field_name)
        if column.null_count:
            error_type = (
                RunIdMismatchError if field_name == "run_id" else SchemaVersionMismatchError
            )
            raise error_type(f"{path.name} contains null {field_name} values")
        if not (pa.types.is_string(column.type) or pa.types.is_large_string(column.type)):
            raise ProvenanceError(f"{path.name} {field_name} must be a string column")
        if any(not value for value in column.to_pylist()):
            error_type = (
                RunIdMismatchError if field_name == "run_id" else SchemaVersionMismatchError
            )
            raise error_type(f"{path.name} contains empty {field_name} values")

    run_ids = set(identity.column("run_id").to_pylist())
    schema_versions = set(identity.column("schema_version").to_pylist())
    if len(run_ids) != 1:
        raise RunIdMismatchError(f"{path.name} contains multiple run IDs: {sorted(run_ids)!r}")
    if len(schema_versions) != 1:
        raise SchemaVersionMismatchError(
            f"{path.name} contains multiple schema versions: {sorted(schema_versions)!r}"
        )
    return metadata.num_rows, run_ids.pop(), schema_versions.pop()


def _json_identity(path: Path) -> tuple[int | None, str, str]:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ProvenanceError(f"Cannot read JSON artifact {path.name}: {error}") from error
    if not isinstance(payload, dict):
        raise ProvenanceError(f"{path.name} must contain a JSON object")
    try:
        run_id = payload["run_id"]
        schema_version = payload["schema_version"]
    except KeyError as error:
        raise ProvenanceError(f"{path.name} lacks {error.args[0]} provenance field") from error
    if not isinstance(run_id, str) or not run_id:
        raise RunIdMismatchError(f"{path.name} run_id must be a non-empty string")
    if not isinstance(schema_version, str) or not schema_version:
        raise SchemaVersionMismatchError(f"{path.name} schema_version must be a non-empty string")
    rows = payload.get("row_count")
    if rows is not None and (not isinstance(rows, int) or isinstance(rows, bool) or rows < 0):
        raise ProvenanceError(f"{path.name} has an invalid row_count")
    return rows, run_id, schema_version


def _artifact_identity(path: Path) -> tuple[int | None, str, str]:
    if path.suffix == ".parquet":
        return _parquet_identity(path)
    if path.suffix == ".json":
        return _json_identity(path)
    raise ProvenanceError(f"Unsupported artifact type: {path.suffix or '<none>'}")


def register_artifact(
    manifest: RunManifest,
    *,
    name: str,
    path: Path,
    run_directory: Path,
) -> RunManifest:
    """Validate and register a file, returning a new immutable manifest value."""
    if not name or name in manifest.artifacts:
        raise ProvenanceError(f"Artifact name is empty or already registered: {name!r}")
    if path.is_symlink():
        raise ProvenanceError(f"Artifact cannot be a symbolic link: {path}")
    path = path.resolve()
    run_directory = run_directory.resolve()
    if not path.is_file():
        raise ArtifactMissingError(f"Artifact does not exist: {path}")
    if path.parent != run_directory:
        raise ProvenanceError(f"Artifact must be directly inside run directory: {path}")
    if any(record.filename == path.name for record in manifest.artifacts.values()):
        raise ProvenanceError(f"Artifact filename is already registered: {path.name}")
    row_count, run_id, schema_version = _artifact_identity(path)
    if run_id != manifest.run_id:
        raise RunIdMismatchError(
            f"{path.name} belongs to run {run_id!r}, expected {manifest.run_id!r}"
        )
    if schema_version != manifest.schema_version:
        raise SchemaVersionMismatchError(
            f"{path.name} uses schema {schema_version!r}, expected {manifest.schema_version!r}"
        )

    record = ArtifactRecord(
        filename=path.name,
        sha256=sha256_file(path),
        row_count=row_count,
        run_id=run_id,
        schema_version=schema_version,
    )
    artifacts = {**manifest.artifacts, name: record}
    return replace(manifest, artifacts=artifacts)


def manifest_payload(manifest: RunManifest) -> dict[str, Any]:
    """Convert immutable manifest values into canonical JSON primitives."""
    return {
        "run_id": manifest.run_id,
        "seed_source": manifest.seed_source,
        "random_seed": manifest.random_seed,
        "generated_at": manifest.generated_at,
        "schema_version": manifest.schema_version,
        "source_file": manifest.source_file,
        "source_sha256": manifest.source_sha256,
        "time_windows": dict(manifest.time_windows),
        "anomaly_taxonomy": list(manifest.anomaly_taxonomy),
        "anomaly_counts": dict(manifest.anomaly_counts),
        "run_parameters": dict(manifest.run_parameters),
        "code_version": manifest.code_version,
        "artifacts": {name: asdict(record) for name, record in manifest.artifacts.items()},
    }


def write_manifest(manifest: RunManifest, run_directory: Path) -> Path:
    """Write canonical JSON exactly once for an immutable run."""
    if manifest.schema_version != SCHEMA_VERSION:
        raise SchemaVersionMismatchError(
            f"Cannot write schema {manifest.schema_version!r}; expected {SCHEMA_VERSION!r}"
        )
    run_directory.mkdir(parents=True, exist_ok=True)
    run_directory = run_directory.resolve()
    path = run_directory / "manifest.json"
    content = (json.dumps(manifest_payload(manifest), indent=2, sort_keys=True) + "\n").encode()
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=run_directory,
            prefix=".manifest-",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary_path, path)
    except FileExistsError as error:
        raise FileExistsError(f"Manifest already exists: {path}") from error
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    return path


def read_manifest(path: Path) -> RunManifest:
    """Load a typed manifest from its canonical JSON representation."""
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (OSError, json.JSONDecodeError) as error:
        raise ManifestFormatError(f"Cannot read manifest {path}: {error}") from error
    if not isinstance(payload, dict):
        raise ManifestFormatError("Manifest must contain a JSON object")

    expected_fields = set(RunManifest.__dataclass_fields__)
    missing = expected_fields.difference(payload)
    unknown = set(payload).difference(expected_fields)
    if missing or unknown:
        raise ManifestFormatError(
            f"Manifest fields are invalid; missing={sorted(missing)}, unknown={sorted(unknown)}"
        )
    raw_artifacts = payload.pop("artifacts", {})
    if not isinstance(raw_artifacts, dict):
        raise ManifestFormatError("Manifest artifacts must be an object")
    artifacts: dict[str, ArtifactRecord] = {}
    filenames: set[str] = set()
    for name, record in raw_artifacts.items():
        if not isinstance(name, str) or not name or not isinstance(record, dict):
            raise ManifestFormatError("Every artifact must have a non-empty name and object record")
        expected_record_fields = set(ArtifactRecord.__dataclass_fields__)
        if set(record) != expected_record_fields:
            raise ManifestFormatError(f"Artifact {name!r} has missing or unknown fields")
        try:
            artifact = ArtifactRecord(**record)
        except (TypeError, ValueError) as error:
            raise ManifestFormatError(
                f"Artifact {name!r} has an invalid record: {error}"
            ) from error
        if not isinstance(artifact.filename, str) or not artifact.filename:
            raise ManifestFormatError(f"Artifact {name!r} has an invalid filename")
        filename = Path(artifact.filename)
        if filename.name != artifact.filename or filename.is_absolute():
            raise ManifestFormatError(f"Artifact {name!r} filename must be a plain basename")
        if artifact.filename in filenames:
            raise ManifestFormatError(f"Artifact filename is duplicated: {artifact.filename}")
        invalid_digest = not isinstance(artifact.sha256, str) or len(artifact.sha256) != 64
        invalid_digest = (
            invalid_digest
            or any(character not in "0123456789abcdef" for character in artifact.sha256)
            if isinstance(artifact.sha256, str)
            else True
        )
        if invalid_digest:
            raise ManifestFormatError(f"Artifact {name!r} has an invalid SHA-256 digest")
        if not isinstance(artifact.run_id, str) or not artifact.run_id:
            raise ManifestFormatError(f"Artifact {name!r} has an invalid run_id")
        if not isinstance(artifact.schema_version, str) or not artifact.schema_version:
            raise ManifestFormatError(f"Artifact {name!r} has an invalid schema_version")
        if artifact.row_count is not None and (
            not isinstance(artifact.row_count, int)
            or isinstance(artifact.row_count, bool)
            or artifact.row_count < 0
        ):
            raise ManifestFormatError(f"Artifact {name!r} has an invalid row_count")
        filenames.add(artifact.filename)
        artifacts[name] = artifact

    taxonomy = payload.pop("anomaly_taxonomy", ())
    if not isinstance(taxonomy, (list, tuple)) or not all(
        isinstance(value, str) and value for value in taxonomy
    ):
        raise ManifestFormatError("Manifest anomaly_taxonomy must contain non-empty strings")
    if not isinstance(payload.get("run_id"), str) or not payload["run_id"]:
        raise ManifestFormatError("Manifest run_id must be a non-empty string")
    if not isinstance(payload.get("seed_source"), str) or not payload["seed_source"]:
        raise ManifestFormatError("Manifest seed_source must be a non-empty string")
    if (
        not isinstance(payload.get("random_seed"), int)
        or isinstance(payload["random_seed"], bool)
        or payload["random_seed"] < 0
    ):
        raise ManifestFormatError("Manifest random_seed must be a non-negative integer")
    if not isinstance(payload.get("schema_version"), str) or not payload["schema_version"]:
        raise ManifestFormatError("Manifest schema_version must be a non-empty string")
    generated_at = payload.get("generated_at")
    if not isinstance(generated_at, str) or not generated_at:
        raise ManifestFormatError("Manifest generated_at must be a non-empty timestamp")
    try:
        datetime.fromisoformat(generated_at)
    except ValueError as error:
        raise ManifestFormatError("Manifest generated_at must be an ISO-8601 timestamp") from error
    for field_name in ("source_file", "source_sha256", "code_version"):
        value = payload.get(field_name)
        if value is not None and not isinstance(value, str):
            raise ManifestFormatError(f"Manifest {field_name} must be a string or null")

    time_windows = payload.get("time_windows")
    if not isinstance(time_windows, dict) or not all(
        isinstance(key, str) and key and isinstance(value, str) and value
        for key, value in time_windows.items()
    ):
        raise ManifestFormatError("Manifest time_windows must map strings to strings")
    anomaly_counts = payload.get("anomaly_counts")
    if not isinstance(anomaly_counts, dict) or not all(
        isinstance(key, str)
        and key
        and isinstance(value, int)
        and not isinstance(value, bool)
        and value >= 0
        for key, value in anomaly_counts.items()
    ):
        raise ManifestFormatError(
            "Manifest anomaly_counts must map strings to non-negative integers"
        )
    run_parameters = payload.get("run_parameters")
    if not isinstance(run_parameters, dict) or not all(
        isinstance(key, str)
        and key
        and isinstance(value, (int, str))
        and not isinstance(value, bool)
        and (not isinstance(value, str) or bool(value))
        for key, value in run_parameters.items()
    ):
        raise ManifestFormatError(
            "Manifest run_parameters must map strings to integers or non-empty strings"
        )
    try:
        return RunManifest(**payload, anomaly_taxonomy=tuple(taxonomy), artifacts=artifacts)
    except (TypeError, ValueError) as error:
        raise ManifestFormatError(f"Manifest values are invalid: {error}") from error


def validate_manifest(run_directory: Path) -> RunManifest:
    """Reject any missing, mixed, mutated, or schema-incompatible artifact."""
    run_directory = run_directory.resolve()
    manifest_path = run_directory / "manifest.json"
    if manifest_path.is_symlink():
        raise ProvenanceError(f"Manifest cannot be a symbolic link: {manifest_path}")
    if not manifest_path.is_file():
        raise ArtifactMissingError(f"Manifest does not exist: {manifest_path}")
    manifest = read_manifest(manifest_path)

    if manifest.schema_version != SCHEMA_VERSION:
        raise SchemaVersionMismatchError(
            f"Manifest uses schema {manifest.schema_version!r}, expected {SCHEMA_VERSION!r}"
        )
    try:
        SeedSource(manifest.seed_source)
    except ValueError as error:
        supported = ", ".join(source.value for source in SeedSource)
        raise ProvenanceError(
            f"Manifest seed source {manifest.seed_source!r} is unsupported; "
            f"expected one of {supported}"
        ) from error
    if run_directory.name != manifest.run_id:
        raise RunIdMismatchError(
            f"Run directory {run_directory.name!r} does not match {manifest.run_id!r}"
        )

    for name, record in manifest.artifacts.items():
        filename = Path(record.filename)
        if filename.name != record.filename or filename.is_absolute():
            raise ManifestFormatError(f"Artifact {name!r} filename must be a plain basename")
        unresolved_path = run_directory / filename
        if unresolved_path.is_symlink():
            raise ProvenanceError(f"Artifact {name!r} cannot be a symbolic link")
        path = unresolved_path.resolve()
        if path.parent != run_directory:
            raise ProvenanceError(f"Artifact {name!r} escapes the run directory")
        if not path.is_file():
            raise ArtifactMissingError(f"Registered artifact {name!r} is missing: {path}")
        if record.run_id != manifest.run_id:
            raise RunIdMismatchError(f"Artifact record {name!r} has a different run ID")
        if record.schema_version != manifest.schema_version:
            raise SchemaVersionMismatchError(f"Artifact record {name!r} has a different schema")
        actual_hash = sha256_file(path)
        if actual_hash != record.sha256:
            raise ArtifactHashMismatchError(
                f"Artifact {name!r} hash mismatch: expected {record.sha256}, got {actual_hash}"
            )
        row_count, run_id, schema_version = _artifact_identity(path)
        if run_id != manifest.run_id:
            raise RunIdMismatchError(f"Artifact {name!r} belongs to run {run_id!r}")
        if schema_version != manifest.schema_version:
            raise SchemaVersionMismatchError(f"Artifact {name!r} uses schema {schema_version!r}")
        if row_count != record.row_count:
            raise ArtifactRowCountMismatchError(
                f"Artifact {name!r} row count is {row_count}, expected {record.row_count}"
            )
    return manifest
