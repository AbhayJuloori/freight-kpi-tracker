"""Tests for immutable derived artifacts and atomic public evidence export."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import freight_v2.export as export_module
from freight_v2.cli import main
from freight_v2.config import SCHEMA_VERSION, SeedSource
from freight_v2.detection import DETECTION_METHODS
from freight_v2.export import export_portfolio_bundle, validate_public_bundle
from freight_v2.provenance import ArtifactHashMismatchError, validate_manifest
from freight_v2.run_builder import build_run

DERIVED_ARTIFACTS = {
    "anomaly_flags",
    "lane_week_trends",
    "operational_alerts",
    "evaluation",
    "data_quality",
}
PUBLIC_FILES = {"manifest.json", "network.json", "alerts.json", "evaluation.json", "lanes"}
FORBIDDEN_PUBLIC_TERMS = {
    "is_anomaly",
    "anomaly_type",
    "changed_fields",
    "injected_magnitude",
    "anomaly_group_id",
    "private_key",
    "rsa_key",
}


@pytest.fixture(scope="module")
def built_run(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_run(
        artifact_root=tmp_path_factory.mktemp("export-runs"),
        seed_source=SeedSource.TEST,
        rows=500,
        random_seed=511,
        anomaly_seed=512,
        fixture_name="export",
    )


def _all_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*") if path.is_file())


def test_build_registers_complete_selected_derived_artifacts(built_run: Path) -> None:
    manifest = validate_manifest(built_run)
    assert set(manifest.artifacts) >= DERIVED_ARTIFACTS
    assert manifest.artifacts["anomaly_flags"].row_count == 500 * len(DETECTION_METHODS)
    for name in DERIVED_ARTIFACTS:
        assert (built_run / manifest.artifacts[name].filename).is_file()

    evaluation = json.loads((built_run / "evaluation.json").read_text(encoding="utf-8"))
    assert evaluation["run_id"] == manifest.run_id
    assert evaluation["schema_version"] == SCHEMA_VERSION
    assert sum(state["selected"] for state in evaluation["sensitivity_grid"]) == 1


def test_export_is_bounded_cross_referenced_and_truth_free(built_run: Path, tmp_path: Path) -> None:
    output = tmp_path / "public" / "freight" / "v2"
    assert export_portfolio_bundle(built_run, output) == output.resolve()
    payload = validate_public_bundle(output)
    assert {path.name for path in output.iterdir()} == PUBLIC_FILES
    assert payload["run_id"] == built_run.name
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["representative_lanes"]
    assert len(payload["alert_details"]) > 0
    roles = [entry["role"] for entry in payload["representative_lanes"]]
    assert roles == sorted(roles, key={"high": 0, "medium": 1, "data_quality": 2}.get)
    assert len(list((output / "lanes").glob("*.json"))) == len(payload["alert_details"])
    assert {entry["alert_id"] for entry in payload["alert_details"]} == {
        alert["alert_id"]
        for alert in json.loads((output / "alerts.json").read_text(encoding="utf-8"))["alerts"]
    }

    for path in _all_files(output):
        artifact = json.loads(path.read_text(encoding="utf-8"))
        assert artifact["run_id"] == built_run.name
        assert artifact["schema_version"] == SCHEMA_VERSION
        lowered = path.read_text(encoding="utf-8").lower()
        assert "private_key" not in lowered
        assert "rsa_key" not in lowered
        if path.parent.name == "lanes":
            assert not any(term in lowered for term in FORBIDDEN_PUBLIC_TERMS)

    evaluation = json.loads((output / "evaluation.json").read_text(encoding="utf-8"))
    states = evaluation["sensitivity_grid"]
    assert 0 < len(states) <= 25
    assert all(state["config_id"] for state in states)
    assert sum(state["selected"] for state in states) == 1


def test_export_is_byte_deterministic_for_one_validated_run(
    built_run: Path, tmp_path: Path
) -> None:
    first = export_portfolio_bundle(built_run, tmp_path / "first")
    second = export_portfolio_bundle(built_run, tmp_path / "second")
    first_files = {path.relative_to(first): path.read_bytes() for path in _all_files(first)}
    second_files = {path.relative_to(second): path.read_bytes() for path in _all_files(second)}
    assert first_files == second_files


def test_run_artifact_mismatch_is_rejected_before_export(built_run: Path, tmp_path: Path) -> None:
    evaluation_path = built_run / "evaluation.json"
    original = evaluation_path.read_bytes()
    try:
        payload = json.loads(original)
        payload["run_id"] = "another-run"
        evaluation_path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(ArtifactHashMismatchError):
            export_portfolio_bundle(built_run, tmp_path / "rejected")
    finally:
        evaluation_path.write_bytes(original)


def test_failed_staging_validation_preserves_exact_existing_target(
    built_run: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "existing"
    output.mkdir()
    marker = output / "keep.txt"
    marker.write_text("previous-good-bundle", encoding="utf-8")

    def fail_validation(_: Path) -> dict[str, object]:
        raise ValueError("forced staged validation failure")

    monkeypatch.setattr(export_module, "validate_public_bundle", fail_validation)
    with pytest.raises(ValueError, match="forced"):
        export_portfolio_bundle(built_run, output)
    assert marker.read_text(encoding="utf-8") == "previous-good-bundle"
    assert set(output.iterdir()) == {marker}


def test_cli_exports_latest_validated_run(
    built_run: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "cli-public"
    assert (
        main(
            [
                "export-portfolio",
                "--artifact-root",
                str(built_run.parent),
                "--latest",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert Path(capsys.readouterr().out.strip()) == output.resolve()
    validate_public_bundle(output)
