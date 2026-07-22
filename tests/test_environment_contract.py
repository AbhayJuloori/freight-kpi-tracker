"""Contract tests for the portable Freight v2 development environment."""

from __future__ import annotations

import importlib
import importlib.metadata
import sys
import tomllib
from pathlib import Path

import pytest

from freight_v2.cli import main

ROOT = Path(__file__).resolve().parents[1]


def _project_config() -> dict:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)


def test_supported_python_range() -> None:
    assert (3, 11) <= sys.version_info[:2] < (3, 13)


def test_pyproject_exposes_portable_core_and_cli() -> None:
    project = _project_config()["project"]
    assert project["requires-python"] == ">=3.11,<3.13"
    assert project["scripts"]["freight-v2"] == "freight_v2.cli:main"

    dependencies = "\n".join(project["dependencies"]).lower()
    for package in ("pandas", "numpy", "pyarrow", "duckdb"):
        assert package in dependencies

    for hosted_or_legacy_package in (
        "snowflake-connector-python",
        "dash",
        "plotly",
        "matplotlib",
        "seaborn",
    ):
        assert hosted_or_legacy_package not in dependencies


def test_core_requirements_are_exact_and_synchronized() -> None:
    expected = {
        "pandas": "2.3.3",
        "numpy": "2.0.2",
        "pyarrow": "23.0.1",
        "duckdb": "1.5.4",
    }
    declared = _project_config()["project"]["dependencies"]
    assert set(declared) == {f"{package}=={version}" for package, version in expected.items()}
    assert set((ROOT / "requirements.txt").read_text().splitlines()) == set(declared)

    for package, version in expected.items():
        assert importlib.metadata.version(package) == version


@pytest.mark.parametrize("package", ["freight_v2", "pandas", "numpy", "pyarrow", "duckdb"])
def test_portable_package_import_surface(package: str) -> None:
    assert importlib.import_module(package) is not None


def test_cli_help_and_version_are_loadable(capsys: pytest.CaptureFixture[str]) -> None:
    for option, expected in (("--help", "Portable freight"), ("--version", "2.0.0.dev0")):
        with pytest.raises(SystemExit) as exit_info:
            main([option])
        assert exit_info.value.code == 0
        assert expected in capsys.readouterr().out


def test_private_keys_are_absent_and_ignored() -> None:
    assert not (ROOT / "rsa_key.p8").exists()
    assert not (ROOT / "rsa_key.pub").exists()

    ignore_rules = (ROOT / ".gitignore").read_text()
    assert "*.p8" in ignore_rules
    assert "*.pem" in ignore_rules
    assert "rsa_key*" in ignore_rules


def test_historical_carrier_scorecard_preserves_shipment_grain() -> None:
    sql = (ROOT / "sql" / "03_views_powerbi.sql").read_text()
    scorecard = sql.split("CREATE OR REPLACE VIEW VW_CARRIER_SCORECARD AS", 1)[1].split(
        "-- KPI 5", 1
    )[0]

    assert "WITH flagged_shipments AS" in scorecard
    assert "SELECT DISTINCT shipment_id" in scorecard
    assert "LEFT JOIN flagged_shipments" in scorecard
    assert "LEFT JOIN ANOMALY_FLAGS" not in scorecard
    assert "SUM(is_flagged)" in scorecard


def test_historical_dashboard_keeps_mode_and_uses_honest_chart_names() -> None:
    source = (ROOT / "scripts" / "dashboard.py").read_text()
    weekly = source.split("def fig_weekly_cpl", 1)[1].split("def fig_lane_spend_bar", 1)[0]
    carrier = source.split("def fig_carrier_scorecard", 1)[1].split("def eval_table", 1)[0]

    assert 'groupby("mode")["cpl"]' in weekly
    assert '.resample("W")' in weekly
    assert 'color="mode"' in weekly
    assert "def fig_lane_heatmap" not in source
    assert "def fig_lane_spend_bar" in source
    assert "Horizontal Spend Bar" in source
    assert 'groupby(["carrier_id", "mode"])' in carrier
    assert 'color="mode"' in carrier
    assert "Carrier Cost vs Service by Mode" in carrier
