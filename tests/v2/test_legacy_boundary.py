"""Tests that historical Snowflake support cannot leak into Freight v2 defaults."""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load_script(name: str):
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"legacy_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("script_name", ["load_snowflake", "validate_load"])
def test_legacy_scripts_require_explicit_external_key(
    script_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_script(script_name)
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_FILE", raising=False)
    with pytest.raises(RuntimeError, match="explicit SNOWFLAKE_PRIVATE_KEY_FILE"):
        module._private_key_path()


@pytest.mark.parametrize("script_name", ["load_snowflake", "validate_load"])
def test_legacy_scripts_reject_missing_key_file(
    script_name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_script(script_name)
    missing = tmp_path / "missing-key.p8"
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(missing))
    with pytest.raises(FileNotFoundError, match="does not exist"):
        module._private_key_path()


@pytest.mark.parametrize("script_name", ["load_snowflake", "validate_load"])
def test_legacy_scripts_reject_relative_or_repository_local_keys(
    script_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _load_script(script_name)
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", "README.md")
    with pytest.raises(ValueError, match="absolute path"):
        module._private_key_path()

    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(ROOT / "README.md"))
    with pytest.raises(ValueError, match="outside this repository"):
        module._private_key_path()


@pytest.mark.parametrize("script_name", ["load_snowflake", "validate_load"])
def test_legacy_scripts_accept_explicit_external_key(
    script_name: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_script(script_name)
    key_path = tmp_path / "legacy-key.p8"
    key_path.write_text("test-only-placeholder", encoding="utf-8")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", str(key_path))
    assert module._private_key_path() == key_path.resolve()


def test_legacy_loader_resolves_data_and_sql_from_repository_not_caller_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    module = _load_script("load_snowflake")
    assert module.PROCESSED_DIR == ROOT / "data" / "processed"
    assert module.SQL_DIR == ROOT / "sql"


def test_v2_source_does_not_import_snowflake() -> None:
    for path in (ROOT / "src" / "freight_v2").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not any(name == "snowflake" or name.startswith("snowflake.") for name in imports)


@pytest.mark.parametrize("script_name", ["load_snowflake", "validate_load"])
def test_importing_legacy_module_does_not_load_optional_dependencies(script_name: str) -> None:
    before = set(sys.modules)
    _load_script(script_name)
    added_roots = {name.partition(".")[0] for name in set(sys.modules).difference(before)}
    assert added_roots.isdisjoint({"snowflake", "dotenv"})
