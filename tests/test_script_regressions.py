"""Regression tests for script entrypoints and validation helpers."""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("snowflake.connector")

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import evaluate_anomaly
import validate_load


class _StaticCursor:
    def __init__(self, result):
        self.result = result

    def execute(self, query):
        self.query = query

    def fetchone(self):
        return (self.result,)


class _ValidationCursor:
    def __init__(self):
        self.executed = []
        self.last_query = ""

    def execute(self, query):
        self.last_query = " ".join(query.split())
        self.executed.append(self.last_query)

    def fetchone(self):
        query = self.last_query
        scalar_results = {
            "SELECT COUNT(*) FROM SHIPMENTS": 75_000,
            "SELECT COUNT(*) FROM FUEL_SURCHARGES": 120,
            "SELECT COUNT(*) FROM CARRIER_RATES": 1_000,
            "SELECT COUNT(*) FROM GENERATION_RUNS": 1,
            "SELECT COUNT(*) FROM LANE_WEEK_TRENDS": 12,
            "SELECT COUNT(*) FROM ANOMALY_FLAGS": 250,
            "SELECT COUNT(*) FROM SHIPMENTS WHERE shipment_id IS NULL": 0,
            "SELECT COUNT(*) FROM SHIPMENTS WHERE ship_date IS NULL": 0,
            "SELECT COUNT(*) FROM SHIPMENTS WHERE mode IS NULL": 0,
            "SELECT COUNT(*) FROM SHIPMENTS WHERE carrier_id IS NULL": 0,
            "SELECT COUNT(*) FROM SHIPMENTS WHERE total_cost IS NULL": 0,
            "SELECT COUNT(*) FROM SHIPMENTS WHERE run_id IS NULL": 0,
            (
                "SELECT COUNT(DISTINCT af.shipment_id)::FLOAT / COUNT(DISTINCT s.shipment_id) "
                "AS flag_rate FROM SHIPMENTS s LEFT JOIN ANOMALY_FLAGS af "
                "ON s.shipment_id = af.shipment_id"
            ): 0.10,
        }
        if query not in scalar_results:
            raise AssertionError(f"Unexpected scalar query: {query}")
        return (scalar_results[query],)

    def fetchall(self):
        query = self.last_query
        if query == "SELECT flag_type, COUNT(*) FROM ANOMALY_FLAGS GROUP BY 1 ORDER BY 1":
            return [("IQR", 100), ("ZSCORE", 150)]
        if query == "SELECT mode, SUM(on_time_flag)::FLOAT / COUNT(*) AS rate FROM SHIPMENTS GROUP BY mode ORDER BY mode":
            return [("FTL", 0.91), ("LTL", 0.93), ("PARCEL", 0.96)]
        raise AssertionError(f"Unexpected tabular query: {query}")

    def close(self):
        return None


class _ValidationConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        return None


def test_evaluate_anomaly_normalizes_uppercase_flag_columns(monkeypatch, tmp_path, capsys):
    truth = pd.DataFrame(
        {
            "shipment_id": ["S1", "S2", "S3"],
            "is_anomaly": [1, 0, 1],
        }
    )
    flags_path = tmp_path / "anomaly_flags_export.csv"
    pd.DataFrame(
        {
            "FLAG_TYPE": ["ZSCORE", "IQR"],
            "SHIPMENT_ID": ["S1", "S3"],
        }
    ).to_csv(flags_path, index=False)

    (tmp_path / "generation_metadata.json").write_text(
        json.dumps({"seed_source": "TEST", "run_id": "run-123"})
    )

    monkeypatch.setattr(evaluate_anomaly, "PROCESSED_DIR", tmp_path)
    monkeypatch.setattr(
        evaluate_anomaly,
        "parse_args",
        lambda: SimpleNamespace(local=False, flags=flags_path),
    )
    monkeypatch.setattr(evaluate_anomaly.pd, "read_parquet", lambda path: truth)

    evaluate_anomaly.main()

    report = json.loads((tmp_path / "evaluation_report.json").read_text())
    assert set(report["per_method"]) == {"ZSCORE", "IQR"}
    assert "DISTINCT-SHIPMENT" in capsys.readouterr().out


def test_check_warns_when_null_count_is_nonzero(capsys):
    validate_load.check(
        _StaticCursor(3),
        "NULL run_id",
        "SELECT COUNT(*) FROM SHIPMENTS WHERE run_id IS NULL",
        None,
    )
    output = capsys.readouterr().out
    assert "[WARN] NULL run_id: 3 (expected 0)" in output


def test_validate_load_main_checks_generation_and_trend_tables(monkeypatch, capsys):
    cursor = _ValidationCursor()
    monkeypatch.setattr(validate_load, "get_conn", lambda: _ValidationConn(cursor))

    validate_load.main()

    output = capsys.readouterr().out
    assert "GENERATION_RUNS" in output
    assert "LANE_WEEK_TRENDS" in output
    assert "NULL run_id: 0 (expected 0)" in output
    assert any("SELECT COUNT(*) FROM GENERATION_RUNS" == query for query in cursor.executed)
    assert any("SELECT COUNT(*) FROM LANE_WEEK_TRENDS" == query for query in cursor.executed)
    assert any("SELECT COUNT(*) FROM SHIPMENTS WHERE run_id IS NULL" == query for query in cursor.executed)
