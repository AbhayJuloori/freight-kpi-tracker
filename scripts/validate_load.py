"""Legacy post-load validation for the historical Snowflake v1 deployment."""

import os
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LEGACY_WARNING = (
    "LEGACY SNOWFLAKE PATH: this command validates the retired v1 warehouse, "
    "not the portable Freight v2 engine."
)

EXPECTED_ROWS = {
    "SHIPMENTS": 75_000,
    "FUEL_SURCHARGES": 100,  # ~130 weeks, allow some variance
    "CARRIER_RATES": 1_000,  # 25 carriers × 3 modes × N lanes
}


def _load_optional_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError:
        return
    load_dotenv()


def _private_key_path() -> Path:
    configured = os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE")
    if not configured:
        raise RuntimeError(
            "Legacy Snowflake access requires an explicit SNOWFLAKE_PRIVATE_KEY_FILE path"
        )
    configured_path = Path(configured).expanduser()
    if not configured_path.is_absolute():
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_FILE must be an absolute path")
    path = configured_path.resolve()
    if path.is_relative_to(REPOSITORY_ROOT):
        raise ValueError("Snowflake private keys must be stored outside this repository")
    if path.suffix.lower() not in {".p8", ".pem"}:
        raise ValueError("Snowflake private key must use a .p8 or .pem extension")
    if not path.is_file():
        raise FileNotFoundError(f"Configured Snowflake private key does not exist: {path}")
    return path


def get_conn():
    key_path = _private_key_path()
    try:
        import snowflake.connector
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Legacy Snowflake support is not installed; run `pip install -e '.[legacy]'`"
        ) from error
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=str(key_path),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FREIGHT_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "LOGISTICS"),
    )


def check(cursor, label: str, query: str, expected=None):
    cursor.execute(query)
    result = cursor.fetchone()[0]
    if expected is None:
        is_null_check = label.startswith("NULL ")
        if is_null_check:
            status = "OK" if result == 0 else "WARN"
            expectation = " (expected 0)"
        else:
            status = "OK" if result not in (None, 0, 0.0, "") else "WARN"
            expectation = " (expected non-zero/non-empty result)"
    else:
        status = "OK" if result >= expected else "WARN"
        expectation = f" (expected >= {expected:,})"
    print(f"  [{status}] {label}: {result:,}{expectation}")
    return result


def main():
    print(LEGACY_WARNING)
    _load_optional_dotenv()
    conn = get_conn()
    cursor = conn.cursor()

    print("\n=== Row Counts ===")
    for table, min_rows in EXPECTED_ROWS.items():
        check(cursor, table, f"SELECT COUNT(*) FROM {table}", min_rows)
    check(cursor, "GENERATION_RUNS", "SELECT COUNT(*) FROM GENERATION_RUNS", 1)
    check(cursor, "LANE_WEEK_TRENDS", "SELECT COUNT(*) FROM LANE_WEEK_TRENDS", 1)
    check(cursor, "ANOMALY_FLAGS", "SELECT COUNT(*) FROM ANOMALY_FLAGS", 1)

    print("\n=== Null Checks (SHIPMENTS) ===")
    for col in ["shipment_id", "ship_date", "mode", "carrier_id", "total_cost", "run_id"]:
        check(cursor, f"NULL {col}", f"SELECT COUNT(*) FROM SHIPMENTS WHERE {col} IS NULL", None)

    print("\n=== Anomaly Flag Rate ===")
    cursor.execute("""
        SELECT
            COUNT(DISTINCT af.shipment_id)::FLOAT / COUNT(DISTINCT s.shipment_id) AS flag_rate
        FROM SHIPMENTS s
        LEFT JOIN ANOMALY_FLAGS af ON s.shipment_id = af.shipment_id
    """)
    flag_rate = cursor.fetchone()[0]
    status = "OK" if 0.05 <= flag_rate <= 0.20 else "WARN"
    print(f"  [{status}] Anomaly flag rate: {flag_rate:.1%} (expected 5-20%)")

    print("\n=== Anomaly Methods Breakdown ===")
    cursor.execute("SELECT flag_type, COUNT(*) FROM ANOMALY_FLAGS GROUP BY 1 ORDER BY 1")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,}")

    print("\n=== On-Time Rate by Mode ===")
    cursor.execute("""
        SELECT mode, SUM(on_time_flag)::FLOAT / COUNT(*) AS rate
        FROM SHIPMENTS GROUP BY mode ORDER BY mode
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:.1%}")

    cursor.close()
    conn.close()
    print("\nValidation complete.")


if __name__ == "__main__":
    main()
