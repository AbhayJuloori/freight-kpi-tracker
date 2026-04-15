"""
Post-load validation: row counts, null checks, anomaly flag rate sanity check.
Usage: python scripts/validate_load.py
"""
import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

EXPECTED_ROWS = {
    "SHIPMENTS": 75_000,
    "FUEL_SURCHARGES": 100,   # ~130 weeks, allow some variance
    "CARRIER_RATES": 1_000,   # 25 carriers × 3 modes × N lanes
}


def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FREIGHT_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "LOGISTICS"),
    )


def check(cursor, label: str, query: str, expected=None):
    cursor.execute(query)
    result = cursor.fetchone()[0]
    status = "OK" if (expected is None or result >= expected) else "WARN"
    print(f"  [{status}] {label}: {result:,}" + (f" (expected >= {expected:,})" if expected else ""))
    return result


def main():
    conn = get_conn()
    cursor = conn.cursor()

    print("\n=== Row Counts ===")
    for table, min_rows in EXPECTED_ROWS.items():
        check(cursor, table, f"SELECT COUNT(*) FROM {table}", min_rows)
    check(cursor, "ANOMALY_FLAGS", "SELECT COUNT(*) FROM ANOMALY_FLAGS")

    print("\n=== Null Checks (SHIPMENTS) ===")
    for col in ["shipment_id", "ship_date", "mode", "carrier_id", "total_cost"]:
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
