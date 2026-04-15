"""
Load processed CSVs into Snowflake using PUT + COPY INTO.
Requires .env with SNOWFLAKE_* credentials.
Usage: python scripts/load_snowflake.py
"""
import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

PROCESSED_DIR = Path("data/processed")
SQL_DIR = Path("sql")

TABLES = [
    ("FUEL_SURCHARGES", PROCESSED_DIR / "fuel_surcharges.csv"),
    ("CARRIER_RATES",   PROCESSED_DIR / "carrier_rates.csv"),
    ("SHIPMENTS",       PROCESSED_DIR / "shipments.csv"),
]


def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE", "rsa_key.p8"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FREIGHT_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "LOGISTICS"),
    )


def run_sql_file(cursor, path: Path):
    """Execute a multi-statement SQL file, splitting on semicolons."""
    sql = path.read_text()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        cursor.execute(stmt)


def stage_and_copy(cursor, table_name: str, csv_path: Path):
    abs_path = csv_path.resolve()
    print(f"  Staging {abs_path} → @%{table_name}")
    cursor.execute(
        f"PUT file://{abs_path} @%{table_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
    )
    print(f"  COPY INTO {table_name}")
    cursor.execute(f"""
        COPY INTO {table_name}
        FROM @%{table_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null')
        )
        ON_ERROR = 'CONTINUE'
        PURGE = TRUE
    """)
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"  {table_name}: {count:,} rows loaded")


def main():
    print("Connecting to Snowflake...")
    conn = get_conn()
    cursor = conn.cursor()

    # Bootstrap schema
    print("\nCreating schema...")
    run_sql_file(cursor, SQL_DIR / "01_ddl_schema.sql")

    # Load tables
    print("\nLoading tables...")
    for table_name, csv_path in TABLES:
        if not csv_path.exists():
            raise FileNotFoundError(f"{csv_path} not found — run generate_synthetic.py first")
        stage_and_copy(cursor, table_name, csv_path)

    # Run anomaly detection
    print("\nRunning anomaly detection...")
    run_sql_file(cursor, SQL_DIR / "02_anomaly_detection.sql")

    # Create Power BI views
    print("\nCreating Power BI views...")
    run_sql_file(cursor, SQL_DIR / "03_views_powerbi.sql")

    cursor.close()
    conn.close()
    print("\nDone. Snowflake is ready for Power BI.")


if __name__ == "__main__":
    main()
