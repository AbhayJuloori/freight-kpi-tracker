"""
Load processed CSVs into Snowflake using PUT + COPY INTO.
Requires .env with SNOWFLAKE_* credentials.
Usage: python scripts/load_snowflake.py
"""
import json
import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

PROCESSED_DIR = Path('data/processed')
SQL_DIR = Path('sql')

TABLES = [
    ('FUEL_SURCHARGES',  PROCESSED_DIR / 'fuel_surcharges.csv'),
    ('CARRIER_RATES',    PROCESSED_DIR / 'carrier_rates.csv'),
    ('SHIPMENTS',        PROCESSED_DIR / 'shipments.csv'),
]


def get_conn():
    return snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        private_key_file=os.environ.get('SNOWFLAKE_PRIVATE_KEY_FILE', 'rsa_key.p8'),
        warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.environ.get('SNOWFLAKE_DATABASE', 'FREIGHT_DB'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'LOGISTICS'),
    )


def run_sql_file(cursor, path: Path):
    sql = path.read_text()
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    for stmt in statements:
        cursor.execute(stmt)


def insert_generation_run(cursor, metadata: dict) -> None:
    cursor.execute(
        '''
        INSERT INTO GENERATION_RUNS
            (run_id, generated_at, seed_source, faf5_file, n_shipments, n_anomalies, anomaly_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''',
        (
            metadata['run_id'],
            metadata['generated_at'],
            metadata['seed_source'],
            metadata.get('faf5_file'),
            metadata['n_shipments'],
            metadata['n_anomalies'],
            metadata['anomaly_rate'],
        ),
    )
    print(f"  GENERATION_RUNS: inserted run_id={metadata['run_id']}")


def stage_and_copy(cursor, table_name: str, csv_path: Path):
    abs_path = csv_path.resolve()
    print(f'  Staging {abs_path} -> @%{table_name}')
    cursor.execute(
        f'PUT file://{abs_path} @%{table_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE'
    )
    print(f'  COPY INTO {table_name}')
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
    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    count = cursor.fetchone()[0]
    print(f'  {table_name}: {count:,} rows loaded')


def main():
    metadata_path = PROCESSED_DIR / 'generation_metadata.json'
    if not metadata_path.exists():
        raise FileNotFoundError(
            f'{metadata_path} not found — run generate_synthetic.py first'
        )
    metadata = json.loads(metadata_path.read_text())

    print('Connecting to Snowflake...')
    conn = get_conn()
    cursor = conn.cursor()

    print('Creating schema...')
    run_sql_file(cursor, SQL_DIR / '01_ddl_schema.sql')

    print('Loading tables...')
    insert_generation_run(cursor, metadata)
    for table_name, csv_path in TABLES:
        if not csv_path.exists():
            raise FileNotFoundError(f'{csv_path} not found — run generate_synthetic.py first')
        stage_and_copy(cursor, table_name, csv_path)

    print('Running anomaly detection...')
    run_sql_file(cursor, SQL_DIR / '02_anomaly_detection.sql')

    print('Creating views...')
    run_sql_file(cursor, SQL_DIR / '03_views_powerbi.sql')

    cursor.close()
    conn.close()
    print(f"Done. seed_source={metadata['seed_source']} run_id={metadata['run_id']}")


if __name__ == '__main__':
    main()
