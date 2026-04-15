# Freight Cost Anomaly & KPI Tracker

## Stack
- Python 3.9+, pandas, numpy, scipy, Faker
- Snowflake (free trial) via snowflake-connector-python
- Power BI Desktop for dashboards
- SQL: DDL + anomaly detection (z-score, IQR, moving avg) + Power BI views

## Data Sources
- BTS FAF5 (real): regional commodity flows 2022-2024, ~87MB CSV from ORNL
- Synthetic: 75k shipment records generated using FAF5 as distribution seed
  - Includes: carrier rates, fuel surcharges (EIA diesel trajectory), on-time flags
  - 7% injected anomalies for validation

## Project Layout
- scripts/: Python ETL pipeline (download → generate → load → validate)
- sql/: DDL schema, anomaly detection, Power BI views
- data/raw/: FAF5 CSVs (gitignored)
- data/processed/: intermediate parquets (gitignored)
- tests/: pytest unit + integration tests

## Execution Model
Codex writes all files; Claude orchestrates only.
Correct Codex invocation: codex exec --json --sandbox <mode> "<prompt>"

## Snowflake Schema
FREIGHT_DB.LOGISTICS — 4 tables: SHIPMENTS, CARRIER_RATES, FUEL_SURCHARGES, ANOMALY_FLAGS
