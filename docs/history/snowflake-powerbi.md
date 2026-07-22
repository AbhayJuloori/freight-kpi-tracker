# Historical Snowflake and Power BI Architecture

## Status

This document records the original Freight KPI Tracker architecture. It is not the runtime
architecture for Freight v2. The original Snowflake trial account has been deleted, and the
portfolio experience no longer depends on Snowflake, Power BI, a cloud account, or a private key.

## What the original system demonstrated

The first implementation used a conventional warehouse-and-BI flow:

```text
FAF5-seeded synthetic shipment CSVs
  → Snowflake PUT and COPY INTO
  → FREIGHT_DB.LOGISTICS tables
  → SQL anomaly flags and lane-week views
  → Power BI-ready views and CSV exports
  → local Dash dashboard
```

The warehouse contained generation-run provenance, shipments, carrier rates, weekly fuel
surcharges, shipment-level anomaly flags, and lane-week trends. Snowflake SQL implemented the
original Z-score and IQR rules. `sql/03_views_powerbi.sql` created reporting views for carrier
scorecards, regional cost, anomaly volume, executive KPIs, and lane risk. The `powerbi/data/`
exports allowed the analysis to be reviewed without direct warehouse access.

This work remains relevant portfolio evidence: it shows warehouse modelling, staged bulk loads,
SQL analytical views, key-pair authentication, and BI handoff. It is presented as a historical
deployment, not as a currently available service.

## Why v2 moved away from it

The trial account expired, but portability was the more important design reason. A public
portfolio should not stop working because a warehouse trial ends, credentials rotate, or a
visitor lacks access to a proprietary BI tool. Freight v2 therefore owns its analytical state in
versioned Parquet files, validates those files with an immutable manifest, and uses DuckDB only as
an optional embedded query engine. The portfolio consumes a small, validated static evidence
bundle.

The migration also prevents the old dashboard from silently combining artifacts from different
runs. Every v2 artifact carries a run ID and schema version and is checked against its registered
SHA-256 hash before use.

## Credential boundary

The old repository-root `rsa_key.p8` and public-key files were deleted from the working tree and
private-key patterns are ignored. The associated account no longer exists. Published Git history
has not been rewritten because that is a separate destructive operation requiring explicit
authorization.

The retained legacy scripts never default to a repository key. Reproducing the historical path
requires all of the following to be provided deliberately:

- installation of the `legacy` dependency extra;
- a separately managed Snowflake account and role configuration;
- `SNOWFLAKE_ACCOUNT` and `SNOWFLAKE_USER`;
- an explicit `SNOWFLAKE_PRIVATE_KEY_FILE` pointing to a key outside the repository.

No Freight v2 command imports the Snowflake connector or reads these variables.
