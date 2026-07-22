# Legacy Snowflake Reproduction Boundary

The SQL and loader scripts in this repository describe Freight KPI Tracker v1. They are retained
for historical review and optional reproduction only. Freight v2 does not require or invoke them.

To reproduce v1 with a separately managed Snowflake account:

```bash
python3.11 -m venv .venv-legacy
.venv-legacy/bin/pip install -e '.[legacy]'
export SNOWFLAKE_ACCOUNT='your-account'
export SNOWFLAKE_USER='your-user'
export SNOWFLAKE_PRIVATE_KEY_FILE='/absolute/path/outside-this-repository/key.p8'
.venv-legacy/bin/python scripts/load_snowflake.py
.venv-legacy/bin/python scripts/validate_load.py
```

The scripts fail closed when the explicit key path is absent or does not resolve to a file. Do not
copy credentials into this repository. See `docs/history/snowflake-powerbi.md` for the original
data flow, the Power BI handoff, and the rationale for the portable v2 architecture.
