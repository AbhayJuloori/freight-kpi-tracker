# Data Flow Architecture

```
BTS FAF5 CSV (real regional freight data)
    ↓ download_data.py
data/raw/faf5_2022_2024.csv
    ↓ generate_synthetic.py (uses FAF5 as distribution seed)
data/processed/shipments.csv
data/processed/carrier_rates.csv
data/processed/fuel_surcharges.csv
    ↓ load_snowflake.py (PUT + COPY INTO)
Snowflake: FREIGHT_DB.LOGISTICS
    ├── SHIPMENTS (75k rows)
    ├── CARRIER_RATES (25 carriers × 3 modes × ~900 lanes)
    ├── FUEL_SURCHARGES (130 weeks)
    └── ANOMALY_FLAGS (populated by anomaly_detection.sql)
    ↓ Power BI DirectQuery / Import
Dashboard: 4 pages (Executive, Carrier, Cost, Anomaly Explorer)
```

## Anomaly Detection Methods

1. **Z-Score by lane × mode** — flags |z| > 2.5 on cost_per_lb
2. **IQR fences** — flags outside Q1 - 1.5×IQR / Q3 + 1.5×IQR
3. **4-week moving average deviation** — flags weeks where cost deviates >20% from rolling avg

## Power BI Pages
- Executive Summary: total shipments, avg cost, on-time rate, anomaly count
- Carrier Performance: on-time rate by carrier, cost scatter by mode
- Cost Analysis: cost/lb trend by mode, map by origin state
- Anomaly Explorer: flagged shipments table with drill-through
