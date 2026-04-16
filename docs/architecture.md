# Data Flow Architecture

```
BTS FAF5 CSV (data/raw/faf5_2022_2024.csv)
    ↓ generate_synthetic.py [--faf5-path | --use-priors] [--n N]
    │   → Derives lane weights and mode distribution from FAF5 OD pairs
    │   → Generates CARRIER_RATES first (unique PKs), then derives shipments from it
    │   → Emits run_id into every output artifact
    ↓
data/processed/
    ├── shipments.csv          (N rows, includes run_id, base_rate_per_cwt, base_cost)
    ├── carrier_rates.csv      (25 carriers × 3 modes × 30 lanes, unique PKs)
    ├── fuel_surcharges.csv    (weekly diesel trajectory)
    ├── anomaly_ground_truth.parquet  (ground truth is_anomaly per shipment)
    └── generation_metadata.json     (run_id, seed_source, FAF5 path, stats)
    ↓ load_snowflake.py (PUT + COPY INTO)
Snowflake: FREIGHT_DB.LOGISTICS
    ├── GENERATION_RUNS        (one row per pipeline run, seed_source, run_id)
    ├── SHIPMENTS              (N rows, FK to GENERATION_RUNS)
    ├── CARRIER_RATES          (rate dimension, 100% join coverage with SHIPMENTS)
    ├── FUEL_SURCHARGES        (130 weeks)
    ├── ANOMALY_FLAGS          (Z-score + IQR shipment-level flags only)
    └── LANE_WEEK_TRENDS       (rolling 4-week deviation, lane×week granularity)
    ↓ evaluate_anomaly.py --local (or --flags from Snowflake export)
data/processed/evaluation_report.json
    (precision/recall/F1/FPR per method + distinct-shipment headline, tagged with seed_source)
    ↓ dashboard.py
http://localhost:8050  (Dash app, no Snowflake needed)
```

## Anomaly Detection Methods

1. **Z-Score by lane × mode** (`ANOMALY_FLAGS`) — flags `|z| > 2.5` on `cost_per_lb`; requires `≥10` shipments per lane×mode
2. **IQR fences by lane × mode** (`ANOMALY_FLAGS`) — flags outside `Q1 - 1.5×IQR` / `Q3 + 1.5×IQR`; requires `≥10` shipments
3. **4-week rolling deviation** (`LANE_WEEK_TRENDS`) — lane-week aggregates only; `is_anomalous=1` when `|pct_deviation| > 20%` AND `≥4` prior observed weeks; not propagated to individual shipments

## Evaluation

`evaluate_anomaly.py --local` recomputes Z-score and IQR in pandas (mirrors SQL logic) and reports:
- Per-method: precision, recall, F1, FPR vs `anomaly_ground_truth.parquet`
- Distinct-shipment (deduped union of all methods): the headline metric for portfolio/resume use
- All metrics tagged with `seed_source` — claims are only credible when `seed_source=FAF5`

## Dashboard

Run `make dashboard` → opens at `http://localhost:8050`
- KPI cards, cost-per-lb violin by mode, weekly trend with anomaly spikes
- Top-20 lane heatmap by anomaly density
- Carrier scorecard (on-time rate vs avg cost, bubble = volume)
- Evaluation metrics panel (requires `make evaluate` first)
