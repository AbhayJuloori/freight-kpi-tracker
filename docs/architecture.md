# Data Flow Architecture

```text
BTS FAF5 CSV (data/raw/faf5_2022_2024.csv)
    ↓ generate_synthetic.py [--faf5-path PATH | --use-priors] [--n N]
    │   → Derives lane weights and mode distribution from FAF5 OD pairs
    │   → Generates CARRIER_RATES first with unique PKs, then derives shipments from it
    │   → Emits run_id into every output artifact
    ↓
data/processed/
    ├── shipments.csv                 (N rows, includes run_id, base_rate_per_cwt, base_cost)
    ├── carrier_rates.csv             (25 carriers × 3 modes × 30 lanes, unique PKs)
    ├── fuel_surcharges.csv           (weekly diesel trajectory)
    ├── anomaly_ground_truth.parquet  (ground-truth is_anomaly per shipment)
    └── generation_metadata.json      (run_id, seed_source, FAF5 path, generation stats)
    ↓ load_snowflake.py (PUT + COPY INTO)
Snowflake: FREIGHT_DB.LOGISTICS
    ├── GENERATION_RUNS        (one row per pipeline run, keyed by run_id)
    ├── SHIPMENTS              (N rows, FK to GENERATION_RUNS)
    ├── CARRIER_RATES          (rate dimension, 100% join coverage with SHIPMENTS)
    ├── FUEL_SURCHARGES        (weekly fuel history)
    ├── ANOMALY_FLAGS          (shipment-level Z-score and IQR flags only)
    └── LANE_WEEK_TRENDS       (lane × mode × week rolling deviation signal)
    ↓ evaluate_anomaly.py --local (or --flags from Snowflake export)
data/processed/evaluation_report.json
    (precision/recall/F1/FPR per method + distinct-shipment headline, tagged with seed_source)
    ↓ dashboard.py
http://localhost:8050  (Dash app reading local artifacts; no Snowflake connection required)
```

## Anomaly Detection Methods

1. **Z-Score by lane × mode** (`ANOMALY_FLAGS`) flags `|z| > 2.5` on `cost_per_lb` and requires at least 10 shipments per lane/mode segment.
2. **IQR fences by lane × mode** (`ANOMALY_FLAGS`) flags values outside `Q1 - 1.5×IQR` and `Q3 + 1.5×IQR`, also with a 10-shipment minimum.
3. **4-week rolling deviation** (`LANE_WEEK_TRENDS`) operates on weekly lane aggregates only; `is_anomalous = 1` when `|pct_deviation| > 20%` and at least 4 prior observed weeks exist. These trend anomalies are not projected back onto individual shipments.

## Evaluation Framework

`evaluate_anomaly.py --local` recomputes Z-score and IQR in pandas using the same shipment-level logic as the Snowflake SQL and writes `data/processed/evaluation_report.json`.

It reports:

- Per-method precision, recall, F1, and false-positive rate against `anomaly_ground_truth.parquet`
- A distinct-shipment headline metric based on the deduplicated union of all methods
- Provenance tags from `generation_metadata.json`, including `seed_source` and `run_id`

Claims about anomaly quality are only credible when the run is traceable through `GENERATION_RUNS` and `seed_source=FAF5`.

## Dash Dashboard

Run `make dashboard` to start the local Dash app at `http://localhost:8050`.

The dashboard reads local processed artifacts and presents:

- KPI cards for shipment volume, anomaly rate, average cost per pound, and on-time rate
- Cost-per-pound distribution views with anomaly overlays
- Weekly trend charts with lane-week spike highlighting from `LANE_WEEK_TRENDS`
- Carrier and lane performance views
- An evaluation panel sourced from `evaluation_report.json`
- A provenance footer showing `seed_source`, `generated_at`, and `run_id`
