# Freight KPI Tracker — Improvements Design

**Date:** 2026-04-15  
**Status:** Approved

## Overview

Six targeted improvements addressing data credibility, schema correctness, anomaly detection quality, test coverage, evaluation rigor, and dashboard usability.

---

## Section 1: Data Generation (`scripts/generate_synthetic.py`)

### FAF5 as actual seed

- On startup, scan `data/raw/faf5_*.csv`. If absent and `--use-priors` not passed: raise `FileNotFoundError` with a clear message explaining what to download and from where.
- If `--use-priors` is passed: print a loud warning to stderr (`WARNING: --use-priors active; using hardcoded priors, not FAF5 data`) and continue with existing MODE_PROBS and lane pool.
- When FAF5 is present: parse origin/destination zone frequencies to weight the lane pool; parse mode splits by commodity to replace hardcoded `MODE_PROBS`.

### CARRIER_RATES as source of truth

- `generate_carrier_rates()` runs first. Lane IDs sampled **without replacement** per carrier×mode combo — no duplicate `(carrier_id, mode, lane_id)` PKs.
- `generate_shipments()` receives the rates DataFrame. For each shipment: sample a row from CARRIER_RATES → inherit `carrier_id`, `mode`, `lane_id`, `base_rate_per_cwt`. Compute:
  - `base_cost = base_rate_per_cwt × weight_cwt`
  - `total_cost = base_cost × (1 + fuel_surcharge_pct)`
- Join coverage between SHIPMENTS and CARRIER_RATES = 100% by construction.

### Schema fix

SHIPMENTS column `base_rate` → two columns:
- `base_rate_per_cwt FLOAT` — the per-hundredweight rate looked up from CARRIER_RATES
- `base_cost FLOAT` — the computed cost before fuel surcharge

### Seed provenance

After generation, write `data/processed/generation_metadata.json`:
```json
{
  "seed_source": "FAF5" | "PRIORS",
  "faf5_file": "<path or null>",
  "generated_at": "<ISO timestamp>",
  "n_shipments": 75000,
  "n_anomalies": 5250,
  "anomaly_rate": 0.07
}
```
Every downstream artifact (evaluation report, dashboard footer) reads this file and tags its output with `seed_source`. Resume claims are only credible when `seed_source = "FAF5"`.

---

## Section 2: SQL Schema (`sql/01_ddl_schema.sql`)

### SHIPMENTS changes

- Remove `base_rate FLOAT`
- Add `base_rate_per_cwt FLOAT NOT NULL`
- Add `base_cost FLOAT NOT NULL`
- Add `run_id VARCHAR(36)` — FK to `GENERATION_RUNS(run_id)` for row-level traceability

### New GENERATION_RUNS table

```sql
CREATE OR REPLACE TABLE GENERATION_RUNS (
    run_id          VARCHAR(36)   NOT NULL PRIMARY KEY,
    generated_at    TIMESTAMP_NTZ NOT NULL,
    seed_source     VARCHAR(20)   NOT NULL,  -- FAF5 | PRIORS | HYBRID | TEST
    faf5_file       VARCHAR(500),
    n_shipments     INT           NOT NULL,
    n_anomalies     INT           NOT NULL,
    anomaly_rate    FLOAT         NOT NULL
);
```

### New LANE_WEEK_TRENDS table

```sql
CREATE OR REPLACE TABLE LANE_WEEK_TRENDS (
    week_start        DATE         NOT NULL,
    lane_id           VARCHAR(10)  NOT NULL,
    mode              VARCHAR(10)  NOT NULL,
    shipment_count    INT          NOT NULL,
    avg_cost_per_lb   FLOAT        NOT NULL,
    rolling_4wk_avg   FLOAT,
    pct_deviation     FLOAT,
    is_anomalous      SMALLINT     NOT NULL DEFAULT 0,
    PRIMARY KEY (week_start, lane_id, mode)
);
```

Rolling signal is only trusted after `>= 4` prior observed weeks of data for that lane×mode. Early weeks are stored but `is_anomalous` remains 0.

### ANOMALY_FLAGS

No structural changes. MOVING_AVG method removed from inserts. Z-score and IQR remain shipment-level flags.

---

## Section 3: Anomaly Detection (`sql/02_anomaly_detection.sql`)

### Methods 1 & 2: unchanged

Z-score (by lane×mode, `|z| > 2.5`) and IQR (by lane×mode, 1.5×IQR fence) stay in `ANOMALY_FLAGS`. Logic unchanged.

### Method 3: MOVING_AVG → LANE_WEEK_TRENDS

Remove the `MOVING_AVG` insert into `ANOMALY_FLAGS`. Replace with a new block that:
1. Computes weekly avg CPL per lane×mode
2. Rolls a 4-week window (`ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING`)
3. Marks `is_anomalous = 1` where `pct_deviation > 0.20` **and** at least 4 prior weeks exist for that lane×mode
4. Inserts/merges results into `LANE_WEEK_TRENDS`

This separates shipment-level outlier detection from lane-level trend anomalies — different granularities, different tables, different query paths.

---

## Section 4: Evaluation Metrics (`scripts/evaluate_anomaly.py`)

### Inputs

- `data/processed/anomaly_ground_truth.parquet` — shipment-level truth labels
- `data/processed/anomaly_flags_export.csv` — exported from Snowflake `ANOMALY_FLAGS`; or `--local` flag to recompute Z-score and IQR in Python from `shipments.csv`, skipping Snowflake
- `data/processed/generation_metadata.json` — for provenance tagging

### Outputs

**Per-flag-row metrics** (each flag row is a positive prediction):
| method | precision | recall | f1 | fpr |
|--------|-----------|--------|----|-----|

**Distinct-shipment metrics** (deduplicated union across all methods — the headline number):
| precision | recall | f1 | fpr |

Both blocks tagged with `seed_source` from `generation_metadata.json`.

Writes `data/processed/evaluation_report.json`. Summary printed to stdout.

---

## Section 5: Integration Test (`tests/test_integration.py`)

Single end-to-end test on a 500-shipment fixture. No Snowflake connection. Fully deterministic (seeded RNG).

**Steps:**
1. Call generator with `--use-priors` and `n=500`
2. Assert stderr contains a priors-mode warning (`"priors" in stderr.lower()`) — no assertion on exact text
3. Assert `generation_metadata.json` has `seed_source = "PRIORS"`
4. Assert CARRIER_RATES has zero duplicate `(carrier_id, mode, lane_id)` PKs
5. Assert 100% of shipment `(carrier_id, mode, lane_id)` tuples exist in CARRIER_RATES
6. Run Z-score and IQR logic in Python against the 500-shipment fixture
7. Join against ground truth, assert per-method thresholds:
   - Z-score: `precision > 0.30`, `recall > 0.20`
   - IQR: `precision > 0.25`, `recall > 0.25`

Thresholds are method-specific and set conservatively for 500 rows — they catch catastrophic regressions, not statistical noise.

---

## Section 6: Dashboard (`scripts/dashboard.py`)

Replaces static `dashboard.html` with a **Dash + Plotly** app.

### Stack
- `dash`, `dash-bootstrap-components` for layout
- `plotly_dark` template, accent palette: amber (anomalies), steel-blue (normal), red (high-severity)
- Reads local CSVs/parquets + `generation_metadata.json` + `evaluation_report.json` on startup — no Snowflake required

### Layout

| Row | Content |
|-----|---------|
| 1 | 4 KPI cards: total shipments, anomaly rate, avg cost/lb, on-time % |
| 2 | Cost-per-lb violin by mode + anomaly overlay; dropdowns: mode, carrier, date range |
| 3 | Weekly avg CPL time-series per mode with anomaly spike highlights |
| 4 | Top-20 lane heatmap by total cost, colored by anomaly density |
| 5 | Carrier scorecard: on-time % vs avg cost, bubble = shipment volume |
| 6 | Evaluation panel: precision/recall/F1 table from `evaluation_report.json` |
| Footer | `seed_source` + `generated_at` from `generation_metadata.json` |

### New Makefile target
```makefile
dashboard:
    python scripts/dashboard.py
```

### Dependencies added to `requirements.txt`
- `dash`
- `dash-bootstrap-components`

---

## Files Changed

| File | Change |
|------|--------|
| `scripts/generate_synthetic.py` | FAF5 seeding, CARRIER_RATES-first generation, schema fix, provenance metadata |
| `scripts/evaluate_anomaly.py` | New — evaluation metrics |
| `scripts/dashboard.py` | Replace static HTML with Dash app |
| `scripts/load_snowflake.py` | Add GENERATION_RUNS insert, updated column names |
| `sql/01_ddl_schema.sql` | Schema changes: SHIPMENTS columns, new GENERATION_RUNS + LANE_WEEK_TRENDS |
| `sql/02_anomaly_detection.sql` | Remove MOVING_AVG from ANOMALY_FLAGS, add LANE_WEEK_TRENDS population |
| `tests/test_integration.py` | New — end-to-end fixture test |
| `tests/test_generate_synthetic.py` | Update column assertions for base_rate_per_cwt + base_cost |
| `docs/architecture.md` | Update to reflect new tables and data flow |
| `requirements.txt` | Add dash, dash-bootstrap-components |
| `Makefile` | Add dashboard target |
