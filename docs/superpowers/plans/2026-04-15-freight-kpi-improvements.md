# Freight KPI Tracker Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix data credibility (FAF5 seeding, join coverage, schema units), improve anomaly detection granularity, add evaluation metrics, add an integration test, and replace the static dashboard with a Dash app.

**Architecture:** `generate_synthetic.py` is rewritten to (1) parse FAF5 OD pairs as distribution weights, (2) generate CARRIER_RATES first with unique PKs, (3) derive each shipment's carrier/mode/lane/rate by sampling from CARRIER_RATES, and (4) emit a `run_id` into every output artifact. Anomaly SQL is split: shipment-level flags stay in `ANOMALY_FLAGS`; lane-week trend detection moves to a new `LANE_WEEK_TRENDS` table. A new `evaluate_anomaly.py` computes precision/recall/F1/FPR per method. A new `dashboard.py` Dash app reads local CSVs and self-documents its `seed_source`.

**Tech Stack:** Python 3.9, pandas, numpy, scipy, Dash, dash-bootstrap-components, Plotly, Snowflake (SQL only — no Snowflake connection needed for dashboard or integration test), pytest.

**Working directory for all commands:** `/Users/abhayjuloori/projects/freight-kpi-tracker`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `sql/01_ddl_schema.sql` | Modify | Add `base_rate_per_cwt`, `base_cost`, `run_id` to SHIPMENTS; add `GENERATION_RUNS`, `LANE_WEEK_TRENDS` tables |
| `sql/02_anomaly_detection.sql` | Modify | Remove MOVING_AVG from ANOMALY_FLAGS; add LANE_WEEK_TRENDS population block |
| `scripts/generate_synthetic.py` | Rewrite | FAF5 seeding, CARRIER_RATES-first generation, run_id propagation, CLI args |
| `tests/test_generate_synthetic.py` | Modify | Update column assertions for `base_rate_per_cwt` + `base_cost`; add PK uniqueness + join coverage tests |
| `scripts/load_snowflake.py` | Modify | Load GENERATION_RUNS from metadata; add LANE_WEEK_TRENDS to load list |
| `scripts/evaluate_anomaly.py` | Create | Compute precision/recall/F1/FPR per method against ground truth; write `evaluation_report.json` |
| `tests/test_integration.py` | Create | End-to-end 500-shipment fixture: generate → local anomaly flags → assert metrics |
| `scripts/dashboard.py` | Create | Dash app reading local CSVs; KPI cards, charts, evaluation panel, provenance footer |
| `requirements.txt` | Modify | Add `dash`, `dash-bootstrap-components` |
| `Makefile` | Modify | Add `dashboard`, `evaluate` targets; add `generate` args support |
| `docs/architecture.md` | Modify | Update to reflect new tables, data flow, Dash dashboard |

---

## Task 1: SQL Schema — DDL and Anomaly Detection

**Files:**
- Modify: `sql/01_ddl_schema.sql`
- Modify: `sql/02_anomaly_detection.sql`

- [ ] **Step 1: Rewrite `sql/01_ddl_schema.sql`**

Replace the entire file with:

```sql
-- ============================================================
-- FREIGHT_DB schema bootstrap
-- Run once after Snowflake account setup
-- ============================================================

CREATE DATABASE IF NOT EXISTS FREIGHT_DB;
CREATE SCHEMA IF NOT EXISTS FREIGHT_DB.LOGISTICS;

USE SCHEMA FREIGHT_DB.LOGISTICS;

-- ── Generation run provenance ────────────────────────────────
CREATE OR REPLACE TABLE GENERATION_RUNS (
    run_id          VARCHAR(36)   NOT NULL PRIMARY KEY,
    generated_at    TIMESTAMP_NTZ NOT NULL,
    seed_source     VARCHAR(20)   NOT NULL,  -- FAF5 | PRIORS | HYBRID | TEST
    faf5_file       VARCHAR(500),
    n_shipments     INT           NOT NULL,
    n_anomalies     INT           NOT NULL,
    anomaly_rate    FLOAT         NOT NULL
);

-- ── Core shipment fact table ─────────────────────────────────
CREATE OR REPLACE TABLE SHIPMENTS (
    shipment_id         VARCHAR(12)   NOT NULL PRIMARY KEY,
    ship_date           DATE          NOT NULL,
    origin_city         VARCHAR(50)   NOT NULL,
    dest_city           VARCHAR(50)   NOT NULL,
    mode                VARCHAR(10)   NOT NULL,   -- PARCEL | LTL | FTL
    carrier_id          VARCHAR(12)   NOT NULL,
    weight_lbs          FLOAT         NOT NULL,
    base_rate_per_cwt   FLOAT         NOT NULL,   -- $/cwt from CARRIER_RATES lookup
    base_cost           FLOAT         NOT NULL,   -- base_rate_per_cwt * weight_cwt
    fuel_surcharge_pct  FLOAT         NOT NULL,
    total_cost          FLOAT         NOT NULL,
    on_time_flag        SMALLINT      NOT NULL,   -- 1=on-time, 0=late
    lane_id             VARCHAR(10)   NOT NULL,   -- e.g. IL-CA
    run_id              VARCHAR(36)   REFERENCES GENERATION_RUNS(run_id)
)
CLUSTER BY (ship_date, mode, lane_id);

-- ── Carrier rate dimension ───────────────────────────────────
CREATE OR REPLACE TABLE CARRIER_RATES (
    carrier_id          VARCHAR(12)   NOT NULL,
    mode                VARCHAR(10)   NOT NULL,
    lane_id             VARCHAR(10)   NOT NULL,
    base_rate_per_cwt   FLOAT         NOT NULL,
    effective_date      DATE          NOT NULL,
    PRIMARY KEY (carrier_id, mode, lane_id, effective_date)
);

-- ── Weekly fuel surcharge table ──────────────────────────────
CREATE OR REPLACE TABLE FUEL_SURCHARGES (
    week_start              DATE    NOT NULL PRIMARY KEY,
    fuel_price_per_gallon   FLOAT   NOT NULL,
    surcharge_pct           FLOAT   NOT NULL
);

-- ── Shipment anomaly flags (Z-score + IQR only) ──────────────
CREATE OR REPLACE TABLE ANOMALY_FLAGS (
    flag_id             VARCHAR(20)   NOT NULL PRIMARY KEY,
    shipment_id         VARCHAR(12)   NOT NULL REFERENCES SHIPMENTS(shipment_id),
    flag_type           VARCHAR(30)   NOT NULL,   -- ZSCORE | IQR
    lane_id             VARCHAR(10),
    mode                VARCHAR(10),
    region              VARCHAR(50),
    cost_value          FLOAT,
    z_score             FLOAT,
    flag_date           DATE          NOT NULL,
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── Lane-week trend anomalies (rolling deviation signal) ─────
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

- [ ] **Step 2: Rewrite `sql/02_anomaly_detection.sql`**

Replace the entire file with:

```sql
-- ============================================================
-- ANOMALY DETECTION — populates ANOMALY_FLAGS and LANE_WEEK_TRENDS
-- Run after each data load. Truncate first to avoid dupes.
-- ============================================================
USE SCHEMA FREIGHT_DB.LOGISTICS;

TRUNCATE TABLE ANOMALY_FLAGS;
TRUNCATE TABLE LANE_WEEK_TRENDS;

-- ── Method 1: Z-Score by lane × mode ────────────────────────
INSERT INTO ANOMALY_FLAGS (
    flag_id, shipment_id, flag_type, lane_id, mode,
    region, cost_value, z_score, flag_date
)
WITH lane_stats AS (
    SELECT
        lane_id,
        mode,
        AVG(total_cost / NULLIF(weight_lbs, 0))    AS mean_cpl,
        STDDEV(total_cost / NULLIF(weight_lbs, 0)) AS std_cpl
    FROM SHIPMENTS
    GROUP BY lane_id, mode
    HAVING COUNT(*) >= 10
),
scored AS (
    SELECT
        s.shipment_id,
        s.lane_id,
        s.mode,
        s.origin_city                                           AS region,
        s.total_cost                                           AS cost_value,
        s.ship_date,
        (s.total_cost / NULLIF(s.weight_lbs, 0) - ls.mean_cpl)
            / NULLIF(ls.std_cpl, 0)                            AS z_score
    FROM SHIPMENTS s
    JOIN lane_stats ls ON s.lane_id = ls.lane_id AND s.mode = ls.mode
)
SELECT
    'ZSCORE_' || shipment_id,
    shipment_id,
    'ZSCORE',
    lane_id,
    mode,
    region,
    cost_value,
    z_score,
    ship_date
FROM scored
WHERE ABS(z_score) > 2.5;


-- ── Method 2: IQR on cost_per_lb ────────────────────────────
INSERT INTO ANOMALY_FLAGS (
    flag_id, shipment_id, flag_type, lane_id, mode,
    region, cost_value, z_score, flag_date
)
WITH iqr_bounds AS (
    SELECT
        lane_id,
        mode,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_cost / NULLIF(weight_lbs, 0)) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_cost / NULLIF(weight_lbs, 0)) AS q3
    FROM SHIPMENTS
    GROUP BY lane_id, mode
    HAVING COUNT(*) >= 10
),
bounds AS (
    SELECT
        lane_id,
        mode,
        q1 - 1.5 * (q3 - q1) AS lower_fence,
        q3 + 1.5 * (q3 - q1) AS upper_fence
    FROM iqr_bounds
)
SELECT
    'IQR_' || s.shipment_id,
    s.shipment_id,
    'IQR',
    s.lane_id,
    s.mode,
    s.origin_city,
    s.total_cost,
    NULL,
    s.ship_date
FROM SHIPMENTS s
JOIN bounds b ON s.lane_id = b.lane_id AND s.mode = b.mode
WHERE (s.total_cost / NULLIF(s.weight_lbs, 0)) > b.upper_fence
   OR (s.total_cost / NULLIF(s.weight_lbs, 0)) < b.lower_fence;


-- ── Lane-week trend detection → LANE_WEEK_TRENDS ────────────
-- Rolling signal trusted only after >= 4 prior observed weeks.
INSERT INTO LANE_WEEK_TRENDS (
    week_start, lane_id, mode, shipment_count,
    avg_cost_per_lb, rolling_4wk_avg, pct_deviation, is_anomalous
)
WITH weekly_avg AS (
    SELECT
        DATE_TRUNC('WEEK', ship_date)              AS week_start,
        lane_id,
        mode,
        COUNT(*)                                   AS shipment_count,
        AVG(total_cost / NULLIF(weight_lbs, 0))   AS avg_cpl
    FROM SHIPMENTS
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 5
),
prior_week_counts AS (
    SELECT
        week_start,
        lane_id,
        mode,
        COUNT(*) OVER (
            PARTITION BY lane_id, mode
            ORDER BY week_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_weeks
    FROM weekly_avg
),
rolling AS (
    SELECT
        w.week_start,
        w.lane_id,
        w.mode,
        w.shipment_count,
        w.avg_cpl,
        AVG(w.avg_cpl) OVER (
            PARTITION BY w.lane_id, w.mode
            ORDER BY w.week_start
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS rolling_4wk_avg,
        p.prior_weeks
    FROM weekly_avg w
    JOIN prior_week_counts p
        ON w.week_start = p.week_start
        AND w.lane_id = p.lane_id
        AND w.mode = p.mode
)
SELECT
    week_start,
    lane_id,
    mode,
    shipment_count,
    avg_cpl                                           AS avg_cost_per_lb,
    rolling_4wk_avg,
    (avg_cpl - rolling_4wk_avg)
        / NULLIF(rolling_4wk_avg, 0)                  AS pct_deviation,
    CASE
        WHEN prior_weeks >= 4
         AND rolling_4wk_avg IS NOT NULL
         AND ABS((avg_cpl - rolling_4wk_avg) / NULLIF(rolling_4wk_avg, 0)) > 0.20
        THEN 1
        ELSE 0
    END                                               AS is_anomalous
FROM rolling;


-- Verify
SELECT flag_type, COUNT(*) AS flags FROM ANOMALY_FLAGS GROUP BY flag_type ORDER BY 1;
SELECT COUNT(DISTINCT shipment_id) AS unique_flagged_shipments FROM ANOMALY_FLAGS;
SELECT COUNT(*) AS trend_rows, SUM(is_anomalous) AS anomalous_weeks FROM LANE_WEEK_TRENDS;
```

- [ ] **Step 3: Commit**

```bash
git add sql/01_ddl_schema.sql sql/02_anomaly_detection.sql
git commit -m "feat: split schema — add GENERATION_RUNS, LANE_WEEK_TRENDS; remove MOVING_AVG from ANOMALY_FLAGS"
```

---

## Task 2: Rewrite `generate_synthetic.py`

**Files:**
- Modify: `scripts/generate_synthetic.py`

- [ ] **Step 1: Replace the entire file**

```python
"""
Generate synthetic shipment records using FAF5 regional data as distribution seed.

Usage:
    python scripts/generate_synthetic.py                        # requires data/raw/faf5_*.csv
    python scripts/generate_synthetic.py --use-priors           # loud warning, no FAF5 needed
    python scripts/generate_synthetic.py --faf5-path /path/to/faf5.csv
    python scripts/generate_synthetic.py --use-priors --n 500   # small fixture for tests

Outputs (data/processed/):
    shipments.csv, carrier_rates.csv, fuel_surcharges.csv,
    anomaly_ground_truth.parquet, generation_metadata.json
"""
import argparse
import json
import sys
import uuid
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
DEFAULT_N_SHIPMENTS = 75_000
PROCESSED_DIR = Path("data/processed")
RAW_DIR = Path("data/raw")

MODES = ["PARCEL", "LTL", "FTL"]
MODE_PROBS_PRIOR = [0.40, 0.35, 0.25]  # fallback when --use-priors

FAF5_MODE_MAP = {1: "PARCEL", 2: "LTL", 3: "FTL", 4: "FTL", 5: "LTL", 6: "FTL", 7: "LTL"}

# FIPS code → state abbreviation for states present in CITY_POOL
FIPS_TO_STATE = {
    4: "AZ", 6: "CA", 8: "CO", 12: "FL", 13: "GA",
    17: "IL", 18: "IN", 21: "KY", 24: "MD", 25: "MA",
    26: "MI", 27: "MN", 29: "MO", 36: "NY", 37: "NC",
    39: "OH", 41: "OR", 42: "PA", 47: "TN", 48: "TX",
    49: "UT", 53: "WA",
}

CITY_POOL = [
    "Chicago,IL", "Los Angeles,CA", "New York,NY", "Dallas,TX",
    "Atlanta,GA", "Seattle,WA", "Denver,CO", "Memphis,TN",
    "Houston,TX", "Detroit,MI", "Philadelphia,PA", "Phoenix,AZ",
    "Minneapolis,MN", "Kansas City,MO", "Charlotte,NC", "Portland,OR",
    "Cincinnati,OH", "Nashville,TN", "Salt Lake City,UT", "Miami,FL",
    "Boston,MA", "St. Louis,MO", "Louisville,KY", "Columbus,OH",
    "Indianapolis,IN", "San Antonio,TX", "San Jose,CA", "Baltimore,MD",
    "Pittsburgh,PA", "Cleveland,OH",
]

# state → list of cities from CITY_POOL
STATE_TO_CITIES: dict[str, list[str]] = {}
for _city in CITY_POOL:
    _state = _city.split(",")[1].strip()
    STATE_TO_CITIES.setdefault(_state, []).append(_city)

CARRIER_POOL = [f"CARRIER_{i:03d}" for i in range(1, 26)]

RATE_SCHEDULE = {
    "PARCEL": (12.0, 4.0),
    "LTL": (18.5, 5.5),
    "FTL": (8.0, 2.0),
}


# ── CLI ──────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic freight data")
    p.add_argument("--use-priors", action="store_true",
                   help="Use hardcoded priors instead of FAF5 (emits loud warning)")
    p.add_argument("--faf5-path", type=Path, default=None,
                   help="Explicit path to FAF5 CSV; overrides auto-detection")
    p.add_argument("--n", type=int, default=DEFAULT_N_SHIPMENTS,
                   help="Number of shipments to generate (default: 75000)")
    return p.parse_args()


# ── FAF5 resolution ──────────────────────────────────────────

def resolve_faf5_path(explicit: Path | None) -> Path | None:
    """Return the FAF5 file to use, or None if not found."""
    if explicit is not None:
        return explicit
    candidates = sorted(RAW_DIR.glob("faf5_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def load_faf5_distributions(faf5_path: Path) -> tuple[dict[str, int], np.ndarray]:
    """Parse FAF5 OD pairs to derive lane weights and mode distribution.

    Returns:
        lane_weights: dict mapping "STATE-STATE" → OD pair count
        mode_probs: array of shape (3,) for [PARCEL, LTL, FTL]
    """
    df = pd.read_csv(faf5_path, usecols=["fr_orig", "fr_dest", "dms_mode"], low_memory=False)

    def zone_to_state(zone: object) -> str | None:
        try:
            fips = int(str(int(zone)).zfill(3)[:2])
            return FIPS_TO_STATE.get(fips)
        except (ValueError, TypeError):
            return None

    df["orig_state"] = df["fr_orig"].apply(zone_to_state)
    df["dest_state"] = df["fr_dest"].apply(zone_to_state)
    df = df.dropna(subset=["orig_state", "dest_state"])

    # Lane weights: count OD state-pair flows
    lane_counts = df.groupby(["orig_state", "dest_state"]).size()
    lane_weights: dict[str, int] = {
        f"{o}-{d}": int(cnt) for (o, d), cnt in lane_counts.items()
    }

    # Mode distribution: aggregate dms_mode → PARCEL/LTL/FTL
    mode_series = df["dms_mode"].map(FAF5_MODE_MAP).dropna()
    mode_counts = mode_series.value_counts()
    mode_probs = np.array(
        [mode_counts.get(m, 0) for m in MODES], dtype=float
    )
    if mode_probs.sum() == 0:
        raise ValueError("FAF5 mode column produced no usable mode values after mapping")
    mode_probs /= mode_probs.sum()

    return lane_weights, mode_probs


# ── All possible lanes from CITY_POOL ────────────────────────

def _all_lane_ids() -> list[str]:
    states = sorted(STATE_TO_CITIES.keys())
    return [f"{o}-{d}" for o in states for d in states if o != d]


# ── Data generators ──────────────────────────────────────────

def generate_fuel_surcharges() -> pd.DataFrame:
    weeks = pd.date_range("2022-01-03", "2024-06-24", freq="W-MON")
    n = len(weeks)
    diesel = np.concatenate([
        np.linspace(3.50, 5.80, 26),
        np.linspace(5.80, 4.20, 26),
        np.linspace(4.20, 4.00, 26),
        np.linspace(4.00, 3.80, 26),
        np.linspace(3.80, 3.90, 26),
    ])[:n]
    surcharge_pct = ((diesel - 2.50) / 2.50 * 0.30).round(4)
    return pd.DataFrame({
        "week_start": weeks.strftime("%Y-%m-%d"),
        "fuel_price_per_gallon": diesel.round(4),
        "surcharge_pct": surcharge_pct,
    })


def generate_carrier_rates(
    rng: np.random.Generator,
    lane_weights: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Generate carrier rate table with unique (carrier_id, mode, lane_id) PKs."""
    all_lanes = _all_lane_ids()
    n_lanes_per_group = min(30, len(all_lanes))

    if lane_weights is not None:
        weights = np.array([lane_weights.get(l, 0) for l in all_lanes], dtype=float)
        weights += 1.0  # Laplace smoothing: ensure every lane has non-zero weight
        weights /= weights.sum()
    else:
        weights = None  # uniform

    rows = []
    for carrier in CARRIER_POOL:
        for mode in MODES:
            mean, std = RATE_SCHEDULE[mode]
            carrier_factor = rng.uniform(0.85, 1.15)
            chosen_lanes = rng.choice(all_lanes, n_lanes_per_group, replace=False, p=weights)
            for lane in chosen_lanes:
                lane_factor = rng.uniform(0.90, 1.10)
                rate = max(2.0, rng.normal(mean * carrier_factor * lane_factor, std * 0.3))
                rows.append({
                    "carrier_id": carrier,
                    "mode": mode,
                    "lane_id": lane,
                    "base_rate_per_cwt": round(rate, 4),
                    "effective_date": "2023-01-01",
                })
    return pd.DataFrame(rows)


def generate_shipments(
    rates_df: pd.DataFrame,
    fuel_df: pd.DataFrame,
    n: int,
    rng: np.random.Generator,
    mode_probs: np.ndarray,
    run_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate shipments by sampling from CARRIER_RATES rows.

    Join coverage = 100% by construction: every shipment's
    (carrier_id, mode, lane_id) exists in rates_df.
    """
    start = pd.Timestamp("2023-01-01")
    end = pd.Timestamp("2024-06-30")
    dates = pd.to_datetime(rng.integers(start.value, end.value, n))

    # Sample from rates_df rows (with replacement — multiple shipments per rate row is fine)
    rate_idx = rng.integers(0, len(rates_df), n)
    sampled = rates_df.iloc[rate_idx].reset_index(drop=True)

    # Assign origin/dest cities consistent with lane_id
    origins = []
    dests = []
    for lane_id in sampled["lane_id"]:
        orig_state, dest_state = lane_id.split("-")
        origins.append(rng.choice(STATE_TO_CITIES[orig_state]))
        dests.append(rng.choice(STATE_TO_CITIES[dest_state]))

    weight_lbs = rng.lognormal(mean=6.5, sigma=1.2, size=n).clip(1, 40_000)
    weight_cwt = weight_lbs / 100

    base_rate_per_cwt = sampled["base_rate_per_cwt"].to_numpy()
    base_costs = (base_rate_per_cwt * weight_cwt).clip(1.0)

    # Fuel surcharge lookup by week
    fuel_df = fuel_df.copy()
    fuel_df["week_start"] = pd.to_datetime(fuel_df["week_start"])
    fuel_lookup = fuel_df.set_index("week_start")["surcharge_pct"].to_dict()
    week_starts = dates.to_series().dt.to_period("W").dt.start_time.values
    fuel_pcts = np.array([fuel_lookup.get(pd.Timestamp(w), 0.15) for w in week_starts])

    total_costs = base_costs * (1 + fuel_pcts)

    # Inject 7% anomalies
    anomaly_mask = rng.random(n) < 0.07
    total_costs[anomaly_mask] *= rng.uniform(2.5, 5.0, int(anomaly_mask.sum()))

    # On-time flags
    on_time_base = {"PARCEL": 0.92, "LTL": 0.87, "FTL": 0.94}
    on_time = np.array([
        int(rng.random() < (on_time_base[m] * (0.6 if anomaly_mask[i] else 1.0)))
        for i, m in enumerate(sampled["mode"])
    ])

    shipments = pd.DataFrame({
        "shipment_id": [f"SHP{i:07d}" for i in range(n)],
        "ship_date": dates.strftime("%Y-%m-%d"),
        "origin_city": origins,
        "dest_city": dests,
        "mode": sampled["mode"].to_numpy(),
        "carrier_id": sampled["carrier_id"].to_numpy(),
        "weight_lbs": weight_lbs.round(1),
        "base_rate_per_cwt": base_rate_per_cwt.round(4),
        "base_cost": base_costs.round(2),
        "fuel_surcharge_pct": fuel_pcts.round(4),
        "total_cost": total_costs.round(2),
        "on_time_flag": on_time,
        "lane_id": sampled["lane_id"].to_numpy(),
        "run_id": run_id,
    })

    ground_truth = pd.DataFrame({
        "shipment_id": shipments["shipment_id"],
        "is_anomaly": anomaly_mask.astype(int),
        "run_id": run_id,
    })

    return shipments, ground_truth


# ── Main ─────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    rng = np.random.default_rng(SEED)
    run_id = str(uuid.uuid4())

    # Resolve FAF5
    lane_weights: dict[str, int] | None = None
    mode_probs = np.array(MODE_PROBS_PRIOR)
    faf5_file_used: str | None = None

    if args.use_priors:
        print(
            "WARNING: --use-priors active; using hardcoded priors, NOT FAF5 data. "
            "Any claims about FAF5-seeded generation are invalid for this run.",
            file=sys.stderr,
        )
        seed_source = "PRIORS"
    else:
        faf5_path = resolve_faf5_path(args.faf5_path)
        if faf5_path is None:
            raise FileNotFoundError(
                "No FAF5 file found in data/raw/. "
                "Run 'make download' to fetch faf5_2022_2024.csv, "
                "or pass --use-priors to use hardcoded distributions."
            )
        print(f"Loading FAF5 distributions from {faf5_path}...")
        lane_weights, mode_probs = load_faf5_distributions(faf5_path)
        faf5_file_used = str(faf5_path)
        seed_source = "FAF5"
        print(f"  FAF5 mode distribution: {dict(zip(MODES, mode_probs.round(3)))}")
        print(f"  FAF5 lane pool: {len(lane_weights):,} OD pairs")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating fuel surcharges...")
    fuel_df = generate_fuel_surcharges()
    fuel_df.to_csv(PROCESSED_DIR / "fuel_surcharges.csv", index=False)

    print("Generating carrier rates...")
    rates_df = generate_carrier_rates(rng, lane_weights=lane_weights)
    rates_df.to_csv(PROCESSED_DIR / "carrier_rates.csv", index=False)
    dup_pks = rates_df.duplicated(subset=["carrier_id", "mode", "lane_id"]).sum()
    if dup_pks > 0:
        raise RuntimeError(f"CARRIER_RATES has {dup_pks} duplicate PKs — this is a bug")
    print(f"  {len(rates_df):,} rate records | 0 duplicate PKs")

    print(f"Generating {args.n:,} shipments...")
    shipments_df, ground_truth_df = generate_shipments(
        rates_df, fuel_df, args.n, rng, mode_probs, run_id
    )
    shipments_df.to_csv(PROCESSED_DIR / "shipments.csv", index=False)
    ground_truth_df.to_parquet(PROCESSED_DIR / "anomaly_ground_truth.parquet", index=False)

    n_anomalies = int(ground_truth_df["is_anomaly"].sum())

    metadata = {
        "run_id": run_id,
        "seed_source": seed_source,
        "faf5_file": faf5_file_used,
        "generated_at": pd.Timestamp.now().isoformat(),
        "n_shipments": args.n,
        "n_anomalies": n_anomalies,
        "anomaly_rate": round(n_anomalies / args.n, 4),
    }
    (PROCESSED_DIR / "generation_metadata.json").write_text(json.dumps(metadata, indent=2))

    print(f"  {args.n:,} shipments | {n_anomalies:,} anomalies ({n_anomalies/args.n:.1%})")
    print(f"  run_id: {run_id}")
    print(f"  seed_source: {seed_source}")
    print(f"\nOutputs in {PROCESSED_DIR}/")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run existing unit tests to see what breaks**

```bash
.venv/bin/pytest tests/test_generate_synthetic.py -v 2>&1 | head -60
```

Expected: some failures on `base_rate` column references — fix in Task 3.

- [ ] **Step 3: Commit**

```bash
git add scripts/generate_synthetic.py
git commit -m "feat: rewrite generate_synthetic — FAF5 seeding, CARRIER_RATES-first, run_id, CLI args"
```

---

## Task 3: Update `test_generate_synthetic.py`

**Files:**
- Modify: `tests/test_generate_synthetic.py`

- [ ] **Step 1: Replace the file**

```python
"""Unit tests for generate_synthetic.py"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from generate_synthetic import (
    MODES,
    generate_carrier_rates,
    generate_fuel_surcharges,
    generate_shipments,
)

SEED = 42


def _make_rng() -> np.random.Generator:
    return np.random.default_rng(SEED)


def test_fuel_surcharges_shape():
    df = generate_fuel_surcharges()
    assert len(df) > 100
    assert set(df.columns) == {"week_start", "fuel_price_per_gallon", "surcharge_pct"}


def test_fuel_surcharges_range():
    df = generate_fuel_surcharges()
    assert df["fuel_price_per_gallon"].between(3.0, 7.0).all()
    assert df["surcharge_pct"].between(0.0, 1.0).all()


def test_carrier_rates_no_duplicate_pks():
    rates = generate_carrier_rates(_make_rng())
    dupes = rates.duplicated(subset=["carrier_id", "mode", "lane_id"]).sum()
    assert dupes == 0, f"CARRIER_RATES has {dupes} duplicate (carrier_id, mode, lane_id) PKs"


def test_carrier_rates_columns():
    rates = generate_carrier_rates(_make_rng())
    assert {"carrier_id", "mode", "lane_id", "base_rate_per_cwt", "effective_date"}.issubset(
        set(rates.columns)
    )


def test_shipments_row_count():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 500, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert len(ships) == 500


def test_shipments_columns():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    expected = {
        "shipment_id", "ship_date", "origin_city", "dest_city", "mode",
        "carrier_id", "weight_lbs", "base_rate_per_cwt", "base_cost",
        "fuel_surcharge_pct", "total_cost", "on_time_flag", "lane_id", "run_id",
    }
    assert expected.issubset(set(ships.columns))


def test_base_cost_equals_rate_times_weight():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    # For non-anomaly rows: base_cost ≈ base_rate_per_cwt * weight_lbs / 100
    # Anomalies inflate total_cost but not base_cost, so check base_cost directly
    computed = (ships["base_rate_per_cwt"] * ships["weight_lbs"] / 100).clip(lower=1.0).round(2)
    assert (ships["base_cost"] - computed).abs().max() < 0.01


def test_join_coverage_100_percent():
    """Every shipment (carrier_id, mode, lane_id) must exist in CARRIER_RATES."""
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 500, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    rate_keys = set(zip(rates["carrier_id"], rates["mode"], rates["lane_id"]))
    ship_keys = set(zip(ships["carrier_id"], ships["mode"], ships["lane_id"]))
    missing = ship_keys - rate_keys
    assert len(missing) == 0, f"{len(missing)} shipment keys not found in CARRIER_RATES"


def test_modes_valid():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert set(ships["mode"].unique()).issubset(set(MODES))


def test_on_time_flag_binary():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert set(ships["on_time_flag"].unique()).issubset({0, 1})


def test_anomaly_injection_rate():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    _, gt = generate_shipments(rates, fuel, 2000, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    rate = gt["is_anomaly"].mean()
    assert 0.04 <= rate <= 0.10, f"Anomaly rate {rate:.2%} outside expected 4-10%"


def test_total_cost_positive():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    assert (ships["total_cost"] > 0).all()


def test_lane_id_format():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, _ = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "test-run-id")
    sample = ships["lane_id"].iloc[0]
    parts = sample.split("-")
    assert len(parts) == 2
    assert all(len(p) == 2 for p in parts)


def test_run_id_propagated():
    fuel = generate_fuel_surcharges()
    rates = generate_carrier_rates(_make_rng())
    ships, gt = generate_shipments(rates, fuel, 200, _make_rng(), np.array([0.4, 0.35, 0.25]), "my-run-123")
    assert (ships["run_id"] == "my-run-123").all()
    assert (gt["run_id"] == "my-run-123").all()
```

- [ ] **Step 2: Run tests**

```bash
.venv/bin/pytest tests/test_generate_synthetic.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_generate_synthetic.py
git commit -m "test: update unit tests for new schema (base_rate_per_cwt, base_cost, run_id, join coverage)"
```

---

## Task 4: Update `scripts/load_snowflake.py`

**Files:**
- Modify: `scripts/load_snowflake.py`

- [ ] **Step 1: Replace the file**

```python
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

PROCESSED_DIR = Path("data/processed")
SQL_DIR = Path("sql")

TABLES = [
    ("GENERATION_RUNS",  None),                               # inserted from metadata, not CSV
    ("FUEL_SURCHARGES",  PROCESSED_DIR / "fuel_surcharges.csv"),
    ("CARRIER_RATES",    PROCESSED_DIR / "carrier_rates.csv"),
    ("SHIPMENTS",        PROCESSED_DIR / "shipments.csv"),
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


def insert_generation_run(cursor, metadata: dict) -> None:
    cursor.execute(
        """
        INSERT INTO GENERATION_RUNS
            (run_id, generated_at, seed_source, faf5_file, n_shipments, n_anomalies, anomaly_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            metadata["run_id"],
            metadata["generated_at"],
            metadata["seed_source"],
            metadata.get("faf5_file"),
            metadata["n_shipments"],
            metadata["n_anomalies"],
            metadata["anomaly_rate"],
        ),
    )
    print(f"  GENERATION_RUNS: inserted run_id={metadata['run_id']}")


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
    metadata_path = PROCESSED_DIR / "generation_metadata.json"
    if not metadata_path.exists():
        raise FileNotFoundError(
            f"{metadata_path} not found — run generate_synthetic.py first"
        )
    metadata = json.loads(metadata_path.read_text())

    print("Connecting to Snowflake...")
    conn = get_conn()
    cursor = conn.cursor()

    print("\nCreating schema...")
    run_sql_file(cursor, SQL_DIR / "01_ddl_schema.sql")

    print("\nLoading tables...")
    insert_generation_run(cursor, metadata)

    for table_name, csv_path in TABLES:
        if csv_path is None:
            continue
        if not csv_path.exists():
            raise FileNotFoundError(f"{csv_path} not found — run generate_synthetic.py first")
        stage_and_copy(cursor, table_name, csv_path)

    print("\nRunning anomaly detection...")
    run_sql_file(cursor, SQL_DIR / "02_anomaly_detection.sql")

    print("\nCreating views...")
    run_sql_file(cursor, SQL_DIR / "03_views_powerbi.sql")

    cursor.close()
    conn.close()
    print(f"\nDone. seed_source={metadata['seed_source']} run_id={metadata['run_id']}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add scripts/load_snowflake.py
git commit -m "feat: load_snowflake inserts GENERATION_RUNS from metadata; removes MOVING_AVG dependency"
```

---

## Task 5: Create `scripts/evaluate_anomaly.py`

**Files:**
- Create: `scripts/evaluate_anomaly.py`

- [ ] **Step 1: Write the file**

```python
"""
Evaluate anomaly detection methods against ground truth.

Usage:
    python scripts/evaluate_anomaly.py --local
        Recomputes Z-score and IQR flags from local shipments.csv (no Snowflake).

    python scripts/evaluate_anomaly.py --flags data/processed/anomaly_flags_export.csv
        Uses a CSV exported from Snowflake ANOMALY_FLAGS.

Output:
    Prints per-method and distinct-shipment metrics tagged with seed_source.
    Writes data/processed/evaluation_report.json.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

PROCESSED_DIR = Path("data/processed")


# ── Local flag recomputation (mirrors SQL logic) ──────────────

def compute_zscore_flags(df: pd.DataFrame, threshold: float = 2.5) -> set[str]:
    df = df.copy()
    df["cpl"] = df["total_cost"] / df["weight_lbs"].clip(lower=1e-6)
    stats = (
        df.groupby(["lane_id", "mode"])["cpl"]
        .agg(mean="mean", std="std", count="count")
        .reset_index()
        .query("count >= 10")
    )
    merged = df.merge(stats, on=["lane_id", "mode"], how="inner")
    merged["z"] = (merged["cpl"] - merged["mean"]) / merged["std"].clip(lower=1e-6)
    return set(merged.loc[merged["z"].abs() > threshold, "shipment_id"])


def compute_iqr_flags(df: pd.DataFrame) -> set[str]:
    df = df.copy()
    df["cpl"] = df["total_cost"] / df["weight_lbs"].clip(lower=1e-6)
    stats = (
        df.groupby(["lane_id", "mode"])["cpl"]
        .agg(
            q1=lambda x: x.quantile(0.25),
            q3=lambda x: x.quantile(0.75),
            count="count",
        )
        .reset_index()
        .query("count >= 10")
    )
    stats["lower"] = stats["q1"] - 1.5 * (stats["q3"] - stats["q1"])
    stats["upper"] = stats["q3"] + 1.5 * (stats["q3"] - stats["q1"])
    merged = df.merge(stats, on=["lane_id", "mode"], how="inner")
    flagged = merged.loc[
        (merged["cpl"] > merged["upper"]) | (merged["cpl"] < merged["lower"]),
        "shipment_id",
    ]
    return set(flagged)


# ── Metrics ──────────────────────────────────────────────────

def compute_metrics(flagged: set[str], truth: pd.DataFrame) -> dict:
    """Compute precision, recall, F1, FPR for a set of flagged shipment_ids."""
    n_total = len(truth)
    n_positive = int(truth["is_anomaly"].sum())
    n_negative = n_total - n_positive

    tp = int(truth.loc[truth["shipment_id"].isin(flagged), "is_anomaly"].sum())
    fp = len(flagged) - tp
    fn = n_positive - tp
    tn = n_negative - fp

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "fpr": round(fpr, 4),
        "n_flagged": len(flagged),
    }


# ── CLI ──────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--local", action="store_true",
                       help="Recompute flags from local shipments.csv")
    group.add_argument("--flags", type=Path,
                       help="Path to anomaly_flags_export.csv from Snowflake")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    truth = pd.read_parquet(PROCESSED_DIR / "anomaly_ground_truth.parquet")
    meta = json.loads((PROCESSED_DIR / "generation_metadata.json").read_text())
    seed_source = meta["seed_source"]
    run_id = meta["run_id"]

    if args.local:
        ships = pd.read_csv(PROCESSED_DIR / "shipments.csv")
        zscore_ids = compute_zscore_flags(ships)
        iqr_ids = compute_iqr_flags(ships)
        method_flags = {"ZSCORE": zscore_ids, "IQR": iqr_ids}
    else:
        flags_df = pd.read_csv(args.flags)
        method_flags = {
            method: set(grp["shipment_id"])
            for method, grp in flags_df.groupby("flag_type")
        }

    print(f"\n=== Anomaly Evaluation | seed_source={seed_source} | run_id={run_id} ===\n")

    per_method = {}
    for method, flagged in method_flags.items():
        m = compute_metrics(flagged, truth)
        per_method[method] = m
        print(f"[{method}] precision={m['precision']:.3f}  recall={m['recall']:.3f}  "
              f"f1={m['f1']:.3f}  fpr={m['fpr']:.3f}  flagged={m['n_flagged']:,}")

    # Distinct-shipment (union of all methods)
    all_flagged = set().union(*method_flags.values())
    distinct = compute_metrics(all_flagged, truth)
    print(f"\n[DISTINCT-SHIPMENT (all methods)] "
          f"precision={distinct['precision']:.3f}  recall={distinct['recall']:.3f}  "
          f"f1={distinct['f1']:.3f}  fpr={distinct['fpr']:.3f}  "
          f"flagged={distinct['n_flagged']:,}")

    report = {
        "run_id": run_id,
        "seed_source": seed_source,
        "per_method": per_method,
        "distinct_shipment": distinct,
    }
    out_path = PROCESSED_DIR / "evaluation_report.json"
    out_path.write_text(json.dumps(report, indent=2))
    print(f"\nReport written to {out_path}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify it runs with `--local` (requires generated data)**

If `data/processed/` has CSVs from a previous run:
```bash
.venv/bin/python scripts/evaluate_anomaly.py --local
```
Expected: prints per-method precision/recall table and writes `data/processed/evaluation_report.json`.

If no data yet, skip this step — the integration test will validate it.

- [ ] **Step 3: Commit**

```bash
git add scripts/evaluate_anomaly.py
git commit -m "feat: add evaluate_anomaly.py — precision/recall/F1/FPR per method with provenance tagging"
```

---

## Task 6: Create `tests/test_integration.py`

**Files:**
- Create: `tests/test_integration.py`

- [ ] **Step 1: Write the file**

```python
"""
Integration test: generate (--use-priors, n=500) → local anomaly detection → metrics.

No Snowflake connection required. Fully deterministic (seeded RNG).
"""
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from evaluate_anomaly import compute_iqr_flags, compute_metrics, compute_zscore_flags
from generate_synthetic import generate_carrier_rates, generate_fuel_surcharges, generate_shipments

SEED = 42
N_FIXTURE = 500


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    """Generate fixture data in-process for full isolation."""
    rng = np.random.default_rng(SEED)
    run_id = "integration-test-fixture"
    fuel_df = generate_fuel_surcharges()
    rates_df = generate_carrier_rates(rng)
    ships_df, gt_df = generate_shipments(
        rates_df, fuel_df, N_FIXTURE, rng, np.array([0.4, 0.35, 0.25]), run_id
    )
    return {"ships": ships_df, "rates": rates_df, "gt": gt_df, "run_id": run_id}


def test_use_priors_warning_emitted(tmp_path):
    """Running with --use-priors must emit a priors warning to stderr."""
    result = subprocess.run(
        [sys.executable, "scripts/generate_synthetic.py", "--use-priors", "--n", "50"],
        capture_output=True, text=True,
        cwd=str(Path(__file__).parent.parent),
    )
    assert result.returncode == 0, f"Generator failed: {result.stderr}"
    assert "priors" in result.stderr.lower(), (
        f"Expected priors warning in stderr; got: {result.stderr!r}"
    )


def test_generation_metadata_seed_source(tmp_path):
    """generation_metadata.json must have seed_source=PRIORS when --use-priors used."""
    result = subprocess.run(
        [sys.executable, "scripts/generate_synthetic.py", "--use-priors", "--n", "50"],
        capture_output=True, text=True,
        cwd=str(Path(__file__).parent.parent),
    )
    assert result.returncode == 0
    meta = json.loads((Path("data/processed/generation_metadata.json")).read_text())
    assert meta["seed_source"] == "PRIORS"


def test_carrier_rates_no_duplicate_pks(generated):
    rates = generated["rates"]
    dupes = rates.duplicated(subset=["carrier_id", "mode", "lane_id"]).sum()
    assert dupes == 0, f"CARRIER_RATES has {dupes} duplicate PKs"


def test_join_coverage_100_percent(generated):
    ships = generated["ships"]
    rates = generated["rates"]
    rate_keys = set(zip(rates["carrier_id"], rates["mode"], rates["lane_id"]))
    ship_keys = set(zip(ships["carrier_id"], ships["mode"], ships["lane_id"]))
    missing = ship_keys - rate_keys
    assert len(missing) == 0, f"{len(missing)} shipment keys not in CARRIER_RATES"


def test_base_cost_semantics(generated):
    ships = generated["ships"]
    computed = (ships["base_rate_per_cwt"] * ships["weight_lbs"] / 100).clip(lower=1.0).round(2)
    assert (ships["base_cost"] - computed).abs().max() < 0.01


def test_zscore_precision_recall(generated):
    ships = generated["ships"]
    gt = generated["gt"]
    flagged = compute_zscore_flags(ships)
    m = compute_metrics(flagged, gt)
    assert m["precision"] > 0.30, f"Z-score precision {m['precision']:.3f} below threshold"
    assert m["recall"] > 0.20, f"Z-score recall {m['recall']:.3f} below threshold"


def test_iqr_precision_recall(generated):
    ships = generated["ships"]
    gt = generated["gt"]
    flagged = compute_iqr_flags(ships)
    m = compute_metrics(flagged, gt)
    assert m["precision"] > 0.25, f"IQR precision {m['precision']:.3f} below threshold"
    assert m["recall"] > 0.25, f"IQR recall {m['recall']:.3f} below threshold"
```

- [ ] **Step 2: Run the integration test**

```bash
.venv/bin/pytest tests/test_integration.py -v
```

Expected: all tests pass. If `test_use_priors_warning_emitted` or `test_generation_metadata_seed_source` fail because `data/processed/` doesn't exist, create it first: `mkdir -p data/processed`.

- [ ] **Step 3: Run full test suite**

```bash
.venv/bin/pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: add integration test — 500-row fixture, join coverage, precision/recall per method"
```

---

## Task 7: Create `scripts/dashboard.py`

**Files:**
- Create: `scripts/dashboard.py`

- [ ] **Step 1: Write the file**

```python
"""
Interactive Freight Analytics Dashboard
Run: python scripts/dashboard.py
Then open http://localhost:8050
"""
import json
from pathlib import Path

import dash
import dash_bootstrap_components as dbc
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, dash_table

PROCESSED_DIR = Path("data/processed")

# ── Colour palette ────────────────────────────────────────────
C_NORMAL = "#4C8BF5"      # steel-blue
C_ANOMALY = "#F5A623"     # amber
C_HIGH = "#E53935"        # red
C_BG = "#1E1E2E"
C_SURFACE = "#2A2A3E"
C_TEXT = "#E0E0E0"
C_MUTED = "#888"

TEMPLATE = "plotly_dark"


# ── Data loading ──────────────────────────────────────────────

def load_data():
    ships = pd.read_csv(PROCESSED_DIR / "shipments.csv", parse_dates=["ship_date"])
    gt = pd.read_parquet(PROCESSED_DIR / "anomaly_ground_truth.parquet")
    meta = json.loads((PROCESSED_DIR / "generation_metadata.json").read_text())

    eval_report = None
    eval_path = PROCESSED_DIR / "evaluation_report.json"
    if eval_path.exists():
        eval_report = json.loads(eval_path.read_text())

    return ships, gt, meta, eval_report


def compute_local_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Recompute Z-score and IQR flags in pandas. Adds 'flagged' column."""
    df = df.copy()
    df["cpl"] = df["total_cost"] / df["weight_lbs"].clip(lower=1e-6)

    # Z-score
    stats = (
        df.groupby(["lane_id", "mode"])["cpl"]
        .agg(mean="mean", std="std", count="count")
        .reset_index()
        .query("count >= 10")
    )
    merged = df.merge(stats, on=["lane_id", "mode"], how="left")
    merged["z"] = (merged["cpl"] - merged["mean"]) / merged["std"].clip(lower=1e-6)
    zscore_ids = set(merged.loc[merged["z"].abs() > 2.5, "shipment_id"])

    # IQR
    iqr_stats = (
        df.groupby(["lane_id", "mode"])["cpl"]
        .agg(
            q1=lambda x: x.quantile(0.25),
            q3=lambda x: x.quantile(0.75),
            count="count",
        )
        .reset_index()
        .query("count >= 10")
    )
    iqr_stats["lower"] = iqr_stats["q1"] - 1.5 * (iqr_stats["q3"] - iqr_stats["q1"])
    iqr_stats["upper"] = iqr_stats["q3"] + 1.5 * (iqr_stats["q3"] - iqr_stats["q1"])
    merged2 = df.merge(iqr_stats, on=["lane_id", "mode"], how="left")
    iqr_ids = set(merged2.loc[
        (merged2["cpl"] > merged2["upper"]) | (merged2["cpl"] < merged2["lower"]),
        "shipment_id"
    ])

    df["flagged"] = df["shipment_id"].isin(zscore_ids | iqr_ids).astype(int)
    return df


def compute_lane_week_trends(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cpl"] = df["total_cost"] / df["weight_lbs"].clip(lower=1e-6)
    df["week_start"] = df["ship_date"].dt.to_period("W").dt.start_time
    weekly = (
        df.groupby(["week_start", "lane_id", "mode"])
        .agg(shipment_count=("shipment_id", "count"), avg_cpl=("cpl", "mean"))
        .reset_index()
        .sort_values(["lane_id", "mode", "week_start"])
    )
    weekly["rolling_4wk"] = (
        weekly.groupby(["lane_id", "mode"])["avg_cpl"]
        .transform(lambda x: x.shift(1).rolling(4, min_periods=4).mean())
    )
    weekly["pct_dev"] = (
        (weekly["avg_cpl"] - weekly["rolling_4wk"]) /
        weekly["rolling_4wk"].clip(lower=1e-6)
    )
    weekly["prior_weeks"] = weekly.groupby(["lane_id", "mode"]).cumcount()
    weekly["is_anomalous"] = (
        (weekly["pct_dev"].abs() > 0.20) & (weekly["prior_weeks"] >= 4)
    ).astype(int)
    return weekly


# ── KPI cards ─────────────────────────────────────────────────

def kpi_card(title: str, value: str, color: str = C_TEXT) -> dbc.Card:
    return dbc.Card(
        dbc.CardBody([
            html.P(title, style={"color": C_MUTED, "fontSize": "0.8rem", "marginBottom": "4px"}),
            html.H4(value, style={"color": color, "fontWeight": "bold"}),
        ]),
        style={"backgroundColor": C_SURFACE, "border": "none"},
    )


# ── Chart builders ────────────────────────────────────────────

def fig_violin_cpl(df: pd.DataFrame) -> go.Figure:
    df = df.copy()
    df["anomaly_label"] = df["flagged"].map({0: "Normal", 1: "Flagged"})
    fig = px.violin(
        df, x="mode", y="cpl", color="anomaly_label",
        color_discrete_map={"Normal": C_NORMAL, "Flagged": C_ANOMALY},
        box=True, points=False,
        labels={"cpl": "Cost per lb ($)", "mode": "Mode", "anomaly_label": ""},
        title="Cost-per-lb Distribution by Mode",
        template=TEMPLATE,
    )
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT)
    return fig


def fig_weekly_cpl(df: pd.DataFrame, trends: pd.DataFrame) -> go.Figure:
    # Aggregate all modes for simplicity; one trace per mode
    df_weekly = (
        df.groupby(["ship_date", "mode"])
        .agg(avg_cpl=("cpl", "mean"))
        .reset_index()
        .rename(columns={"ship_date": "week"})
    )
    df_weekly["week"] = pd.to_datetime(df_weekly["week"])
    df_weekly = df_weekly.resample("W", on="week").agg(avg_cpl=("avg_cpl", "mean")).reset_index()

    # Anomalous weeks (any lane)
    anomaly_weeks = trends.loc[trends["is_anomalous"] == 1, "week_start"].unique()

    fig = px.line(
        df_weekly, x="week", y="avg_cpl",
        labels={"avg_cpl": "Avg Cost/lb ($)", "week": ""},
        title="Weekly Avg Cost-per-lb (All Modes)",
        template=TEMPLATE,
        color_discrete_sequence=[C_NORMAL],
    )
    for wk in anomaly_weeks:
        fig.add_vline(x=pd.Timestamp(wk), line_color=C_ANOMALY, line_width=1, opacity=0.4)
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT)
    return fig


def fig_lane_heatmap(df: pd.DataFrame) -> go.Figure:
    top_lanes = (
        df.groupby("lane_id")["total_cost"].sum()
        .nlargest(20).index.tolist()
    )
    subset = df[df["lane_id"].isin(top_lanes)]
    agg = subset.groupby("lane_id").agg(
        total_cost=("total_cost", "sum"),
        anomaly_count=("flagged", "sum"),
        total_count=("shipment_id", "count"),
    ).reset_index()
    agg["anomaly_density"] = agg["anomaly_count"] / agg["total_count"].clip(lower=1)
    agg = agg.sort_values("total_cost", ascending=True)

    fig = px.bar(
        agg, x="total_cost", y="lane_id", orientation="h",
        color="anomaly_density",
        color_continuous_scale=["#4C8BF5", "#F5A623", "#E53935"],
        labels={"total_cost": "Total Freight Spend ($)", "lane_id": "Lane", "anomaly_density": "Anomaly Density"},
        title="Top 20 Lanes by Total Spend (colored by anomaly density)",
        template=TEMPLATE,
    )
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT, height=550)
    return fig


def fig_carrier_scorecard(df: pd.DataFrame) -> go.Figure:
    agg = df.groupby("carrier_id").agg(
        on_time_rate=("on_time_flag", "mean"),
        avg_cost=("total_cost", "mean"),
        volume=("shipment_id", "count"),
    ).reset_index()
    fig = px.scatter(
        agg, x="avg_cost", y="on_time_rate", size="volume",
        hover_name="carrier_id",
        color="on_time_rate",
        color_continuous_scale=["#E53935", "#F5A623", "#4C8BF5"],
        labels={"avg_cost": "Avg Cost/Shipment ($)", "on_time_rate": "On-Time Rate", "volume": "Shipments"},
        title="Carrier Scorecard — On-Time Rate vs Avg Cost",
        template=TEMPLATE,
    )
    fig.update_layout(paper_bgcolor=C_BG, plot_bgcolor=C_BG, font_color=C_TEXT)
    return fig


def eval_table(eval_report: dict | None) -> dbc.Card:
    if eval_report is None:
        return dbc.Card(
            dbc.CardBody(html.P("Run `make evaluate` to generate evaluation metrics.", style={"color": C_MUTED})),
            style={"backgroundColor": C_SURFACE, "border": "none"},
        )
    rows = []
    for method, m in eval_report["per_method"].items():
        rows.append({
            "Method": method,
            "Precision": f"{m['precision']:.3f}",
            "Recall": f"{m['recall']:.3f}",
            "F1": f"{m['f1']:.3f}",
            "FPR": f"{m['fpr']:.3f}",
            "Flagged": f"{m['n_flagged']:,}",
        })
    d = eval_report["distinct_shipment"]
    rows.append({
        "Method": "ALL (distinct)",
        "Precision": f"{d['precision']:.3f}",
        "Recall": f"{d['recall']:.3f}",
        "F1": f"{d['f1']:.3f}",
        "FPR": f"{d['fpr']:.3f}",
        "Flagged": f"{d['n_flagged']:,}",
    })
    table = dash_table.DataTable(
        data=rows,
        columns=[{"name": c, "id": c} for c in rows[0].keys()],
        style_header={"backgroundColor": C_BG, "color": C_MUTED, "fontWeight": "bold"},
        style_cell={"backgroundColor": C_SURFACE, "color": C_TEXT, "border": "1px solid #333"},
        style_data_conditional=[
            {"if": {"filter_query": '{Method} = "ALL (distinct)"'},
             "fontWeight": "bold", "color": C_ANOMALY},
        ],
    )
    return dbc.Card(
        dbc.CardBody([html.H6("Anomaly Detection Evaluation", style={"color": C_TEXT}), table]),
        style={"backgroundColor": C_SURFACE, "border": "none"},
    )


# ── App layout ────────────────────────────────────────────────

def build_layout(ships, gt, meta, eval_report):
    ships = compute_local_flags(ships)
    ships["cpl"] = ships["total_cost"] / ships["weight_lbs"].clip(lower=1e-6)
    trends = compute_lane_week_trends(ships)

    # KPI values
    total_ships = f"{len(ships):,}"
    anomaly_rate = f"{ships['flagged'].mean():.1%}"
    avg_cpl = f"${ships['cpl'].mean():.3f}"
    on_time = f"{ships['on_time_flag'].mean():.1%}"

    layout = dbc.Container([
        # Title
        dbc.Row(dbc.Col(
            html.H3("Freight Cost Anomaly & KPI Tracker",
                    style={"color": C_TEXT, "marginTop": "20px", "marginBottom": "4px"}),
        )),

        # KPI cards
        dbc.Row([
            dbc.Col(kpi_card("Total Shipments", total_ships), md=3),
            dbc.Col(kpi_card("Anomaly Rate", anomaly_rate, C_ANOMALY), md=3),
            dbc.Col(kpi_card("Avg Cost/lb", avg_cpl), md=3),
            dbc.Col(kpi_card("On-Time Rate", on_time, C_NORMAL), md=3),
        ], className="mb-3"),

        # Violin + weekly trend
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_violin_cpl(ships)), md=6),
            dbc.Col(dcc.Graph(figure=fig_weekly_cpl(ships, trends)), md=6),
        ], className="mb-3"),

        # Lane heatmap
        dbc.Row(dbc.Col(dcc.Graph(figure=fig_lane_heatmap(ships))), className="mb-3"),

        # Carrier scorecard + eval table
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_carrier_scorecard(ships)), md=7),
            dbc.Col(eval_table(eval_report), md=5),
        ], className="mb-3"),

        # Footer
        dbc.Row(dbc.Col(
            html.P(
                f"seed_source={meta['seed_source']} | "
                f"run_id={meta['run_id']} | "
                f"generated_at={meta['generated_at'][:19]}",
                style={"color": C_MUTED, "fontSize": "0.75rem", "marginTop": "8px"},
            )
        )),
    ], fluid=True, style={"backgroundColor": C_BG, "minHeight": "100vh", "padding": "0 24px"})

    return layout


def main():
    ships, gt, meta, eval_report = load_data()
    app = dash.Dash(
        __name__,
        external_stylesheets=[dbc.themes.CYBORG],
        title="Freight KPI Tracker",
    )
    app.layout = build_layout(ships, gt, meta, eval_report)
    app.run(debug=False, host="0.0.0.0", port=8050)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add scripts/dashboard.py
git commit -m "feat: replace static dashboard.html with Dash app — dark theme, KPI cards, anomaly overlays, eval panel"
```

---

## Task 8: `requirements.txt`, `Makefile`, `docs/architecture.md`

**Files:**
- Modify: `requirements.txt`
- Modify: `Makefile`
- Modify: `docs/architecture.md`

- [ ] **Step 1: Update `requirements.txt`**

Add after the `plotly` line:
```
dash==2.17.1
dash-bootstrap-components==1.6.0
```

- [ ] **Step 2: Install new dependencies**

```bash
.venv/bin/pip install dash==2.17.1 dash-bootstrap-components==1.6.0
```

Expected: installs without errors.

- [ ] **Step 3: Update `Makefile`**

Add after the `lint:` block and before `clean:`:

```makefile
evaluate:
	$(PYTHON) scripts/evaluate_anomaly.py --local

dashboard:
	$(PYTHON) scripts/dashboard.py
```

Also update the `generate` target to document new args:

```makefile
generate:
	$(PYTHON) scripts/generate_synthetic.py

generate-priors:
	$(PYTHON) scripts/generate_synthetic.py --use-priors

generate-fixture:
	$(PYTHON) scripts/generate_synthetic.py --use-priors --n 500
```

- [ ] **Step 4: Replace `docs/architecture.md`**

```markdown
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
```

- [ ] **Step 5: Run full test suite one final time**

```bash
.venv/bin/pytest tests/ -v
```

Expected: all tests pass.

- [ ] **Step 6: Commit everything**

```bash
git add requirements.txt Makefile docs/architecture.md
git commit -m "chore: add dash deps, Makefile targets (evaluate, dashboard, generate-fixture), update architecture docs"
```
