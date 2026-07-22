# Freight KPI Tracker v2

Freight v2 is a portable freight-operations investigation system. It turns one validated
network snapshot into reproducible cost baselines, normalized exception evidence, held-out
evaluation, and a ranked operational review queue. The public portfolio consumes a bounded
static evidence bundle; it does not require Snowflake, Power BI, a hosted database, an API key,
or a live Python service.

The original Snowflake/Power BI implementation remains documented as project history. It is not
the current runtime.

## What the system does

```text
FAF5 lane/mode distribution prior or deterministic test fixture
  -> time-versioned carrier rates + weekly fuel scenario + normal shipments
  -> isolated typed anomaly injection and separate ground truth
  -> immutable Parquet/JSON run with SHA-256 manifest
  -> baseline-only expected-cost model
  -> calibration-selected detectors
  -> held-out evaluation
  -> transparent alert prioritization
  -> validated static portfolio evidence
```

The analytical windows are fixed:

- baseline: 2023-01-02 through 2023-12-31;
- calibration: 2024-01-01 through 2024-03-31;
- held-out evaluation: 2024-04-01 through 2024-06-30.

Every carrier/lane/mode rate calendar has six contiguous effective periods. Every shipment must
resolve exactly one active rate version and one mode/week fuel record. Missing, overlapping, or
duplicate temporal keys fail validation.

## Accepted evidence run

The accepted public run is `cc024261-003c-4461-83f0-71e041694a54`, generated from the local
`faf5_2022_2024.csv` distribution source and 75,000 synthetic shipment invoices. The source file
and every run artifact are checksum-bound in the manifest.

Held-out evaluation at the calibration-selected operating point:

| Metric | Result |
|---|---:|
| Evaluation shipments | 12,500 |
| Precision | 68.4% |
| Recall | 77.4% |
| F1 | 0.726 |
| False-positive rate | 7.33% |
| Review volume | 2,405 (19.24%) |
| False negatives | 480 |
| Estimated excess-cost coverage | 100.0% after floating-point rounding |

These are exception-detection metrics, not predicted-cause classification metrics. Per-anomaly
tables are one-vs-rest. The selected configuration was chosen only from calibration data using
the recorded cost-aware rule; evaluation metrics were computed afterward.

## Leakage and aggregation controls

- Normal operations are constructed before anomaly injection.
- Operational shipments never contain `is_anomaly`, cause labels, or generated `expected_*`
  counterfactual columns.
- Cost anomalies cannot mechanically alter service outcomes; only the typed service injector can.
- Baseline statistics are fit on the baseline window only and carry a tamper-detecting fingerprint.
- Rolling signals use strictly preceding observed weeks.
- Evaluation rows cannot change baseline or calibration assignments.
- The evaluator requires the exact shipment x five-detector matrix and fixed method-family
  semantics.
- Shipment KPIs deduplicate `shipment_id`; group evidence deduplicates `evidence_unit_id`; method
  agreement counts distinct method families. The 75,000/78,814 historical join-fan-out failure is
  preserved as a regression test.

See [methodology](docs/methodology.md), [model card](docs/model-card.md), and
[known v1 defects](docs/history/known-v1-defects.md).

## Fuel and source boundary

FAF5 data seeds supported lane and mode distributions. It does not provide the synthetic carrier
invoices displayed by the project. `tons_2024` is a static scenario prior for the full synthetic
run, not a time-varying historical predictor.

Fuel is a deterministic synthetic weekly diesel-index curve with smooth seasonal and trend
components. It contains 78 distinct weekly values per mode and is explicitly **not observed EIA
data**. The exact curve basis is stored on every fuel row. A future observed-data adapter would
need its own source checksum before the project could make an observed-fuel claim.

## Install and run

Requires Python 3.11 or 3.12.

```bash
make install
make test
make lint

# Small deterministic fixture
make fixture

# Full FAF5-seeded immutable run
.venv-v2/bin/freight-v2 build \
  --seed-source FAF5 \
  --faf5-path data/raw/faf5_2022_2024.csv \
  --rows 75000 \
  --output artifacts/runs

# Accept, validate, and export one run
.venv-v2/bin/freight-v2 accept --artifact-root artifacts/runs --latest
.venv-v2/bin/freight-v2 validate --artifact-root artifacts/runs --run accepted
make export-portfolio \
  PORTFOLIO_DATA='/absolute/path/to/portfolio-codex/public/data/freight/v2'
```

New builds are immutable and refuse to overwrite an existing run. Public export is staged and
validated before atomically replacing the exact target bundle.

## Current and historical stack

Current: Python 3.11, pandas, NumPy, PyArrow/Parquet, DuckDB as an optional embedded analytical
dependency, pytest, and Ruff. The public interface is a statically hosted Next.js application.

Historical: Snowflake warehousing, SQL analytical views, Power BI-ready extracts, and a Dash
dashboard. The expired trial account and removed key are not runtime requirements. See
[historical architecture](docs/history/snowflake-powerbi.md).

## Limitations

- Shipment invoices, carrier behavior, rate schedules, fuel history, and anomalies are synthetic.
- FAF5 contributes distribution priors, not observed shipment labels or invoices.
- The public snapshot is static and supports three prevalidated sensitivity states.
- Detector flags identify review-worthy exceptions; they do not prove billing intent or causality.
- Session decisions in the portfolio are local and reset on reload.
