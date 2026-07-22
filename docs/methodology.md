# Freight v2 methodology

## Evidence unit

One immutable run is the unit of evidence. Its manifest binds the run ID, schema version, source
checksum, generation parameters, analysis windows, anomaly counts, artifact row counts, and file
hashes. Downstream commands validate the complete run before reading it and refuse cross-run keys.

## Synthetic network construction

FAF5 contributes a supported lane/mode distribution prior. Synthetic carriers, contract rates,
weights, invoice amounts, fuel surcharges, and service outcomes are then generated independently.
The 2024 tonnage field is used as one static distribution prior across the run; it is not treated
as historical information available to a 2023 prediction.

Carrier rates have six inclusive, contiguous effective periods from 2023 Q1 through 2024 Q2.
Rates are repriced within bounded period changes. Shipment dates are sampled before the rate join,
and each shipment must match exactly one rate interval. Fuel has exact Monday/mode coverage for
all 78 analysis weeks. Its deterministic synthetic curve is non-flat and provenance-labeled.

Normal invoices satisfy:

```text
base_cost = max(base_rate_per_cwt * weight_lbs / 100, minimum_charge)
fuel_surcharge = base_cost * weekly_mode_surcharge_rate
total_cost = base_cost + fuel_surcharge
```

## Typed anomaly injection

Seven mutually exclusive families are injected after normal construction: carrier overcharge,
duplicate fuel surcharge, rate-card override, weight/class mismatch, persistent lane drift,
service deterioration, and data-quality corruption.

Seed-and-shipment hashes assign stable, disjoint family ownership. Direct anomalies are selected
independently within baseline, calibration, and evaluation windows. Grouped lane-drift and service
signals are evaluation-only. Requested prevalence is an upper bound when an eligible ownership
partition lacks capacity. Row order, custom indices, later evaluation mutations, and another
family's requested rate cannot change earlier assignments.

Ground truth is stored separately. Operational shipments contain observable fields only.

## Baselines and detection

Expected invoice components are reconstructed from authoritative rate and fuel artifacts. Robust
segment residual statistics are fit only on baseline rows with a documented fallback hierarchy.
The frozen model is fingerprinted, and the detector reconstructs its scored lineage before use.

Five normalized detectors emit one row per shipment:

1. robust standardized cost residual;
2. IQR upper fence;
3. strictly trailing lane/mode weekly deviation;
4. strictly trailing lane/carrier service deterioration;
5. explicit data-quality rules.

Service evidence uses a multi-observed-week current window and only earlier observed weeks as its
reference. An on-time branch attributes late rows; a transit branch attributes rows at least two
days above the mode contract. Group rows share an evidence ID so downstream aggregation counts
the operational event once while retaining affected shipments.

## Calibration and held-out evaluation

The sensitivity grid is finite and bounded. Configuration selection uses calibration truth only:

```text
utility = 0.60 * excess-cost coverage
        + 0.25 * recall
        - 0.10 * false-positive rate
        - 0.05 * review rate
```

Deterministic tie-breakers prefer coverage, recall, lower false-positive rate, lower review
volume, then configuration ID. Evaluation truth is used only after selection. Mutating evaluation
labels cannot change the selected configuration or any calibration metric.

Union metrics operate at shipment grain. Agreement counts distinct method families, so robust
residual and IQR do not masquerade as two independent cost signals. Group metrics deduplicate
`evidence_unit_id`. Zero-denominator rates are reported as `0.0`.

## Priority model

Operational priority is a transparent fixed-weight score, not a predictive model. Published
components are exposure (35%), persistence (15%), service impact (15%), distinct-family agreement
(15%), data-quality confidence (10%), and evidence support (10%). Confidence separately combines
agreement, persistence, data quality, and support. Components and final scores are bounded; tests
prove exposure and distinct-family agreement are monotone when other inputs are fixed.

## Public export

The public bundle contains network aggregates, the top bounded alert queue, three deterministic
representative investigations, the sensitivity grid, evaluation metrics, and provenance hashes.
It excludes raw FAF5 records, ground-truth causes in operator investigation files, secrets, and
unbounded shipment history. Every file repeats run ID and schema version and is cross-validated
before atomic replacement.
