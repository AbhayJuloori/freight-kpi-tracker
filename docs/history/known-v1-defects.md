# Known v1 defects and corrections

## Status and scope

This note audits the retained Snowflake, SQL, generator, and Dash implementation. It does not
describe the current Freight v2 runtime and does not turn the historical prototype into a live
service. Corrections below either harden the retained v1 artifact or explain how v2 prevents the
same failure.

## Four analytical defects

### 1. Carrier scorecard fan-out

**Defect.** `VW_CARRIER_SCORECARD` joined `SHIPMENTS` directly to `ANOMALY_FLAGS`. A shipment can
have both an IQR and a Z-score flag, so the join duplicated that shipment. `COUNT`, `SUM`, `AVG`,
on-time rate, spend, and the anomaly denominator therefore operated above shipment grain.

**Historical correction.** `sql/03_views_powerbi.sql` now creates a deduplicated
`flagged_shipments` CTE and joins that one-row-per-shipment relation before aggregating. Every
scorecard measure is again computed at shipment grain.

**v2 prevention.** Detection output has a unique shipment/method key, while downstream agreement
and alert aggregation explicitly collapse to shipment grain. Immutable run validation also
prevents combining flags with shipments from another run.

### 2. Weekly dashboard mode collapse

**Defect.** `fig_weekly_cpl` grouped by mode initially, then called a second ungrouped weekly
resample. That second operation removed `mode`, despite the comment claiming one trace per mode,
and rendered a single all-mode line.

**Historical correction.** The Dash transformation now groups by mode while resampling and Plotly
uses `color="mode"`, producing one weekly series per mode.

**v2 prevention.** Lane-week evidence uses explicit lane/mode keys and validated table contracts;
tests cover stable grouping and strictly trailing time behavior.

### 3. Carrier-rate effective-date semantics

**Defect.** The v1 generator emitted one `effective_date` for every rate and sampled a rate row by
carrier/mode/lane without performing an as-of effective-date lookup. The schema suggested a
temporal rate dimension, but shipment costing behaved like a single static rate card.

**Historical status.** This limitation remains part of the v1 generator's history; it should not
be described as temporal contract pricing. Its separate generator correction is intentionally
outside this dashboard/SQL patch.

**v2 prevention.** Rates use explicit effective-start and effective-end bounds. Generation and
expected-cost reconstruction require every shipment date to resolve to its authoritative rate
window and fail on missing, duplicate, or mismatched rate lineage.

### 4. Fuel curve provenance

**Defect.** The v1 weekly diesel series is a hand-authored piecewise-linear scenario. It is useful
for deterministic synthetic cost variation, but it is not an observed EIA time series and should
not be described as one. Its smooth shape can also understate real weekly volatility.

**Historical status.** The v1 values remain reproducible scenario inputs. Public copy must label
them synthetic rather than presenting them as historical fuel observations.

**v2 prevention.** Fuel schedules are versioned run artifacts, their source boundary is recorded,
and expected cost joins the exact mode/week schedule. Any future observed-source adapter must
record its source checksum before an evidence run can claim observed fuel provenance.

## Presentation-label corrections

- The function named `fig_lane_heatmap` rendered a horizontal bar chart. It is now named
  `fig_lane_spend_bar`, and the title explicitly describes a horizontal spend bar colored by
  anomaly density.
- The carrier chart formerly aggregated only by carrier while implying a general scorecard. It now
  plots cost versus on-time service at carrier/mode grain and is titled **Carrier Cost vs Service
  by Mode**. It remains a descriptive scatter plot, not a composite carrier score or causal
  performance ranking.

## Additional v1 leakage findings

### Target-conditioned service outcomes

The v1 generator created a single cost-anomaly mask and then reduced on-time probability for those
same rows. Service performance therefore became a mechanical proxy for the target instead of an
independently generated operational outcome. Freight v2 constructs normal cost and service first;
cost anomalies do not alter service fields, and only the separately typed service-deterioration
injector may change `on_time_flag` or `transit_days`.

### Full-history and self-inclusion leakage

The v1 local and Snowflake detectors calculated mean, standard deviation, quartiles, and grouping
coverage from the complete shipment table, then scored those same rows. A row influenced its own
baseline, and future evaluation-period observations could change historical flags or even change
which grouping hierarchy was selected. Freight v2 fits distribution statistics on the baseline
window only, selects operating thresholds from calibration only, uses strictly preceding observed
weeks for trends, and reserves the evaluation window for final measurement.

### Cross-run and duplicate-truth contamination

The v1 evaluator joined or compared shipment identifiers without enforcing run identity, unique
truth keys, an exact truth-to-shipment key set, or a flagged-ID subset. Duplicate truth rows could
produce impossible metrics such as precision above one. Freight v2 carries `run_id` and schema
version through every artifact, validates immutable hashes and business keys, and evaluates on a
one-to-one `(run_id, shipment_id)` contract.
