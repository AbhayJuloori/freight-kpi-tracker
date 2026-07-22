# Freight v2 — Investigation Workbench Design

**Status:** Approved for implementation  
**Date:** 2026-07-19  
**Primary audience:** Freight operations analyst  
**Portfolio role:** First flagship project in the Living Evidence portfolio

## Vision

Freight v2 is a portable freight-operations investigation system. It turns a validated shipment snapshot into a ranked queue of operational exceptions, lets an analyst inspect the evidence behind an alert, tests how detection thresholds change workload and false alarms, and records a temporary operational decision.

The original repository is a useful prototype, but it is not the specification. The rebuild may change the data model, anomaly generation, detection logic, evaluation, repository structure, and interface wherever required to make the project reproducible, scientifically defensible, and flagship-worthy.

The product is not a generic KPI dashboard and does not pretend to stream live freight data. It is a working investigation environment built around a versioned, validated network snapshot.

## Product thesis

> A summary report tells operations that costs moved. Freight v2 finds the lane, shipment pattern, carrier context, and evidence that explain where the network broke—and helps decide what to do next.

## Reference translation

The interaction design is informed by the user's reference sites without copying them:

- **ITom:** navigation behaves like entering and moving through a place. Freight translates this into moving from network → queue → lane investigation rather than using literal 3D rooms.
- **PostHog Products:** dense functionality sits inside a coherent application frame. Freight uses a consistent operations-instrument chrome rather than unrelated dashboard cards.
- **Josh Jamili:** authored visual rhythm and personality matter more than portfolio conventions. Freight uses a project-specific dark operational language and deliberate transitions.
- **Balaj Marius:** restraint makes distinctive evidence more credible. Freight exposes only information that advances an investigation.
- **Rachel Chen:** projects lead with behavior, maintain a navigable narrative, and document decisions and constraints. Freight opens as a working product and retains an evidence/case-study layer underneath it.

## Goals

1. Make every displayed metric traceable to one canonical run.
2. Prevent future information and ground-truth labels from leaking into detection features or threshold selection.
3. Demonstrate realistic data engineering, anomaly detection, operational prioritization, and product judgment as one system.
4. Run locally without Snowflake, Power BI, a database server, a paid map, an API, or an LLM.
5. Give visitors a complete loop: notice → investigate → test sensitivity → decide → inspect evidence.
6. Work on desktop, tablet, mobile, keyboard navigation, and reduced-motion settings.

## Non-goals

- Real-time carrier integrations or tracking feeds.
- Authentication, multi-user persistence, or enterprise workflow management.
- User-uploaded shipment data in the public portfolio.
- A claim that the synthetic records are observed carrier invoices.
- A live Python backend for the public experience.
- Generative-AI explanations.
- Recreating Power BI or Snowflake in miniature.

## Core experience

### 1. Network pulse

The entry view shows a named, dated network snapshot. Shipment motion establishes normal network activity. Healthy lanes are quiet, developing drift is amber, and confirmed anomalies are red. A small status line identifies the run, coverage window, and number of open exceptions.

The primary action is **Review highest-priority exception**. The network remains explorable, but it does not force the visitor to understand every control before beginning.

### 2. Exception queue

Alerts are ranked by an operational priority score derived from:

- estimated excess cost;
- confidence and agreement across detection methods;
- affected shipment count;
- service-level deterioration;
- persistence across prior observed weeks;
- segment support and data-quality confidence.

Each queue item states the lane, mode, carrier scope, short reason, severity, and estimated exposure. Color is never the only indication of status.

### 3. Lane investigation

Selecting an exception focuses the network and opens an investigation surface with:

- expected versus observed cost;
- historical baseline using only prior observations;
- shipment-level distribution;
- carrier comparison;
- rate, weight, fuel, and residual cost decomposition;
- late-delivery context;
- detection-method evidence and segment support;
- affected shipments and data-quality warnings.

The interface distinguishes **what was observed**, **what the system inferred**, and **what remains uncertain**.

### 4. Sensitivity lab

The analyst can adjust a small set of meaningful parameters:

- robust shipment threshold;
- IQR multiplier;
- rolling-deviation threshold;
- required method agreement.

The alert queue and evaluation summary update locally. The interface explicitly shows the tradeoff between recall, false-positive rate, and review workload. Parameters use bounded values supported by the exported evaluation grid; the browser does not fabricate model results between unsupported configurations.

### 5. Operational decision

For the selected alert, the analyst records one temporary action:

- escalate carrier;
- review contract/rate;
- monitor lane;
- dismiss as explainable;
- mark for data-quality review.

The decision updates the local queue and creates a session-only investigation trail. Reloading resets the product; the UI states this clearly.

### 6. Evidence mode

A persistent evidence control opens:

- data lineage and source boundaries;
- run ID, timestamps, schema version, and artifact hashes;
- anomaly-generation taxonomy;
- detection definitions;
- train/calibration/evaluation time windows;
- precision, recall, F1, false-positive rate, and review-load tradeoffs;
- known limitations;
- the original Snowflake and Power BI architecture as project history.

The case-study narrative follows the working product rather than replacing it.

## Analytical architecture

```text
FAF5 distribution source or deterministic fixture
  → source validation
  → carrier rates, fuel history, and base shipments
  → typed anomaly injection with isolated ground truth
  → immutable run directory with Parquet + manifest
  → time-safe anomaly detection
  → held-out evaluation and sensitivity grid
  → operational alert prioritization
  → validated portfolio evidence export
```

### Portable storage

DuckDB is an embedded analytical dependency, not a hosted service. Parquet remains the durable interchange format. The project must also expose plain Python entrypoints so the core pipeline is not coupled to an interactive DuckDB process.

Canonical outputs live beneath an immutable run directory:

```text
artifacts/runs/{run_id}/
  manifest.json
  shipments.parquet
  carrier_rates.parquet
  fuel_surcharges.parquet
  anomaly_ground_truth.parquet
  anomaly_flags.parquet
  lane_week_trends.parquet
  operational_alerts.parquet
  evaluation.json
  data_quality.json
```

No test may write into `artifacts/runs/` or overwrite a canonical run.

### Run manifest

The manifest contains:

- schema version;
- run ID;
- generation timestamp;
- seed source (`FAF5`, `PRIORS`, or `TEST`);
- source file name and checksum when applicable;
- random seed;
- row counts;
- time-window boundaries;
- anomaly taxonomy and counts;
- SHA-256 hashes for every run artifact;
- code version when Git metadata is available.

Every downstream command validates the manifest and refuses to combine artifacts with different run IDs or hashes.

### Data generation

FAF5 is used only to seed supported lane and mode distributions; portfolio copy must not imply that generated shipment invoices are observed FAF5 transactions.
The `tons_2024` distribution is a static scenario prior for the entire synthetic run, not a
time-varying feature or evidence of a historical 2023 backtest. The portfolio must state this
boundary explicitly.

The generator constructs normal operations first, including:

- carrier/lane/mode rate schedules with non-overlapping effective periods;
- shipment weights;
- weekly fuel surcharges;
- seasonal and regional volume variation;
- carrier service probabilities;
- deterministic row identity and run identity.

Rate selection is temporal: a shipment resolves exactly one rate version whose effective
interval contains its ship date. Reusing one timeless rate per carrier/lane/mode across the
entire 18-month analysis window is explicitly invalid. Weekly fuel indices must cover every
analysis week, vary over time, remain within documented curve bounds, and never use a silent
fallback when a week is missing.

Typed anomalies are injected only after the normal process is constructed:

- carrier overcharge;
- duplicate or incorrect fuel surcharge;
- rate-card override;
- weight/classification mismatch;
- persistent lane drift;
- service deterioration;
- data-quality corruption.

Ground-truth cause labels live in the evaluation artifact and are never available to detection features.
Operational shipment artifacts contain observed invoice components only; counterfactual
`expected_*` fields are recomputed later from authoritative rate and fuel tables and are not
stored by generation or injection.

Injection uses a stable seed-and-shipment-ID ownership partition for anomaly families. Within
each family, direct anomaly rows are ranked independently inside baseline, calibration, and
evaluation windows; persistent lane drift and service deterioration remain evaluation-only
group signals. Requested rates are deterministic upper bounds: realized counts can be lower
if a family's eligible ownership partition is exhausted. This makes injections non-overlapping,
row-order invariant, independent across anomaly-family rate changes, and prevents evaluation
rows from changing baseline or calibration assignments.

### Leakage-safe detection

The current full-history grouping is replaced by time-aware evaluation:

1. **Baseline window:** establishes supported lane/mode/carrier behavior.
2. **Calibration window:** chooses thresholds from past data only.
3. **Evaluation window:** measures performance on later unseen shipments.

Methods:

- robust standardized residual using median/MAD or a similarly documented robust scale;
- lane/mode IQR fences fit on prior/calibration observations;
- rolling lane-week deviation using strictly preceding observed weeks;
- explicit data-quality rules for impossible or inconsistent records.

Future observations, evaluation labels, and injected-cause fields must never enter baseline features or threshold selection. Tests must fail if evaluation-period mutations change earlier baselines.

Because one shipment may trigger multiple methods, every shipment-level KPI or carrier/lane
aggregate must deduplicate to shipment grain before joining flags. Method agreement counts
distinct method families; it must never multiply shipment volume, spend, service, or cost
aggregates. Weekly analytical outputs retain mode in their final grouping rather than
collapsing a mode-aware intermediate table into one all-mode line.

### Expected cost and excess cost

Expected cost is decomposable and auditable. It is derived from the applicable carrier rate, shipment weight, and fuel schedule, with documented lane/carrier adjustments where supported. The system does not use a black-box model merely to make the project appear more sophisticated.

Estimated excess cost is the non-negative difference between observed cost and the validated expected-cost baseline. Data-quality anomalies may be prioritized without assigning a monetary exposure when the amount is not trustworthy.

### Operational priority

Priority is a transparent composite, not a second predictive model. Component values and weights are exported so the frontend can explain the ranking. The score must be monotonic in exposure and confidence unless a documented data-quality penalty applies.

### Evaluation

Evaluation reports overall and anomaly-type performance on the held-out period:

- precision;
- recall;
- F1;
- false-positive rate;
- false negatives by anomaly type;
- review volume;
- excess-cost coverage;
- performance by mode and segment support.

The exporter provides a finite, prevalidated sensitivity grid. Frontend controls select among those real evaluation results.

## Historical Snowflake/Power BI work

Snowflake and Power BI remain part of the documented project evolution, not the runtime dependency. The rebuilt README will state that the original implementation used Snowflake warehousing and Power BI reporting, while v2 moved to open, embedded infrastructure for reproducibility.

The current private key is removed from the working tree and key patterns are ignored. Rewriting already-published Git history is a separate destructive Git operation and is not performed implicitly.

## Portfolio evidence contract

The pipeline exports a bounded public bundle:

```text
public/data/freight/v2/
  manifest.json
  network.json
  alerts.json
  evaluation.json
  lanes/{lane_id}.json
```

The bundle contains no secrets, raw FAF5 files, ground-truth labels for hidden investigation states, or unnecessary personal data. Each file repeats the run ID and schema version. Portfolio build validation fails on mismatch.

## Frontend architecture

The project route becomes a project-specific application rather than the current generic entry page.

Planned component boundaries:

- `FreightWorkbench` — state orchestration and screen layout;
- `FreightNetwork` — accessible SVG network and route focus;
- `ExceptionQueue` — ranked, filterable investigation list;
- `LaneInvestigation` — evidence, decomposition, shipments, and method agreement;
- `SensitivityLab` — bounded precomputed evaluation states;
- `DecisionComposer` — session-only decision and rationale;
- `InvestigationTrail` — local state history;
- `EvidenceDrawer` — provenance, methods, limitations, and project history;
- `FreightCaseStudy` — concise narrative following the interactive system.

State is held in a reducer with explicit actions. URL state may identify a lane or panel, but no server persistence is required.

## Visual system

The surrounding portfolio remains light and editorial. Entering Freight transitions into a dark operational instrument:

- carbon-black workspace;
- off-white operational text;
- muted steel linework;
- amber for developing drift;
- red only for confirmed operational anomalies;
- cyan/blue reserved for selected analytical evidence;
- mono typography for identifiers and measurements;
- portfolio sans for narrative and decisions.

The network is a custom SVG abstraction, avoiding paid maps and geographic-detail noise. Motion communicates shipment flow, route focus, queue re-ranking, and state transitions. It does not decorate static content.

## Responsive behavior

- **Desktop:** network, exception queue, and investigation surface coexist.
- **Tablet:** network remains primary; queue and investigation use a controlled split/drawer.
- **Mobile:** a step flow—Pulse → Queue → Investigation → Decision → Evidence—with persistent back/context controls.

No content is lost on mobile. Wide charts gain focus views rather than horizontal page overflow.

## Accessibility

- Full keyboard access to network alternatives, queue, controls, drawer, and decisions.
- Visible focus states.
- Text/status icons supplement every color state.
- SVG network has a structured textual alternative and route summaries.
- Reduced motion freezes shipment particles and uses discrete state transitions.
- Controls have explicit names, values, constraints, and result announcements.
- Dialog/drawer focus is trapped and restored correctly.

## Loading and failure states

- Initial data loading shows the workspace skeleton and run identity.
- Schema/run mismatch blocks the workbench and displays a precise evidence-integrity error.
- Missing lane detail keeps the queue available and offers a retry/back action.
- Empty filtered queues explain the active filters and provide a reset.
- Invalid URL state falls back to the highest-priority alert.

The interface never substitutes fabricated data after an error.

## Performance

- Initial public bundle remains small enough for fast static delivery.
- Lane details load on demand.
- Network motion pauses offscreen.
- No map SDK, charting framework, or animation runtime is added unless native SVG/CSS and the existing motion dependency are insufficient.
- Expensive filtering is memoized only after measurement shows it is necessary.

## Verification and release gates

### Analytical gates

- Clean Python 3.11 environment installation succeeds.
- Unit and integration tests pass.
- Tests use temporary output directories.
- Deterministic fixture hashes are stable.
- Run/hash mismatch is rejected.
- Temporal leakage tests pass.
- Ground-truth fields are absent from detector inputs.
- Evaluation metrics reproduce from the exported run.
- Full-run claims require `seed_source=FAF5`.

### Frontend gates

- TypeScript, lint, and production build pass.
- Evidence-contract validation passes at build time.
- All primary interactions work at desktop and mobile widths.
- Keyboard and focus flows work.
- Reduced-motion mode is meaningful.
- No horizontal overflow at supported widths.
- Runtime console has no application errors.
- Project navigation back to the portfolio remains intact.

### Claim gate

Every public number is either derived from the versioned evidence bundle or explicitly labeled as an assumption, scenario, or historical result.

## Definition of done

Freight v2 is complete when a visitor can enter the network, select a real exported exception, understand why it was flagged, inspect the operational and statistical evidence, test supported thresholds, record a decision, and verify the provenance—without a hosted backend or unsupported claim—and when the repository can reproduce that experience from a clean environment.
