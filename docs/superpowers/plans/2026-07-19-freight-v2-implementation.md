# Freight v2 Implementation Plan

## Goal

Rebuild Freight KPI Tracker into a portable, leakage-safe analytical engine and a portfolio-hosted Investigation Workbench. Every public claim must be generated from one validated run, while the public experience must require no hosted backend, paid service, account, or live database.

## Architecture

The Freight repository owns source ingestion, deterministic shipment generation, typed anomaly injection, time-safe detection, held-out evaluation, alert prioritization, provenance, and evidence export. The portfolio repository owns the interactive workbench and case-study presentation. Their only integration boundary is the versioned public evidence contract.

## Tech stack

- Python 3.11.15
- pandas 2.3.3
- NumPy 2.0.2
- PyArrow 23.0.1
- DuckDB 1.5.4
- pytest 9.1.1
- Ruff 0.15.22
- Next.js 15.5.18
- React 19.2.5
- TypeScript 5.9.3
- Existing Framer Motion 12.38.0 only where CSS/SVG cannot express the state transition cleanly

## Repositories

- Analytical engine: `/Users/abhayjuloori/projects/freight-kpi-tracker`
- Portfolio frontend: `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex`

## Execution rules

1. Preserve unrelated dirty files in both repositories.
2. Do not rewrite published Git history without separate explicit authorization.
3. Use tests before implementation for provenance, leakage, and metric logic.
4. Never run a production Next build while its development server is using the same `.next` directory.
5. Do not advance to another flagship project until Freight passes all release gates and the user reviews it.

---

## Phase 1 — Repository safety and portable foundation

### Task 1: Remove runtime credentials and pin the environment

**Modify**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/.gitignore`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/requirements.txt`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/Makefile`

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/.python-version`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/pyproject.toml`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/test_environment_contract.py`

**Delete from the working tree**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/rsa_key.p8`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/rsa_key.pub`

**Implementation**

1. Add a failing test asserting the supported Python range and package import surface.
2. Pin the versions in the Tech stack and expose `freight-v2` as a console entrypoint.
3. Add ignore rules for private-key formats, generated artifacts, DuckDB files, and temporary export directories.
4. Replace Make targets with `.venv`-backed `install`, `test`, `lint`, `fixture`, `run`, and `export-portfolio` commands.
5. Keep historical Snowflake dependencies outside the default install; no current command may default to `rsa_key.p8`.

**Verify**

```bash
python3.11 -m venv .venv-v2
.venv-v2/bin/pip install -e '.[dev]'
.venv-v2/bin/python -m pytest tests/test_environment_contract.py -q
.venv-v2/bin/ruff check src tests
test ! -e rsa_key.p8 && test ! -e rsa_key.pub
```

Expected: environment test and lint pass; key files are absent from the working tree. No history rewrite occurs.

### Task 2: Define immutable run and table contracts

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/__init__.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/config.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/contracts.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/provenance.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_contracts.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_provenance.py`

**Implementation**

1. Write failing tests for required shipment, rate, fuel, truth, flag, trend, and alert columns.
2. Define schema version `2.0.0` and supported seed sources.
3. Implement deterministic run IDs for test fixtures and UUID run IDs for normal runs.
4. Implement SHA-256 file hashing, manifest write/read, artifact registration, and manifest validation.
5. Reject missing files, mismatched run IDs, row-count drift, schema mismatch, and hash mismatch with precise exceptions.
6. Keep all path dependencies injectable so tests use `tmp_path` exclusively.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_contracts.py tests/v2/test_provenance.py -q
```

Expected: tests prove that a one-byte artifact mutation and a different run ID both fail validation.

### Task 3: Preserve historical Snowflake/Power BI work without runtime coupling

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/docs/history/snowflake-powerbi.md`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/legacy/snowflake/README.md`

**Modify**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/scripts/load_snowflake.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/scripts/validate_load.py`

**Implementation**

1. Document the original Snowflake schema, key-pair authentication, Power BI exports, and why v2 moved to open embedded infrastructure.
2. Make legacy scripts require an explicit key path and emit a clear legacy warning; remove the unsafe repository-root key default.
3. Ensure default v2 installation and commands do not import Snowflake.

**Verify**

```bash
rg -n 'rsa_key\.p8|SNOWFLAKE_PRIVATE_KEY_FILE' scripts src Makefile README.md docs
.venv-v2/bin/python -c 'import freight_v2'
```

Expected: only historical documentation mentions the deleted key name; importing v2 requires no Snowflake package.

---

## Phase 2 — Deterministic data and typed anomaly generation

### Task 4: Build source distribution and normal-operation generation

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/sources.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/generation.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_sources.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_generation.py`

**Implementation**

1. Port FAF5 lane/mode extraction behind an explicit source adapter.
2. Keep a deterministic priors fixture for tests and public reproducibility.
3. Generate time-versioned rate cards with non-overlapping effective intervals, then resolve
   exactly one active version for every shipment date.
4. Generate weekly, non-flat fuel history with complete week coverage and bounded curve
   changes; never silently substitute a default for a missing week.
5. Generate seasonal volume, supported lane/carrier/mode combinations, shipment weights,
   service outcomes, and decomposable observed invoice components. Do not persist
   counterfactual `expected_*` fields in operational shipment artifacts.
6. Add deterministic dates spanning baseline, calibration, and evaluation windows.
7. Write failing tests for temporal rate uniqueness/coverage, fuel variation/coverage,
   referential integrity, cost identities, supported ranges, time-window coverage, and determinism.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_sources.py tests/v2/test_generation.py -q
```

Expected: identical seeds produce identical table hashes; all shipment foreign keys resolve.

### Task 5: Add typed anomalies without leaking labels

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/anomalies.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_anomalies.py`

**Implementation**

1. Define the approved anomaly taxonomy: carrier overcharge, duplicate fuel surcharge, rate override, weight/class mismatch, persistent lane drift, service deterioration, and data-quality corruption.
2. Inject anomalies after normal invoice construction; expected costs are recomputed later
   from authoritative rate and fuel tables.
3. Keep cause and ground-truth columns in a separate evaluation table.
4. Ensure operational tables contain only observable fields.
5. Prevent overlapping injections unless the fixture explicitly requests a mixed-cause scenario.
6. Test that each anomaly changes only its documented observable fields and remains reproducible.
7. Partition anomaly-family ownership by stable seed-and-shipment-ID hashes, rank direct rows
   independently within temporal windows, and keep grouped anomalies evaluation-only. Treat
   requested rates as deterministic upper bounds when an eligible family partition is exhausted.
8. Test row-order/custom-index invariance, cross-family rate independence, and that evaluation
   append/remove/mutation cannot change baseline or calibration labels or observables.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_anomalies.py -q
```

Expected: detector input schemas contain no `is_anomaly`, `anomaly_type`, or injected-cause fields.

### Task 6: Write immutable run artifacts

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/run_builder.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_run_builder.py`

**Implementation**

1. Write Parquet tables and manifest into a temporary staging directory.
2. Validate schemas, row counts, run IDs, and hashes before atomic promotion to `artifacts/runs/{run_id}`.
3. Refuse to overwrite an existing run directory.
4. Add a small `TEST` fixture command and a full `FAF5` command.
5. Add `validate --artifact-root PATH --latest` so verification can resolve a generated run without an unknown ID.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_run_builder.py -q
.venv-v2/bin/freight-v2 build --seed-source TEST --rows 5000 --output /tmp/freight-v2-plan-fixture
```

Expected: one validated run is created under the supplied temporary output; the repository's canonical artifact directory is untouched.

---

## Phase 3 — Leakage-safe detection and evaluation

### Task 7: Implement expected-cost residuals and time-safe baselines

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/baselines.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_baselines.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_temporal_leakage.py`

**Implementation**

1. Calculate expected cost from rate card, billed weight, and matched fuel schedule.
2. Fit segment medians, MAD scales, and IQR bounds using baseline rows only; reserve
   calibration rows for threshold selection and freeze both before evaluation.
3. Use explicit fallback hierarchy when a lane/mode/carrier segment lacks support.
4. Record support level and baseline source on every scored row.
5. Add mutation tests proving that changing evaluation-period values cannot alter baseline/calibration statistics.
6. Add tests proving that ground-truth columns are rejected from the scoring frame.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_baselines.py tests/v2/test_temporal_leakage.py -q
```

Expected: future-data mutation leaves all earlier fitted values byte-for-byte identical.

### Task 8: Implement detectors and lane-week trends

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/detection.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_detection.py`

**Implementation**

1. Implement robust residual, IQR, strictly trailing lane-week deviation, and data-quality detectors.
2. Emit one normalized flag schema with method, threshold, score, support, and reason fields.
3. Ensure rolling signals use preceding observed weeks only.
4. Keep method agreement derivable without double-counting shipments.
5. Test edge cases: zero MAD/IQR, sparse segments, missing weeks, zero weight, and data-quality records.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_detection.py -q
```

Expected: sparse/degenerate segments degrade to documented fallbacks rather than producing infinite scores.

### Task 9: Build held-out evaluation and bounded sensitivity grid

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/evaluation.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_evaluation.py`

**Implementation**

1. Join held-out flags to ground truth by shipment and run ID.
2. Calculate overall and anomaly-type precision, recall, F1, FPR, review volume, excess-cost coverage, and false-negative counts.
3. Deduplicate union metrics by shipment and distinct method family; add a regression proving
   multi-method flags cannot fan out shipment counts, spend, cost, or service aggregates.
4. Evaluate a finite parameter grid used by the public sensitivity controls.
5. Choose and record the default operating point using a documented cost-aware rule, never evaluation F1 alone.
6. Test all-zero, all-positive, duplicate-flag, and run-mismatch cases.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_evaluation.py -q
```

Expected: hand-calculated fixtures match every reported metric and duplicate flags do not inflate counts.

### Task 10: Prioritize operational alerts transparently

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/prioritization.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_prioritization.py`

**Implementation**

1. Aggregate flagged shipments into operational alert units.
2. Calculate estimated exposure, persistence, service impact, method agreement, data quality, and support.
3. Apply documented weights and export every component.
4. Enforce monotonicity tests for exposure and confidence.
5. Produce stable human-readable reasons from deterministic rules.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_prioritization.py -q
```

Expected: increasing exposure or method agreement cannot lower priority when other inputs are fixed.

---

## Phase 4 — Evidence export and analytical release candidate

### Task 11: Export and validate the portfolio evidence contract

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/export.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/src/freight_v2/cli.py`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/tests/v2/test_export.py`

**Target output**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/public/data/freight/v2/manifest.json`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/public/data/freight/v2/network.json`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/public/data/freight/v2/alerts.json`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/public/data/freight/v2/evaluation.json`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/public/data/freight/v2/lanes/*.json`

**Implementation**

1. Export only the bounded public fields from one validated run.
2. Repeat schema version and run ID in every file.
3. Select representative lanes deterministically from high, medium, and data-quality alerts.
4. Include supported sensitivity states, not interpolated metrics.
5. Write into a temporary directory, validate all cross-file references, then atomically replace the target export directory.
6. Keep ground-truth causes out of operator-visible lane files while retaining aggregate evaluation by anomaly type.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests/v2/test_export.py -q
.venv-v2/bin/freight-v2 export-portfolio --run accepted --output '/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/public/data/freight/v2'
```

Expected: export validation passes and no file contains secret/key material or a mismatched run ID.

### Task 12: Generate the canonical full-run evidence

**Modify**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/README.md`

**Create**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/docs/methodology.md`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/docs/model-card.md`

**Implementation**

1. Run the complete suite before generating evidence.
2. Generate a full `FAF5`-seeded run from the available raw source.
3. Inspect anomaly-type prevalence, split balance, sparse segments, metrics, and alert reasons.
4. Correct generator/detector defects through tests; do not tune on the held-out evaluation window.
5. Accept the run by writing `artifacts/accepted-run.json` only after validation; this pointer contains the run ID and manifest hash.
6. Export the accepted run to the portfolio.
7. Record exact commands, source boundaries, selected operating point, limitations, and historical architecture.

**Verify**

```bash
.venv-v2/bin/python -m pytest tests -q
.venv-v2/bin/ruff check src tests
.venv-v2/bin/ruff format --check src tests
.venv-v2/bin/freight-v2 build --seed-source FAF5 --faf5-path data/raw/faf5_2022_2024.csv --rows 75000 --output artifacts/runs
.venv-v2/bin/freight-v2 accept --artifact-root artifacts/runs --latest
.venv-v2/bin/freight-v2 validate --run accepted
```

Expected: one full validated run exists; all public JSON comes from it; README numbers match the generated evaluation exactly.

---

## Phase 5 — Portfolio evidence model and route architecture

### Task 13: Add TypeScript evidence contracts and build-time validation

**Create**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/lib/freight/types.ts`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/lib/freight/data.ts`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/lib/freight/validate.ts`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/scripts/validate-freight-evidence.mjs`

**Modify**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/package.json`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/lib/projects.ts`

**Implementation**

1. Define exact TypeScript interfaces matching schema `2.0.0`.
2. Load public evidence only through one validation module.
3. Reject cross-file run/schema mismatch, duplicate alert IDs, missing lane files, invalid severity/method enums, and out-of-range metrics.
4. Add `validate:evidence` before production build.
5. Remove the obsolete external Freight demo concept and update verified project copy from the accepted run.

**Verify**

```bash
npm run validate:evidence
npm run typecheck
```

Expected: valid artifacts pass; a temporary run-ID mutation makes validation fail with the offending file name.

### Task 14: Give Freight a project-specific route

**Create**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/freight-project-page.tsx`

**Modify**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/app/work/[slug]/page.tsx`

**Implementation**

1. Render the Freight project-specific application only for `freight-kpi-tracker`.
2. Preserve the generic case-study route for the other three projects until their rebuilds begin.
3. Keep metadata, portfolio back navigation, and route static generation intact.
4. Ensure invalid slugs still return the existing not-found behavior.

**Verify**

```bash
npm run typecheck
npm run build
```

Expected: all five existing routes still generate and Freight uses its dedicated component.

---

## Phase 6 — Investigation Workbench interface

### Task 15: Build the workbench state model and operational shell

**Create**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/freight-workbench.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/freight-reducer.ts`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/workbench-header.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/styles/freight-workbench.css`

**Modify**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/app/layout.tsx`

**Implementation**

1. Define explicit state/actions for selected alert, panel, sensitivity point, filters, decision trail, evidence drawer, and mobile step.
2. Build the dark project workspace within the light portfolio transition.
3. Add run status, primary action, evidence control, back navigation, loading, and evidence-integrity failure states.
4. Import project CSS once through the app layout.

**Verify**

```bash
npm run lint
npm run typecheck
```

Expected: state transitions are exhaustive and no project styling leaks into other pages.

### Task 16: Build the network pulse and exception queue

**Create**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/freight-network.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/shipment-particles.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/exception-queue.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/status-symbol.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/hooks/use-reduced-motion-safe.ts`

**Implementation**

1. Render the custom SVG network from exported nodes/edges.
2. Animate shipment flow only while visible and motion is allowed.
3. Provide an equivalent keyboard/list route selector.
4. Build the ranked queue with severity, reason, exposure, agreement, support, filters, empty state, and selected state.
5. Make **Review highest-priority exception** focus/select the first active alert.

**Verify**

```bash
npm run lint
npm run typecheck
```

Then inspect desktop/mobile screenshots and keyboard selection in the local browser.

Expected: selection synchronizes network and queue; reduced-motion mode has no continuous particles.

### Task 17: Build lane investigation and cost evidence

**Create**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/lane-investigation.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/cost-decomposition.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/baseline-chart.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/method-evidence.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/shipment-table.tsx`

**Implementation**

1. Load selected lane detail with a precise loading/missing state.
2. Separate observed facts, inferred expected cost, and uncertainty/support.
3. Build accessible SVG cost decomposition and historical baseline views.
4. Explain each triggering method, score, threshold, support, and agreement.
5. Add a compact shipment evidence table with meaningful sorting and mobile focus mode.

**Verify**

```bash
npm run lint
npm run typecheck
```

Expected: every number shown is present in the selected lane export; no client-side invented values.

### Task 18: Build sensitivity, decisions, and evidence mode

**Create**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/sensitivity-lab.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/decision-composer.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/investigation-trail.tsx`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/evidence-drawer.tsx`

**Implementation**

1. Bind controls only to exported sensitivity-grid values.
2. Announce recall/FPR/review-load changes accessibly.
3. Add the five approved decisions and a session-only queue/trail update.
4. State that reload resets decisions.
5. Build a focus-managed evidence drawer with lineage, split windows, methods, metrics, limitations, hashes, and Snowflake/Power BI history.

**Verify**

```bash
npm run lint
npm run typecheck
```

Then test decision/reset behavior, drawer focus restoration, and keyboard controls in the local browser.

Expected: sensitivity values always map to an exported evaluation row; no unsupported interpolation occurs.

### Task 19: Add the concise case-study narrative

**Create**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/freight-case-study.tsx`

**Implementation**

1. Follow the workbench with Problem, System, Data, Leakage Controls, Evaluation, Design Decisions, Historical Architecture, and Limitations.
2. Add a sticky section index on large screens and compact jump navigation on mobile.
3. Reuse generated evidence rather than duplicating metric strings.
4. Link the repository and methodology/model-card documents.

**Verify**

```bash
rg -n '[0-9]+\.?[0-9]*%' components/freight lib/projects.ts
npm run lint
npm run typecheck
```

Expected: hard-coded percentages appear only when they are labeled assumptions or historical figures.

### Task 20: Responsive, accessibility, and motion polish

**Modify**

- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/styles/freight-workbench.css`
- Freight components created in Tasks 15–19 as required by verified defects

**Implementation**

1. Validate desktop three-surface, tablet split/drawer, and mobile step-flow layouts.
2. Add visible focus, semantic landmarks, non-color statuses, screen-reader summaries, and live result announcements.
3. Verify loading, empty, error, success, disabled, hover, and focus states.
4. Keep motion state-driven and freeze continuous animation under reduced motion.
5. Fix measured overflow/performance issues without adding speculative abstractions.

**Verify**

Use browser QA at 390×844, 768×1024, and a desktop viewport; test keyboard-only and reduced-motion behavior.

Expected: no horizontal overflow, trapped focus, clipped controls, illegible charts, or console application errors.

---

## Phase 7 — Integrated verification and handoff

### Task 21: Analytical clean-room verification

**Commands**

```bash
cd /Users/abhayjuloori/projects/freight-kpi-tracker
python3.11 -m venv /tmp/freight-v2-verification
/tmp/freight-v2-verification/bin/pip install -e '.[dev]'
/tmp/freight-v2-verification/bin/python -m pytest tests -q
/tmp/freight-v2-verification/bin/ruff check src tests
/tmp/freight-v2-verification/bin/ruff format --check src tests
/tmp/freight-v2-verification/bin/freight-v2 build --seed-source TEST --rows 5000 --output /tmp/freight-v2-output
/tmp/freight-v2-verification/bin/freight-v2 validate --artifact-root /tmp/freight-v2-output --latest
```

**Verify manually**

- No test changed `artifacts/runs` or `data/processed`.
- The accepted FAF5 evidence still validates after test execution.
- Git status contains no generated cache/output outside ignored locations.

### Task 22: Portfolio production verification

**Commands**

```bash
cd '/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex'
npm run validate:evidence
npm run lint
npm run typecheck
npm run build
```

After the production build completes, restart the development server and perform browser QA for:

- portfolio → Freight transition;
- primary exception flow;
- network/queue synchronization;
- lane detail loading;
- sensitivity results;
- all five decision actions;
- evidence drawer focus;
- mobile step flow;
- reduced motion;
- back navigation;
- runtime console errors.

### Task 23: Claims and repository review

**Inspect**

- `/Users/abhayjuloori/projects/freight-kpi-tracker/README.md`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/docs/methodology.md`
- `/Users/abhayjuloori/projects/freight-kpi-tracker/docs/model-card.md`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/lib/projects.ts`
- `/Users/abhayjuloori/Documents/Portfolio Project/portfolio-codex/components/freight/`

**Review gates**

1. Search for stale `75,000`, `99.9%`, `0.924`, Snowflake-current, Power-BI-current, live-data, and unsupported AI language.
2. Verify every remaining numeric claim against the accepted export.
3. Confirm private-key files are absent from the working tree and ignore rules cover future keys.
4. Confirm historical architecture is clearly labeled historical.
5. Confirm unrelated user changes were not staged or overwritten.

### Task 24: User review checkpoint

Present the complete working Freight v2 locally with:

- analytical verification results;
- accepted run identity and source boundary;
- frontend build/runtime verification;
- known limitations;
- exact files changed in both repositories;
- the remaining separate choice about rewriting public Git history to purge the obsolete key.

Do not start Retail Demand Intelligence until the user is satisfied with Freight v2.
