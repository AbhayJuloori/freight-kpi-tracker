# Freight v2 model card

## Intended use

Freight v2 is a portfolio-grade investigation simulator for demonstrating reproducible freight
analytics, exception detection, evaluation discipline, and operational interface design. It helps
an analyst decide which synthetic lane/carrier evidence to review first.

It is not a production carrier-audit service, a causal fraud detector, a payment-blocking system,
or a substitute for contract and invoice review.

## Inputs and outputs

Inputs are a source distribution prior plus synthetic rate, fuel, shipment, and service artifacts.
Outputs are expected-cost residuals, normalized detector evidence, held-out metrics, and ranked
operational alerts. Public investigations distinguish observed values, inferred expectations, and
uncertainty/support.

## Data boundaries

- FAF5 seeds lane/mode distributions only.
- Invoices and carrier performance are synthetic.
- Fuel is a labeled deterministic scenario, not observed EIA data.
- Ground-truth cause fields never enter detector inputs.
- The public bundle is one static accepted snapshot.

## Evaluation

The accepted 75,000-row FAF5-seeded run evaluates on 12,500 later shipments. At the operating
point selected from calibration data, precision is 98.3%, recall is 100.0%, F1 is 0.991, FPR is
0.36%, and review volume is 2,162 shipments. Results depend on the synthetic anomaly taxonomy and
prevalence. Per-type metrics are one-vs-rest exception metrics, not cause-classification accuracy.

## Important failure modes

- Lane-week trends can identify legitimate neighboring shipments, so they remain contextual
  evidence and do not independently trigger shipment review.
- Sparse groups fall back or remain unevaluable rather than inventing support.
- Data-quality corruption can make monetary exposure unavailable.
- Synthetic curves are smoother and more controlled than production freight operations.
- Static distribution priors do not represent temporal economic change.

## Safeguards

- immutable hashed artifacts and exact run/schema keys;
- temporal rate/fuel coverage validation;
- baseline/calibration/evaluation separation;
- frozen-model fingerprint and scored-lineage reconstruction;
- truth-column rejection;
- exact six-method matrix validation;
- shipment, evidence-unit, and distinct-family deduplication;
- bounded sensitivity states and calibration-only operating-point selection;
- transparent priority components and deterministic reasons.

## Human review

Alerts are evidence for investigation. A human should inspect contract terms, rate versions, fuel
rules, shipment documents, carrier context, and data-quality warnings before taking operational or
financial action. The public decision composer records only a temporary session choice and makes
no external change.
