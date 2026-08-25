# Scope

## In scope

- Full local use of the owner-authorized MLG-ULB benchmark.
- Exact schema validation, invalid-row handling, duplicate visibility, stable source row IDs, and chronological partitioning.
- Logistic-regression baseline, XGBoost challenger, Platt calibration, predeclared selection rule, held-out evaluation, and bootstrap intervals.
- Threshold and cases-per-1,000 controls, deterministic queue bounds, inline consequence metrics, queue filters, CSV export, and record drilldown.
- Local health, aggregate metrics, summary, queue, record, and exact-contract score APIs.
- Automated data, model-artifact, policy, API, empty-state, error-state, and smoke verification.

## Out of scope

- Live transaction streaming and automatic payment decisions.
- Fabricated card, customer, merchant, channel, location, or feedback fields.
- Operational loss avoided, reviewer handling time, fairness, drift, throughput, latency, or uptime claims.
- Raw dataset or row-level artifact redistribution.
- Paid cloud resources, deployment, push, publication, merge, history rewrite, or portfolio mutation without a separate owner gate.

## Completion boundary

Local P0 through P6 delivery is complete when repository checks, full-data replay, browser behavior, evidence records, decisions, and internal delivery checks pass. The project remains `building` at that boundary because deployment, scale evidence, publication, and portfolio work require explicit external approvals.
