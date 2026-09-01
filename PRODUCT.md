# Product

## Platform

web

## Users

The primary user is a fraud operations lead reviewing a retrospective batch. They choose a review budget and score threshold. An analyst then inspects the bounded, ranked queue. A secondary audience is a technical reviewer evaluating whether the data and modelling evidence support the interface.

## Product Purpose

Financial Payments Fraud Decision Workbench turns the full private MLG-ULB benchmark into a reproducible decision product. Success means the user can see how review capacity and threshold change fraud captured, fraud missed, false positives, and queue volume without implying a live payment decision.

## Positioning

The workbench binds model evaluation to analyst capacity. It exposes both controls, identifies which one is binding, and keeps every aggregate tied to one immutable chronological model run.

## Operating Context

Training runs locally from the full git-ignored dataset. Local and hosted consoles read the same immutable scored holdout produced by that chronological run, not a substitute dataset or live transaction stream. The hosted container mounts the active artifact run read-only from private object storage.

## Capabilities and Constraints

- Validate all 284,807 source rows and preserve valid duplicates.
- Compare a logistic-regression baseline with an XGBoost challenger.
- Fit calibration and choose the shipping model without test-label selection.
- Adjust threshold and review capacity, inspect a bounded queue, export the current view, and drill into an anonymized transaction.
- Compare the active policy with a retrospective capacity frontier and minimum workload for 75% through 100% observed recall.
- Use only `Time`, `Amount`, `V1` through `V28`, and the retrospective `Class` outcome.
- Do not claim card, customer, merchant, channel, geography, real-time processing, operational feedback, causal explanation, or automated declines.
- Keep source rows, model files, and private evidence out of public Git history.

## Evidence on Hand

The full source is present at `data/raw/creditcard.csv`. Local validation records the shape and quality aggregates. Ignored run artifacts under `artifacts/` contain the selected model, scored chronological holdout, evaluation, partition report, and manifest. The public Cloud Run demonstration mounts the complete active run from private object storage and exposes only anonymized benchmark rows and model outputs. No customer, production-readiness, live-volume, or service-level evidence exists.

## Product Principles

- Capacity before spectacle.
- Honest states before convenient defaults.
- A simpler model wins when added complexity does not clear its selection gate.
- Model signals are associations, not causal reasons.
- Every visible number resolves to the loaded run and current policy controls.

## Accessibility & Inclusion

The web console must support keyboard interaction, visible focus, reduced motion, responsive layouts, readable metric definitions, and color-independent state labels. Text and controls target WCAG 2.1 AA contrast.
