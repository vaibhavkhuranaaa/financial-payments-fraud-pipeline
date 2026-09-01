# Financial Payments Fraud Decision Workbench

![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Tests](https://img.shields.io/badge/tests-22%20passing-19734b)
![PR--AUC](https://img.shields.io/badge/test%20PR--AUC-0.7744-255f73)
![Review policy](https://img.shields.io/badge/recall%20at%201%20per%201000-68.0%25-c26b27)

An evidence-backed fraud review product built on the full MLG-ULB `creditcard.csv` benchmark. It connects model selection to a bounded analyst queue and keeps the raw source and row-level artifacts out of Git.

The product uses the full 284,807-row benchmark and its real trained run artifacts. It does not generate a substitute product dataset. The [public decision workbench](https://fraud-decision-workbench-lfpwcuk37a-uc.a.run.app) serves the complete scored holdout from a cost-bounded Cloud Run service.

## What it does

- Validates all 284,807 private source rows and preserves valid duplicates.
- Splits by chronology into train, calibration, and untouched test partitions.
- Compares a class-weighted logistic-regression baseline with XGBoost.
- Selects model complexity on calibration evidence, then evaluates the selected model on test.
- Calibrates probabilities and converts a review budget into a deterministic ranked queue.
- Serves a responsive Dash console plus health, metrics, summary, queue, record, and score APIs.
- Distinguishes zero capacity, no eligible records, missing artifacts, corrupt artifacts, and invalid scoring input.

## Architecture

```text
private creditcard.csv
        |
        v
schema validation + stable row IDs
        |
        v
chronological train / calibration / test
        |
        +--> logistic baseline ----+
        |                          |
        +--> XGBoost challenger ---+--> calibration gate
                                           |
                                           v
                                 immutable local run artifacts
                                           |
                                           v
                                  Dash / Flask workbench
```

The approved local topology is deliberately one process. The source has no card, merchant, customer, or live-event identity, so the previous Kafka, Spark, Redis, SQL, CDC, and cloud stack was retired instead of being fed fabricated fields. See [architecture](docs/architecture.md).

## Evaluation

The selected logistic model reached 0.7744 PR-AUC on the 56,962-row chronological test partition, with a 95% bootstrap interval of 0.6704 to 0.8501. XGBoost reached 0.7962 on test but did not clear the predeclared calibration lift, so it remained diagnostic.

At one review per 1,000 transactions, the bounded queue reviewed 56 rows, captured 51 of 75 observed fraud rows, and included 5 false positives. Retrospective precision was 91.1% and recall was 68.0%. The strategy view makes clear that this is a capacity-bound operating point: 80% observed recall needs 120 reviews, while 90% needs 2,430. These are post-hoc scenarios, not deployment thresholds. Full definitions and limitations are in [the evaluation report](evaluation/report.md) and [metric glossary](docs/metric-glossary.md).

## Run locally

Use Python 3.11. Keep the owner-authorized full file at `data/raw/creditcard.csv`.

```sh
python3.11 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
make train
make check
make smoke
make demo
```

Open `http://127.0.0.1:8050`. The same runtime can be built with `make docker-build`; mount the ignored `artifacts/` directory read-only.

CI runs lint, 22 hermetic tests, replay-contract checks, formatting, container configuration, and an image build without requiring the private dataset.

## Data

The workbench uses the [MLG-ULB credit card fraud benchmark](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud). Download access and reuse terms remain between the user and source provider. The repository does not redistribute the CSV or trained artifact files. The public service returns anonymized scored holdout rows for its review queue and drill-down workflow.

## Limits

- Retrospective benchmark, not a live payment system.
- No card, customer, merchant, channel, geography, analyst feedback, or intervention outcomes.
- `Class` is used only for training and retrospective evaluation, never as a scoring feature.
- Model signals are associations, not causal explanations.
- Only 57 fraud rows occur in calibration and 75 in test, so uncertainty is material.
- No production-readiness, availability, throughput, latency, loss-avoided, or fairness claim is supported.
- The raw dataset and trained artifact files are not redistributed.

## Scaling and hosting

The public demonstration runs one scale-to-zero Cloud Run instance at most. It mounts the immutable active run read-only from private object storage and applies a process-wide sustained request ceiling with a bounded burst allowance. The raw CSV is never uploaded. The public queue contains only anonymized benchmark rows and model outputs; it has no cardholder, merchant, or account identity.

The hosting configuration is a cost-bounded portfolio demonstration, not a production availability, security, latency, throughput, or scaling claim.
