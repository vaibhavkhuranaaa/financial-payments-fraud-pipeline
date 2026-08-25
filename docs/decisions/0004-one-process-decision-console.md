# Decision 0004: Serve one local decision console

## Decision

Use one Dash and Flask process backed by the immutable scored holdout and selected model.

## Why

One process is the smallest topology that supports the real decision: adjust threshold and capacity, understand consequences, inspect a bounded queue, and score an exact anonymized feature vector. It also makes missing and corrupt artifacts distinguishable from a legitimate empty queue.

## Alternatives rejected

- Separate API, Redis cache, SQL database, and dashboard services.
- Fixed process-start threshold with no capacity control.
- A static report with no interaction.
- Merchant, card, or customer drilldowns unsupported by the source.

## Not done

No automatic decline, analyst writeback, authentication, or external hosting.

## Changed

Added health, metrics, summary, queue, record, and score APIs plus responsive controls, inline definitions, CSV export, record detail, model signals, and honest empty and error states.
