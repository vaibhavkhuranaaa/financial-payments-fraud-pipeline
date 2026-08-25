# Decision 0002: Use an immutable local run contract

## Decision

Validate every source row, assign a stable source row ID, preserve equal timestamps within one chronological partition, and write a complete versioned run before advancing `latest.json`.

## Why

The source is one immutable local file. Atomic local artifacts provide reproducibility and failure recovery without pretending that a queue, warehouse, or workflow engine exists. Private rejected rows retain actionable diagnostics.

## Alternatives rejected

- Overwrite flat model files in place.
- Drop exact duplicates without an evidence-backed business key.
- Random train-test splitting.
- Update the current pointer before all files exist.

## Not done

No raw or row-level artifact enters Git.

## Changed

Added schema validation, rejection reasons, deterministic chronology, replay identity, immutable run directories, and fail-closed loading.
