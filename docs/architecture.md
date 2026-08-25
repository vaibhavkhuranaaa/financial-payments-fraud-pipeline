# Architecture

## Local topology

The product has two bounded stages:

1. `src.pipeline.train` validates the private CSV, partitions it by chronology, fits the baseline and challenger, calibrates probabilities, applies the selection gate, evaluates the held-out test period, and atomically writes one ignored run directory.
2. `src.dashboard.app` loads the current complete run into one Dash and Flask process. Pure policy functions recompute the queue and consequences when threshold or capacity changes.

The artifact contract contains `manifest.json`, `validation.json`, `partitions.json`, `evaluation.json`, `model.joblib`, and `scored_test.parquet`. `latest.json` changes only after the run directory is complete. An incomplete pointer fails closed.

## Data boundaries

The scoring feature contract is exactly `Time`, `V1` through `V28`, and `Amount`. `Class` is a retrospective label and cannot enter the score request. Source row ID is local lineage, not a payment identifier.

Raw source data, scored rows, model files, and screenshots remain ignored. Only aggregate evaluation is versioned in the repository.

## Interfaces

| Interface | Purpose |
| --- | --- |
| `GET /healthz` | Artifact readiness |
| `GET /api/metrics` | Aggregate run evaluation |
| `GET /api/summary` | Policy consequences for threshold and capacity |
| `GET /api/queue` | Bounded ranked transactions |
| `GET /api/transactions/<row_id>` | Anonymized record and top model signals |
| `POST /api/score` | Exact feature-contract scoring |
| `/` | Interactive decision console |

## Failure behavior

- Missing source or schema drift stops training before a model is written.
- Invalid rows carry rejection reasons in a private artifact.
- Incomplete or corrupt runs return artifact errors and HTTP 503, not zero metrics.
- Zero review capacity produces a policy empty state.
- Filters with no matching queue records produce a distinct empty state.
- Invalid score payloads return HTTP 400 with contract diagnostics.

## Scaled design, not deployed

The same immutable run contract could move to object storage, with scheduled compute for training and an authenticated container for the console. Central logs, data freshness, model freshness, and rollback would be required. No scale or cost claim is made because this topology has not been provisioned.
