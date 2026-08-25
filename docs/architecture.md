# Architecture

## Training and serving topology

The product has two bounded stages:

1. `src.pipeline.train` validates the private CSV, partitions it by chronology, fits the baseline and challenger, calibrates probabilities, applies the selection gate, evaluates the held-out test period, and atomically writes one ignored run directory.
2. `src.dashboard.app` loads the current complete run into one Dash and Flask process. Pure policy functions recompute the queue and consequences when threshold or capacity changes. The default immutable policy summary is cached at artifact load.

The artifact contract contains `manifest.json`, `validation.json`, `partitions.json`, `evaluation.json`, `model.joblib`, and `scored_test.parquet`. `latest.json` changes only after the run directory is complete. An incomplete pointer fails closed.

## Data boundaries

The scoring feature contract is exactly `Time`, `V1` through `V28`, and `Amount`. `Class` is a retrospective label and cannot enter the score request. Source row ID is local lineage, not a payment identifier.

Raw source data, scored rows, model files, and screenshots remain ignored. Only aggregate evaluation is versioned in the repository.

The public Cloud Run service mounts the complete active run from private object storage. The raw CSV is not uploaded. The service can expose anonymized scored rows and model outputs but has no cardholder, merchant, or account identity.

## Interfaces

| Interface | Purpose |
| --- | --- |
| `GET /health` | Artifact readiness |
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
- A process-wide token bucket returns HTTP 429 when the configured public request ceiling is exhausted.

## Public demonstration boundary

Cloud Run is fixed at one instance maximum and scales to zero when idle. A sustained request ceiling and bounded burst protect the public demonstration from unbounded request work. The container uses one CPU, 2 GiB of memory, four application threads, and a read-only artifact mount.

Training remains local and owner-operated. Central monitoring, authentication, scheduled retraining, rollback automation, and a production service objective are outside this portfolio demonstration.
