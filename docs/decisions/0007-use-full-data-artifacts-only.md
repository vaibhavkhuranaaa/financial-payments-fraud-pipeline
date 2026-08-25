# Use full-data artifacts only

## Decision

Run the product only from the complete owner-provided benchmark and the real model artifacts trained from it. Do not maintain a hosted substitute dataset or generated product queue.

## Why

One data path keeps model evaluation, policy metrics, queue records, and drill-down behavior tied to the same complete chronological run.

## Alternatives rejected

- A hosted demonstration built from generated records would create a second product data path.
- A sampled benchmark would weaken the evidence behind rare-event metrics.
- A static dashboard would not preserve policy controls or record drill-down.

## Not done

No generated, sampled, or substitute hosted dataset is retained. The raw source remains local and is never uploaded. No production-scale claim is made.

## Changed

The generated demo artifacts and substitute scorer were removed. Local and hosted containers require the real complete artifact run. Decision 0009 bounds the public service that mounts that run.
