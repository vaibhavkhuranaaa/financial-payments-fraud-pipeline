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

No hosted deployment or production-scale claim is retained. Raw source and run artifacts remain local inputs to the product.

## Changed

The deployment blueprint, generated demo artifacts, substitute scorer, dashboard disclosure, and hosted-demo claims were removed. The container again requires the real artifact mount.
