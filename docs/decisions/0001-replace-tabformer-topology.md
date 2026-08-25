# Decision 0001: Replace the TabFormer topology

## Decision

Use the full private MLG-ULB `creditcard.csv` benchmark and retire the TabFormer-specific streaming, card-history, banking, CDC, analytics, and cloud topology.

## Why

The selected source has only `Time`, `V1` through `V28`, `Amount`, and `Class`. It can support retrospective ranking and capacity analysis but cannot support card, customer, merchant, channel, location, or operational event semantics. Preserving the old topology would require fabricated fields and train-serve skew.

## Alternatives rejected

- Treat MLG-ULB as a drop-in model-only replacement.
- Generate synthetic card and merchant identifiers.
- Keep the old architecture as unused portfolio decoration.

## Not done

No raw dataset redistribution, deployment, history rewrite, push, or publication.

## Changed

Added the new product boundary and removed obsolete source, contracts, tests, infrastructure, analytics, and documentation.
