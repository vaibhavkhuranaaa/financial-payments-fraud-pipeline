# Decision 0006: Expose the capacity frontier

## Decision

Treat the 68.0% recall result as one capacity-bound operating point. Add a retrospective capacity frontier, minimum workload by recall target, count and amount capture metrics, queue efficiency, and capacity ceiling to the product. Keep the immutable selected model and default policy unchanged.

## Why

A fraud strategy lead needs to distinguish model ranking quality from analyst workload. The original console showed the consequences of one policy but did not reveal what additional capacity could buy. On the scored holdout, 80% recall requires 120 reviews while 90% requires 2,430. Showing that bend prevents both underinvestment based on the 68% headline and unjustified pursuit of recall at any cost.

## Alternatives rejected

- Replace the selected model with XGBoost because its held-out result is stronger. Rejected because it did not clear the predeclared calibration selection gate.
- Change the default queue to the post-hoc 80% scenario. Rejected because the holdout labels have already been observed and no analyst-cost, customer-friction, or fraud-loss objective exists.
- Report source amount as prevented loss. Rejected because the dataset contains transaction amount, not intervention or loss outcomes.
- Optimize a single F1 threshold. Rejected because F1 hides the separate operational costs of missed fraud and non-fraud reviews.

## Not done

No production threshold, automated decline policy, handling-time model, loss-avoidance claim, fairness assessment, drift claim, deployment, or customer-impact estimate was added.

## Changed

The policy layer now computes capacity ceilings, workload efficiency, amount recall, score-ranked frontiers, and recall-target workloads. The dashboard leads with a director brief, groups coverage, review economics, and observed source amount, adds a frontier and scenario table, fixes chart containment and sticky queue headers, and keeps score distribution as a model diagnostic.
