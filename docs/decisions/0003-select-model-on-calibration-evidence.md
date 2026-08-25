# Decision 0003: Select model complexity on calibration evidence

## Decision

Ship the calibrated class-weighted logistic-regression baseline because XGBoost improved calibration PR-AUC by only 0.0064, below the predeclared 0.02 replacement gate.

## Why

The challenger must earn added complexity before test evidence is considered. Logistic regression met the operational policy need and remained the selected model. XGBoost test performance is diagnostic and does not reverse the earlier decision.

## Alternatives rejected

- Select XGBoost from its stronger held-out test PR-AUC.
- Tune a threshold to maximize test F1.
- Report accuracy as the primary imbalanced-class metric.
- Hide a challenger that did not clear the gate.

## Not done

No causal interpretation, loss-avoided claim, fairness claim, or live performance claim.

## Changed

Added a named baseline, fixed challenger, Platt calibration, capacity-derived threshold, held-out PR-AUC and Brier metrics, deterministic bootstrap intervals, and explicit model selection evidence.
