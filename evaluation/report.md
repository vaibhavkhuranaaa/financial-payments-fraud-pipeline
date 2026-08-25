# Evaluation report

Run `d641b4fbec8c6a38` validates all 284,807 source rows and uses a chronology-preserving 60/20/20 train, calibration, and test split. Equal elapsed timestamps stay in one partition.

The class-weighted logistic-regression baseline was selected. XGBoost improved calibration PR-AUC by 0.0064, below the predeclared 0.02 complexity gate. Its stronger held-out test result is reported as a diagnostic and was not used to reverse selection.

## Held-out ranking

| Model | PR-AUC | 95% bootstrap interval | Brier score | Selection status |
| --- | ---: | ---: | ---: | --- |
| Logistic regression | 0.7744 | 0.6704 to 0.8501 | 0.000456 | Selected |
| XGBoost | 0.7962 | 0.6949 to 0.8694 | 0.000418 | Diagnostic only |

## Capacity policy

At one review per 1,000 test transactions, the calibration-selected threshold is 0.2818. Capacity is binding. The queue contains 56 transactions, captures 51 of 75 observed fraud rows, misses 24, and includes 5 false positives. Retrospective precision is 91.1% and recall is 68.0%.

The 68.0% recall is an operating point, not the ranking ceiling. The queue reviews only 0.098% of holdout rows. It captures 57.3% of observed fraud source amount, or 4,427.58 of 7,729.26 source units. Source amount is not realized loss or loss avoided.

## Retrospective capacity frontier

The table below answers the strategy question: what is the smallest score-ranked queue that reaches each observed recall target on this holdout? These are post-hoc diagnostic scenarios. They must not be used as deployment thresholds without a new validation design and operating-cost inputs.

| Recall target | Minimum reviews | Reviews per 1,000 | Queue precision | Non-fraud reviews | Amount recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| 75% | 79 | 1.39 | 72.2% | 22 | 65.9% |
| 80% | 120 | 2.11 | 50.0% | 60 | 66.1% |
| 85% | 217 | 3.81 | 29.5% | 153 | 69.4% |
| 90% | 2,430 | 42.66 | 2.8% | 2,362 | 74.3% |
| 95% | 7,762 | 136.27 | 0.9% | 7,690 | 83.1% |
| 100% | 17,541 | 307.94 | 0.4% | 17,466 | 100.0% |

The useful frontier bends sharply after 80% to 85% recall. Relative to the current policy, the 80% scenario adds 64 reviews for 9 additional fraud captures. Moving from 80% to 85% adds 97 reviews for 4 captures. Moving from 85% to 90% adds 2,213 reviews for another 4 captures. Without analyst cost, customer-friction, or fraud-loss inputs, the evidence supports scenario comparison but not a single economically optimal policy.

## Limits

The source is an anonymized static benchmark. It has no card, customer, merchant, channel, geography, analyst disposition, intervention outcome, or live event semantics. The test split contains only 75 fraud rows, and the bootstrap interval is correspondingly wide. These results do not establish production performance, causal explanations, throughput, latency, or loss avoided.

The machine-readable aggregate is in `evaluation/summary.json`. Row-level scores, source data, and model artifacts remain local and gitignored.
