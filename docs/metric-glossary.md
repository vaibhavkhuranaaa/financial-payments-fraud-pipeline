# Metric glossary

| Metric | Definition | Decision use | Important limit |
| --- | --- | --- | --- |
| PR-AUC | Area under the precision-recall curve | Compare rare-event ranking without choosing a threshold | Sensitive to class prevalence |
| ROC-AUC | Area under the receiver operating characteristic curve | Secondary ranking diagnostic | Can look strong under severe imbalance |
| Brier score | Mean squared error of probability and binary outcome | Check calibrated probability quality | Depends on prevalence and retrospective labels |
| Review capacity | Maximum reviewed transactions per 1,000 test rows | Bound analyst workload | Not measured analyst handling time |
| Threshold | Minimum calibrated probability eligible for review | Exclude lower-scored rows before capacity ranking | Capacity may still be stricter |
| Precision | Captured fraud divided by reviewed transactions | Estimate useful work in the bounded queue | Retrospective outcome, not analyst disposition |
| Recall | Captured fraud divided by all observed fraud | Make missed fraud visible | Does not quantify loss avoided |
| False positives | Reviewed rows observed as non-fraud | Show wasted review capacity | No downstream customer impact is known |
| Reviews per capture | Reviewed transactions divided by captured fraud | Express queue effort for each captured fraud | Excludes analyst handling time and case complexity |
| Queue lift | Queue precision divided by holdout fraud prevalence | Compare ranked review efficiency with random selection | Depends on the retrospective holdout prevalence |
| Amount recall | Source amount on captured fraud divided by source amount on all observed fraud | Show whether count recall also covers higher-amount fraud | Source amount is not realized loss or loss avoided |
| Capacity ceiling | Recall obtained from the top-ranked rows when the score threshold does not restrict the selected capacity | Show whether threshold leaves capacity value unused | A retrospective bound, not a validated operating target |
| Minimum workload for recall | Smallest score-ranked holdout queue containing the target share of observed fraud | Quantify how quickly additional capture becomes expensive | Post-hoc and label-dependent; never a deployment threshold by itself |
| Bootstrap interval | 2.5th to 97.5th percentiles from deterministic resampling | Express sampling uncertainty | Does not cover deployment or distribution shift |
| Binding control | Threshold when eligible count is within capacity; capacity otherwise | Explain why queue size changes | Describes only the current holdout policy |
