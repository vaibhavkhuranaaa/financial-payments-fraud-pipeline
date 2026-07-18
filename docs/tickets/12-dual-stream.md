# Ticket 12 — v1.3 candidate: second event stream (auths vs settlements)

**Status: BACKLOG STUB — needs a design pass + user approval before becoming a real ticket. Do not start before v1.2.**

## Idea
Split the single transaction stream into two correlated streams, the way card networks actually work: an **authorization** event at decision time and a **settlement/clearing** event seconds-to-days later (amount can differ — tips, currency, partial capture). Generated synthetically from the TabFormer rows (auth = existing event; settlement = derived with a seeded lag/amount-delta distribution).

## What it would demonstrate
- Stream–stream joins in Spark Structured Streaming with watermarks + late-arrival handling (the hard part of streaming interviews).
- New features: auth-without-settlement rate per card (a real fraud signal), auth/settle amount mismatch.
- Reconciliation mart in dbt (unsettled auths aging report) — a genuinely bank-flavored analytics product.

## Known concerns (why this is a stub, not a spec)
- Synthetic-on-synthetic: the settlement generator's parameters ARE the signal the model would learn — fine for engineering demonstration, must be labeled honestly in the README (engineering realism, not modeling realism).
- Scope: touches producer, streaming job, feature spec, model retrain, dashboard. Roughly a v1.1-sized effort. Decide deliberately.

## Explicitly rejected for this project
Graph/fraud-ring features — modeling-project territory, TabFormer lacks ring structure, and it dilutes the DE narrative.
