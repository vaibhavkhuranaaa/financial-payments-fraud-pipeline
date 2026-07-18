"""Fraud-ops dashboard (Plotly Dash) — ticket 08.

Read-only against the pipeline/scoring internals: everything here consumes
`src.bank.db.get_engine()` (SQL), `API_METRICS_URL` (Prometheus text), and
`models/metrics.json` (model card). It never imports `src.pipeline` or
`src.app` directly.
"""
