# CLAUDE.md — financial-payments-fraud-pipeline

## Project Context
- **Industry:** Financial
- **Role focus:** Data Engineer
- **Portfolio goal:** demonstrate a real streaming/orchestrated data pipeline — not a static notebook. This is the project that speaks directly to DE hiring managers: ingestion, windowed feature engineering, and a served scoring endpoint under latency constraints.

## Data
- **Dataset:** IBM TabFormer — large-scale (24M+ transactions) realistic synthetic credit card transaction data
- **Source:** GitHub `IBM/TabFormer`
- **Access constraints:** open, no credentialing needed. Do not commit the full raw dataset to the repo — use a `data/sample/` subset for local dev and document the download step in the README.

## Required Stack
Python, Azure Event Hubs (Kafka-compatible) for simulated streaming ingestion, Spark Structured Streaming or Flink for windowed feature engineering, XGBoost (or a transformer-based tabular model per TabFormer's own approach) for scoring, Flask scoring microservice, Docker, Azure Container Apps for deployment.

## Standard Repo Structure
```
src/
├── app.py                  # Flask scoring endpoint
├── pipeline/
│   ├── ingestion.py          # Event Hubs consumer / stream simulator
│   ├── features.py           # windowed feature engineering
│   └── train.py               # model training job
notebooks/                   # EDA on TabFormer sample only
data/sample/                 # small TabFormer subset for local dev
tests/
docker/
infra/                        # Azure Event Hubs + Container App configs
.github/workflows/ci.yml
```

## Subagent Ownership
1. **Architect subagent** — confirm structure above, break down ingestion/features/train/API as separate tasks. Run first, commit skeleton.
2. **Pipeline subagent** — owns `src/pipeline/` (ingestion, feature engineering, training job)
3. **API subagent** — owns `src/app.py`, wires trained model into a `/score` endpoint
4. **Infra subagent** — owns `docker/` and `infra/` (Event Hubs setup, Container App deployment, CI)
5. **Docs/test subagent** — owns `tests/`, keeps README in sync, must document latency benchmarks in Key Results

## Hard Constraints
- Do not commit the full 24M-row dataset — sample only, document the full download in README
- No real payment data or PII — TabFormer is synthetic, keep it that way
- Latency must be measured and reported (this is a DE project — throughput/latency numbers are the differentiator, not just model accuracy)

## Definition of Done (v1)
- [ ] Simulated streaming ingestion working (Event Hubs producer/consumer)
- [ ] Windowed feature engineering pipeline
- [ ] Trained fraud model with reported precision/recall at a chosen threshold
- [ ] Flask `/score` endpoint with measured latency
- [ ] Dockerized, runs via `docker compose up`
- [ ] Deployed to Azure Container Apps
- [ ] README complete, tagged `v1.0`
