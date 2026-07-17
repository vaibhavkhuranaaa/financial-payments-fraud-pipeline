# Lineage

End-to-end flow from raw TabFormer CSV to a served fraud score, plus the parallel analytical (dbt) path fed by the same contract.

```mermaid
flowchart LR
    subgraph Source
        A[TabFormer CSV\ndata/sample or data/raw]
    end

    subgraph Ingestion
        B["ingestion.py::to_event()\ncontract validation + PAN tokenization"]
    end

    subgraph Broker
        C[["transactions" topic\nRedpanda local / Event Hubs cloud]]
        C_DLQ[["transactions.dlq"]]
    end

    subgraph Stream["Spark Structured Streaming (features.py::run_stream)"]
        D[Re-validate\n+ enrich()]
        D_Q[(Delta _quarantine)]
        E[Windowed aggregates\nCARD_WINDOWS]
    end

    subgraph Online
        F[(Redis\nfeatures:{card_token})]
    end

    subgraph Offline
        G[(Delta events)]
        H[(Delta card_features)]
    end

    subgraph Train["Offline training (train.py)"]
        I[XGBoost model\nmodels/model.json + threshold.json]
    end

    subgraph Serve["Flask API (app.py)"]
        J["/score\nenrich() + Redis join + predict"]
    end

    subgraph Analytics["dbt (DuckDB)"]
        K[stg_transactions\nanalytical mirror of to_event]
        L[fct_daily_fraud_rate]
        M[fct_merchant_risk]
        N[fct_channel_mix]
    end

    A --> B --> C
    B -. invalid rows .-> C_DLQ
    C --> D
    D -. invalid .-> D_Q
    D --> E
    E --> F
    D --> G
    E --> H
    G --> I
    H --> I
    I --> J
    F --> J

    A -.->|same source CSV,\nsame mapping rules,\nhand-kept in sync| K
    K --> L
    K --> M
    K --> N
```

## Key lineage facts

- **Single source of truth for feature *definitions*:** `src/pipeline/features.py` (`enrich()`, `CARD_WINDOWS`, `FEATURE_COLUMNS`) is imported by both the streaming job and the offline training builder — this is the train/serve-skew prevention mechanism (ADR 0001, decision 4).
- **Single source of truth for event *mapping*:** `src/pipeline/ingestion.py::to_event()` is the only place raw TabFormer rows become contract-v1 events for the live streaming path. The dbt staging model (`stg_transactions.sql`) re-implements the same rules in SQL so `dbt build` can run standalone in CI against the committed sample CSV, without a live Kafka/Spark/Delta stack. **This is a hand-maintained duplication, not a shared library** — SQL can't import the Python module. If `to_event()`'s mapping rules change, `stg_transactions.sql` (and the `country_name_to_iso2`/`mcc_group` dbt macros) must be updated to match. See "What I'd Improve Next" in the README.
- **Two independent validation points:** the producer validates against the JSON-Schema contract before publish; the stream job re-validates on read (defense in depth — it must not trust the wire). Both route invalid records to a dead-letter path (Kafka DLQ topic for the producer, a Delta `_quarantine` table for the stream — see the module docstring in `features.py` for why the stream uses Delta instead of a second Kafka producer).
- **Two consumers of the online/offline feature split:** Redis (`features:{card_token}`) serves the live `/score` path with a hard latency budget; Delta (`{DELTA_ROOT}/events`, `{DELTA_ROOT}/card_features`) is the offline/replayable copy used by training and (if pointed at Delta parquet exports instead of the sample CSV) could back the dbt marts too.
- **dbt is a downstream, parallel consumer of the same contract**, not a step in the online path — it never touches Redis, the model, or the API. It exists purely for fraud-ops analytics (daily fraud-rate trend, merchant/mcc risk, channel mix) over data governed by the same contract as the live pipeline.
