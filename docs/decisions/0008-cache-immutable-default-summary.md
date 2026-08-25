# Decision 0008: Cache the immutable default summary

## Decision

Compute the default policy summary once when a validated artifact run loads. Return that cached value for summary requests without policy parameters. Keep requested threshold and capacity scenarios dynamic.

## Why

The default result is immutable for one artifact run. Recomputing the full scored policy for every identical request wastes CPU and adds burst latency without changing the answer.

## Alternatives rejected

- Add Redis or another distributed cache.
- Keep per-request computation and buy permanent warm capacity.
- Raise the permanent instance ceiling instead of removing repeated work.
- Cache parameterized scenarios without a measured need.

## Not done

No dynamic policy result is cached. No always-on instance, external cache, public performance claim, or production throughput claim was added.

## Changed

Artifact loading now stores the default summary. Default summary and strategy responses reuse it. A test fails if the default summary path recalculates the policy.
