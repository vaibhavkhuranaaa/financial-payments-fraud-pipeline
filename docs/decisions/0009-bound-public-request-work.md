# Decision 0009: Bound public request work

## Decision

Keep the public demonstration at one scale-to-zero Cloud Run instance. Apply one process-wide token bucket with a sustained two-request-per-second ceiling and a 40-request burst allowance.

## Why

The public workbench needs enough burst capacity to load its page assets while preventing unbounded sustained request work. A hard one-instance ceiling makes the in-process limiter global for the active service.

## Alternatives rejected

- Allow unrestricted public requests.
- Permit multiple permanent instances.
- Add a paid gateway or distributed rate-limit store for a portfolio demonstration.
- Keep the service private and make the published product inaccessible.

## Not done

This limiter is not a production abuse-control system. It does not provide identity-aware quotas, distributed coordination, or an availability objective. Its bucket resets when the scale-to-zero container restarts.

## Changed

The runtime reads its request rate and burst from environment variables. Excess requests return HTTP 429 with a retry hint. Local development remains unlimited unless the setting is enabled.
