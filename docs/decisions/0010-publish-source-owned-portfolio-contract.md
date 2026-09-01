# Decision 0010: Publish a source-owned portfolio contract

## Decision

Keep the portfolio manifest and release contract in this repository. Expose one read-only endpoint that reports the deployed source revision. Let the portfolio registry admit the project only when that revision equals public main.

## Why

Project facts should remain owned by the project that produced them. Matching the live revision to the repository prevents a portfolio page from presenting code or evidence that is newer than the deployed product.

## Alternatives rejected

- Hand-copy project facts into the portfolio site.
- Add a project-specific page component to the portfolio application.
- Publish before the live revision matches public main.
- Expose private delivery records or raw artifacts as portfolio evidence.

## Not done

No new service, database, dependency, screenshot, or portfolio-specific dashboard code was added. The endpoint does not expose build logs, environment details, credentials, or artifact metadata.

## Changed

The repository now carries a versioned portfolio manifest and enabled release contract. The dashboard exposes a fail-closed source-revision endpoint, with a test for valid and missing configuration.
