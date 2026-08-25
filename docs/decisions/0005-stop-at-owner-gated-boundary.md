# Decision 0005: Stop at the owner-gated boundary

## Decision

Complete local build, evaluation, evidence, browser verification, graph synchronization, and offline delivery checks while keeping lifecycle `building`.

## Why

Cloud spend, source upload, deployment, Git push, publication, merge, history rewrite, and portfolio mutation change external state and require separate explicit owner approvals. Local completion does not imply those permissions.

## Alternatives rejected

- Treat the instruction to complete the project as permission to deploy or publish.
- Rewrite nonconforming history without approval.
- Claim a permanent demo or scaled evidence that does not exist.

## Not done

P7 through P10 external actions, public demo URL, scale run, force push, merge, and portfolio publication.

## Changed

Recorded the external boundary, kept private data and evidence outside Git, and limited claims to versioned aggregate evaluation.
