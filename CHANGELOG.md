# Changelog

All notable changes to Fornix are recorded here. The project is in an alpha
single-node phase, so entries describe verified repository behavior rather
than promising a stable compatibility contract.

## Unreleased

- Documentation is being consolidated into a public documentation map,
  contributor/security guidance, a human-readable HTTP API reference, and a
  consistent documentation contract for architecture and code changes.
- Repository quality gates include repository-local pre-commit and pre-push
  hooks, reusable Make targets, bounded CI jobs, and cancellation of superseded
  runs.

## 2026-08-21

### Repository quality

- Fixed the GitHub Actions container-to-Postgres address used by the task
  execution smoke on hosted runners.
- Verified Go tests, race tests, vet, formatting, Python checks, vulnerability
  scanning, and the full Postgres/HTTP smoke matrix.
- Added the durable repository ingestion and indexing slice, including
  deterministic mounted-root discovery, resumable checkpoints, chunk/symbol
  lineage, supersession/removal history, and offline embedding gates.

### Security and licensing

- Kept the repository under the MIT License.
- Kept Kronaxis-derived source out of the repository because its BSL 1.1 terms
  are not compatible with direct source reuse here.
- Preserved workspace-scoped authentication, redaction, idempotency, fencing,
  and append-only authority boundaries documented in the architecture notes.

Older architecture and loop-completion notes remain available under
[`docs/`](docs/). They are historical records as well as implementation
evidence; the current cross-cutting qualification summary is
[`docs/14-production-readiness-qualification.md`](docs/14-production-readiness-qualification.md).
