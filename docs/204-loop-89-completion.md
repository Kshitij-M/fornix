# Loop 89 completion — readiness snapshots and operator incident evidence

Status: implemented as a repository-owned advisory observation slice.

## Delivered

- Added typed readiness snapshot and incident annotation contracts with bounded
  identifiers, hashes, lists, timestamps, actor metadata, idempotency, and
  stable-hash rules.
- Added migration `073` for append-only workspace-scoped snapshots, capture
  events, incident annotations, bounded JSON constraints, indexes, and RLS.
- Added transactional `ReadinessStore.Capture`. It evaluates the existing
  release gate inside the same workspace/deployment/release transaction,
  records active evidence IDs and deterministic diagnostics, deduplicates by
  idempotency and canonical snapshot hash, and supports side-effect-free
  dry-run projection.
- Added transactional, idempotent `ReadinessStore.Annotate`, stable cursor
  pagination, workspace/release/snapshot checks, and append-only annotation
  events.
- Added authenticated HTTP routes under
  `/v1/qualification/readiness/snapshots` and operator CLI commands for
  capture, listing, disclosure, annotation, and annotation listing.
- Added qualification RBAC coverage. Read access uses qualification-read;
  capture and annotation use qualification-admin.
- Added contract tests plus PostgreSQL-backed tests for idempotency,
  deterministic hashes, dry-run behavior, concurrent capture deduplication,
  workspace isolation, pagination, and annotation replay.

## Verification

The focused contracts, store, server, and CLI tests pass locally. The full
repository gates include:

```text
make check
go test -race ./...
make qualification-effect-conformance qualification-external-boundary
go build ./...
git diff --check
```

PostgreSQL migration, RLS, concurrency, and crash-boundary tests execute when
`FORNIX_TEST_PG_DSN` is configured. No PostgreSQL DSN is available in this
environment, so those integration fixtures skip explicitly rather than
silently testing a substitute authority.

## Cost and storage impact

Migration `073` adds one bounded snapshot row and one capture event for each
new canonical gate hash, plus one bounded row/event for each incident
annotation. Replayed snapshots do not copy evidence or signed bundles. Gate
facts are read through existing release/evidence indexes and all list paths
are cursor-bounded. Deployment-specific latency, WAL, and index measurements
remain a production qualification responsibility.

## Remaining limitations

- Snapshots are advisory and do not independently attest deployment truth.
- Deployment-owned boundary collection, provider behavior, secret-manager
  authority, mTLS/workload identity, HA/PITR/failover, and load/soak evidence
  remain external production gates.
- Incident annotations are deliberately structured and hash-only; raw logs and
  detailed incident reports belong in an authorized external system or a
  separately governed artifact workflow.

## Next handoff

Task 90 should add explicit freshness policy configuration and a read-only
incident review comparison workflow. It must preserve the same authority
boundary, avoid changing release admission, and qualify stale-snapshot,
retention, recovery, and operator handoff behavior.

Task 90 is now implemented in [`206-loop-90-completion.md`](206-loop-90-completion.md).
