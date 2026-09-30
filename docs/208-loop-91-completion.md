# Loop 91 completion — qualification retention and recovery evidence

Status: implemented as an advisory, repository-owned qualification slice.

## Delivered

- Added `QualificationRetentionPolicy`, retention metadata, bounded sync,
  deterministic retention-plan, and read-only recovery-report contracts under
  `internal/contracts`.
- Added migration 075 with immutable workspace/deployment policy revisions,
  append-only hash-only metadata/events, bounds, indexes, mutation triggers,
  and PostgreSQL RLS.
- Registered retention metadata transactionally with new readiness snapshots,
  incident annotations, and freshness-policy revisions.
- Added idempotent, bounded, cursor-based metadata synchronization for rows
  created before migration 075.
- Added read-only archival planning with latest-record and incident protection;
  no authoritative qualification row is deleted or rewritten.
- Added recovery checks for missing/dangling metadata, source-hash mismatch,
  dangling annotation snapshots, and broken snapshot event references.
- Added authenticated HTTP routes and CLI commands:
  `retention-policy-set`, `retention-policy-get`, `retention-policy-list`,
  `retention-sync`, `retention-plan`, and `retention-recovery`.
- Added contract, cursor, fail-closed store, authorization, and existing
  qualification integration tests.

## Invariants preserved

Readiness, incident, and freshness-policy history remains append-only and is
still the source of truth for qualification observations. Retention metadata
is an operational overlay keyed by source kind, source ID, and source hash.
Plan and recovery output is hash/count/identifier-only and cannot alter release
admission. Workspace predicates and RLS remain mandatory on every new table and
query. External archival, backup, and deletion remain deployment-owned.

## Verification

The affected packages were tested with:

```text
go test ./internal/contracts ./internal/store ./internal/server ./cmd/fornix
```

The full repository gates, race suite, offline qualification conformance,
build, documentation checks, shell checks, and `git diff --check` were also
run for this loop. PostgreSQL integration tests are explicitly skipped when
`FORNIX_TEST_PG_DSN` is unset; this environment did not provide a database DSN.

## Cost and remaining limitations

The overlay is hash-only and bounded. Sync/planning pages cap at 1,000 rows;
plan disclosure caps candidates at 256. Queries use workspace/deployment
indexes and do not copy raw payloads or call providers/tools. No external
archival worker, partition maintenance, backup/PITR, HA/failover, live
provider, secret-manager, mTLS, DNS, or workload-identity behavior is proven
by this loop. Those remain deployment-owned qualifications.
