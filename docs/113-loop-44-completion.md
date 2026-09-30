# Loop 44 completion — universal operation fairness and resource coordination

Status: completed implementation slice; remaining production gates are tracked
in the universal roadmap and are not closed by this note.

## Delivered

- Added migration `045_operation_resource_leases.sql` for workspace-scoped
  current resource leases, append-only lease history, checks, indexes, and RLS.
- Added explicit `OperationResourceLease` contracts and resource lease output
  on generic operation claims.
- Added transactional acquisition, takeover, renewal, release, resource
  fencing, sorted advisory resource locks, and fail-closed missing-resource
  validation.
- Added `ClaimReadyWithOptions` with a bounded workspace-wide `MaxActive`
  quota. The existing `ClaimReady` API remains compatible with an unbounded
  quota default.
- Added `operationworker.Worker.MaxActive` integration so adapter-owned workers
  can opt into the durable quota.
- Added `internal/operationsupervisor`, an explicit-workspace, deterministic
  round-robin scheduler with bounded concurrency, cancellation, per-workspace
  outcomes, and worker-error reporting.
- Added queue, resource serialization, quota, worker heartbeat, expiry,
  takeover, duplicate/concurrency, cancellation, and supervisor unit tests.
- Expanded workspace-isolation qualification to include both resource lease
  tables.
- Preserved target-only operation behavior: resource serialization is opt-in
  through declared operation resources rather than an implicit target guess.

## Verification

The qualification run used a disposable local PostgreSQL database with a fresh
migration catalog. The following passed during this loop:

```text
go test ./internal/store ./internal/operationworker -run 'TestOperationQueue|TestWorker' -count=1 -v
go test ./internal/operationsupervisor -count=1
go test -race ./internal/operationsupervisor ./internal/operationworker -count=1
FORNIX_TEST_PG_DSN=<fresh disposable database> go test ./internal/... -count=1
```

The full internal suite passed after the fresh migration was applied. The
pre-existing local `fornix` database was not modified because it contains an
unrelated migration-035 checksum mismatch; qualification used a disposable
database instead.

## Measurements and limitations

This loop establishes bounded query and storage work, but does not claim
production latency SLOs. Resource coordination adds one current lease row per
distinct declared resource and one append-only history row per ownership
acquisition/takeover/release. Quota claims add a workspace advisory lock and an
indexed active-lease count. A production qualification must measure p50/p95/p99
latency, lock waits, WAL, pool saturation, fairness, and replay/recovery on the
target topology.

The supervisor is intentionally process-local policy. It does not replace
Postgres leases, create a provider dispatcher, add a sandbox, or solve
autoscaling. Those remain open production gates in
[`111-universal-production-roadmap-status.md`](111-universal-production-roadmap-status.md).
