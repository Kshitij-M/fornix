# Loop 5 completion: durable task execution and recovery

Status: complete
Date: 2026-08-18

## Delivered

- Added typed task status, failure-class, retry-budget, and
  `TaskExecutionLease` contracts.
- Added migration `007_task_execution.sql` with workspace columns, attempt and
  retry metadata, dependency edges, task execution leases, composite workspace
  integrity, and expiry/dispatch indexes.
- Added migration `008_workspace_session_key.sql` so session identity is
  composite `(workspace_id, id)` and task assignment uses a matching
  workspace-scoped foreign key.
- Added `internal/store.TaskStore` as the transactional task mutation boundary.
  Create, claim, renew, complete/progress, fail, cancel, takeover, retry, and
  dead-letter transitions update authoritative rows, session bookkeeping,
  leases, idempotency records, and typed lifecycle events atomically.
- Added deterministic dependency-aware claim ordering by `(created_at, id)`;
  direct dependencies must be successful before a task is claimable.
- Added bounded retry classification and deterministic exponential backoff,
  explicit cancellation, and dead-letter transitions when the attempt budget is
  exhausted.
- Added HTTP renew, fail, and cancel routes. Claim responses expose the current
  fence; claimed-task mutation requires the session owner and exact fence.
  Existing unclaimed completion behavior remains compatible with the event
  smoke; the legacy v0.6 smoke now sends the returned fence explicitly.
- Extended the derived task projection to understand the complete lifecycle,
  including no-op lease renewal events and explicit assignment clearing.
- Added Postgres tests for concurrent claims, duplicate delivery, stale-worker
  fencing, expiry/takeover, dependencies, retry/dead-letter, cancellation,
  rollback/crash boundaries, projection rebuild, workspace isolation, and
  measured latency/storage.
- Added the v0.14 task smoke, Makefile command, CI step, development commands,
  and qualification documentation.

## Qualification

- Existing database: migrations `001_initial` through `008_workspace_session_key`
  applied successfully; the pre-existing migration checksums remained valid.
- Fresh database: all eight migrations applied successfully from an empty
  database, including the composite task/dependency foreign keys and task lease
  table.
- Full Go tests passed with Postgres. Task integration coverage passed for
  concurrent claims, stale fencing, takeover, dependency gating, duplicate
  completion, retry/dead-letter, cancellation, and rollback.
- Projection lifecycle rebuild produced the same snapshot hash as incremental
  processing, including created, claimed, renewed, progressed, retried,
  completed, failed, cancelled, and dead-lettered events.
- Task claim plus terminal completion, 20 warm samples: p50 `5.128 ms`, p95
  `5.831 ms`, max `8.722 ms` on the local Docker Postgres instance.
- A workspace with 20 completed tasks occupied approximately `3,520 B` of
  logical task-row data and `40,640 B` of logical event-row data; there were no
  dependency rows in that benchmark. Actual relation size includes indexes,
  page headers, and other workspaces.
- The task smoke and existing v0.6/v0.11 event smokes passed against the
  restarted service.

## Database work and cost

The normal claim transaction locks one session, selects one bounded eligible
task with `SKIP LOCKED`, checks direct dependencies, creates or takes over one
lease row, updates the task and session, appends one event, and commits. Renew
uses one locked lease validation/update and one audit event. Completion,
failure, cancellation, and dead-letter transitions use one task/lease/session
transaction and at most one lifecycle event. No broker, cache, model call,
embedding, or new service was introduced.

Raw event payloads and typed evidence/provenance fields remain append-only;
task rows are current operational state and can be reconstructed or audited
from lifecycle events. The primary storage trade-off is one event per task
transition, which buys deterministic replay and duplicate-delivery safety.

## Remaining limitations

- The dependency implementation is intentionally direct-edge and create-time;
  a future task-graph API needs explicit edge mutation, cycle diagnostics, and
  graph-level scheduling metrics.
- Task leases are current coordination state, not an append-only lease-history
  stream. Lease transition telemetry and retention policy remain deferred.
- The server still has one shared bearer key and no tenant-aware identity or
  authorization model; workspace filtering is an explicit boundary, not a
  complete security system.
- Claim scheduling is deterministic FIFO among eligible rows, not weighted or
  priority-aware. Backoff is bounded and deterministic but not jittered.
- Projection consumption remains an explicit pull/rebuild API; no background
  task or projection worker was added by design.
