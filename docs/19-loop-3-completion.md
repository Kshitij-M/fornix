# Loop 3 completion: deterministic checkpointed projection runtime

Status: complete
Date: 2026-08-17

## Delivered

- Added `internal/projection` with typed `Projection`/`Subscriber` contracts,
  bounded pull batches, durable consumer identity, deterministic results, and
  rebuild-from-zero support.
- Added `TaskProjection`, a derived workspace-scoped view for
  `task.completed`, `task.failed`, and `task.progressed`. It consumes typed
  state-delta paths and never changes authoritative event history.
- Added migration `004_task_projection.sql` for the rebuildable
  `fornix.task_state_projections` table and lookup indexes.
- Extended `EventStore` with transaction-scoped reads, checkpoint row locking,
  reset, and an expected-current checkpoint advance that prevents stale
  writers from moving a cursor backwards.
- Projection updates and checkpoint advancement share one Postgres transaction.
  A pre-commit failure rolls both back; a committed batch can be safely
  replayed by the same or another consumer.
- Added Postgres tests for rebuild/hash equivalence, duplicate delivery,
  malformed events, rollback/crash boundaries, concurrent consumers, stale
  checkpoints, workspace isolation, migration behavior, latency, and replay
  throughput.
- Added the v0.12 Docker-backed smoke, Makefile command, CI integration step,
  development instructions, reference-reuse entry, and this qualification
  report.

## Qualification

The following checks passed against the local Docker/Postgres stack:

- Fresh database: migrations `001_initial` through `005_fornix_schema`
  applied cleanly and created all required tables.
- Existing database: migrations 004 and 005 applied without changing the
  recorded checksums for migrations 001–003.
- `make test` and the focused Postgres event/projection suites passed.
- v0.12 projection smoke passed all runner tests.
- The existing event-store tests and prior HTTP smokes remained green after the
  non-regression checkpoint contract was updated.

## Measured cost and performance

The final v0.12 smoke qualification reported:

- 100 small task events in 10 batches: p50 5.65 ms, p95 8.36 ms, max 8.36 ms
  per `RunBatch` on local Postgres.
- Rebuild of 100 events: 53.7 ms, approximately 1,863 events/s.
- No LLM, embedding, reranker, broker, Redis, NATS, or additional service.
- A batch performs one checkpoint insert-if-needed plus row lock, one ordered
  event read of `batch_size + 1`, one projection read/write per supported
  event, and one checkpoint validation/update. The task row is not touched.
- A populated task projection row measured approximately 200 logical bytes in
  Postgres before index/table overhead. The empty projection table and indexes
  occupied approximately 136 kB in the local database; actual storage grows
  with task count and indexed workspace/status values.
- Raw event payloads remain the larger cost-bearing record and are retained for
  evidence and replay. Projection storage is a deliberate compact read
  optimization, not a replacement for history.

## Remaining limitations

- The runtime is an internal pull API. It does not start a background worker,
  expose a public event stream, or provide broker-based delivery.
- Rebuild should be coordinated as an exclusive operation for a consumer; a
  future lease/fence or subscription ownership record should close the small
  reset-to-replay handoff window.
- Only task lifecycle events currently produce this derived view. Task create,
  claim, coordination messages, memo writes, federation, and watcher events
  still need typed event integration.
- Projection duplicate protection is sequence-based and scoped to the
  projection row. A broader event-to-effect audit ledger is deferred until a
  second projection or side-effecting subscriber justifies it.
- Workspace scope is enforced in the event and projection queries, but the
  service still has one bearer key and no tenant-aware identity/RBAC model.
- There is no artifact blob store, public replay API, metrics exporter, or
  operational backpressure policy yet.
