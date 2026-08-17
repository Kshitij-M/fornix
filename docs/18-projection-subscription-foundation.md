# Loop 3 feature note: deterministic checkpointed projections

Status: implemented
Date: 2026-08-17

## Problem

Loop 2 added an append-only control-event history and durable consumer
checkpoints, but no runtime consumes that history. The existing task/session
tables remain the current-state compatibility surface, while a future Fornix
control plane needs rebuildable derived views and subscribers that can resume
after a process crash without double-applying effects.

## Scope

This slice adds one generic, Postgres-backed checkpointed subscriber runtime
and one derived task projection:

```text
control_events
  → bounded ordered batch after (workspace, consumer) checkpoint
  → deterministic TaskProjection.Apply for task completion events
  → projection update + checkpoint advance in one transaction
```

The projection is not authoritative and never updates `fornix.control_events`.
The first implementation does not add a broker, push delivery, a public event
API, an LLM, or projections for memo/coordination/task-creation paths that do
not yet emit typed events.

## Contracts and invariants

1. `Projection`/`Subscriber` is typed and deterministic. Applying the same
   event to the same projection state twice has one observable effect.
2. A subscriber reads strictly after its workspace-scoped checkpoint and in
   ascending global event sequence. Sequence gaps caused by other workspaces or
   rolled-back inserts are normal.
3. Projection writes and checkpoint advancement commit together or neither is
   durable. A failure before commit leaves both at their previous values.
4. Checkpoints are monotonic. A stale writer cannot move a cursor backwards;
   the checkpoint row is locked for the duration of a batch transaction.
5. Workspaces are mandatory at the runtime boundary and every event/projection
   query is workspace-filtered.
6. Event history remains append-only and is the only replay authority. A
   projection may be updated, reset, and rebuilt, but it cannot alter event
   rows or idempotency records.
7. Task projection application uses typed state-delta paths, not free-form
   payload text. Unsupported event types are acknowledged without changing the
   task view; malformed supported task events fail closed and do not advance
   the checkpoint.
8. Rebuild resets the derived task view and its consumer cursor atomically,
   then replays from sequence zero in bounded batches. A rebuild can be safely
   retried after interruption.
9. Concurrent runs for one consumer serialize on the checkpoint row. Runs with
   different consumer IDs may receive the same event, but projection sequence
   guards make that duplicate delivery idempotent.
10. Projection state hashes exclude wall-clock fields and are stable across
    incremental processing and rebuild.

## API shape

`internal/projection` will expose:

- a `Subscriber` contract with name, consumer ID, batch size, apply, and reset;
- a `Runner.RunBatch` operation returning read/applied/duplicate counts and
  checkpoint movement;
- a `Runner.Rebuild` operation that resets and catches up from sequence zero;
- a small pre-commit failure hook used only to test rollback semantics.

`internal/store.EventStore` will add transaction-scoped batch reads and
checkpoint operations so the projection package never reaches into the pool or
duplicates SQL authority.

## Schema

Migration `004_task_projection.sql` adds the derived
`fornix.task_state_projections` table keyed by `(workspace_id, task_id)`.
It stores status, result, assigned session, last applied event identity and
sequence, application count, a deterministic state hash, and update time.
Loop 2's `fornix.control_checkpoints` remains the durable cursor table; the
subscriber's consumer ID names the projection instance.

## Reference scan and reuse decisions

- Orloj `store/session_store.go`: reuse the transaction boundary, row locking,
  ordered event reads, and lease/fence-aware current-state updates as patterns;
  reimplement with Fornix's pgx store and workspace contract.
- Orloj `store/session_checkpoint_store.go`: reuse checkpoint state hashing,
  replay verification, lineage/sequence validation, and commit-together
  semantics; do not copy its session-specific resource model.
- Orloj `eventbus/memory_bus.go`: use its filter/since-ID vocabulary only as a
  conceptual delivery interface. Its bounded in-memory history is not suitable
  as durable projection authority.
- Orloj `resources/session.go` and `resources/agent.go`: reuse the explicit
  desired/current state separation, normalized resource contracts, and fence
  vocabulary conceptually; no Orloj source is copied.
- Existing task handlers: preserve the current task API and table shape;
  the projection is a derived audit/current view, not a replacement for the
  compatibility tables.
- agentmemory checkpoint/replay/import code: reuse stable content-addressed
  replay and idempotent re-import principles conceptually; its SQLite/iii
  authority and JavaScript runtime are not imported.

All implementation is an independent Go/pgx reimplementation of the studied
patterns. Orloj and agentmemory are Apache-2.0 references. Fornix is released
under MIT with copyright permission confirmed by the project owner.

## Cost and performance budget

- No model, embedding, reranker, broker, cache, or new service.
- One batch uses one Postgres transaction, one checkpoint row lock, one ordered
  event read of `batch_size + 1`, one projection write per supported event, and
  one checkpoint update. The extra row detects `has_more` without another
  round-trip.
- Default batch size is bounded at 100 events for predictable lock duration;
  callers cannot request more than 1,000.
- Target: local Postgres `RunBatch` p95 below 20 ms for 100 task events and
  rebuild throughput above 1,000 events/s for small task payloads. These are
  qualification targets, not guarantees for remote or contended databases.
- Projection storage intentionally duplicates compact task status/result data
  so reads avoid replaying history; raw events remain the durable cost-bearing
  record.

## Acceptance tests

- Fresh and existing databases apply migration 004 cleanly.
- Incremental processing and rebuild-from-zero produce the same task projection
  rows and deterministic state hashes.
- Duplicate delivery and two consumer instances cannot double-count a task
  event or corrupt the projection.
- A simulated failure before commit leaves both projection and checkpoint
  unchanged.
- Re-running after a committed batch is a no-op; replay is safe.
- Stale checkpoint advancement fails or remains at the higher sequence.
- Concurrent consumers preserve workspace isolation and projection integrity.
- Existing Go tests, race checks, CI-equivalent checks, and all v0.5–v0.11
  smokes remain green.
- Report batch latency, replay throughput, SQL/database work, storage impact,
  and limitations.
