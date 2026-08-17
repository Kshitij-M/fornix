# Loop 4 feature note: workspace-scoped consumer leases and fencing

Status: implemented
Date: 2026-08-18

## Problem

Loop 3 makes projection application and checkpoint advancement atomic, but two
instances with the same `(workspace_id, consumer_id)` can still both enter the
runtime at different times. A rebuild can therefore race with incremental
delivery, and a worker that continued after its lease expired has no durable
token proving that it is stale.

This slice adds a Postgres-owned consumer lease and fencing protocol. It is a
coordination guard around the existing event/checkpoint authority; it does not
add a broker, background worker, or task dependency scheduler.

## Scope and API

Migration `006_consumer_leases.sql` adds one current lease row per
`(workspace_id, consumer_id)`. `internal/contracts.ConsumerLease` is the
typed boundary. `internal/store.EventStore` exposes transactional acquire,
renew, release, read, and validate operations. `internal/projection.Runner`
acquires a lease for its stable process owner before `RunBatch` or `Rebuild`;
callers may explicitly acquire, renew, release, and pass a lease to
`RunBatchWithLease`.

The compatibility `RunBatch` API remains available, but it is no longer an
unfenced projection entry point: it acquires the runner's durable lease and
uses its returned fence for the whole transaction.

## Invariants

1. A workspace and consumer identify exactly one current ownership row.
2. An active row has a non-empty owner, a positive fence, no release time, and
   a lease expiry in the future according to the Postgres clock.
3. The first acquisition uses fence `1`; every expired or released takeover
   increments the fence exactly once. Fences never decrease or reset.
4. Re-acquisition by the same active owner is idempotent and does not change
   the fence or expiry. A different active owner receives a typed lease-held
   error.
5. Renew and release require the exact workspace, consumer, owner, and fence.
   Expired, released, or mismatched tokens fail closed.
6. A projection batch locks and validates its lease before reading events,
   applying derived effects, or advancing the checkpoint. The lease row,
   checkpoint row, projection rows, and checkpoint movement share one
   transaction boundary.
7. A takeover cannot occur while an older batch holds the lease row lock. The
   older batch either commits while it owns the row or rolls back; the next
   owner receives a strictly greater fence.
8. Lease state is workspace-scoped. An owner for workspace A cannot validate,
   renew, release, or project in workspace B with the same token.
9. Event history remains append-only and authoritative. Lease state is a
   current coordination record; projection state and checkpoints remain
   rebuildable from event history.
10. Lease TTLs are bounded. The default is 30 seconds and callers cannot
    request more than 10 minutes in this slice, preventing accidental
    indefinite ownership and limiting stale-worker exposure.

## Crash and expiry semantics

- A process crash before a lease transaction commits leaves no acquisition,
  renewal, or release effect.
- A process crash after acquisition leaves a lease that expires naturally;
  takeover increments the fence.
- A process crash after projection/checkpoint commit leaves a committed batch;
  replay observes the advanced checkpoint and produces no second effect.
- A process crash after projection application but before commit rolls back
  both projection and checkpoint. The lease remains the previously committed
  lease state, so the owner can retry while it is valid.
- A lease expiring during a transaction does not permit an interleaving
  takeover because the lease row is locked. A new owner waits for commit or
  rollback, then receives the next fence.

## Schema changes

`fornix.consumer_leases` stores:

- workspace and consumer primary key;
- current owner ID;
- positive `BIGINT` fence;
- lease, acquisition, and renewal timestamps;
- release timestamp for the current row state;
- workspace/consumer/fence integrity checks and expiry lookup index.

The table intentionally stores current lease state rather than duplicating the
append-only event log. Lease transitions are operational coordination metadata;
the event store remains the source for business history and replay.

## Reference reuse and licensing

- Orloj session/task stores: reuse the transaction shape, row locking,
  explicit owner/fence validation, expired-lease takeover, and stale-writer
  rejection as design patterns. No source is copied; Fornix uses typed
  contracts and pgx SQL instead of Orloj’s resource JSON tables.
- Orloj session resources and event bus: reuse the vocabulary of explicit
  claimed-by, lease-until, fence, ordered sequence, and since-cursor delivery.
  The bounded in-memory bus is not used as authority.
- agentmemory leases/frontier/replay: reuse bounded TTLs, explicit acquire /
  renew / release / expiry operations, and idempotent replay principles.
  Its local KV/lock implementation is not imported.
- The reference repositories are studied implementations, not vendored code.
  Orloj and agentmemory are Apache-2.0 references. Fornix remains MIT under
  the repository’s confirmed relicensing permission; no third-party source or
  notice is copied into this slice.

## Cost and performance budget

- No LLM, embedding, reranker, broker, Redis, NATS, or additional service.
- Lease acquisition is one short transaction with an insert-if-needed and one
  locked row read/update. Renewal and release are one conditional update plus
  error classification when the token is invalid.
- Each projection batch adds one lease-row validation/lock to the existing
  checkpoint lock, ordered event read, projection writes, and checkpoint
  update. The target is p95 local overhead below 5 ms for lease validation and
  below 25 ms for a 100-event projection batch, excluding network latency.
- Storage is one compact row per active or known workspace/consumer pair plus
  one index. No raw event or artifact is duplicated.
- The bounded TTL limits the cost of stale ownership while avoiding a cleanup
  worker; expiry is evaluated transactionally by Postgres at acquisition and
  validation time.

## Acceptance tests

- Fresh and existing databases apply migration 006 without changing previous
  migration checksums.
- First acquisition returns fence 1; same-owner acquisition is idempotent;
  concurrent different-owner acquisition permits one active owner.
- Expired takeover returns fence 2 or greater, and the old owner cannot renew,
  release, apply a projection effect, or advance a checkpoint.
- Valid renewal extends expiry without changing the fence; release makes the
  token unusable; a new acquisition increments the fence.
- Workspace A and workspace B can use the same consumer ID independently.
- A rolled-back acquire, renew, release, or projection batch leaves committed
  lease state, projection state, and checkpoint state unchanged.
- Projection/checkpoint commit is protected by the validated lease; replay
  after a committed batch is deterministic and idempotent.
- Concurrent consumers preserve one-owner integrity and no projection state
  or checkpoint regression occurs.
- Existing Go, race, Python, HTTP, event, and projection smokes remain green.
- Qualification reports lease latency, batch latency, SQL/database work,
  storage impact, replay behavior, and remaining limitations.
