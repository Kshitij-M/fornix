# Task Execution Foundation

Status: implementation in progress
Date: 2026-08-18

## Purpose

Fornix already has an append-only event store, deterministic projections, and
workspace-scoped consumer fencing. Its remaining control-plane correctness gap
is task execution: a worker can currently claim a task without a durable task
lease, and an old worker can mutate a task after its session has gone stale.
This slice makes task execution an authoritative Postgres state machine while
keeping event history append-only and replayable.

The slice deliberately does not add a broker, Redis, NATS, an LLM, or another
authority. Workers continue to use the HTTP API; Postgres remains the source of
truth for task state, lease ownership, fencing, dependencies, idempotency, and
events.

## Invariants

1. A task execution lease is unique by `(workspace_id, task_id)`.
2. A takeover increments a monotonically increasing fencing token. The old
   owner is never made current again.
3. Every mutation of a claimed task checks workspace, task, owner, exact fence,
   unreleased state, and database-clock expiry in the same transaction that
   changes the task.
4. The task row, lease release/update, session bookkeeping, idempotency record,
   and lifecycle event commit atomically.
5. Lease expiry is only permission for takeover; it is not proof that the old
   process stopped. Fencing is the stale-worker safety mechanism.
6. A task is claimable only when it is due and every direct dependency in the
   same workspace is terminal-success (`done`). Dependency checks are performed
   inside the claim transaction.
7. Attempts are bounded by `max_attempts`. Retryable failures return to
   `pending` with deterministic exponential backoff; non-retryable failures
   become `failed`; exhausted retryable failures become `deadletter`.
8. Terminal transitions (`done`, `failed`, `cancelled`, `deadletter`) release
   execution ownership and make the worker session idle when it still points at
   the task.
9. Lifecycle events preserve the full request payload and typed state deltas;
   authoritative history is never overwritten. A duplicate idempotency key
   returns the original event and applies no second effect.
10. A legacy unclaimed pending task may still be completed directly so existing
    clients and smoke tests remain compatible. Any claimed task requires an
    owner and fence.

## Schema and lifecycle

Migration 007 adds workspace identity to sessions and tasks, retry metadata,
dependency edges, and `task_execution_leases`. Migration 008 makes session
identity composite `(workspace_id, id)` and adds the matching task foreign key,
so workspace isolation is enforced by Postgres as well as handlers. Lifecycle
events are:

`task.created`, `task.claimed`, `task.lease_renewed`, `task.progressed`,
`task.retry_scheduled`, `task.completed`, `task.failed`,
`task.deadlettered`, and `task.cancelled`.

The existing task projection recognizes these events but remains derived state;
rebuild-from-zero continues to use the event log as authority.

## Reference reuse and licensing

The design reuses patterns, not source code. Orloj supplied the useful
transactional claim, worker-fence, retry, dead-letter, and observability
invariants. agentmemory supplied action/dependency lifecycle and checkpoint
diagnostic patterns. Fornix's existing event store and consumer lease code are
the implementation seams. No Kronaxis source or repository identity is copied;
its BSL 1.1 license is therefore not introduced. Fornix remains independently
implemented under its existing MIT license. Apache-2.0 references are treated as
reference material and no source notice is required by this implementation.

## Determinism and cost budget

The scheduler orders by `(created_at, id)`, evaluates dependencies in SQL, and
uses PostgreSQL `clock_timestamp()` for lease and retry decisions. Backoff is a
bounded deterministic function of the attempt number. Each lifecycle transition
uses one bounded transaction and at most one append-only event. The local
development target is p95 under 15 ms for claim/renew/terminal mutation on a
warm Postgres connection, with no polling loop added to the server. The main
storage cost is one lease row and one event per transition plus one dependency
row per edge; raw event payloads remain available for evidence and replay.

## Acceptance tests

- Fresh and existing databases apply migrations 007 and 008 cleanly.
- Concurrent claimers produce one owner and one current fence.
- Dependency-blocked tasks become claimable only after successful completion.
- Renewal accepts the exact owner/fence and rejects stale, expired, or released
  leases.
- An expired lease can be taken over with a higher fence; the old worker cannot
  complete, fail, cancel, or renew afterward.
- Duplicate completion, failure, and cancellation have one authoritative effect
  and one lifecycle event.
- Retry classification, bounded backoff, cancellation, and dead-letter
  transitions are deterministic.
- A failed transaction leaves task, lease, event, and session state unchanged;
  a committed transition is safely replayable.
- Workspace A cannot claim, mutate, or observe workspace B's tasks.
- Projection rebuild and incremental processing produce the same state hash.
- Existing unit, integration, HTTP smoke, race, vet, build, and Python checks
  remain green.
- Integration tests report mutation latency, transaction/query shape, and
  relation storage impact; limitations are documented after measurement.
