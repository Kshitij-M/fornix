# Durable agent-run scheduler foundation

Status: implementation note for Loop 11
Date: 2026-08-20

## Purpose

This slice adds a Postgres-backed pull scheduler for the bounded agent loop.
The scheduler owns only wake-up and worker ownership. The `agent_runs` row,
the append-only control events, and the model/tool ledgers remain the
authoritative execution history. A worker may run a bounded loop phase only
while its workspace/run lease and fencing token are valid.

The design is an independent Go implementation of three useful reference
patterns: Orloj's `FOR UPDATE SKIP LOCKED` turn claim and lease-fenced
completion, DeepSeek Harness's durable session wake/resume and serialized
persistence barriers, and agentmemory's explicit lease expiry, cleanup, and
checkpoint gates. No reference source is copied. Kronaxis source is not used
because its repository is BSL 1.1; Fornix remains MIT.

## Queue and lease invariants

1. A run is eligible only when it is workspace-scoped, non-terminal, not
   cancelled, and its durable wake time is due. `pending` runs are immediately
   eligible; `awaiting_retry` runs are eligible at `next_retry_at`; an
   `awaiting_approval` run is eligible only after its durable approval is
   `approved`. An `awaiting_external` run is not guessed at by a poller: the
   external completion mutation is its durable wake signal and is itself
   idempotent. This prevents a scheduler from executing work without the
   required human or external fact.
2. Queue selection is deterministic within a workspace: higher
   `scheduler_priority` first, then due time, creation time, and run ID. The
   oldest due run wins within a priority class. This is bounded oldest-first
   fairness; a worker cannot starve a lower-priority run indefinitely if the
   higher-priority stream is finite. An all-workspace poll applies the same
   priority/due ordering globally and uses workspace ID only as a final stable
   tie-breaker, so one workspace cannot win merely because its name sorts
   first.
3. `(workspace_id, run_id)` has at most one active worker lease. A lease row is
   never deleted. Acquisition creates fence 1; takeover, including takeover
   after expiry or release, increments the fence monotonically. An owner may
   reuse an active lease without changing its fence.
4. The database clock decides expiry. Worker-provided timestamps are
   informational only. Heartbeat, release, and checkpoint commit validate the
   owner, fence, non-release, and non-expiry predicates while locking the
   lease row.
5. A scheduler-owned checkpoint and lease renewal share one transaction. The
   run row is compared by `state_version`; the lease row is locked and checked
   before the event append and checkpoint update. A stale owner therefore
   cannot append a transition, move a checkpoint, or extend a lease.
6. A crash before commit leaves both the run checkpoint and lease renewal
   unchanged. A crash after commit leaves a durable state transition; a
   duplicate worker either observes the new state/version or reuses a durable
   model/tool ledger effect. External model and process execution remain
   explicitly at-least-once.
7. Cancellation is a terminal Postgres transition. Queue selection excludes
   cancelled runs, and a worker that returns from an in-flight external call
   loses the state-version or lease check before it can commit subsequent
   work.

## Schema changes

Migration 017 adds scheduling metadata to `fornix.agent_runs`:

- `scheduler_priority` for deterministic priority ordering;
- `next_scheduled_at` for durable wake-up scheduling;
- `schedule_attempts` and `last_worker_id` for operational diagnostics.

It adds `fornix.agent_run_worker_leases`, keyed by workspace and run, with
owner, monotonic fence, lease/heartbeat/release timestamps, and indexes for
expiry and ownership inspection. Existing migration files are immutable and
their checksums are not changed.

Existing runs receive a due `next_scheduled_at` value. The scheduler derives
retry eligibility from the existing `next_retry_at` checkpoint field and
never overwrites authoritative event history.

## Recovery and resumption semantics

`ClaimNext` performs candidate selection, expired-lease takeover, and queue
diagnostic update in one transaction. `Heartbeat` renews only the exact owner
and fence. `Release` makes the current token unusable but retains its row so
the next claim receives a higher fence.

The worker calls the existing bounded orchestrator with the lease in context.
Every loop admission and every checkpoint commit validates that lease. It
automatically resumes due retries and approved tool approvals. External waits
resume through `CompleteExternal`, which already transitions the run exactly
once; polling an unresolved external wait would be unsafe and is deliberately
not treated as runnable work. A future external signal can call the same
idempotent completion boundary without adding a broker.

Backoff is deterministic and bounded: the existing loop delay is capped at
10 seconds for phase retries, while the scheduler stores the resulting due
time and never performs an unbounded sleep. The worker is a pull/reconcile
primitive; deployment may call `RunOnce` from a process loop or a supervisor.

## Reuse and licensing

- Reuse Orloj's SQL claim shape (`FOR UPDATE SKIP LOCKED`), lease-fenced
  mutation ordering, and recovery of expired work.
- Reuse DeepSeek Harness's durable wake/resume separation and persistence
  barrier idea: wake-up is not authority; the committed Postgres checkpoint is.
- Reuse agentmemory's explicit active/expired/released lease lifecycle and
  checkpoint-gated recovery discipline.
- Reuse Fornix's existing task/consumer lease contracts, state-version CAS,
  typed event envelopes, and model/tool idempotency ledgers.
- No source code is copied from any reference repository. No new dependency,
  broker, cache, or database is introduced. The implementation is covered by
  Fornix's MIT license.

## Cost and database budget

The normal `RunOnce` path uses one short claim transaction, one or more
existing loop checkpoint transactions, and one release transaction. A
heartbeat is one indexed update and should run at most once per lease renewal
interval, not per model token. Claim selection uses the workspace/state/due
index and `SKIP LOCKED`; lease takeover updates one primary-key row. The
expected scheduler overhead is sub-millisecond application work plus three to
five Postgres round trips on the local development path, excluding model/tool
latency. The queue metadata adds a few scalar bytes per run; one lease row adds
roughly a few hundred bytes before indexes. Measurements are recorded in the
Loop 11 completion note and are observations, not capacity claims.

## Acceptance tests

- Fresh and existing databases apply migration 017 without changing previous
  checksums.
- Deterministic priority/due/created/ID ordering is preserved under concurrent
  `SKIP LOCKED` claims.
- Two workers cannot own one workspace/run simultaneously; workspaces remain
  isolated.
- Renewal, release, expiry, takeover, and monotonic fence behavior are
  transactional and stale tokens fail closed.
- A scheduler-owned checkpoint renews the lease atomically; a crash before
  commit changes neither, while a crash after commit is replayable.
- A stale worker cannot advance or finalize a run after takeover.
- Due retry and approved approval runs resume; unresolved external waits do
  not execute speculative work; external completion is duplicate-safe.
- Cancellation removes future queue eligibility and blocks later worker
  transitions.
- Duplicate delivery creates one durable run transition/effect.
- Existing unit, Postgres integration, race, vet, build, CI, and smoke checks
  remain green. Latency, SQL work, storage, recovery, and limitations are
  reported.

## Known limitations

This slice provides a durable pull/reconcile worker but not a distributed
notification service, process supervisor, autoscaler, global multi-workspace
fairness scheduler, or HA failover plan. Polling cadence is deployment policy.
External model/tool execution remains at-least-once, and cancellation cannot
interrupt an already-issued remote request unless that provider supports it.
Metrics, retention, backup/restore drills, identity/RBAC, and large artifact
storage remain separate production qualifications.
