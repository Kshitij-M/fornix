# Capability rate-limit retry scheduling foundation

Status: implementation delivered; local unit/race/vet checks pass, with
PostgreSQL-backed qualification still pending an available disposable database.

## Problem

Fornix records a capability rate-limit denial in the append-only admission
history, but the generic workflow currently turns that denial into a failed
step without a durable eligibility time. Operators and schedulers therefore
cannot distinguish “retry later” from “retry now,” and a worker crash after
the denial commits but before the workflow checkpoint commits loses the safe
retry outcome.

This change connects the existing Postgres capability-window authority to the
workflow retry queue. It does not add a broker, timer service, or connector
retry loop.

## Invariants

- Admission history remains append-only and the original decision is never
  rewritten when its retry becomes due.
- A retry deadline is derived inside the admission transaction from committed,
  non-denied decisions in the same `(workspace, connector, capability)` rolling
  window, while holding the existing capability advisory lock.
- The timestamp is a store-derived runtime fact: callers cannot submit or
  serialize it, and it does not change the logical admission input hash.
- Retry is scheduled only for generic read-only/observation workflow steps.
  Effectful work does not use this retry path.
- The connector is not called after a rate-limited admission decision.
- A retryable workflow result must include a persisted `retry_at`. A legacy
  rate-limit denial without a deadline fails closed and is not made immediately
  runnable.
- A crash may be recovered as a scheduled retry only if the matching durable
  admission decision proves the same workspace, operation/hash, capability,
  target, actor, attempt identity, denial reason, and deadline. Other running
  steps retain the existing conservative `recovery_required` behavior.
- The workflow step checkpoint and operation queue deadline are committed in
  the same transaction. Existing database-clock checks prevent both queue
  claims and direct starts before that deadline.

## Deadline derivation

The rolling capability limit counts every recent decision except `denied`
(including approval-pending decisions). With `n` active decisions and limit
`L`, the first time capacity is available is just after the
`(n-L+1)`-th oldest active decision leaves the 60-second window. PostgreSQL's
window predicate includes its lower boundary, so the persisted retry time is
that decision's `created_at + 60 seconds + 1 microsecond`. The quota count and
candidate timestamp use one database-clock observation under the capability
lock. Ties use descending decision ID, matching a reverse scan of the existing
capability-rate index. This avoids caller-clock drift and concurrent
reservation races.

## Schema and compatibility

Migration 080 adds nullable `retry_at TIMESTAMPTZ` to
`fornix.operation_admission_decisions`. Existing decisions remain valid and
unchanged; old rate-limit rows have no deadline and are deliberately not
reinterpreted. New rate-limit decisions persist the calculated deadline with
the decision, its event, and its decision hash. No backfill is attempted
because historical eligibility cannot be reconstructed safely from the
current state alone. The check constraint is installed `NOT VALID` to avoid a
full scan of a potentially large append-only history during migration; it is
still enforced for new rows.

## Recovery and idempotency

Generic workflow admission keys include the durable step attempt. A due retry
starts a new attempt and therefore creates a new admission decision; a
redelivery of the same attempt reads the same immutable decision. If a worker
crashes after a rate denial commits, the runtime may recover the exact matching
decision into `awaiting_retry` without contacting the connector. An allowed
decision does not qualify: a crash after it could have occurred during the
connector read, so the existing conservative recovery state is retained.

## Scope, authorization, and fencing

The decision is read by workspace plus its deterministic idempotency key and
must match the workflow operation and step. Workflow completion still uses the
current workflow lease and task fence; recovery does not bypass either check.
Admission continues to use the existing workspace-scoped capability lock and
RLS context.

## Cost and operations

The change adds one nullable timestamp to an existing admission row and one
small field to its serialized event/decision. The locked rate-window query
counts matching indexed history and, only when saturated, performs a second
ordered index selection for the Lth-newest eligible row; it does not
materialize the full window into application memory or add a table, background
worker, or polling loop. Storage growth is one PostgreSQL timestamp per new
rate-limited decision. Scheduler work remains bounded by the existing
operation claim batch size.

## Reuse and licensing

The implementation reuses Fornix's Postgres admission history, capability
lock, durable workflow wait state, operation retry queue, and workflow fencing.
Reference repositories were studied for retry and rate-queue behavior, but no
third-party implementation code is copied. This adds no licensing dependency.

## Acceptance tests

- Policy decisions include the runtime retry timestamp in a rate-limit
  decision hash while excluding it from the logical admission input hash.
- New schema migration applies to both fresh and existing databases; old
  decisions read with a nil deadline.
- Concurrent admissions at the capability limit receive a single safe
  deadline consistent with the committed rate window.
- A durable rate denial becomes `awaiting_retry`, propagates the same deadline
  to the operation queue, and does not call the connector.
- Queue claim and direct step start fail before the database deadline and
  succeed after it.
- Duplicate admission returns the original decision and deadline.
- Recovery after a persisted denial reconstructs the retry wait without a
  connector call; mismatched, missing-deadline, or allowed decisions do not
  take that path.
- Existing unit, race, vet, smoke, and CI checks remain green.

## Qualification status

PostgreSQL integration, concurrency, migration, and crash-recovery assertions
are part of the acceptance suite. They are not considered qualified unless
they run against an explicitly disposable PostgreSQL database; skipped tests
must be reported as pending rather than passed.
