# Workflow retry deadlines and queue eligibility

Status: design note written before implementation; closes a durable scheduling
gap found during production-qualification review.

## Problem

Workflow step records already contain `next_retry_at`, and the operation queue
already filters on `operations.next_retry_at`. The transition from a workflow
step to the operation projection did not propagate the retry deadline, however,
and `WorkflowStore.StartStep` accepted an `awaiting_retry` step before its
deadline. A delayed retry could therefore become immediately claimable through
the operation queue or be bypassed by a caller acquiring a workflow lease
directly. That defeats bounded backoff and can create hot loops against a
temporarily unavailable provider or a capability rate limit.

## Reference and reuse decisions

- Reuse the existing workflow step `NextRetryAt`, wait expiry, operation
  `next_retry_at`, fenced operation transition, and queue eligibility index.
- Use PostgreSQL `clock_timestamp()` as the sole eligibility clock. App-server
  wall clocks are not authoritative for distributed scheduling.
- Do not add a scheduler, mutable counter, migration, queue, or dependency.
- Preserve the existing `OperationStore.ClaimReady` lease/fence semantics;
  this change only makes its already-declared due-time predicate receive the
  correct timestamp.
- No reference source is copied. No licensing change is required; Fornix
  remains MIT and Kronaxis source remains excluded (BSL 1.1).

## Invariants

1. Completing a workflow step as `awaiting_retry` with a retry wait expiry
   writes that exact deadline to both the step projection and operation
   lifecycle transition in the same transaction.
2. A retry deadline is a not-before boundary. Neither a queue claimant nor a
   direct fenced `StartStep` may start the attempt while PostgreSQL time is
   earlier than the persisted deadline.
3. A retry result without an expiry remains immediately eligible; callers that
   need backoff must supply a durable retry wait and expiry.
4. Starting an eligible retry clears the old deadline as part of the same
   checkpoint/operation transition that starts the attempt.
5. A too-early start fails closed without a workflow transition, event,
   idempotency record, attempt increment, or external connector invocation.
6. Crashes preserve the pre-start checkpoint and deadline. After the deadline,
   normal lease takeover and idempotent start semantics apply.
7. Existing workspace isolation, task fencing, deterministic state hashes,
   effect recovery boundaries, and at-least-once external semantics remain
   unchanged.

## Schema and cost

No schema change is needed: migration 035 already has
`operations.next_retry_at`, and migration 038 already stores
`workflow_step_states.next_retry_at`. The completion transaction copies the
wait expiry into the existing operation transition state. Only a step with a
persisted retry deadline incurs the additional server-clock eligibility query
on `StartStep`; ordinary steps retain their current query count. Queue claims
continue to use the existing due-time predicate and index. No durable row or
artifact is added.

## Acceptance tests

- A future retry wait is preserved in the workflow step and the operation
  projection/transition after one atomic completion.
- The operation queue does not claim the workflow before its retry deadline.
- A caller holding a valid current lease cannot bypass the deadline by calling
  `StartStep` directly; the rejected call leaves hashes, versions, attempts,
  events, and idempotency history unchanged.
- After PostgreSQL time reaches the deadline, the operation is claimable and
  the current fenced owner can start exactly one next attempt.
- A retry without an expiry stays immediately eligible for compatibility.
- Stale worker, task-fence, duplicate delivery, crash rollback, replay, and
  workspace-isolation guarantees remain green.
- Run targeted offline tests, the PostgreSQL-backed deadline scenario,
  compilation, race checks, CI, and documentation validation. Record which
  database checks could not run rather than describing skipped checks as
  passing.

## Operational limitations

This closes workflow retry timing only when an executor returns an explicit
retry wait expiry. It does not compute provider `Retry-After`, select a
capability-wide next slot, create an automatic workflow scheduler, or change
the currently documented operator retry behavior for rate-limited workflows.
Those are separate policies and must not be inferred from this fix.
