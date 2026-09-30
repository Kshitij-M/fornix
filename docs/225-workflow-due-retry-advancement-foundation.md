# Workflow due-retry advancement foundation

Status: implementation plan for the retry-resume correctness repair.

## Problem

Fornix durably records a workflow step's retry deadline and prevents both
queue claims and direct step starts from running before that deadline. The
workflow runtime currently classifies `awaiting_retry` with approval, human,
callback, and external waits and returns before it can attempt the step. As a
result, a caller that advances a workflow after its retry becomes due can
leave the workflow parked indefinitely unless it bypasses the runtime.

## Invariants

- `WorkflowStore.StartStep` remains the final authority for retry eligibility
  and checks `next_retry_at` against PostgreSQL `clock_timestamp()` in the same
  fenced transaction that reserves the next attempt.
- Runtime and caller clocks must never make an early retry eligible.
- An early retry is an ordinary waiting result: it does not append an event,
  consume an idempotency key, increment the attempt, or mutate the checkpoint.
- A due retry follows the existing start → execute → complete path. It uses a
  new deterministic attempt identity and retains workspace, workflow-lease,
  optional task-fence, actor, and budget checks.
- Only `awaiting_retry` is eligible for timer-driven advancement. Approval,
  human, callback, external, and recovery waits continue to require their
  existing explicit resume or recovery path.
- When a run contains multiple unresolved wait kinds, a recovery or explicit
  human/approval/external wait takes precedence over timer retry. The runtime
  must not use one due retry to bypass another step's explicit wait.
- The run-level wait payload mirrors the deterministic pending step that
  determines the run-level wait status, rather than the step that happened to
  complete most recently.
- Multiple retry-waiting steps are considered in canonical plan order. A
  batch executes at most one due retry; the enclosing `Run` may continue after
  ordinary non-waiting progress, but returns after any committed retry wait
  transition. The next retry is resumed by a later scheduler/API advance, so
  configured retry budgets are not truncated by a per-call transition cap and
  the runtime does not spin through retries in one call.
- Because the operation queue has one eligibility timestamp, its deadline is
  the earliest pending retry deadline across the workflow. If any pending
  retry has no deadline, it retains the existing immediately-eligible
  behavior. Completing one due retry recomputes the deadline from the
  remaining retry-waiting steps in the same transaction.
- Run-level retry wait details identify the same earliest pending retry as
  the queue. When the retry budget is exhausted, the step and terminal run
  clear retry wait/deadline details rather than advertising another attempt.
- A retry that is denied again can persist a new deadline and remains
  bounded by the workflow retry budget.

## Schema, API, and implementation scope

No schema migration or new public API is needed. The repair changes the
workflow runtime's waiting-state dispatch and adds a PostgreSQL-backed
regression test that schedules a retry with a deadline already in the past,
then verifies the ordinary runtime advances it exactly once. Existing queue
deadline coverage remains responsible for proving that early starts fail
closed.

## Recovery, ordering, and idempotency

Runtime retry selection is deterministic by step ordinal and ID. A not-yet-due
step is skipped so another due retry in the same run can be selected. The
store rechecks the chosen step's deadline transactionally, closing races
between selection and reservation. A duplicate start or completion continues
to resolve through the existing workflow command idempotency records. If the
process crashes after start, current running-step recovery semantics remain
unchanged; this repair does not infer that execution completed.

After a step completes, the workflow status reducer inspects all step states
before returning to `running`: a remaining recovery or explicit wait continues
to block automatic retry, and a remaining retry keeps the run in
`awaiting_retry`. The top-level wait token/reason is copied from that same
selected pending step. This keeps run status and wait details aligned with
durable step state.

The single operation-queue deadline is recomputed from all retry-waiting
steps whenever a step completion leaves the workflow in `awaiting_retry`. This
ensures a due retry cannot erase a later sibling deadline or turn a still-
waiting workflow into an immediate queue hot loop.

The run-level `Wait` and failure summary are selected from the step that
defines the run's blocking wait. Retry waits use the same earliest-deadline
ordering as the operation queue, with immediate retries first and stable step
identity ordering for ties. If the configured retry budget converts a retry
result to terminal failure, its wait is removed before hashes and history are
written.

## Reuse, licensing, and cost

The implementation reuses Fornix's existing workflow store, durable retry
state, database-clock deadline, operation queue, deterministic identity, and
fencing. It copies no reference-repository code and introduces no dependency
or licensing change. It adds no persistent storage. A due retry performs the
same bounded reads and fenced writes as a normal step; checking a set of
waiting retries is bounded by the workflow's existing maximum step count.
There is no polling loop or sleep.

## Acceptance tests

- `Runtime.Run` resumes a due `awaiting_retry` step through its executor and
  reaches the expected terminal state.
- The retried step increments its attempt exactly once and invokes the
  executor once.
- An early runtime advance returns waiting without changing state version,
  attempt, events, or command idempotency records.
- Multiple waiting retries are selected deterministically, and a not-due
  earlier step does not hide a later due step.
- Completing one retry while another remains waiting preserves the run-level
  retry status and the operation queue's earliest remaining deadline.
- Run-level retry wait details and the operation queue agree on the earliest
  deadline; ties and immediate retries are deterministic.
- Retry-budget exhaustion produces a terminal failure with no retry wait or
  deadline.
- A retry transition returns to the caller/scheduler rather than exhausting
  the retry budget inside one runtime call.
- Other waiting statuses remain parked and are not executed by timer
  advancement.
- Existing database-clock, fencing, duplicate-delivery, replay, budget,
  migration, and smoke checks remain green.

## Qualification note

The integration tests require an explicitly disposable PostgreSQL database.
If one is unavailable, unit/build checks may still be useful, but the durable
retry-resume behavior must be reported as unqualified rather than passed.
