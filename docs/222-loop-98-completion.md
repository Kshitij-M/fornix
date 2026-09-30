# Loop 98 completion — workflow retry deadline enforcement

Status: implementation and offline qualification complete; PostgreSQL-backed
queue qualification is present but unexecuted in this local sandbox.

## Outcome

Workflow retry timing now has one durable not-before boundary across both
worker paths:

```text
workflow step retry wait expiry
  → operation transition.next_retry_at
  → Postgres operation queue due-time predicate
  → fenced StartStep checks Postgres clock again
```

`CompleteStep` copies an explicit retry wait expiry into the existing
operation transition in the same transaction that stores the workflow step.
The operation queue already excluded future `next_retry_at` rows; it now
receives the deadline needed to enforce that predicate. `StartStep` also checks
the persisted step deadline with PostgreSQL `clock_timestamp()`, so a worker
that bypasses queue selection and acquires a direct lease cannot start early.
An early start rolls back its command reservation and leaves the attempt,
state hashes, events, and checkpoint unchanged. Starting after the deadline
clears the old deadline while committing the next fenced attempt.

The typed result contract now rejects mismatched retry state/wait kinds. A
retry result without an explicit retry wait remains immediately eligible for
backward compatibility; no delay is inferred.

## Schema, database work, and storage

No migration or new infrastructure was needed. The implementation reuses
`workflow_step_states.next_retry_at`, `operations.next_retry_at`, the existing
operation transition history, and the queue index. Completing a retry adds no
statement beyond the existing transition. Starting an ordinary step adds no
query; starting a step with a persisted retry deadline performs one
PostgreSQL-clock comparison before mutation. No durable rows or payload bytes
are added, so storage growth is limited to the existing transition record.

No latency, lock-wait, WAL, or throughput figure is claimed. The local
environment has PostgreSQL 14 binaries, but `initdb` fails because the
sandbox rejects the System V shared-memory call PostgreSQL requires. Docker
client access is installed but its daemon socket is denied. Thus this host
cannot provide the disposable database required by the integration test.

## Verification evidence

Passed locally:

- `go test -p 2 ./internal/contracts ./internal/store ./internal/workflow -count=1`
- `go test -race -p 2 ./internal/contracts ./internal/store ./internal/workflow -count=1`
- `make qualification-workflow-retry-deadline`

The package-level commands compile and test the affected code. PostgreSQL-
gated cases are skipped when `FORNIX_TEST_PG_DSN` is absent; they are not
counted as passed. The focused integration scenario
`TestWorkflowRetryDeadlineProtectsQueueAndDirectStarts` is included in the
existing CI workflow-store test step and has an explicit local command:

```sh
FORNIX_TEST_PG_DSN='postgres://…' make qualification-workflow-retry-deadline-postgres
```

It schedules a future retry using the database clock, checks operation and
workflow deadlines, proves early direct starts do not mutate state, proves the
queue does not claim early, then waits for the deadline and verifies the next
attempt can be claimed and started once. This database-backed scenario still
needs a successful CI or disposable-Postgres run.

## Remaining limitations

- The change enforces explicit workflow retry deadlines; it does not compute
  `Retry-After`, exponential backoff, or a capability's next available slot.
- Capability-rate denials are still returned as retryable failures by the
  generic workflow adapter rather than automatically converted to a delayed
  retry wait. Operators still need a fresh submission after the rate window.
- The operation queue's broader fairness, pool sizing, lock contention, and
  long-running soak behavior remain production-qualification work under Issue
  #40.
- The new database integration test must run in CI or another PostgreSQL
  environment before this behavior is called fully database-qualified.

The full universal transformation remains in progress; this loop does not
establish production readiness by itself.

## Next task prompt

**Task 99 — Run the retry-deadline integration qualification and connect
capability rate denial to durable retry scheduling.** Use a disposable
PostgreSQL database matching the CI migration topology. Run the new
`qualification-workflow-retry-deadline-postgres` target and record actual
results. Then, before coding, document and implement an explicit retry-after
contract for durable capability admission: derive the next eligible time from
the authoritative rate-window history, persist/replay it across duplicate
delivery, transition generic read/observation workflow steps into
`awaiting_retry`, and ensure both operation queue and direct fenced step start
respect the deadline. Do not blindly retry an effectful operation or replay a
denied admission under the same key. Add concurrency, crash, stale-worker,
workspace-isolation, and no-connector-call-before-deadline tests. Run the
database-backed qualification on CI/Postgres, report measured database work,
latency and storage, and continue Issue #40 without claiming production
readiness.
