# Loop 100 completion — due workflow retry advancement

Status: runtime and queue-deadline repair implemented; PostgreSQL integration
qualification remains pending a successful disposable-database run.

## Outcome

The generic workflow runtime now resumes due `awaiting_retry` steps through the
same fenced `StartStep → execute → CompleteStep` path used for ordinary work.
The store's PostgreSQL `clock_timestamp()` check remains authoritative. A
retry that is still early returns as waiting without consuming an idempotency
record, advancing an attempt, or appending a transition.

When several steps are waiting for retry, runtime selection is deterministic
by plan order and considers later steps if an earlier deadline has not elapsed.
One retry is executed per advance batch. `Run` returns after a retry-wait
transition, including a transition that schedules another attempt, so
configured retry budgets are not truncated by the per-call transition cap and
one scheduler turn does not drain immediate retries. The runtime does not poll
or sleep when every retry is still early. Recovery of any already-running step
remains ahead of retry dispatch.

The operation queue has one deadline for the entire workflow. Each completion
that leaves a workflow in `awaiting_retry` now recomputes that deadline from
all still-pending retry steps: the earliest timestamp is used, and a pending
retry with no expiry retains the existing immediately-eligible behavior. This
prevents a successful due retry from clearing a later sibling deadline and
causing repeated immediate queue claims. The run-level wait/failure summary is
copied from the same deterministically selected pending step, keeping surfaced
retry timing aligned with queue eligibility. Retry-budget exhaustion clears
the exhausted step's wait and deadline before persisting terminal failure.

## Schema, storage, and database work

- No migration or new table was needed.
- No persistent bytes were added; the existing step `next_retry_at` values and
  operation `next_retry_at` field remain authoritative.
- Retry scheduling reuses the existing transactional operation transition.
  The earliest deadline is computed from the already-loaded bounded workflow
  step set; no extra SQL read is added during completion.
- Each retry-waiting step selection uses the existing fenced `StartStep`
  transaction and database-clock check. A not-due step incurs that bounded
  transaction and is skipped; the count is limited by the workflow's maximum
  step count.
- No database latency, queue throughput, or storage measurement is claimed:
  the required PostgreSQL runtime tests could not run in this environment.

## Tests and qualification

Passed locally:

- Focused database-independent contract/store tests for retry-wait contracts,
  earliest pending deadline selection, run-status reduction, and wait-detail
  selection passed. Retry-budget exhaustion cleanup is covered by a
  PostgreSQL-backed test and remains unrun locally.
- Race-enabled focused contract/store/workflow package checks passed for all
  runnable tests; database-backed runtime cases skipped as described below.
- `go vet ./internal/contracts ./internal/store ./internal/workflow` passed.

The following new PostgreSQL-backed tests were explicitly skipped because
`FORNIX_TEST_PG_DSN` is unset:

- `TestRuntimeResumesDueRetryStep`
- `TestRuntimeEarlyRetryIsAnUnchangedWait`
- `TestRuntimeReturnsAfterOneRetryTransition`
- `TestRuntimeLaterDueRetryIsNotHiddenByEarlierWait`
- `TestWorkflowRetryBudgetExhaustionClearsWait`

The existing queue/direct-start integration test also requires disposable
PostgreSQL. Its qualification command is wired through
`make qualification-workflow-retry-deadline-postgres`; that target now also
runs the new runtime tests in CI. CI itself has not been run from this local
environment. A skipped database test is not considered a pass.

## Cost and limits

There is no new infrastructure, migration, model call, or background polling.
Runtime wall-clock and step-count budgets remain in force. The operation queue
deadline is an eligibility boundary, not a reservation; another operation may
consume shared capability capacity before this retry runs, in which case the
existing admission path can persist a later retry deadline.

## Remaining qualification

Run the full `qualification-workflow-retry-deadline-postgres` target against a
fresh disposable PostgreSQL database, including migrations, queue timing,
fencing, runtime resume, multiple pending deadlines, and replay. Until that
run passes, this repair is implemented but not fully production-qualified.
