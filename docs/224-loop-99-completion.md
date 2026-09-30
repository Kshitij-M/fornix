# Loop 99 completion — capability rate retry scheduling

Status: implementation and local offline qualification complete; PostgreSQL
migration, concurrency, queue, and crash-recovery qualification remains pending
a successful disposable-database run.

## Outcome

Capability rate limits now connect to the durable workflow retry scheduler:

```text
Postgres capability decision history
  → immutable admission denial + retry_at
  → generic read-only step awaiting_retry
  → workflow checkpoint + operation next_retry_at (one transaction)
  → due-time queue claim / fenced direct start
```

Migration 080 adds a nullable `retry_at` to admission decisions. The admission
transaction derives the earliest eligibility time from its rolling 60-second
window using the same database-clock observation and existing workspace /
connector / capability advisory lock. It chooses the Lth-newest accepted or
approval-pending decision (`L` is the capability limit) and adds one
microsecond beyond the inclusive window boundary. The existing partial index
supports the ordered lookup. Caller input cannot supply or serialize this
runtime timestamp; it is excluded from the logical input hash and included in
the durable decision hash.

New rate-limited read-only and observation workflow steps become
`awaiting_retry`. The existing transactional workflow completion path carries
the same deadline into the operation queue. A worker that crashes after the
admission denial commits but before the workflow checkpoint can recover only
when the persisted decision matches the workspace, operation and hash,
capability, target, actor, attempt idempotency key, denial reason, and deadline.
That decision proves the connector was not invoked. Allowed decisions and
unmatched or historical denials without a deadline continue through the
conservative recovery-required path. Effectful steps do not use this retry
shortcut.

## Schema, storage, and database work

- Migration: `080_capability_rate_retry_deadline.sql`.
- Existing admission decisions remain unchanged and read with `retry_at =
  NULL`; no unsafe historical backfill is attempted.
- A `NOT VALID` check constraint enforces that new non-null deadlines belong
  only to rate-limit denials without scanning old append-only history.
- Admission adds no round trip: its existing quota SQL statement now performs
  the count plus, only for a saturated window, an ordered lookup through the
  capability-rate index.
- Each new rate-limit denial stores one nullable PostgreSQL timestamp (8 bytes
  of value data when present, plus normal tuple/page overhead). Workflow and
  operation deadlines reuse existing columns and transitions.
- No latency, lock-wait, WAL, throughput, or real database-storage measurement
  is claimed because PostgreSQL was unavailable in this local sandbox.

## Idempotency and crash semantics

The first denial for a step attempt fixes its deadline. Duplicate submissions
read that exact immutable decision even if current window usage has changed.
A due retry starts a new fenced workflow attempt and therefore receives a new
admission idempotency key. If another caller consumes the newly available
capacity first, the retry is denied again and can be scheduled against the
updated window, subject to the workflow's configured retry budget. A retry
deadline is an eligibility time, not a reservation of future capacity.

Recovery remains conservative for any record that does not prove a
pre-connector denial. The recovery hook is read-only; the normal completion
transaction still enforces workspace ownership, workflow lease fencing, task
fencing, idempotency, and durable event/checkpoint writes.

## Tests and qualification

Passed locally:

- `go test -p 2 ./internal/contracts ./internal/policy ./internal/store ./internal/workflow ./internal/workflows/generic -count=1`
- `go test -race -p 2 ./internal/contracts ./internal/policy ./internal/store ./internal/workflow ./internal/workflows/generic -count=1`
- `go vet ./internal/contracts ./internal/policy ./internal/store ./internal/workflow ./internal/workflows/generic`
- `make qualification-capability-rate-admission`
- `make fmt-check docs-check` (after adding this completion record)
- `git diff --check`

The database-gated store and generic-workflow tests compile but skip locally
because `FORNIX_TEST_PG_DSN` is unset. Their CI commands are wired through
`make qualification-capability-rate-admission-postgres` and
`make qualification-workflow-retry-deadline-postgres`; CI execution is not
claimed by this local run.

The full `go test -p 2 ./... -count=1` command was also attempted. Packages
using `httptest` local listeners failed because this sandbox denies loopback
socket binding (`operation not permitted`): `cmd/fornix-watcher`,
`internal/adapters/httpapi`, `internal/connector`, `internal/credentials`,
`internal/model`, and `internal/qualification`. Other packages in that run
passed. These environment failures are separate from the focused package
results above; the complete suite still needs a normal CI/host run.

The host denied writes to Go's default build-cache location, so verification
used a task-scoped cache under `/private/tmp`; it is removed after the checks.

## Remaining limitations

- The limit is workspace/connector/capability scoped, not account-global or
  provider-global. Those broader quotas require their own authoritative scope.
- The deadline marks earliest eligibility, not guaranteed service order or
  capacity reservation; a later retry may be delayed again by competing work.
- Rate-limit workflow retries consume the existing bounded workflow retry
  budget; policy owners must configure that budget appropriately for their
  capability's rate.
- Database migration, transactional concurrency, real queue timing, and
  crash-recovery assertions remain unqualified locally until run against a
  disposable PostgreSQL instance or in CI.
