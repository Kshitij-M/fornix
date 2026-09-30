# Loop 105 completion — atomic effect results and credential lease fencing

Status: implementation complete with focused offline verification. PostgreSQL
crash/concurrency qualification remains pending; this is not a production-
readiness declaration.

## Outcome

The effect dispatcher now commits the terminal effect transition, its optional
domain-effect-link transition, and the requested operation result (including
the operation transition and authority link) in one workspace transaction.
Duplicates of a committed terminal dispatch read the existing immutable
result. A terminal effect with a missing requested result fails with
`ErrOperationResultRecoveryRequired`; an acknowledged effect whose result was
lost during a crash is not dispatched again. This keeps external delivery
at-least-once and makes the local uncertainty visible rather than synthesizing
success.

Credential leases now require positive fence and revocation-epoch metadata.
The `LeaseResolver` contract includes current-authority validation, and the
model, HTTP, and federation adapters perform that check before sending
credential bytes. Resolver/validation failures release and clear any returned
lease material, and HTTP credential failures do not expose resolver error
text.

No schema migration, dependency, broker, or service was added. Existing
Postgres tables, transaction helpers, failure hooks, immutable result records,
and durable credential-lease authority remain the source of truth.

## Changes and compatibility

- `OperationStore.RecordResultTx` reuses the fenced result/event/authority-link
  write path inside a caller-owned transaction. `RecordResult` remains the
  standalone transaction wrapper.
- Dispatcher finalization uses that API. A failure hook after result and
  authority-link writes proves rollback of the effect/link/result composition
  when run against PostgreSQL.
- Legacy terminal effects without results are detectable and fail closed on
  duplicate delivery. Provider-specific recovery remains explicit; the
  generic dispatcher cannot recover a response payload that existed only in
  process memory.
- `LeaseResolver` implementers must now provide `ValidateLease`; this is an
  intentional interface tightening. Test/fake authorities can implement the
  method deterministically, but production authorities must consult their
  current durable lease state.
- Lease validation is repeated at the final model/HTTP/federation boundary,
  so revocation between acquisition and use is rejected. No credential value
  is added to an event, operation result, error, or evidence record.

## Verification

Passed locally:

- focused lease shape, resolver validation, cleanup, and redaction tests in
  `internal/credentials`;
- focused OpenAI tool projection/ID tests and the new immediate credential
  revalidation regression in `internal/model`;
- the new HTTP immediate-revalidation regression in
  `internal/adapters/httpapi`;
- compile-only checks for `internal/federation`, `internal/store`,
  `internal/effectdispatch`, and `internal/server`;
- `go vet` for all affected Go packages;
- `make fmt-check docs-check` (241 Markdown files) and `git diff --check`.

The full `internal/credentials` test package was attempted but its existing
`httptest.NewServer` case cannot bind loopback in this sandbox (`bind:
operation not permitted`). The new model and HTTP lease regressions were
written to run without opening a listener and pass locally.

The dispatcher integration tests were compiled but not executed against a
database: `FORNIX_TEST_PG_DSN` is not configured here, and the local PostgreSQL
installation lacks the supported pgvector topology. CI's existing
`make smoke-universal-effect-dispatch` integration step selects all
`TestDispatcher...` cases against its disposable pgvector service, including
the new rollback, result replay, legacy-gap, and duplicate-delivery cases.
`make smoke-universal-trust` now selects the new credential boundary tests.
CI itself was not run from this uncommitted worktree.

No local performance benchmark is claimed without PostgreSQL.

## Cost and storage

There is no new table, migration, payload, or retained credential material.
The successful path retains the same effect, link, operation result,
transition, and authority-link records it already required, but avoids a
second transaction boundary for the result. SQL statement count is unchanged
within the final composition; it is one transaction/commit instead of two.
Credential validation adds one bounded authority check immediately before
egress (and, for the function adapter, one at acquisition). Latency, lock
waits, and throughput have not been measured locally.

## Critic and remaining limits

- Atomic local commit does not make an external provider call atomic. If the
  provider replied and the process lost the response before commit, Fornix
  retains the acknowledged/uncertain state and requires domain-specific
  reconciliation; it does not blindly call the provider a second time.
- Historical terminal effects created by the older two-transaction path may
  still lack an operation result. They are now reported as recovery-required,
  but the missing result cannot be reconstructed from its hash alone.
- PostgreSQL lock order, failure-injection rollback, and concurrent duplicate
  delivery must be exercised on the supported disposable database before
  claiming database qualification.
- An injected `LeaseResolver` remains an authority boundary: its
  `ValidateLease` implementation must actually consult its source of truth.
  The Go interface makes that obligation explicit but cannot prove the
  behavior of a deployment-owned implementation.
- Secret material can be zeroed in owned byte buffers, but Go strings created
  for HTTP authorization cannot be reliably erased; providers keep them
  short-lived and never persist or log them.

Issue #40 remains open. Backup/restore, HA/PITR, live provider recovery,
deployment secret-manager conformance, RLS, and topology-specific load/soak
evidence remain production gates.
