# Loop 106 completion — managed model credentials and final egress fencing

Status: implementation slice complete with focused offline tests and a
repository-wide compile-only test pass. Database, full HTTP integration, and
deployment qualification remain open.

## Outcome

The normal server composition now reaches the existing workspace-scoped
credential lease authority for opted-in OpenAI-compatible model calls. The
CLI can construct its configured secret-manager adapter for model-only
deployments; federation polling is no longer an accidental prerequisite.
Production configuration requires an explicit namespaced logical credential
reference and HTTPS provider/credential-manager endpoints, and production
server startup fails closed if no lease authority is injected. The
environment-key compatibility path remains development-only.

Model, HTTP connector, and federation requests now revalidate a credential
lease after request construction and immediately before outbound HTTP I/O.
The HTTP adapter's authority-bound path cannot fall back to its legacy direct
resolver. The shared credential request helper drops the request's
`Authorization` header after transport returns on both success and error;
owned secret byte buffers returned together with resolver errors are cleared.
The implementation does not claim reliable zeroization of immutable Go
strings or of copies retained by a custom transport.

Two adjacent correctness fixes are included from independent review. Task
fence validation now locks the task row before its lease row, matching the
task mutation lock order. A duplicate dispatch in a non-reserved effect state
replays the durable operation result when present and returns an explicit
recovery-required error when the result is missing; it no longer reports a
false successful duplicate.

Secret-manager response decoding clears partially decoded secret bytes even if
a later metadata field is malformed. Credential lease release now uses a fresh
two-second cleanup context, so request cancellation does not suppress release
for context-aware resolvers. A resolver that ignores Go context deadlines can
still block its caller; lease expiry remains the safety backstop. The provider
request retry keeps the same logical/provider idempotency key; this does not
change the at-least-once guarantee when a remote provider ignores or does not
support idempotency keys.

No schema migration, dependency, broker, or new runtime service was added.

## Verification performed

Passed:

- Focused offline tests passed for credential lease validation and bounded
  release, partial-secret cleanup, malformed managed-response cleanup,
  production HTTPS configuration, model retry identity, and related gateway
  retry/fallback behavior. The same focused tests passed under `-race` for the
  credentials, config, and model packages.
- Dispatcher duplicate-race and missing-domain-link integration tests compile,
  but all are skipped locally because `FORNIX_TEST_PG_DSN` is unset. Their
  transaction and concurrency behavior is not claimed as locally verified.
- `go test ./... -run '^$' -count=1`: every Go package and test package
  compiled; tests were intentionally not executed by this compile-only pass.
- `go vet ./...` passed.
- `make fmt-check`, `make docs-check` (243 Markdown files), and
  `git diff --check` passed.

The focused suite was rerun after adding the transport-error cleanup assertion;
both success and error paths leave no authorization header on the owned
request object.

Not verified in this environment:

- PostgreSQL concurrency, task lock-order, transaction rollback, and credential
  lease persistence tests: `FORNIX_TEST_PG_DSN` is unset.
- Full loopback HTTP integration tests: this sandbox cannot bind loopback
  listeners, as established in earlier qualification attempts. The offline
  revocation tests are specifically constructed to fail before network I/O.
- PostgreSQL-backed dispatcher race, domain-link recovery, task-fence, and
  crash-recovery assertions remain unverified until the disposable database
  CI job runs.
- CI, Docker smokes, live secret-manager/provider calls, or production latency.

These are open evidence requirements, not passing checks. No local latency,
throughput, or SQL-count benchmark is claimed.

## Cost and storage

There is no new persistent table or stored credential payload, so the direct
schema/storage delta is zero. Requests use existing credential references and
leases. Managed resolution and the immediate pre-egress validation add
bounded manager/database work on the model path; exact query count and latency
must be measured against the supported Postgres and manager topology. The
duplicate-dispatch correction prevents a missing local result from being
mistaken for completed work but deliberately requires explicit reconciliation
for that uncertain external outcome.

## Critic and remaining limits

- The application validates a lease immediately before calling `http.Client.Do`,
  but a remote provider call cannot be made atomic with local lease revocation.
  Revocation after the final check may race with network transmission; strict
  instantaneous revocation would require the credential authority or egress
  proxy to participate in the send boundary.
- Production startup checks composition and reference syntax, not that the
  logical credential is provisioned or that the external manager is reachable.
- The final local lease check does not query the external secret manager. A
  deployment must keep an issued source version valid for the lease TTL and
  publish emergency rotation/revocation to the Fornix credential reference
  before invalidating the manager-side version. Manager-only rotations are not
  immediately visible to already-issued leases.
- Go strings used to construct bearer headers cannot be guaranteed erased.
- Dispatcher crash/replay behavior and deterministic database lock order still
  need the supported disposable-Postgres CI job.
- Full HTTP credential cleanup, redirect, egress, and provider compatibility
  tests still need a network-enabled test environment.
- This does not close Issue #40 or qualify backup/restore, HA/PITR, RLS,
  workload identity, deployment secret-manager conformance, or load/soak
  behavior.

## Next evidence gate

Run the existing credential, generic-effect-dispatch, and multi-domain
workflow smoke suites against CI's disposable PostgreSQL + pgvector topology;
run the full HTTP/provider tests in the network-enabled CI runner; then record
recovery, RLS, and measured throughput results in the production qualification
runbook. Resolve any defects before treating managed OpenAI as production
qualified.
