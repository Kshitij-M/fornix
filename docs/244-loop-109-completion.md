# Loop 109 — Tool sandbox recovery finalization

Status: implementation is present in the current branch; Postgres integration and a real non-local sandbox provider are not qualified.

## User problem

When an external tool may have started but Fornix loses the response, blindly
retrying can repeat a real-world effect. Fornix must inspect the exact reserved
attempt and publish a proven outcome atomically, or leave the run in recovery.

## Delivered in this worktree

- Added a typed recovery-finalization request that binds workspace, tool run,
  expected versions, recovery fence, sandbox identity, completed observation,
  authenticated actor, and idempotency key.
- Added exact-backend sandbox reconciliation. It calls `ReconcileAttempt`, not
  `RunAttempt`; local-process recovery, backend fallback, and unknown-as-absent
  behavior fail closed.
- Added a Postgres coordinator that uses the existing effect lease and tables.
  A single transaction advances the generic effect through verification,
  reconciles the domain link, stores the bounded/redacted result and any
  artifacts, and appends the terminal tool event. The same transaction also
  records failure-injection points for rollback tests.
- Added `POST /v1/tools/recovery`, protected by workspace-scoped
  `operation:execute` authorization. It requires expected effect/link versions,
  returns `202` for non-completed observations, and never returns raw output.
- Added contract, exact-provider, no-reexecution, stale-identity, authorization
  mapping, transaction rollback, stale-fence, duplicate, concurrency, and
  redaction tests. No migration or new dependency was required.
- Documented the API in [the HTTP reference](53-http-api-reference.md) and the
  invariants/remaining provider requirements in
  [the feature note](243-tool-sandbox-recovery-foundation.md).

## Verification performed

- Compile-only: `go test -p 2 ./internal/contracts ./internal/tool ./internal/store ./internal/server -run '^$' -count=1` — passed.
- Focused contract tests, sandbox-provider tests, and route-permission mapping
  tests passed.
- The new Postgres-backed store test was selected but skipped because the
  required test DSN is absent; its rollback, stale-fence, duplicate,
  concurrency, and redaction assertions have not executed here.
- `FORNIX_TEST_PG_DSN` is not configured. The Postgres-backed store test was
  therefore skipped; atomicity, SQL/RLS behavior, transaction crash recovery,
  and concurrent database finalization remain unverified here.
- The broader repository suite and production smokes were not run as part of
  this slice.

## Cost and storage

This path performs one bounded provider lookup and, on a completed result, a
single workspace transaction with two generic effect transitions, one domain
link transition, one tool result/event update, and existing artifact writes
only when output exceeds the inline threshold. No new tables, indexes,
services, or dependencies were added. Database latency, throughput, and storage
growth were not measured because the test database is unavailable.

## Remaining limitations

- The default sandbox registry contains only local process execution; it has
  no crash-reconcilable runtime provider. Thus the new recovery endpoint is
  intentionally unavailable for actual non-local recovery in the shipped
  default configuration.
- Postgres integration/race qualification, a real sandbox provider, host-crash
  recovery drills, and a full release smoke remain open.
- This loop does not complete Issue #40 or the universal transformation goal.

No commit or pull request was created. The existing dirty worktree was
preserved.
