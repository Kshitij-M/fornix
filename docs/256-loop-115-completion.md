# Loop 115 — Atomic tool-result/effect finalization and interrupted-dispatch recovery

Status: Implemented locally; PostgreSQL qualification pending.

## Outcome

The tool adapter can now finalize its specialized result from inside the
dispatcher’s existing workspace transaction. That transaction commits the
verified external-effect state, reconciled domain link, generic operation
result, tool result/artifacts/evidence, terminal tool event, observations, and
cleanup intent together. A failure in any of those writes rolls the whole
finalization back.

The recovery coordinator also accepts a bounded set of nonterminal dispatch
states. Under a fresh effect lease, it atomically moves an interrupted
dispatching, dispatched, or acknowledged effect and its linked tool run into
the recovery-required state before asking an attempt-aware sandbox for the
exact persisted attempt. It never re-executes the tool. If a previously
persisted response hash exists, a different observed result is rejected.

## Implementation

- Added an internal transaction-finalizer seam to effect dispatch; callbacks
  run only for a verified effect, reconciled link, and matching durable
  operation-result hash.
- Added ToolRunStore.FinalizeEffectResultTx, which verifies stable tool/run
  identity and task/agent-run fences before using the existing transactional
  result/artifact/event/observation/cleanup path.
- Wired the server tool adapter into that transaction. Tool failure status is
  reflected in both the specialized tool run and generic operation result.
- Added fenced Prepare recovery to normalize only the allowed interrupted
  states, plus replay, hash-conflict, and rollback tests.
- Added feature and qualification documentation. No migration or runtime
  dependency was required.

## Verification

Executed locally:

    go test ./... -count=1
    go test -race ./... -count=1
    go vet ./...
    make fmt-check docs-check package-check

All commands passed. PostgreSQL integration cases were skipped because
FORNIX_TEST_PG_DSN is not configured. A local PostgreSQL binary exists, but
pg_isready reports no server and this installation lacks the required vector
extension. Docker is unavailable in this sandbox, so these results do not
qualify migrations, transaction rollback, row locking, fencing, or workspace
RLS. The CI PostgreSQL job must execute those tests before this change is
treated as database-qualified.

No latency benchmark was run. The success path adds no transaction or network
round trip: it moves the tool result write into the existing generic final
transaction. The recovery preparation adds one bounded Postgres transaction;
the sandbox inspection remains an external, read-only call. Storage schema
growth is zero; successful runs retain the same tool result, evidence,
artifacts, and cleanup metadata as before.

## Remaining limitations

- No Moby/OCI Engine client, container lifecycle, or cleanup queue consumer is
  implemented or registered.
- The default local-process backend cannot inspect a process after host or
  control-plane failure. Exact recovery is available only to an installed,
  trusted, attempt-aware provider.
- Existing databases that already contain the former verified effect +
  reconciled link + nonterminal tool-run split are not auto-repaired by this
  slice; they need explicit operator investigation because the external result
  body may not be durably available.
- PostgreSQL-backed tests and live crash/restart behavior remain unverified
  until the disposable CI/deployment qualification environment runs them.
