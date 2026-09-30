# Loop 32 completion: durable generic connector execution

Status: implemented on the Issue #38/#40 universal-transformation branch.

This loop connects the typed connector registry to the Postgres operation
authority for a first safe execution path. It makes read-only and observation
work executable through the same authenticated operation surface while
keeping external effects explicitly out of scope until their durable
reservation and verification path is used.

## Delivered

- Added migration 040 with immutable workspace-scoped hash-only operation
  results and append-only database protection.
- Added fenced `AttachPlan` storage. Connector plans are normalized,
  persisted, and linked to the `created → planned` transition atomically.
- Added fenced, idempotent `RecordResult` storage. Result insertion,
  operation-step projection updates, append-only transition event, and
  terminal operation projection share one transaction.
- Added duplicate result reads that return the committed result without
  requiring a still-active lease, which makes lost-response retries safe.
- Added authenticated `POST /v1/operations/{id}/execute` and a matching
  `fornix operation execute --id ...` command.
- Required exact trusted capability lookup and authenticated actor matching.
- Restricted the first public executor to read-only and observation
  capabilities. Effectful capabilities are rejected without durable effect
  admission/reservation.
- Added crash rollback, result idempotency, trusted read execution,
  workspace/authentication, and effectful rejection tests.
- Added Make and CI qualification targets and the design/completion notes.

## Authority and failure semantics

Postgres remains the authority for operation identity, leases, state history,
result records, and replay. The process-local registry remains the explicit
capability catalog and trust gate. The executor never stores raw connector
output, credentials, prompts, or provider diagnostics in the operation result.
Those belong to the domain evidence/artifact authorities.

If a connector fails before a result commit, the operation is durably failed
with a typed failure hash. If the process fails before the result transaction
commits, the result row and terminal transition roll back together. This slice
does not invoke effectful capabilities, so it cannot claim exactly-once remote
execution.

## Verification performed

- `go test ./internal/store ./internal/server ./internal/connector`;
- PostgreSQL-backed plan/result atomicity, duplicate, crash, trusted execution,
  and effectful rejection tests;
- `gofmt` and `git diff --check`;
- `make smoke-universal-execution` after the qualification DSN is supplied.

The final repository `make check`, race, build, and universal smoke results are
recorded in the handoff after this loop is integrated.

## Measured local cost

The execution control plane adds a bounded plan transaction when a plan is not
already present and one result transaction after adapter execution. Result
storage is bounded by the typed JSON contract and hash-only references. It
adds no broker, worker service, or external infrastructure. The local
qualification must measure operation lease, plan, connector, result-commit,
and replay latency separately; these are regression signals, not capacity
claims.

## Remaining universal gates

The executor is not yet a background operation worker, multi-step generic
scheduler, external-effect dispatcher, callback verifier, secret-manager
integration, central egress authority, or high-availability deployment. It
also does not yet persist a generic evidence/artifact link for every adapter.
Those are the next Issue #38/#40 slices before a production-readiness claim.
