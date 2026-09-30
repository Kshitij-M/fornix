# Loop 74A completion — agent-run fenced model and tool effects

Status: implemented repository-owned mutation-boundary slice; not a
production-readiness declaration.

## Delivered

- Added explicit agent-run reference, owner, and monotonic fence fields to
  model and tool request contracts, durable model-call records, and durable
  tool-run records.
- Added normalization that rejects partial fences and cross-workspace
  references. The request hash includes the agent-run binding, so a duplicate
  idempotency key cannot be silently rebound to another run or worker.
- Added migration 065 with compatibility-safe columns, complete-binding
  checks, non-negative fence checks, and workspace/run lookup indexes.
- Bound the agent loop's current worker lease into every model and tool request
  it creates. Standalone legacy calls remain valid when they carry no agent-run
  binding.
- Added transaction-local lease validation before model-call reservation,
  model attempt increment, model completion, tool reservation, tool start, and
  tool completion. Postgres locks the authoritative lease row and effect row;
  stale owners fail closed before durable mutation.
- Preserved the explicit at-least-once external boundary. A provider or local
  process that crossed execution may still finish after lease loss, but its
  stale result cannot become authoritative.
- Added contract, agent-loop propagation, model-ledger, tool-ledger, duplicate,
  workspace, takeover, and stale-worker tests. PostgreSQL-backed tests are
  automatically enabled by `FORNIX_TEST_PG_DSN` and remain skipped locally when
  no disposable database is configured.

## Verification

Passed locally on the feature branch:

```text
go test ./internal/contracts ./internal/agentloop ./internal/model ./internal/tool ./internal/store
gofmt on all changed Go files
```

The new integration cases use the existing migration harness and are designed
to run in the PostgreSQL CI job. No Docker image, database volume, provider,
credential, broker, or additional storage was created for this slice.

## Cost and storage impact

Each model-call and tool-run row gains three small scalar columns and one
partial workspace/run index. No payload is duplicated. Bound effect requests
perform lease validation inside the existing transaction; they do not add a
network round trip. The expected storage increase is O(number of model calls +
number of tool runs), independent of output size.

## Remaining limitations

- Local process execution and remote providers remain at-least-once effects;
  this slice prevents stale durable commits but cannot reverse an external
  side effect already in flight.
- The generic server worker still claims only read/observation operation
  plans. Effectful execution remains on the authority-aware dispatcher and
  connector-owned reconciliation path.
- Deployment-owned work remains open: managed secret/KMS integration, live
  connector idempotency and verification, sandbox backend qualification,
  backup/restore and HA/PITR, retention scale, load/soak, and operational
  support evidence.

## Next task prompt

Task 74 — execute deployment-owned live connector and recovery drills.

Read the chats directory, `AGENTS.md`, docs 14, 90, 111, 159, 163, 165, 167,
169, 171, and this completion note. In isolated deployment-owned systems,
qualify live HTTP/API, SQL, federation, model, embedding, and effectful
connectors; verify provider idempotency, response verification, credential
lease rotation/revocation, mTLS/workload identity, PostgreSQL promotion and
reconnect, WAL/archive/PITR, backup/restore, retention takeover,
partition-maintenance, load/soak, and measured RPO/RTO. Emit only bounded,
redacted `QualificationReport` evidence, validate it offline with the native
CLI, preserve raw evidence outside the report, and do not claim production
readiness until the deployment gates pass.
