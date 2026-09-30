# Loop 73 completion — server-composed generic operation worker

Status: implemented on `feat/issue-40-production-qualification`; this is a
repository-owned execution-composition slice, not live production
qualification.

## Delivered

- Added `PrincipalForIdentity`, a workspace-bound active-identity lookup for
  server-owned background work. Disabled identities and cross-workspace actor
  references fail closed.
- Added `ReadOnlyOnly` queue filtering. The Postgres claim query requires a
  persisted non-empty plan whose every step is `read_only` or `observation`;
  unplanned, unknown, and effectful work is not claimable by the server worker.
- Added dynamic workspace-inventory replacement to the fair supervisor while
  preserving round-robin position and allowing an empty inventory to stop
  claims safely.
- Composed the worker into the server when `FORNIX_WORKER_ENABLED=true`.
  Configuration bounds are explicit:
  `FORNIX_OPERATION_WORKER_MAX_CONCURRENT`,
  `FORNIX_OPERATION_WORKER_CLAIM_BATCH`,
  `FORNIX_OPERATION_WORKER_MAX_ACTIVE`, and
  `FORNIX_OPERATION_WORKER_POLL_INTERVAL_SECONDS`.
- Added durable transition/result handling under the exact operation and task
  fences, hash-only failure details, redacted worker logs, and no generic
  external-effect dispatch.
- Added Postgres queue-filter tests, supervisor inventory/fairness tests,
  configuration-bound tests, and server worker integration tests (skipped
  locally when `FORNIX_TEST_PG_DSN` is absent).

## Verification

Passed locally:

```text
go test ./internal/config ./internal/operationsupervisor ./internal/operationworker ./internal/server ./internal/store -count=1
go test -race ./internal/operationsupervisor ./internal/operationworker ./internal/server ./internal/store
make check
make package-check
python3 scripts/check_docs.py
git diff --check
```

The database-backed server-worker tests require a disposable Postgres DSN and
are executed in the integration CI job. No Docker image, provider, credential,
broker, or additional storage is required by this slice.

## Cost, storage, and remaining limitations

Each polling round adds one bounded active-workspace inventory page and one
bounded queue claim per selected workspace. Defaults are four concurrent
workspace turns, eight claims per turn, four active leases per workspace, and
a two-second interval. The worker creates only existing operation transition,
result, event, and authority-link rows; it stores no raw adapter payload.

This does not implement a generic effectful worker, provider reconciliation,
weighted scheduling, host sandboxing, HA/PITR, live connector semantics,
deployment pool qualification, or production SLO evidence. Effectful work
still requires the existing authority-aware dispatcher and connector-owned
verification/recovery path.

## Next task prompt

Task 74 — execute deployment-owned live connector and recovery drills.

Read the chats directory, `AGENTS.md`, docs 14, 90, 111, 159, 163, 165,
167, 169, and this completion note. Using isolated deployment-owned systems,
qualify live HTTP/API, SQL, federation, model, embedding, and effectful
connectors; verify provider idempotency, response verification, credential
lease rotation/revocation, mTLS/workload identity, PostgreSQL promotion and
reconnect, WAL/archive/PITR, backup/restore, retention takeover,
partition-maintenance, load/soak, and measured RPO/RTO. Emit only bounded,
redacted `QualificationReport` evidence, validate it offline with the native
CLI, preserve raw evidence outside the report, and do not claim production
readiness until the deployment gates pass.
