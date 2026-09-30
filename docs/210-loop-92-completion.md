# Loop 92 completion — deployment qualification evidence refresh

Status: implemented on the current feature branch; not committed or pushed by
this task.

## Delivered

- Added typed, bounded refresh request/item/report/result contracts with closed
  evidence kinds, explicit `as_of`, workspace-scoped actor validation, stable
  request hashes, and hash-only redacted reports.
- Added migration 076 for append-only workspace/deployment refresh runs,
  item identities, lifecycle metadata, RLS, uniqueness, and mutation triggers.
- Extracted the existing evidence-link mutation into a transaction-owned
  helper and added one atomic multi-item refresh operation. Predecessors are
  superseded; raw accepted signed imports remain the authority and are never
  copied or overwritten.
- Added deterministic idempotency and conflict handling for aggregate refresh
  requests, bounded list/get reads, fixed-time expiry checks, and dry-run
  rollback semantics.
- Added authenticated HTTP routes:
  `/refresh`, `/refresh/plan`, `/refreshes`, and `/refreshes/{id}`.
- Added CLI commands `refresh`, `refresh-plan`, `refresh-list`, and
  `refresh-get`. The input file is strict, bounded JSON containing only typed
  import references.
- Added API/RBAC tests, contract normalization/hash tests, CLI redaction and
  unknown-field tests, and a PostgreSQL integration test covering atomicity,
  idempotency, history preservation, and dry-run behavior.
- Added Make/CI coverage and updated the API reference, operator runbook,
  readiness qualification, roadmap, and documentation index.

## Verification

Passed locally:

```text
go test ./...
make qualification-deployment-refresh
```

The repository-wide Go suite and the Task 92 contract/server/CLI tests passed.
The new PostgreSQL refresh integration test was discovered and skipped safely
because `FORNIX_TEST_PG_DSN` was unset. No Docker, Postgres, provider, model,
deployment, or external system was started by this task.

The following remain required in CI or an explicitly supplied disposable
Postgres environment:

- migration 076 application against a fresh schema;
- migration 076 upgrade against an existing schema;
- concurrent refresh and RLS isolation execution;
- crash/rollback execution at transaction boundaries.

The store test is intentionally wired to the same explicit DSN contract as
the existing qualification suites and never falls back to the developer
database.

## Cost and storage impact

Each committed refresh adds one run row, at most six item rows, one event, and
the existing per-kind supersession/link events. No signed bytes or deployment
payloads are duplicated. A dry run performs bounded reads and rolls back.
The operation takes one workspace/deployment advisory transaction lock and
evaluates the gate before and after the bounded item set. Network/model/tool
work is zero.

No production latency, SQL timing, storage growth, or concurrency throughput
claim is made from this local run because no disposable PostgreSQL instance
was available. Those measurements belong in the CI/deployment qualification
report with the database version, schema revision, item count, and fixed
`as_of` recorded.

## Remaining limitations

Fornix still does not execute the deployment, backup/restore, PITR, failover,
DNS, mTLS, workload-identity, credential-manager, or provider-idempotency
operation represented by evidence. The deployment-owned scheduler and
observation publisher must produce and sign those facts. Remote external
execution remains at-least-once; this refresh operation only makes the
accepted evidence linkage atomic and replayable.

Next planned slice: Task 93, production-owned qualification execution and
recovery scheduling integration around this refresh boundary.
