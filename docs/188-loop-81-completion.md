# Loop 81 completion — deployment release/evidence gate

Status: implemented on the current feature branch; PostgreSQL integration
execution remains an environment-gated CI/deployment qualification.

## Delivered

Task 81 adds a durable, workspace-scoped release/evidence index over the
existing signed qualification-import authority.

- `internal/contracts/deployment_evidence.go` defines immutable release,
  hash-only evidence-link, page, and deterministic gate contracts.
- Migration `068_deployment_evidence_gate.sql` adds release identities,
  append-only release/evidence event history, evidence links, uniqueness,
  indexes, and transaction-local workspace RLS.
- `internal/store/deployment_evidence.go` implements transactional release
  registration, duplicate replay, evidence linking, bounded pagination,
  read-only gate evaluation, snapshot binding, and stale-snapshot rejection.
- `internal/server/deployment_evidence.go` exposes authenticated HTTP routes
  for release registration/listing/inspection, evidence linking/listing, and
  gate evaluation. The gate does not execute deployments or external effects.
- `cmd/fornix/qualification_cli.go` exposes the equivalent bounded operator
  commands: `release-register`, `release-list`, `release-get`, `evidence-link`,
  `evidence-list`, and `release-gate`.
- RBAC maps release/evidence reads to `qualification:read` and mutations to
  `qualification:admin`; actor and workspace identity come from the
  authenticated request context.
- Contract tests cover stable gate hashes, canonical ordering, closed
  vocabularies, duplicate required kinds, and raw-content non-disclosure.
- PostgreSQL integration tests cover idempotent registration and linking,
  gate readiness, cross-workspace reads, and rejection of evidence from a
  later trust snapshot.
- Make, CI, HTTP API, qualification runbook, production-readiness, roadmap,
  and implementation-index documentation are updated.

## Invariants

1. A release is immutable and binds to the exact trust-snapshot revision and
   hash current at registration time.
2. A link references an accepted signed import; it never duplicates the raw
   signed bytes, prompts, credentials, or deployment payload.
3. A new link must match the release deployment, target hash, report,
   manifest, source, and trust snapshot facts.
4. Repeating an idempotency key with the same identity returns the original
   durable row; conflicting identity fails closed.
5. A gate is ready only when all required kinds are present, passed, resolved
   or not applicable, current, and bound to the release snapshot.
6. Gate evaluation is read-only and replayable. It never calls a provider,
   executes a migration, runs a backup, changes an external system, or claims
   exactly-once deployment behavior.

## Verification

Passed locally without a database:

```text
go test ./internal/contracts ./internal/store ./internal/server
go test ./cmd/fornix ./internal/server ./internal/contracts ./internal/store
go test ./internal/contracts -run '^TestDeployment' -count=1 -v
make fmt-check
```

The PostgreSQL tests compile and skip when `FORNIX_TEST_PG_DSN` is absent. CI
runs `make qualification-deployment-evidence` against its disposable Postgres
service. A local run with an explicit disposable DSN is:

```sh
FORNIX_TEST_PG_DSN='postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=disable' \
  make qualification-deployment-evidence
```

No local Postgres service was available during this loop, so migration
application, RLS execution, concurrent transactions, and measured database
latency remain CI/deployment evidence rather than local measurements. The
new index performs bounded reads and one transaction for each write; storage
growth is hash/reference metadata plus append-only audit events, while raw
qualification bytes remain stored exactly once in the import authority.

## Remaining limitations

- Fornix does not perform deployment operations, backup/restore, failover,
  PITR, provider calls, or external-effect reconciliation.
- The gate is only as truthful as the deployment-owned signed evidence and
  trust-distribution ceremony. HSM/KMS custody, workload identity, mTLS,
  release/image signature verification, and hosted topology remain open.
- Startup currently qualifies the current trust snapshot; it does not yet
  require a release gate hash before every deployment or effectful operation.
- The repository still needs deployment-specific load/soak, backup/restore,
  HA/PITR, migration rollback, live provider, sandbox, and artifact-integrity
  qualification.

## Next task prompt

**Task 82 — Bind release evidence to startup, artifact verification, and
deployment admission.** Read the chats directory, `AGENTS.md`, the latest
universal roadmap/completion notes, and the Task 79–81 qualification notes.
Implement the smallest production-quality slice that lets a deployment publish
bounded signed release/image metadata, bind it to the current trust snapshot
and Task 81 gate hash, and make readiness/effect admission fail closed when
the release is missing, revoked, stale, or unverifiable. Preserve historical
release/evidence rows, never execute live deployment operations, add crash,
replay, rollback, RLS, redaction, and duplicate tests, update the CLI/API,
Make/CI, runbook, measurements, and remaining limitations. Keep Postgres as
the authority and do not introduce a broker or another service.
