# Loop 72 completion — deterministic built-in adapter qualification matrix

Status: implemented on `feat/issue-40-production-qualification`; this is a
bounded fixture/replay qualification slice, not live production certification.

## Delivered

- Added `internal/qualification.RunMatrix`, a bounded deterministic runner for
  explicitly named connector registries and typed operation requests.
- Sorted matrix entries before execution and rejected duplicate names, missing
  registries, nil contexts, cross-workspace requests, invalid names, and entry
  counts outside the 1–64 bound.
- Reused the existing connector conformance boundary, admission rules, effect
  authority, and redacted `QualificationReport` contract.
- Added monotonic outcome aggregation so failed or blocked adapter evidence
  cannot be hidden by a later passing entry.
- Added fixture coverage for HTTP, SQL, repository, and fake incident reads,
  plus a fail-closed effectful remediation case.
- Added the offline `make qualification-adapter-matrix` command, package
  checks, CI coverage, and the operator runbook section.
- Updated the public roadmap and documentation map. The native
  `fornix qualification validate|hash --file PATH` command remains the
  offline handoff for deployment-produced reports.

## Verification

The following checks passed after implementation:

```text
make qualification-adapter-matrix
make package-check
make check
go test -race ./internal/connector ./internal/contracts ./internal/qualification ./internal/store
go build -trimpath ./cmd/fornix ./cmd/fornix-eval ./cmd/fornix-watcher
python3 scripts/check_docs.py
git diff --check
```

The matrix uses local fixtures and does not contact a provider, database,
secret manager, broker, external effect, or Docker runtime. No OpenAI or other
provider key is required. No live deployment evidence was manufactured.

## Cost, storage, and remaining limitations

The matrix is process-local and capped at 64 entries. It creates no migration,
durable row, artifact, image, or build cache. Its report is bounded by the
existing qualification contract and excludes raw requests, responses,
credentials, SQL, and provider errors. Live calls remain at-least-once unless
the adapter supplies its own idempotency and verification authority.

Task 72 does not execute or certify live provider semantics, credential
rotation, HA promotion, client reconnect, WAL/PITR, partition maintenance,
measured RPO/RTO, or load/soak behavior. Those require deployment-owned
systems, operators, protected raw evidence, and the existing offline report
validator.

## Next task prompt

Task 73 — execute deployment-owned live connector and recovery drills.

Read the chats directory, `AGENTS.md`, docs 14, 90, 111, 159, 163, 165,
167, and this completion note. Using isolated deployment-owned systems only,
run HTTP, SQL, federation, model, embedding, and effectful connector
qualification; verify provider idempotency and outcome reconciliation; execute
mTLS/workload-identity rotation and revocation; run PostgreSQL primary/standby
promotion, reconnect, WAL/archive, PITR, backup/restore,
partition-maintenance, retention takeover, and measured RPO/RTO drills; and
record redacted results in `QualificationReport`. Validate the report with
`fornix qualification validate`, preserve raw evidence outside the public
report, add bounded load/soak and failure-injection evidence, and do not claim
production readiness until deployment gates pass.
