# Loop 71 completion — live provider and recovery qualification evidence

Status: implemented on `feat/issue-40-production-qualification`; this is an
evidence and validation foundation, not a production certification.

## Delivered

- Added bounded contracts for `QualificationReport`, `QualificationCase`,
  `QualificationMeasurement`, and `RecoveryDrill`.
- Added deterministic normalization and hashing across adapter, authority,
  certificate, topology, retention, load, backup, PITR, failover, partition,
  and identity-rotation evidence categories.
- Rejected raw DSNs, credentials, prompts, SQL, certificate bytes, arbitrary
  errors, duplicate cases, invalid hashes, unmeasured recovery values, passed
  drills without evidence, and oversized reports.
- Added `internal/qualification.Builder` to adapt the existing redacted
  connector conformance report into the common qualification envelope while
  preserving workspace isolation and evidence hashes.
- Added offline native CLI validation:
  `fornix qualification validate --file PATH` and
  `fornix qualification hash --file PATH`.
- Added `scripts/qualification/recovery-evidence.sh` and
  `make qualification-recovery-evidence` for validating deployment-produced
  reports without contacting providers or databases.
- Updated the public qualification runbook, documentation map, and universal
  roadmap with the evidence boundary and remaining deployment gates.

## Verification

The following focused checks passed after implementation:

```text
go test ./cmd/fornix ./internal/qualification ./internal/contracts ./internal/connector -count=1
```

The full repository checks also passed:

```text
make check
go test -race ./internal/connector ./internal/contracts ./internal/qualification ./internal/store
go build -trimpath ./cmd/fornix ./cmd/fornix-eval ./cmd/fornix-watcher
```

No live provider, secret manager, external effect, HA promotion, PITR
restore, or certificate authority was invoked by this implementation. The
native validator is intentionally offline and cannot manufacture those facts.

## Cost, storage, and limitations

The report and builder are in-memory and bounded by 128 cases, 32 measurements
per case, 32 recovery drills, and 128 KiB serialized size. No migration,
database row, provider call, or Docker image is added. Operators may retain a
validated report through the existing artifact path, but raw deployment
evidence remains outside the public report.

Task 71 does not prove live provider semantics, provider idempotency, outage
handling, mTLS/workload-identity deployment correctness, automatic failover,
PITR, measured RPO/RTO, partition scheduling, or production load/soak. Those
remain deployment-owned qualification gates.

## Next task prompt

Task 72 — Execute deployment-owned live connector and recovery drills.

Read the chats directory, `AGENTS.md`, the production qualification runbook,
universal roadmap, Task 68–71 notes, connector/adapters, credential authority,
certificate, backup/restore, federation retention, and pool qualification
implementations. Using isolated deployment-owned systems only, run the HTTP,
SQL, federation, model, embedding, and effectful connector conformance
checks; qualify provider idempotency and verification; execute mTLS/workload
identity rotation and revocation; run PostgreSQL primary/standby promotion,
client reconnect, WAL/archive, PITR, backup/restore, partition-maintenance,
retention takeover, and measured RPO/RTO drills; and record all results in a
redacted `QualificationReport`. Validate it with the native CLI, preserve raw
evidence outside the public report, redact credentials and payloads, and fail
closed on unknown outcomes. Add load/soak and failure-injection evidence
without claiming production readiness until the deployment gates pass.
