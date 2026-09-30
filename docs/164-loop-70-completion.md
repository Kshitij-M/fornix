# Loop 70 completion — live adapter and PostgreSQL topology qualification

Status: implemented on `feat/issue-40-production-qualification`; this is a
qualification foundation, not a production certification.

## Delivered

- Added a bounded `ConformanceReport` over the existing connector registry.
  Reports contain capability identity, deterministic case outcomes, bounded
  error codes, observed duration, and a stable hash that excludes timing.
- Blocked effectful adapter qualification by default. An operator must opt in
  explicitly before the existing authority-aware conformance suite may invoke
  an external effect. This keeps fixture/replay/CI paths side-effect safe.
- Added deterministic redaction tests covering stable hashes, cross-workspace
  rejection, effectful opt-in, and forbidden credential/prompt/error content.
- Added the opt-in `TestPostgresTopologyQualification` harness. It applies
  numbered migrations to an explicitly supplied target, rejects standby and
  read-only endpoints, checks WAL/archive facts, verifies transaction-local
  workspace context across commit/rollback/concurrency, checks pooled-context
  hygiene, and reports bounded acquire p50/p95/p99 observations.
- Added `scripts/qualification/postgres-topology.sh`, the `make
  qualification-postgres-topology` command, package checks, environment
  examples, runbook instructions, roadmap status, and this completion note.

## Verification

The following checks were run for this slice:

```text
go test ./internal/connector ./internal/store -run 'Test(ConformanceReport|PostgresTopologyQualification)' -count=1
make package-check
python3 scripts/check_docs.py
git diff --check
```

After the report-token hardening, the repository-wide qualification also
passed:

```text
make check
go test -race ./internal/connector ./internal/store
go build -trimpath ./cmd/fornix ./cmd/fornix-eval ./cmd/fornix-watcher
```

The live topology qualification was not invoked: no deployment DSN was
provided and the temporary PostgreSQL/pgvector image was not present locally.
This is intentional storage hygiene and leaves no temporary container, image,
or build cache behind.

The topology test is expected to skip unless
`FORNIX_TOPOLOGY_PG_DSN` is supplied. No live provider, external connector,
HA failover, PITR restore, or certificate authority was invoked in the
default verification path. Those facts remain deployment-owned evidence.

## Cost, storage, and limitations

The conformance report is process-local and bounded to one capability's
cases. The topology harness performs at most the configured 512 operations,
16 workers, and 32 pooled connections. It creates no new durable schema or
qualification rows; a deployment may store a redacted report through the
existing artifact path if required. Timing is diagnostic and intentionally
excluded from the replay identity.

This does not prove multi-node availability, automatic client reconnection,
backup encryption, WAL durability, PITR correctness, partition maintenance,
measured RPO/RTO, provider idempotency, certificate revocation, or production
load/soak behavior. Those are the next qualification boundary.

## Next task prompt

Task 71 — Run live provider conformance and production HA/PITR drills.

Read the chats directory, `AGENTS.md`, and the latest production qualification,
universal roadmap, connector, credential, federation, backup, and Task 70
notes. Using only deployment-owned test authorities and explicitly isolated
databases, qualify the HTTP, SQL, federation, model, embedding, and effectful
connector paths; prove workload-identity/mTLS certificate rotation and
revocation; run primary/standby promotion, client reconnect, WAL/archive, PITR,
partition-maintenance, and measured RPO/RTO drills; and add bounded load/soak
evidence. Keep all external effects opt-in, redact credentials and payloads,
preserve workspace/fence/replay invariants, and fail closed on unknown
outcomes. Do not claim production readiness without deployment evidence.
