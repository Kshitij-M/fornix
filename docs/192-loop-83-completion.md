# Loop 83 completion — generic effect admission-reference consumption

Status: implemented on the current feature branch; external deployment truth
and hosted production qualification remain deployment-owned.

## Delivered

- `internal/contracts/release_admission.go` adds the bounded
  `DeploymentAdmissionReference` contract. It binds workspace, deployment,
  release, artifact, gate, trust-snapshot, and decision hashes without raw
  manifests, signatures, credentials, or provider payloads.
- `OperationRequest` accepts the optional hash-only reference, normalizes its
  workspace, and includes it in the canonical operation identity. Retries
  cannot replace release authority facts while retaining the same operation
  hash.
- `DeploymentEvidenceStore.ValidateAdmissionReferenceTx` re-evaluates the
  current release gate and verification inside a caller-owned Postgres
  transaction. It rejects not-ready, stale, expired, revoked, mismatched, and
  cross-workspace facts without rewriting historical rows.
- `OperationStore.ReserveEffect` consumes that authority before inserting a
  new effect reservation. The existing operation/effect idempotency and
  recovery authorities remain the sole mutation boundary; no second release
  table, cache, broker, or service was introduced.
- `FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS` is explicit in development
  and automatically enabled in production. Duplicate historical reservations
  remain readable, while new effect reservations fail closed when strict mode
  lacks a reference or the authority is unavailable.
- The server composes the existing deployment-evidence store into the generic
  operation store. The HTTP operation create body and existing CLI request-file
  path therefore expose the same typed field without a parallel API or CLI
  authority.
- HTTP API reference, qualification runbook, production-readiness summary,
  roadmap, `.env.example`, Make, CI, and the public task index document the
  reference shape, strict-mode configuration, at-least-once boundary, and
  limitations.

## Verification

Contract tests cover bounded/redacted references, stable reference hashes,
operation identity sensitivity, and workspace isolation. Store tests cover
current and stale reference validation in the same transaction as the
deployment authority and the strict/optional operation modes. Existing release
verification tests now also exercise the default required evidence set through
the shared transaction helper.

The following checks are required for the final handoff:

```text
go test ./...
go test -race ./...
go vet ./...
make check
make fmt-check
make package-check
make docs-check
make qualification-admission-reference
```

Without `FORNIX_TEST_PG_DSN`, PostgreSQL integration tests skip rather than
touching a developer database. CI supplies a disposable PostgreSQL instance;
that environment is the source of concurrency, RLS, migration, rollback, and
transaction-isolation evidence.

## Cost and performance report

The implementation adds no migration, table, index, process, cache, broker,
or payload store. On a new effect reservation, it adds bounded reads of one
release, its current trust snapshot, gate links, and one verification inside
the existing reservation transaction. The reference adds only bounded JSON
and hash material to the existing operation request row.

The local unit suite does not measure meaningful Postgres latency because no
database DSN is available. CI should capture reservation p50/p95 latency,
statement counts, blocked-admission counts, and relation growth separately
from connector/provider time. This task deliberately does not make a hosted
SLO or storage-growth claim.

## Remaining limitations

- Fornix does not verify a registry, image, binary, manifest, signature, or
  attestation against an external deployment system.
- Fornix does not execute rollout/rollback, contact a provider, or prove live
  target state.
- Provider-side execution remains at-least-once/uncertain and must use the
  existing effect recovery and reconciliation state machine.
- Production still requires deployment-specific RLS role separation,
  workload identity/mTLS, secret-manager/KMS custody, live connector/provider
  conformance, HA/PITR/failover drills, and load/soak evidence.

## Next task prompt

**Task 84 — Qualify generic effect-authority conformance across adapters and
recovery paths.** Read the chats directory, `AGENTS.md`, the latest universal
roadmap, and Tasks 79–83. Add a bounded adapter-conformance registry and
fail-closed startup inventory proving that every production-composed effectful
adapter supplies the operation admission reference, capability/policy/trust
facts, credential lease, egress facts, task/operation fences, and durable
effect reservation before dispatch. Add dry-run qualification reports,
duplicate/crash/RLS tests, CI/Make/docs updates, measurements, and explicit
limitations. Do not execute live deployments, create a second authority, or
claim exactly-once provider behavior.
