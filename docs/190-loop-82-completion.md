# Loop 82 completion — release verification and startup admission

Status: implemented on the current feature branch; PostgreSQL integration and
external artifact truth remain deployment-owned qualification.

## Delivered

- `internal/contracts/release_admission.go` adds hash-only release verification
  and deterministic admission-decision contracts.
- Migration `069_release_admission_verification.sql` adds workspace-scoped
  verification rows, append-only revoke/register events, uniqueness, expiry,
  indexes, constraints, and RLS.
- `internal/store/release_admission.go` adds transactional verification
  registration, exact gate/snapshot/release binding, duplicate replay,
  revocation, read-only admission evaluation, stable decision hashes, and
  fail-closed reasons.
- HTTP routes provide verification registration/disclosure/revocation and
  admission evaluation under the existing qualification RBAC boundary.
- CLI commands provide `release-verify`, `verification-get`,
  `verification-revoke`, and `release-admission`.
- Production configuration can require a release admission decision through
  `FORNIX_REQUIRE_QUALIFICATION_RELEASE=true` (automatic for production),
  `FORNIX_QUALIFICATION_DEPLOYMENT_ID`, and
  `FORNIX_QUALIFICATION_RELEASE_ID`. Readiness reports a bounded release
  status and remains unavailable when the configured release is not admitted.
- Documentation, Make, CI, HTTP API reference, runbook, roadmap, and public
  implementation index are updated.

## Verification semantics

Registration requires an existing immutable release, a ready Task 81 gate,
matching release/target hashes, the exact current trust snapshot, and the
caller-supplied gate hash. It stores only artifact, attestation, source, and
authority hashes. Expiration is evaluated without rewriting the row;
revocation appends an event and preserves the row. Admission is a read-only
projection, not an execution token and not proof that a registry, image,
binary, or deployment target actually performed the claimed work.

## Tests

Unit/contract tests cover stable redacted decision hashes, closed artifact
kinds, expiry windows, and deterministic blocked-reason ordering. PostgreSQL
integration tests cover verification idempotency, gate binding, ready and
wrong-artifact decisions, revocation, stale-gate rejection, and cross-
workspace reads. They skip locally without `FORNIX_TEST_PG_DSN` and run in CI
through `make qualification-release-admission`.

Local checks passed for this slice:

```text
go test ./internal/contracts ./internal/store ./internal/server ./internal/config ./cmd/fornix
```

The full repository checks remain required after the final documentation and
CI edits. No local PostgreSQL endpoint was available for executing migrations,
RLS, concurrent transaction races, or startup against a real release row.

## Remaining limitations

- Fornix does not ingest or cryptographically verify registry/image/binary
  signatures, contact a deployment platform, execute rollout/rollback, or
  validate live target state.
- Startup requires an explicit configured release ID but does not yet make
  every generic effectful operation consume the admission decision.
- Artifact/reference integrity remains hash-only at this boundary; raw
  deployment manifests and signature evidence remain deployment-owned.
- Hosted HSM/KMS, workload identity/mTLS, registry trust, HA/PITR,
  backup/restore, live provider, sandbox, and load/soak evidence remain open.

## Next task prompt

**Task 83 — Qualify deployment admission consumption and artifact/reference
integrity.** Read the chats directory, `AGENTS.md`, and the latest Tasks 79–82
architecture/completion notes. Add a bounded adapter-facing admission check
that generic effectful operations can reference without becoming a second
authority. Verify release/artifact/gate/snapshot/reference hashes, reject
stale, revoked, expired, or cross-workspace facts, refresh startup state
deterministically, and preserve append-only history. Add concurrency, crash,
replay, rollback, RLS, redaction, duplicate, and stale-admission tests. Update
API/CLI, CI, Make, public docs, measurements, and limitations. Do not execute
live deployment operations or add a broker/service.
