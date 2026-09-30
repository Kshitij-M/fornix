# Loop 84 completion — adapter authority conformance

Status: implemented repository qualification slice; hosted provider,
credential-manager, network-boundary, and deployment qualification remain
deployment-owned.

## Delivered

- Added the hash-only `AuthorityConformanceReport` inventory for every
  registered workspace. Effectful connector capabilities must implement the
  authority-aware execution and effect-description seams and advertise
  idempotency, verification, and an external-effect execution profile.
- Added deterministic all-workspace validation. Read-only and observation-only
  workspaces remain valid without an effectful adapter; missing effectful
  controls fail closed.
- Added the `effectdispatch.ConformanceRegistry` and a stable server manifest
  for model completion/streaming, embedding generation/reconciliation, tools,
  and change application. Registration is synchronized and rejects incomplete
  authority requirements.
- Added the hash-only deployment-admission reference to dynamic child
  operations. Built-in server adapters receive the latest qualified reference;
  `OperationStore` still revalidates it in the same Postgres transaction as
  effect reservation, so the process cache is not a second authority.
- Added strict startup validation for every registered workspace when signed
  authority or release-admission-for-effects is enabled. The authenticated
  authority status exposes the static manifest hash for drift detection.
- Added an offline operator command and qualification script:
  `fornix qualification adapter-conformance` and
  `make qualification-effect-conformance`.
- Added contract, deterministic-hash, multi-workspace, incomplete-manifest,
  redaction, CLI, and existing authority smoke coverage. No migration, table,
  broker, provider call, or new service was introduced.

## Verification

The focused checks completed in the local environment:

```text
go test ./internal/connector ./internal/effectdispatch ./internal/server ./internal/config ./cmd/fornix
make smoke-universal-authority-conformance
go run ./cmd/fornix qualification adapter-conformance
```

The full repository checks remain required before merge:

```text
go test ./...
go test -race ./...
go vet ./...
make check
make fmt-check
make package-check
make docs-check
```

PostgreSQL-backed integration tests remain skipped locally when
`FORNIX_TEST_PG_DSN` is unset. CI's disposable PostgreSQL job remains the
source of evidence for RLS, transaction ordering, effect reservation,
workspace isolation, and crash/recovery behavior.

## Cost and storage report

The conformance inventory is in-memory and O(C log C) per workspace for C
registered capabilities, plus O(A log A) for A dynamic manifest entries. It
adds no persistent storage, relation, index, cache, provider request, or
external network work. Dynamic effects add no new Postgres authority reads
beyond the existing operation/admission/effect reservation path; they now
carry the bounded deployment reference and rely on the existing same-
transaction revalidation.

No meaningful Postgres latency or storage-growth measurement can be claimed
locally because no `FORNIX_TEST_PG_DSN` is configured. A deployment should
measure startup inventory time, effect-reservation p50/p95, blocked missing-
reference counts, and manifest hash drift separately from provider latency.

## Critique and remaining limitations

This task proves local composition, not the behavior of arbitrary external
systems. The manifest cannot prove that a remote API honors idempotency, that
a process or network sandbox is strong, that a secret manager is available,
or that a qualified release is actually running. The exact credential lease,
egress/destination policy, live connector/provider protocol, HA/PITR,
failover, partition maintenance, load/soak, and multi-region evidence remain
open production gates. Uncertain external outcomes remain recovery-required;
Fornix makes no exactly-once claim.

## Next task prompt

**Task 85 — Qualify the managed credential and controlled-egress envelope for
every external effect.** Add a provider-neutral, hash-only egress/credential
conformance report and require the exact credential lease, source version,
revocation, expiry, destination policy, and network-boundary facts before
model, connector, embedding, or federation dispatch. Reuse existing Postgres
credential leases, admission, effect reservation, and deployment-admission
authorities. Do not add a secret store, proxy, broker, or second authority;
keep fake/read-only development offline and do not claim that a process
manifest proves a hosted network boundary.
