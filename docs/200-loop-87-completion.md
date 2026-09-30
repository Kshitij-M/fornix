# Loop 87 completion — deployment observation publisher boundary

Status: implemented as an offline deployment-owned publishing slice.

## Outcome

Deployment automation now has an explicit, typed boundary for turning
already-collected redacted observations into the signed bundle consumed by
Task 86. Fornix validates the evidence shape and signs the bounded subject; it
does not perform the deployment checks or authorize the resulting operation.

## Delivered

- Added `BoundaryPublisherOptions` with explicit external-effect mode and
  explicit `AsOf` time requirements for library callers.
- Added `PublishBoundaryBundle` and `ValidateBoundaryBundle` helpers over the
  existing qualification normalization, hash, and Ed25519 implementation.
- Added a secret-free `BoundaryPublisherSummary` and strict timestamp parser.
- Added `fornix qualification boundary-sign` for explicit offline signing of
  a redacted bundle, with process-only key use and no implicit output overwrite.
- Added `fornix qualification boundary-validate` for offline signature,
  scope, evidence, cardinality, and expiry validation.
- Required external-effect publication/validation to contain exactly one
  measured, passed, non-expired observation at the requested validation time.
- Preserved ordinary multi-observation bundles for non-effect evidence while
  rejecting missing observations, ambiguous effect evidence, invalid hashes,
  tampering, and implicit library wall-clock use.
- Added feature, architecture, and completion documentation describing the
  deployment/repository responsibility split.

## Verification

The repository gates were run after the implementation:

```text
go test ./...
go test -race ./...
go vet ./...
make check
make qualification-effect-conformance qualification-external-boundary
go build ./...
git diff --check
```

The first combined gate attempt exposed a local command-orchestration issue
where a follow-up cache cleanup raced a still-running Go process. The gates
were then rerun as isolated actions with no concurrent Go process; the final
`make check` and race suite completed successfully. PostgreSQL-backed tests
remain conditional on `FORNIX_TEST_PG_DSN`; no local disposable database was
configured, so those tests were skipped explicitly.

## Cost and storage impact

The publisher adds no migration, database row, cache, network call, model
call, or container. It performs bounded JSON normalization and Ed25519 signing
over the existing 128 KiB qualification limit. CLI output is hash-only and
the output file is written with the existing restrictive atomic writer.
Deployment-side check cost, network latency, and provider charges remain
outside Fornix and must be recorded by the deployment runbook that produces
the source hashes.

## Remaining limitations

- The publisher does not collect secret-manager, workload-identity/mTLS,
  DNS/rebinding, proxy/firewall, provider-idempotency, or recovery evidence.
- A valid signature proves integrity and authorized publication only; it does
  not prove that a deployment observation was truthful.
- Evidence-link revocation/replacement and freshness operations remain the
  next control-plane slice.
- Provider behavior remains at-least-once and deployment-specific; no local
  command claims exactly-once external execution.
- HA, PITR, failover, partition maintenance, and topology load/soak evidence
  remain open production qualification gates.

## Next handoff

Task 88 should add durable freshness, revocation, replacement, and readiness
operations for linked deployment evidence while retaining immutable imports,
receipts, hashes, and historical decisions.
