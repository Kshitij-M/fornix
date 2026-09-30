# Loop 85 completion — managed credentials and controlled egress

Status: implemented repository qualification slice; deployment-owned secret
manager, workload identity, hosted network, live-provider, and production
topology qualification remain open.

## Delivered

- Added the typed, hash-only `ExternalBoundaryAuthority` envelope. It carries
  the exact normalized egress-policy hash, destination-policy hash, explicit
  network-boundary mode, and network-boundary hash. Partial, malformed,
  unsupported, or secret-bearing values fail closed.
- Added migration `070_external_boundary_authority.sql`. It binds those facts
  to generic operation effects, operation authority links, immutable
  domain-effect links, and workspace-scoped federation poll attempts. Empty
  compatibility values preserve historical rows; new strict production paths
  require complete hashes.
- Threaded boundary facts through the dispatcher, child operations, model and
  embedding providers, HTTP connector, federation controlled transport,
  effect reservations, authority links, domain links, and result validation.
  The durable stores compare all boundary facts during duplicate reservation
  and same-transaction link binding; a changed boundary is a conflict.
- Added strict production configuration with
  `FORNIX_REQUIRE_EXTERNAL_BOUNDARY_AUTHORITY`. Generic effects and
  federation polling reject a missing external boundary before dispatch when
  enabled. Fake providers, read-only paths, and local lifecycle fixtures stay
  offline unless an operator enables strict live effects.
- Extended the deterministic adapter conformance manifest with an explicit
  external-boundary requirement for network effects. Added offline commands:
  `fornix qualification adapter-conformance` and
  `fornix qualification external-boundary`, plus
  `make qualification-external-boundary` and CI/package checks.
- Added contract tests for deterministic hashes, partial-envelope rejection,
  secret-free serialization, domain-link identity binding, and incomplete
  network-adapter manifests. Existing model, connector, federation, server,
  store, smoke, and replay paths remain covered.

## Verification

Focused checks completed locally:

```text
go test ./internal/contracts ./internal/effectdispatch ./internal/store ./internal/connector ./internal/model ./internal/server
go test ./internal/store ./internal/federation ./internal/server
go run ./cmd/fornix qualification external-boundary
```

The complete repository checks remain required before merge:

```text
go test ./...
go test -race ./...
go vet ./...
make check
make fmt-check
make package-check
make docs-check
make smoke-universal-authority-conformance
make qualification-effect-conformance
make qualification-external-boundary
```

PostgreSQL-backed integration and migration checks are skipped locally when
`FORNIX_TEST_PG_DSN` is unset. CI's disposable PostgreSQL job remains the
source of evidence for migration-upgrade ordering, RLS, transaction-local
workspace context, duplicate reservation, crash rollback, federation
boundary persistence, and stale-fence behavior.

## Cost, latency, and storage report

Boundary normalization and conformance manifests are bounded in-memory work.
Each external effect and federation poll adds four short hash/mode columns and
one structural consistency check; it does not duplicate policy JSON, URLs,
credentials, response bodies, or provider diagnostics. Reservation and poll
transactions reuse existing indexed credential/deployment validation and add no
network round trip. The local environment has no configured PostgreSQL DSN, so
this loop does not claim database p50/p95 latency, WAL growth, or storage
throughput. Deployments should measure effect/poll reservation latency,
missing-boundary rejection counts, boundary drift, credential-source expiry
failures, and relation growth by effect class.

## Critique and remaining limitations

The envelope is a durable identity of policy inputs, not proof that the host,
proxy, DNS resolver, service mesh, network namespace, secret manager, or
remote provider behaved correctly. `controlled_transport` proves that Fornix
constructed its bounded client; it does not prove isolation outside the
process. `deployment_attested` is an allowed contract mode but requires a
deployment-owned attestation source and verification ceremony. Remote model,
connector, and federation execution remain at-least-once; uncertain outcomes
remain recovery-required. Live provider idempotency, mTLS/workload identity,
DNS-rebinding, HA/PITR, failover, load/soak, and secret-manager evidence are
still open production gates.

## Next task prompt

**Task 86 — Qualify deployment-owned credential, network-boundary, and
live-provider evidence.** Add bounded hash-only evidence for secret-manager/
KMS resolution, mTLS/workload identity, DNS/rebinding and proxy/firewall
enforcement, provider idempotency, and external recovery observations. Bind it
to the existing release admission and `ExternalBoundaryAuthority` without
storing secrets or introducing a proxy, broker, secret store, or deployment
executor. Require explicit opt-in for live effects, preserve deterministic
fake/read-only development, and distinguish repository composition evidence
from deployment proof. Never claim exactly-once remote execution.
