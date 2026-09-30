# Loop 48 completion: central outbound policy and egress boundary

Status: implemented on the universal production-qualification branch.

This loop closes the immediate gap identified after role-separated Postgres
isolation, durable credential leases, and signed connector trust: model and
connector HTTP traffic now share one actual transport boundary.

## Delivered

- Added `connector.EgressPolicy` with destination, request-byte,
  response-byte, timeout, and explicit proxy controls.
- Added deterministic policy normalization and a secret-free stable hash.
- Added a controlled HTTP client that re-authorizes each request URL,
  resolves DNS through an injectable resolver, rejects unsafe private-network
  answers by default, disables proxying by default, and removes TLS dial
  bypasses.
- Added bounded request and response body wrappers that enforce limits even
  when content length is unknown.
- Added bounded redirect handling with per-hop destination re-authorization.
- Replaced the HTTP connector's local transport implementation with the shared
  egress client.
- Placed the OpenAI-compatible provider and Ollama chat/embedding paths behind
  the same egress client without changing their public provider contracts.
- Added durable lease-authority validation to HTTP connector credential use,
  matching the existing model-provider behavior.
- Added focused tests for request/response budgets, DNS rebinding/private
  address rejection, redirect denial, and uninspectable transports.

## Verification

The focused qualification run passed:

```text
go test ./internal/connector ./internal/model ./internal/adapters/httpapi -count=1
```

The complete repository matrix, including race, vet, build, role-separated
Postgres qualification, local runtime, and universal smoke checks, must remain
green before this branch is merged.

## Measured local cost

The slice adds no migration, durable row, broker, network service, or
container. It adds bounded in-process policy checks, resolver calls when a
new connection is needed, and one small read wrapper around response bodies.
Exact p50/p95/p99 network and resolver measurements remain deployment-specific
and are not claimed by this local test suite.

## Remaining Issue #38 gates

Managed external secret-manager integration, durable signed trust/catalog
distribution, complete transactional operation-to-policy/effect linkage,
sandbox qualification, backup/restore, HA, retention, load/soak, adversarial
security suites, and live connector conformance remain open. Fornix is a
stronger universal control-plane foundation, not yet a production-readiness
declaration for every production system.
