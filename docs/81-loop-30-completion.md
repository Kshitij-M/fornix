# Loop 30 completion: credential leases and capability trust admission

Status: implemented on the Issue #40 production-qualification branch.

This loop hardens the universal adapter boundary after generic operation
authority became externally inspectable. It adds exact workspace-scoped trust
snapshots for executable capability definitions and a lease-shaped credential
resolver seam. It does not claim that local files are an external secret
manager or that a process-local snapshot is a signed supply-chain catalog.

## Delivered

- Added immutable `connector.TrustPolicy` snapshots that pin a workspace,
  revision, connector identity hash, and exact capability definition hash.
- Made trust snapshot hashes independent of registration order and rejected
  duplicate, malformed, cross-workspace, or altered entries.
- Added compatibility-mode registries for isolated unit/development use and
  an explicit `RequireTrustPolicy` mode for production composition.
- Installed trusted built-in snapshots during server composition and refreshed
  workspace snapshots after bootstrap and incident connector registration.
- Added `credentials.Lease`, `LeaseResolver`, and `LeaseResolverFunc` with
  bounded TTLs, purpose/workspace/reference scope checks, expiry checks, and
  secret-free disclosure.
- Added `httpapi.NewConnectorWithLeaseResolver`. When configured, the HTTP
  adapter acquires a short-lived lease immediately before the request and
  releases it after the response; legacy development resolvers remain
  source-compatible.
- Corrected the operator CLI so a typed operation request file's own
  idempotency key is used by default; an explicit CLI key remains checked for
  consistency by the server rather than silently replacing the request key.
- Added focused tests for trust order independence, altered definitions,
  untrusted workspaces, lease expiry/scope, TTL bounding, redaction, and
  leased HTTP execution/release.
- Added the implementation note in
  [`80-credential-egress-trust-foundation.md`](80-credential-egress-trust-foundation.md).

## Authority and failure semantics

Trust is a fail-closed admission fact, not an executor or external effect
authority. The operation/admission stores remain responsible for durable
authorization, idempotency, fencing, approval, effect state, and replay.
Credential leases are intentionally ephemeral at this layer; an external
secret-manager adapter must own rotation, revocation, audit, and durable lease
state. The HTTP adapter never stores or returns the lease secret, and replay
never reacquires one.

## Verification performed

- `go test ./internal/credentials ./internal/connector ./internal/adapters/httpapi`
- `go test ./internal/server ./internal/connector ./internal/credentials ./internal/adapters/httpapi ./internal/workflows/incident` with the local PostgreSQL DSN
- `gofmt` and `git diff --check`

The leased HTTP test verified one credential acquisition, one outbound request,
one release, and no secret in the operation result. Trust tests verified that
registration alone does not expand a required policy.

## Measured local cost

Trust admission is an in-process lookup and hash comparison with no SQL or
network work. Lease acquisition adds one resolver call per credential-bearing
outbound operation; its latency, cache hit rate, revocation lag, and failure
rate must be reported by each external secret-manager adapter. No migration or
new service was introduced.

## Remaining Issue #40 gates

This loop does not yet provide signed connector catalogs, external secret
manager implementations, a network egress proxy, tenant RLS/equivalent
database enforcement, quotas/backpressure, worker fairness, retention/cold
tiers, backup/restore, HA/failover, adversarial confused-deputy tests, or
load/soak/failure qualification. The HTTP binding still owns its adapter-local
host/path/redirect/private-network controls; a shared egress authority remains
future work.
