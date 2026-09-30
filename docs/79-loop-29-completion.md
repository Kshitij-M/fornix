# Loop 29 completion: universal operation qualification surface

Status: implemented on the Issue #40 production-qualification branch.

This loop adds the first operator-facing qualification slice for the universal
control plane. It does not close Issue #40; it makes the generic operation
authority inspectable through the same authenticated HTTP and CLI surfaces and
proves the most important authority, fence, replay, and workspace invariants
through those boundaries.

## Delivered

- Added `operation:read`, `operation:create`, and `operation:execute` RBAC
  permissions with deny-by-default route mapping.
- Added authenticated routes for generic operation creation, inspection,
  operation-lease acquisition/takeover, renewal, release, fenced transitions,
  and replay.
- Made actor identity come from the authenticated principal. Caller-supplied
  actor values cannot impersonate another workspace identity.
- Added bounded request decoding and a CLI JSON-file reader with a 1 MiB
  request/plan limit and syntax validation.
- Added `fornix operation create`, `get`, `lease`, `transition`, and `replay`
  commands. Transition fencing is carried in `X-Operation-Fence`; the CLI
  never stores or prints credentials.
- Preserved the existing Postgres `OperationStore` as the sole authority. The
  HTTP layer does not create a second state machine or invoke any connector.
- Accepted empty bodies for lease and replay commands while still rejecting
  malformed JSON and oversized bodies.
- Added HTTP integration coverage for duplicate create and transition,
  monotonic lease takeover, stale-fence rejection, replay verification, and
  cross-workspace denial.
- Added route authorization and CLI bounded-input tests.
- Added a Docker/CI-compatible CLI smoke covering create, duplicate delivery,
  lease renewal/release/takeover, stale-fence rejection, transition, and
  replay.

## Verification performed

The focused qualification run was executed against a clean local PostgreSQL
database with the full migration set. It passed the authenticated route tests,
the duplicate/fence/replay/workspace integration test, and the local latency
qualification:

```text
generic operation HTTP create samples=20 p50=2.347ms p95=5.033083ms max=5.033083ms
```

The latency sample is a local regression signal for one authenticated create
per request on the developer database. It is not a production capacity claim;
deployment qualification must repeat it with the target PostgreSQL topology,
connection pool, concurrency, data volume, and failure profile.

The branch-wide checks also passed:

- `make check` — all Go tests, `go vet`, Python checks, documentation checks,
  package checks, shell syntax checks, and CLI/runtime tests;
- targeted `go test -race` for the CLI, server, contracts, stores, connectors,
  and workflow packages;
- `make smoke-universal-operation` — HTTP integration tests plus the Docker/CI-
  compatible CLI smoke;
- `make build` — server, watcher, and evaluation binaries;
- the existing operation-admission, connector, workflow, and multi-domain
  smoke targets.

The full existing smoke sweep was also run against the local PostgreSQL
database and development-auth server. Its prior repository, ingestion,
receipt, change, validation, policy, connector, workflow, multi-domain, local
runtime, and universal-operation targets remained green.

## Authority and crash semantics

No new migration is needed for this adapter slice. Operation projections,
leases, transitions, idempotency records, linked resources, linked sources,
and append-only operation events already exist in migrations 035–037. The
store commits the operation decision and its event atomically. A crash before
commit leaves no visible decision; a crash after commit is replayable. An
external connector call remains at-least-once and must be reconciled through
the existing effect authority; replay never calls the connector.

## Measured local cost

The HTTP adapter adds bounded JSON decode and authorization work but no network
call beyond the client request and no new service. Create and transition retain
the store's existing transaction and row-lock profile. Lease and replay are
bounded by the existing TTL and replay limits. Exact p50/p95 latency, SQL
statement count, WAL/storage growth, and replay throughput belong in the
branch-wide Issue #40 qualification report rather than being presented as
unmeasured production capacity.

## Remaining Issue #40 gates

This loop does not yet qualify row-level tenant enforcement, external secret
manager leases, outbound destination policy, signed connector/capability
trust, quotas/backpressure, worker fairness, retention/cold storage,
backup/restore, HA/failover, migration drills, adversarial prompt-injection
and confused-deputy behavior, load/soak/failure injection, or operational
support bundles. Those remain explicit gates in the universal production
qualification plan.
