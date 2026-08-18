# Fornix production-readiness qualification

Status: alpha single-node control and retrieval substrate.

Fornix is runnable and testable, but it is not yet a production-grade,
multi-tenant harness for huge projects. The current system has a durable
Postgres control database, typed event history, deterministic projections,
task coordination, retrieval, and code indexing.

## Verified capabilities

- Numbered, embedded, checksum-validated migrations.
- Liveness/readiness endpoints, request IDs, body limits, timeouts, and
  graceful shutdown.
- Concurrent task claiming and completion.
- Workspace-scoped idempotency, append-only event history, replay, and
  monotonic checkpoints.
- Transactional projection updates with rebuild, duplicate protection, crash
  rollback tests, concurrency tests, and workspace isolation.
- Durable workspace-scoped projection consumer leases with monotonically
  increasing fencing tokens, expiry/takeover, stale-owner rejection, and
  checkpoint authorization.
- Durable workspace-scoped task execution leases with monotonically increasing
  fences, expiry/takeover, stale-worker rejection, dependency-aware ordering,
  bounded retry/dead-letter transitions, cancellation, and atomic lifecycle
  events.
- Deterministic staged retrieval with repeatable-read snapshots, workspace
  isolation, hard item/byte/token budgets, bounded graph expansion, gated
  vector search, evidence hashes, provenance, stable ordering, and context
  hashes.
- Immutable workspace-scoped evidence records with computed raw hashes,
  append-only typed provenance edges, supersession/contradiction metadata,
  bounded deterministic traversal, and gist/detail/raw disclosure budgets.
- Docker-backed Go/Python checks, Postgres integration tests, CI, and smoke
  tests.

## Production gaps

- One shared bearer key; no tenant-aware identities, roles, scoped credentials,
  rotation, revocation, or audit policy.
- Not every mutation path emits typed events yet.
- No content-addressed artifact store for large outputs, raw prompt/tool
  capture, or general memory compiler.
- Evidence raw bytes are currently bounded Postgres payloads; there is no
  object-backed large-artifact disclosure or retention-tier compactor yet.
- No backup/restore drill, high-availability plan, capacity benchmark, metrics
  exporter, or operational backpressure policy.
- The projection runtime is an internal pull API; no background subscriber or
  public replay API is provided yet.
- Lease transitions are current coordination state rather than an append-only
  audit stream; operational metrics and lease-history retention are deferred.

## Qualification commands

```sh
make check
make build
make smoke
make smoke-projection
make smoke-leases
make smoke-tasks
make smoke-retrieval
make smoke-provenance
```

Postgres-backed results and measured latency/storage/replay throughput are
recorded in the loop completion notes under `docs/`.
