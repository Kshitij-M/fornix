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
- Docker-backed Go/Python checks, Postgres integration tests, CI, and smoke
  tests.

## Production gaps

- One shared bearer key; no tenant-aware identities, roles, scoped credentials,
  rotation, revocation, or audit policy.
- Task claims still need dependency-aware scheduling, expiring leases, fencing,
  retry budgets, cancellation, and dead-letter handling.
- Not every mutation path emits typed events yet.
- No content-addressed artifact store for large outputs, raw prompt/tool
  capture, or general memory compiler.
- No SQL-first context compiler with hard total-token budgets and provenance
  explanations.
- No backup/restore drill, high-availability plan, capacity benchmark, metrics
  exporter, or operational backpressure policy.
- The projection runtime is an internal pull API; no background subscriber or
  public replay API is provided yet.

## Qualification commands

```sh
make check
make build
make smoke
make smoke-projection
```

Postgres-backed results and measured latency/storage/replay throughput are
recorded in the loop completion notes under `docs/`.
