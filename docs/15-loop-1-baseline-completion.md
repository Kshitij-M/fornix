# Loop 1 completion: working baseline

Status: complete historical qualification record.

The first loop made Fornix reproducibly runnable as a single-node development
baseline.

## Delivered

- Go service and watcher entrypoints with one version source.
- Typed configuration, Postgres store, readiness/liveness endpoints, graceful
  shutdown, request IDs, body limits, and server timeouts.
- Numbered, embedded, checksum-validated migrations guarded by a Postgres
  advisory lock.
- Reproducible Python dependencies and a Compose watcher profile.
- Docker-backed build, tests, CI, and HTTP/indexing/MCP smoke coverage.

## Boundary

This loop established reproducibility, not production completeness. Event
history, idempotency, checkpoints, and rebuildable projections were added in
Loops 2 and 3. Workspace identities, task leases/fences, dependencies,
retrieval planning, artifact storage, and operational hardening remain future
work.
