# Loop 2 completion: typed control-plane event/state-delta substrate

Status: complete
Date: 2026-08-16

## Delivered

- Added typed contracts in `internal/contracts/events.go` for versioned event
  envelopes, workspace scope, actor/task/session references, state deltas,
  artifact references, provenance, idempotency, causation/correlation IDs, and
  raw JSON payloads.
- Added migrations `002_control_events.sql` and
  `003_control_events_append_only.sql` for append-only event history,
  workspace-scoped idempotency records, durable consumer checkpoints, indexes,
  and a database-level UPDATE/DELETE guard.
- Added `internal/store.EventStore` with transactional append/append-in-tx,
  duplicate resolution, ordered reads, bounded replay, and monotonic
  checkpoints.
- Integrated `POST /v1/task/:id/complete`. Task/session state, event history,
  and an idempotency reservation now commit together. The endpoint returns
  `event_id`, `event_sequence`, and `deduped`.
- Added contract tests, Postgres concurrency/duplicate/order/replay/raw
  payload/checkpoint/rollback/append-only tests, a measured append latency
  test, the v0.11 HTTP smoke, CI integration coverage, and development docs.

## Qualification results

- `make check build`: passed.
- `go test -race ./...`: passed.
- `govulncheck ./...`: no vulnerabilities reachable by the code; one
  vulnerability exists in a required-but-not-called module, as reported by the
  tool.
- Fresh Postgres: migrations 001, 002, and 003 applied cleanly.
- Existing Postgres: restart applied migration 003 without changing the
  checksums of migrations 001 or 002; subsequent readiness passed.
- Postgres integration tests: passed, including 24 concurrent unique writers,
  16 concurrent duplicate deliveries, conflict rejection, deterministic
  replay/order, raw payload round-trip, monotonic checkpoints, rollback retry,
  and database-enforced append-only history.
- `make smoke`: baseline v0.10 smoke passed 16/16 and v0.11 event smoke passed.
- Existing v0.5, v0.6, v0.7, and v0.8 smokes: passed.

## Cost and storage impact

- No LLM, embedding, reranker, broker, or new service was introduced.
- A new task completion transaction performs a task row lock/read, one
  idempotency reservation when a key is supplied, one event insert/sequence
  allocation, the task update, and the existing session update. A duplicate
  delivery performs the reservation conflict check and reads the original
  event; it does not rewrite task/session state.
- The measured `EventStore.Append` test on the local Docker/Postgres stack was
  20 samples with p50 0.63 ms, p95 0.84 ms, and max 1.63 ms in the final run.
  Earlier runs ranged up to 2.12 ms p95 under local resource contention; both
  are below the feature-note target of 10 ms for local append overhead.
- The final local database held 31 event rows occupying 160 kB including table
  and indexes, with an average logical event row of approximately 509 bytes.
  Storage is payload-sensitive because raw bytes are intentionally retained.

## Remaining limitations

- Only task completion is integrated. Task creation/claim, coordination
  messages, memo writes, federation, and watcher ingestion remain
  current-state or compatibility paths.
- There is no public event read/replay API or projection worker yet; the store
  API is internal and checkpoints are not coupled to a subscriber runtime.
- The global sequence is an ordering cursor, not a gap-free commit counter;
  rolled-back sequence allocations can leave gaps by design.
- Workspace scope is explicit but the starter still uses one bearer key and
  does not provide tenant-aware identity/RBAC. Callers must not put secrets in
  raw event payloads.
- Large artifacts are referenced but there is no content-addressed artifact
  store in this slice. There is also no lease/fence model for task claims.

The next architectural seam is a checkpointed projection/subscription runtime
that consumes this history and incrementally builds typed control-plane views.
