# Fornix

Fornix is an efficiency-first AI harness for large, long-running projects.
It combines deterministic control-plane state, evidence-preserving event
history, checkpointed projections, task coordination, and cost-aware retrieval
behind one Go/Postgres service.

The design principle is simple: use exact structured state and deterministic
routing first; spend model tokens only when ambiguity requires reasoning.

## Current capabilities

- Go HTTP service backed by PostgreSQL and pgvector.
- Memo storage, full-text search, optional local embeddings, and code-symbol
  indexing.
- Session heartbeats, capability-based task claiming, coordination, and
  federation compatibility endpoints.
- Typed event envelopes, state deltas, provenance, artifact references,
  idempotency, raw payload retention, and monotonic checkpoints.
- Deterministic checkpointed projection runtime with rebuildable task state,
  workspace-scoped consumer leases, and monotonic fencing.
- Workspace-scoped task execution leases with monotonic fencing, dependency-
  aware claims, transactional renewal/completion/failure/cancellation, bounded
  retries, and dead-letter transitions.
- Deterministic `/v1/retrieve` planning over structured SQL, PostgreSQL FTS,
  bounded symbol/provenance expansion, and gated caller-supplied pgvector;
  context packs have evidence hashes, stable content hashes, and hard budgets.
- Docker-backed development environment, CI, integration tests, and smoke
  tests.

The service is an alpha single-node control and retrieval substrate. Identity,
tenant-aware authorization, public replay APIs, artifact storage, and
production operations remain on the roadmap.

## Quickstart

```sh
cp .env.example .env
make dev-up
make dev-run
curl http://localhost:8201/readyz
```

For the complete development workflow, read [`DEVELOPMENT.md`](DEVELOPMENT.md)
and [`AGENTS.md`](AGENTS.md). The architecture and qualification records live
in [`docs/`](docs/).

## Repository layout

```text
cmd/fornix/                 service entrypoint
cmd/fornix-watcher/         filesystem watcher and code indexer loop
internal/contracts/         typed control-plane contracts
internal/projection/        deterministic subscribers and derived views
internal/server/            HTTP handlers
internal/store/             Postgres authority and embedded migrations
scripts/                    import, indexing, MCP, and smoke helpers
docs/                       architecture, reference, and qualification notes
```

## License

Copyright (c) 2026 Kshitij Mohan.

Fornix is released under the [MIT License](LICENSE).
