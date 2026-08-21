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
- Immutable workspace-scoped evidence records with computed raw hashes,
  typed provenance edges, supersession/contradiction history, and bounded
  deterministic gist/detail/raw disclosure.
- Typed model gateway with explicit provider registration, deterministic fake
  and Ollama providers, opt-in OpenAI-compatible chat, bounded retries and
  budgets, pre-content fallback, redacted evidence, and a durable model-call
  ledger.
- Deterministic `/v1/tools/execute` with explicit structured-argv tool
  registration, deny-by-default policy, durable approval decisions, bounded
  local execution, task fencing, idempotent tool-run records, and typed
  lifecycle events.
- Deterministic `/v1/agent/run` orchestration with durable context compilation,
  model/tool ordering, hard budgets, approval/retry/cancellation/external
  waits, task fencing, idempotent transitions, and replayable checkpoints.
- Postgres-backed agent-run scheduling with deterministic due-queue ordering,
  workspace/run worker leases, monotonic takeover fences, heartbeats, atomic
  lease-renewed checkpoints, cancellation propagation, and crash recovery.
- Workspace-scoped identities, deterministic RBAC, fail-closed capability
  admission, hashed/expirable/revocable/rotatable API keys, non-secret
  credential references, append-only authorization audit, and authenticated
  actor propagation.
- Durable workspace-scoped observations, trace spans, cost attribution, fixed
  dimension metrics, and offline replay evaluation. `GET
  /v1/observability/metrics` is a bounded authorized snapshot; evaluation
  replay never calls remote providers or external tools.
- Append-only workspace-scoped retrieval-surface capture on normal retrieval,
  authenticated operator APIs for dataset/surface/evaluation lifecycle, and a
  deterministic offline `fornix-eval` CLI. Dry runs and CLI replay never call
  models, tools, brokers, or external systems; oversized durable reports use
  the existing artifact plane.
- Deterministic operator CLI/API/MCP surfaces for workspace bootstrap, identity,
  role and API-key lifecycle, bounded ingest bookkeeping, task/run inspection,
  evaluation, metrics, artifact/evidence disclosure, and a complete fake-model
  reference workflow.
- Durable workspace-scoped repository ingestion with deterministic mounted-root
  discovery, ignore/path/symlink safety, manifest identity, bounded checkpointed
  batches, chunk/symbol lineage, source supersession/removal history, optional
  embeddings, and resumable CLI/API/MCP operations.
- Docker-backed development environment, CI, integration tests, and smoke
  tests.

The service is an alpha single-node control and retrieval substrate. OAuth/SSO,
external KMS/secret-manager integration,
general projection replay APIs, external artifact storage, and production
operations remain on the roadmap. Postgres-backed artifact storage,
artifact-backed output integration, durable observability/evaluation, and
retrieval-surface operator evaluation are available in the current slice.
Local
compatibility authentication is explicit:
`FORNIX_AUTH_MODE=development`; production uses `FORNIX_AUTH_MODE=workspace`.

The operator bootstrap path uses `FORNIX_BOOTSTRAP_KEY` only for the narrow
`POST /v1/operator/workspaces/bootstrap` route. It is compared in memory and
never stored or printed by the reference workflow. Run the complete local path
with `make smoke-reference-workflow`; OpenAI remains an explicit opt-in smoke
through `FORNIX_OPENAI_API_KEY` and is never written to a file or log.

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
internal/model/             provider registry, gateway, and provider adapters
internal/tool/              tool registry, policy, approvals, and executor
internal/scheduler/         durable agent-run pull worker and recovery loop
internal/projection/        deterministic subscribers and derived views
internal/server/            HTTP handlers
internal/store/             Postgres authority and embedded migrations
scripts/                    import, indexing, MCP, and smoke helpers
docs/                       architecture, reference, and qualification notes
```

## License

Copyright (c) 2026 Kshitij Mohan.

Fornix is released under the [MIT License](LICENSE).
