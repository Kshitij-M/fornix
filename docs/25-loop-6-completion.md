# Loop 6 completion: deterministic retrieval and bounded context

Status: complete
Date: 2026-08-18

## Delivered

- Added typed `RetrievalRequest`, `RetrievalPlan`, `RetrievalTrace`,
  `ContextItem`, and `ContextPack` contracts with workspace scope, evidence
  hashes, provenance, hard budgets, optional caller-supplied embeddings, and
  deterministic request/plan identity.
- Added migration `009_retrieval_workspace_scope.sql`. Existing memo, chunk,
  and symbol rows are backfilled to `default`; uniqueness and indexes are now
  workspace-local. Symbol graph expansion requires both endpoints to belong to
  the requested workspace.
- Added `internal/retrieval` as a read-only repeatable-read Postgres runtime.
  Its ordered stages are structured SQL, PostgreSQL lexical search, bounded
  one-hop symbol graph/provenance expansion, and vector search only when a
  caller supplies a validated 768-dimensional embedding and earlier evidence
  does not satisfy the request.
- Added deterministic score clamping, stage-priority tie-breaking, evidence
  deduplication, stable provenance merging, explicit skip/failure trace states,
  UTF-8-safe truncation, abstention, and stable context hashes.
- Added `POST /v1/retrieve` without adding a broker, cache, model call, or
  second authority. Existing memo, chunk/RAG, symbol, and graph endpoints now
  bind their source rows to the request workspace.
- Added unit and Postgres tests for plans, embedding validation, duplicate
  delivery, concurrent readers, replay-stable ordering/hashes, hard budgets,
  truncation, abstention, graph expansion, vector gating, provenance, and
  workspace isolation.
- Added v0.15 smoke coverage, Makefile/development commands, CI integration,
  and architecture/qualification documentation.

## Qualification

- Fresh database: a temporary empty Postgres database applied migrations 001
  through 009 and passed the workspace-scoped retrieval test; the temporary
  database was removed after the check.
- Existing database: all migrations remained checksum-valid and the full Go
  suite passed against the development Postgres instance.
- `make vet`, `make build`, `make python-check`, and `git diff --check` passed.
- The full smoke chain v0.10 through v0.15 passed. The v0.15 HTTP smoke
  confirmed repeated requests returned the same context hash and that a
  workspace could not retrieve the other workspace's memo.
- Isolated warm local Postgres measurement, 20 retrievals over 10 memos: p50
  `1.622 ms`, p95 `2.025 ms`, max `6.642 ms`, and average `3.00` SQL queries
  per request. During the complete smoke chain, the same test measured p50
  `2.040 ms`, p95 `14.656 ms`, and max `21.218 ms` while other integration
  tests were using the database. The lexical path used three bounded source
  queries; graph adds one bounded query and a justified vector path uses three
  bounded vector queries.
- The full `fornix.memos` relation occupied `1,531,904 B` in the development
  database at measurement time. The 10-row test workspace contributed about
  `1,070 B` of logical title/content/hash data; relation size includes indexes,
  page overhead, and unrelated workspaces.

## Database work and cost

Retrieval opens one read-only `REPEATABLE READ` transaction and performs only
bounded indexed SQL reads. A satisfied structured/lexical request skips graph
and vector work. No LLM or embedding provider is called by this path; vector
cost is explicit in the caller's embedding payload and the existing pgvector
storage/index footprint. Context compilation is in-process and enforces item,
byte, and conservative model-independent token budgets before returning.

The migration adds three workspace columns, workspace-local uniqueness, and
workspace-first retrieval indexes. It does not duplicate raw content or
replace authoritative source rows with summaries or embeddings.

## Remaining limitations

- Retrieval source representations are currently memo, chunk, symbol, and raw
  event renderings. A general gist/detail/raw artifact table and object-backed
  large-output disclosure are still future memory-plane work.
- Graph expansion is one hop over the existing code-symbol edge table. General
  temporal/causal memory graphs, stale-edge lifecycle, and multi-hop policies
  need a separate schema and evaluation task.
- Vector quality depends on caller choice of embedding model; Fornix currently
  validates dimension and cost gating but does not evaluate semantic quality.
- The endpoint is pull-based and authentication is still the existing shared
  bearer key. Tenant identities, scoped credentials, backups, capacity
  benchmarks, and operational backpressure remain production gaps.
