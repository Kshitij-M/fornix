# Deterministic retrieval and bounded context foundation

Status: complete
Date: 2026-08-18

## Purpose

This slice adds the first production-shaped retrieval boundary to Fornix. It
turns a workspace-scoped request into a deterministic, observable plan and a
bounded context pack while keeping PostgreSQL the only authority. Retrieval
reads a repeatable-read snapshot of existing memos, chunks, symbols, symbol
edges, tasks, and control events. It never mutates those authoritative rows,
replaces source content with an embedding, or treats a projection as truth.

The default escalation path is:

```text
structured SQL → lexical SQL → bounded symbol/provenance expansion →
caller-supplied vector search only when earlier stages are insufficient
```

The vector stage does not invoke Ollama or an LLM. A caller must explicitly
provide a validated embedding, and the planner must observe measurable need
before the stage runs.

## Retrieval invariants

1. Every request has a non-empty workspace scope. Every stage binds that scope
   in SQL; a result from another workspace is not eligible for deduplication or
   compilation.
2. Plans, stage order, score formulas, candidate limits, normalization, and
   tie-breaking are deterministic functions of the request and fixed versioned
   policy. Wall-clock timings are trace metadata and never affect ordering or
   the context hash.
3. Results carry a typed source reference and evidence hash. The hash is over
   the authoritative source representation, not over a truncated context
   rendering. Context truncation therefore cannot make evidence unverifiable.
4. Duplicate source/evidence delivery has one compiled effect. The highest
   deterministic score wins; equal-score ties use source kind and source
   reference. Provenance is merged in stable order.
5. Hard item, byte, and token limits are enforced by the compiler. A result
   that cannot fit is truncated only at a valid UTF-8 boundary, marked as
   truncated, and remains linked to its original evidence. If no non-empty
   representation fits, the item is abstained.
6. A context pack is allowed to abstain. Empty or low-confidence retrieval is
   represented explicitly and is safer than inventing a summary or relaxing a
   caller's budget.
7. Structured and lexical stages may satisfy a request early. Graph and vector
   stages are skipped when the target result count/confidence or a hard budget
   is already satisfied. A missing embedding, disabled stage, or no graph
   anchor is an explicit skip reason.
8. The context hash is calculated from the normalized request identity and the
   ordered compiled item content, source references, evidence hashes, scores,
   and provenance. It excludes timings, database query counts, and other
   operational noise.

## Contracts and API

`internal/contracts/retrieval.go` defines `RetrievalRequest`, `RetrievalPlan`,
`RetrievalTrace`, `ContextItem`, and `ContextPack`. Requests contain workspace
scope, query, exact source references and optional structured filters, hard
budgets, confidence/target gates, graph/vector switches, and an optional
caller-supplied embedding. Items contain source kind/reference, evidence hash,
representation, score, provenance, original size, and truncation state.

The HTTP slice is `POST /v1/retrieve`. It is a read-only boundary and returns
the plan, trace, context pack, and stable content hash in one response. The
existing memo, RAG, and symbol endpoints remain available; the new endpoint is
the first consumer of one unified staged retrieval implementation.

## Schema changes

Migration `009_retrieval_workspace_scope.sql` adds an explicit
`workspace_id` to memos, chunks, and symbols, backfills existing rows to
`default`, replaces global content/identity uniqueness with workspace-local
uniqueness, and adds workspace-first indexes. Symbol edges remain keyed by
symbol IDs; graph queries join both endpoints and require the same requested
workspace, preserving existing edge identity while preventing cross-workspace
expansion.

Existing ingestion and read handlers bind the default/request workspace to
these columns. Existing rows are not rewritten beyond the additive workspace
backfill and constraint/index migration. Events, task rows, and projections
are already workspace-scoped and are queried through their existing boundaries.

## Reference scan and reuse decision

Repositories/files searched:

- `reference_repos/ClawMem/src/retrieval-gate.ts`, `graph-traversal.ts`,
  `search-utils.ts`, and replay/evaluation code.
- `reference_repos/agentmemory/src/state/hybrid-search.ts`,
  `state/search-index.ts`, `functions/graph-retrieval.ts`,
  `functions/diagnostics.ts`, and checkpoint/replay functions.
- `reference_repos/fornixdb/fornixdb/tiers.py`, `core.py`, `tokens.py`,
  provenance/lineage and budget tests.
- Fornix's current `internal/server/server.go`, `internal/server/rag.go`,
  memo/chunk/symbol schema, event store, and workspace contracts.

Closest implementations:

- ClawMem provides retrieval/noise gates and bounded beam/Dijkstra-style graph
  traversal with eligibility predicates.
- agentmemory provides hybrid stream diagnostics, stable RRF tie handling,
  graph expansion, and evaluation-oriented telemetry.
- FornixDB provides gist/detail/raw disclosure, source references, retention
  awareness, and tests that treat context cost as a measurable budget.

Behavior copied or intentionally not copied:

- Reimplemented independently: explicit stage escalation, SQL eligibility,
  stable source ordering, evidence-preserving truncation, and hard budgets.
- Intentionally not copied: SQLite/in-memory authority, eager all-stream RRF,
  LLM reranking, model calls on the retrieval hot path, automatic summaries,
  and unbounded graph traversal. Those choices conflict with Fornix's
  Postgres-only, deterministic, cost-first control plane.

License/provenance action:

- No reference source code is copied. Fornix remains MIT. ClawMem and FornixDB
  are MIT; agentmemory is Apache-2.0. Their architecture is treated as
  reference material only. Kronaxis/BSL source is not used or introduced.

Deterministic fallback:

- Exact structured SQL and PostgreSQL FTS are always preferred. Graph expands
  only from bounded symbol anchors. Vector search is disabled without an
  explicit embedding or when earlier evidence meets the gate. Failure of an
  optional stage leaves prior deterministic results and trace diagnostics
  intact; it does not silently broaden scope.

## Cost and database budget

The initial policy defaults to at most 20 context items, 32 KiB of compiled
content, and 8,192 estimated tokens, with bounded per-stage candidate windows.
The production target is p95 below 25 ms for a warm structured/lexical query
and below 75 ms for a justified graph/vector query on the local development
Postgres instance. A successful request uses one repeatable-read transaction;
each enabled stage uses bounded indexed SQL statements, and no model call is
made by this slice.

Storage impact is limited to three workspace columns, workspace-local unique
indexes, and workspace-first retrieval indexes. No new table, broker, cache,
vector store, or raw evidence copy is introduced. The main variable cost is
the existing pgvector index/table footprint when callers choose vector
retrieval; trace counters report stage usage, candidates, database queries,
compiled bytes/tokens, truncation, abstention, and vector skips.

## Acceptance tests

- Fresh and existing databases apply migration 009 cleanly and preserve all
  prior migration checksums.
- Identical requests over an unchanged snapshot produce the same normalized
  plan, stage ordering, result ordering, and context hash.
- Structured/lexical satisfaction skips graph and vector work; vector is also
  skipped without a caller embedding or when disabled.
- Hard item, byte, and token budgets are never exceeded; truncation is stable
  and evidence hashes remain those of the full authoritative source.
- Duplicate candidates from multiple stages produce one item with stable
  provenance and no duplicate context effect.
- Graph expansion is bounded, deterministic, provenance-linked, and cannot
  cross workspace boundaries.
- A vector or optional-stage SQL failure does not discard earlier deterministic
  results and is visible in the trace.
- Workspace A cannot retrieve, deduplicate, or compile workspace B records.
- Existing memo, RAG, symbol, task, projection, lease, smoke, race, vet, build,
  and Python checks remain green.
- Integration tests report p50/p95 latency, SQL query/stage counts, compiled
  storage/byte impact, vector gate rate, and remaining limitations.

## Remaining limitations after this slice

- Source rows still have no general gist/detail/raw artifact table; this slice
  exposes stable representations over existing authoritative text and leaves
  richer disclosure/retention for a later memory-plane task.
- Vector quality depends on a caller-provided embedding model and dimension;
  Fornix does not yet evaluate model quality or provide a routing policy.
- Graph expansion is a bounded one-hop symbol-edge/provenance slice, not a
  general temporal knowledge graph.
- Retrieval is a pull API. Background context subscriptions, cache invalidation,
  identity/authorization scopes, and large-artifact object storage remain
  separate production work.
