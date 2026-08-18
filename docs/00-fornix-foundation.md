# Fornix foundation

Status: active architecture and development contract.

## Purpose

Fornix is an efficiency-first AI harness for long-running, multi-agent work.
The harness owns state, evidence, scheduling, policy, and cost controls. AI
models are used for interpretation, synthesis, and ambiguity—not for work that
SQL, exact lookup, or deterministic routing can perform.

The default retrieval path is:

```text
authoritative structured state
  → deterministic filtering and routing
  → bounded graph/provenance expansion
  → lexical retrieval
  → vector or learned retrieval only when justified
  → query-specific context under a hard budget
```

## Non-negotiable principles

- Preserve append-only raw events and artifacts. Derived summaries, indexes,
  embeddings, and graphs must remain rebuildable.
- Share typed state deltas and evidence references by default, not transcripts.
- Keep private working memory private and make shared state explicitly scoped.
- Treat the task graph as execution state and as durable project memory.
- Escalate retrieval by cost and measure quality, latency, database work, and
  model calls at every stage.
- Postgres is the initial authority. Additional infrastructure requires a
  measured bottleneck and an explicit architecture decision.

## Current implementation

- Go HTTP service backed by PostgreSQL and pgvector.
- Memo storage, full-text search, optional local embeddings, and code-symbol
  indexing.
- Session heartbeats, capability-based task claiming, coordination, and
  federation compatibility endpoints.
- Versioned event envelopes with workspace scope, actor/task/session refs,
  state deltas, artifacts, provenance, causation/correlation IDs, raw payloads,
  idempotency, replay, and durable checkpoints.
- A deterministic checkpointed projection runtime with a rebuildable task
  lifecycle view, protected by workspace-scoped consumer ownership leases and
  monotonic fencing tokens.
- A deterministic task execution state machine with workspace-scoped worker
  leases/fences, dependency-aware claims, retry budgets, cancellation, and
  dead-letter transitions. Lifecycle mutations append typed events atomically.

## Current gaps

- Workspace-aware identities, roles, tenant isolation, and scoped credentials.
- Typed event integration for every mutation path.
- Artifact storage, raw prompt/tool capture, and a general memory compiler.
- SQL-first retrieval planning, progressive gist/detail/raw disclosure, and
  hard total-token context compilation.
- Backups, restore drills, metrics, capacity benchmarks, and operational
  backpressure.

## Development order

1. Establish typed contracts, migrations, event history, and test harness.
2. Add checkpointed projections and deterministic task lifecycle state.
3. Add deterministic retrieval planning and bounded context compilation.
4. Add provenance graphs, selective unfolding, lifecycle consolidation, and
   optional learned routing behind measured evaluation.

Do not begin with a full autonomous swarm or a universal vector-search path.
The deterministic substrate must be observable and benchmarked first.

## Success metrics

Every optimization reports task quality, abstention correctness, coordination
tokens, injected context tokens, stage hit rates, avoided model calls, p50/p95
latency, query count, duplicate work, lease recovery, and evidence recovery.
