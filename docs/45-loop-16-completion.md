# Loop 16 completion — retrieval quality and regression gates

Status: implemented and locally qualified on 2026-08-20.

## Delivered

- Added the Task 16 feature note with reference reuse, licensing, invariants,
  failure semantics, budgets, and acceptance tests.
- Added additive migration `023_retrieval_evaluation_quality.sql`. It extends
  existing append-only evaluation rows with bounded retrieval metrics,
  resolved evidence hashes, baseline run identity, aggregate quality, and
  regression findings. Fresh and already-migrated databases converge through
  the existing checksum-validated migration runner.
- Added authority-backed `ResolveEvidenceHashes`. Resolution is workspace
  scoped, deduplicated, integrity verified against raw/artifact bytes, and
  rejects missing, superseded, contradictory, or cross-workspace evidence.
- Added deterministic scoring for hit@k, reciprocal rank, precision@k,
  recall@k, binary nDCG@k, rank drift, context-hash match, abstention
  correctness, latency, SQL work, and measured/estimated/unknown cost.
- Added deterministic per-metric gates and baseline regression comparisons for
  quality degradation, rank drift, context instability, latency, and cost.
- Added a recorded-surface runner. It sorts cases, uses no model/provider/tool
  calls, supports bounded dry runs, writes idempotent result rows, and stores
  bounded aggregate/report data through the existing artifact-aware evaluator.
- Added pure, integration, workspace-isolation, duplicate, persistence,
  truncation/abstention, invalid-evidence, and regression tests, plus the
  `v0.25-retrieval-quality-smokes.sh` command, Make target, CI coverage, and
  architecture updates.

## Qualification results

| Check | Result |
|---|---|
| Existing Postgres migration path | Pass; migration 023 applied after 022 |
| Focused pure scorer tests | Pass |
| Focused Postgres resolution/persistence tests | Pass |
| Existing store and eval integration suites | Pass |
| Existing Go package compilation | Pass |
| `make check` (tests, vet, Python checks) | Pass |
| `make smoke` v0.10–v0.25 | Pass |
| Shell syntax for all smoke scripts | Pass |

## Local measurements

- The focused Postgres retrieval-quality package completed in approximately
  **0.25–0.30 s** after the container and module caches were warm; this includes
  migration checking, evidence inserts, resolution, dry-run scoring, durable
  result persistence, and duplicate replay.
- One case performs one indexed workspace/hash evidence query plus one
  idempotent result insert; aggregate comparison is in-memory. The resolver
  reads raw bytes only for integrity verification and does not invoke retrieval
  stages or external systems.
- Migration 023 adds three JSONB fields to `eval_results`, three fields to
  `eval_runs`, and one partial baseline index. Storage grows with bounded
  metric/reference/regression JSON, not raw prompts or context text.
- Pure scoring is O(cases × (gold references + ranked evidence)) with bounded
  `MaxEvalCases` and retrieval-k limits. A populated-corpus capacity benchmark
  remains outstanding.
- Docker-backed smoke scripts translate host-loopback Postgres DSNs to
  `host.docker.internal`, keeping local container execution and CI aligned
  without adding infrastructure.

## Remaining limitations

- The public HTTP/CLI evaluation surface and automatic durable capture of every
  retrieval pack are not implemented; callers provide recorded context packs
  to the internal runner.
- Relevance is binary evidence-hash relevance. Graded labels, answer quality,
  a full rank-by-case baseline history, and learned judging are future work.
- Cost and latency accuracy remain bounded by the observations supplied by the
  caller. Unknown provider usage remains explicitly unknown.
- Postgres remains the only authority; no cache, broker, collector, vector
  service, model provider, or external tool is added by this task.
