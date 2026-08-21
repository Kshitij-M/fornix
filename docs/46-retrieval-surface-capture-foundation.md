# Retrieval surface capture and operator evaluation foundation

Status: Task 17 design and implementation contract.

## Objective

Make normal retrieval requests produce a durable, replayable evaluation
surface, then expose bounded operator controls for dataset registration,
recorded retrieval evaluation, baseline comparison, status, and reports. The
capture is a measurement sidecar: PostgreSQL evidence, memo, chunk, symbol,
event, and artifact rows remain authoritative. Evaluation consumes recorded
surfaces and never invokes a model, embedding provider, tool, broker, or
external system.

## Reference scan and reuse decisions

Repositories/files studied:

- ClawMem `src/eval/{gold,metrics,replay,report,run}.ts`, retrieval gate,
  evaluation guide, acceptance script, and eval tests;
- FornixDB `fornixdb/evals.py`, `budget.py`, and evaluation/budget tests;
- agentmemory evaluation runner/types/scoring, replay timeline, diagnostics,
  schemas, and metrics store;
- Orloj evaluation resource contracts, Postgres stores, API/CLI handlers,
  evaluation controller, scorer, telemetry, and tests;
- current Fornix retrieval planner/store/compiler, evidence resolver,
  evaluation/artifact stores, authorization middleware, observability, and
  HTTP routes.

Adaptation:

- Reuse ClawMem's strict recorded-surface boundary and no-contamination replay
  rule. Fornix records only stable identity and evidence references; raw query
  content is excluded from the capture row and report.
- Reuse FornixDB's bounded evaluation/budget model and explicit offline run
  output, but keep Postgres as the authority rather than adding a file-backed
  database or a second index.
- Reuse agentmemory's adapter-independent replay inputs and deterministic
  report ordering. The operator API reads already-recorded surfaces rather
  than re-running retrieval.
- Reuse Orloj's dataset/run lifecycle, cursor-bounded reads, API/CLI shape,
  and durable status/report behavior, narrowed to Fornix's workspace/RBAC and
  no-external-effect contract.

No reference source is copied. Orloj and agentmemory are Apache-2.0; ClawMem
and FornixDB are MIT. Kronaxis Fabric is BSL 1.1 and is not used as source.
Fornix remains MIT.

## Invariants

1. Every surface is workspace-scoped and carries authenticated actor,
   causation, and correlation references. Reads always bind workspace in SQL.
2. A surface contains request hash, request/operation identity, plan hash,
   context hash, selected source/evidence references, budget, trace, timing,
   SQL work, and cost classification. It never contains query text,
   embeddings, prompt text, credentials, or arbitrary user metadata.
3. Surface rows are append-only. `(workspace_id, idempotency_key)` is the
   duplicate-delivery boundary. A same-key/different-payload submission fails
   closed; a same-key/same-payload submission returns the existing row.
4. Automatic capture is installed at the retrieval store boundary so HTTP and
   agent-loop retrievals use the same path. Capture is committed after the
   read-only retrieval snapshot and does not mutate retrieval authority.
5. Evaluation resolves every gold and observed evidence hash through the
   existing authoritative EvidenceStore. Missing, stale, contradictory,
   corrupt, or cross-workspace references fail closed.
6. Surface and case order is sorted by stable IDs. Pagination uses a bounded
   `(captured_at,id)` cursor; arbitrary map iteration and wall-clock values do
   not affect replay/result hashes.
7. Dry-run evaluation performs dataset/surface/evidence reads and pure scoring
   only. It creates no evaluation run, result, artifact, observation, or cost
   row.
8. Durable evaluation uses the existing idempotent EvaluationStore and
   artifact-backed `FinishRun`; oversized reports retain their hash and
   provenance through ArtifactStore without replacing result history.
9. Operator endpoints inherit authenticated workspace RBAC. Dataset creation,
   run execution, surface registration, and status/report reads use explicit
   permissions and never trust a body-supplied actor or workspace.

## Schema

Migration `024_retrieval_surface_capture.sql` adds `retrieval_surfaces` with:

- stable surface/request/idempotency identity and payload hash;
- workspace, actor, causation, and correlation metadata;
- request hash, plan hash, context hash, and bounded retrieval budget;
- trace JSON containing stage statuses/counters/timings but no raw text;
- ordered selected evidence/source references with score, stage, and truncation
  flags;
- duration, SQL query count, cost amount/known/estimated flags, and capture
  timestamp;
- append-only trigger, workspace/time/hash indexes, and bounded JSON checks.

The migration is additive and checksum-validated. It does not alter existing
retrieval/evidence/evaluation authority or rewrite prior history.

## API and CLI

The authenticated operator API supports:

- registering a versioned `EvalDataset`;
- running bounded recorded retrieval evaluation, with dry-run and optional
  baseline run comparison;
- reading an evaluation run/status/report;
- listing captured surfaces with workspace scope, bounded limit, and cursor;
- explicitly registering a pre-recorded surface for offline/import workflows.

The deterministic `fornix-eval` CLI accepts a redacted recorded JSON bundle,
uses the pure scorer, emits canonical JSON, and never contacts a model,
retriever, tool, or remote service. The production HTTP path remains the
authority-resolving path.

## Cost and storage budget

- Normal retrieval adds one bounded surface insert after the existing read
  snapshot, plus the already-existing observability observation/cost writes.
- Surface JSON is capped at 64 KiB; evidence references are bounded by the
  retrieval item limit. No raw query or context text is duplicated.
- Surface listing is index-backed and limited to 100 rows per request. API
  evaluation is capped by `MaxEvalCases` and run batch limits.
- Durable evaluation adds one bounded indexed surface batch read, one
  authoritative evidence-resolution read, one idempotent result write per
  case, and one terminal run update. Oversized reports use one
  content-addressed artifact transaction.
- Target warm-path capture overhead is under 5 ms p95 excluding the existing
  retrieval SQL work. The implementation reports capture latency, SQL work,
  surface bytes, evaluation throughput, and report artifact usage.

## Acceptance tests

- Normal retrieval automatically records one surface with stable hashes,
  ordered evidence references, actor/workspace scope, budgets, trace,
  latency, SQL work, and cost metadata.
- Duplicate capture is idempotent; conflicting reuse fails closed; concurrent
  writers produce one row.
- Surface payloads and reports contain no query text, embeddings, prompts,
  credentials, or arbitrary user text.
- Cross-workspace get/list/register/evaluate operations fail closed.
- Cursor pagination is bounded, stable, and cannot skip or duplicate rows.
- Dataset registration is deterministic and idempotent; conflicting versions
  fail closed.
- Dry-run evaluation creates no durable run, result, artifact, observation, or
  cost rows.
- Durable evaluation resolves surfaces and evidence deterministically,
  preserves baseline gates, and stores oversized reports as artifacts.
- Duplicate evaluation requests replay the same run/results; crash before
  commit leaves no partial authoritative run/result/surface effect.
- HTTP authorization rejects unauthenticated, cross-workspace, and missing
  evaluation capabilities; actor identity is preserved in durable rows.
- Offline CLI output is stable for identical bundles and performs no external
  effects.
- Existing unit, integration, race, build, vet, CI, and smoke checks remain
  green.
