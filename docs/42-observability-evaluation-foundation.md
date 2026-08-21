# Observability, cost accounting, and replay evaluation foundation

Status: Task 15 design note — implementation follows this note.

## Objective

Add a durable, workspace-scoped accounting layer for the existing model,
tool, retrieval, agent, artifact, approval, retry, and scheduler paths. The
slice must make efficiency measurable without turning metrics into a second
source of truth: authoritative events, checkpoints, model calls, tool runs,
retrieval records, evidence, and artifacts remain the replay inputs.

Evaluation is offline and deterministic. It consumes recorded fake-provider
runs and recorded retrieval/tool outcomes; it never invokes a remote model or
external process during replay.

## Invariants

1. **Workspace authority.** Every observation, span, cost entry, metric sample,
   dataset, run, and result is keyed by `workspace_id`. Reads and writes carry
   that predicate and cross-workspace references fail closed.
2. **Idempotent accounting.** A durable accounting item is keyed by a caller
   supplied `(workspace_id, idempotency_key)`. Re-delivery returns the existing
   item when its canonical payload hash matches and fails closed on a hash
   conflict. Duplicate work is represented explicitly; it is not silently
   counted as new useful work.
3. **Bounded dimensions.** Metrics use a fixed allowlist of operation,
   component, outcome, provider, model, stage, and cost category dimensions.
   Request IDs, prompts, credentials, arbitrary metadata, and user text never
   become metric labels. Each dimension is normalized and length-limited.
4. **Trace safety.** Spans and observations preserve actor, causation,
   correlation, task, session, and source references but store only bounded,
   redacted evidence. Raw prompts and secret material are excluded from metric
   rows and reports.
5. **Cost truthfulness.** Measured provider usage is distinguished from
   deterministic estimates. Unknown provider usage or pricing remains unknown;
   a configured estimate is never presented as an invoice or measured usage.
   Model-call ledger values are the reconciliation authority for model usage.
6. **Append-only history.** Observations, spans, cost entries, and metric
   samples are append-only. Evaluation results are immutable per case/run;
   mutable run status is a bounded operational projection. No source event,
   checkpoint, model call, tool run, or artifact is overwritten.
7. **Replay boundary.** Evaluation folds recorded state and recorded response
   references only. It may hash and compare history, context, termination, and
   cost outcomes, but it never crosses the remote model or external-tool
   execution boundary.
8. **Deterministic gates.** Dataset case order, result order, hashes, scoring,
   abstention, cost budgets, and quality gates use canonical JSON and stable
   tie-breaking. Missing evidence or an invalid expected hash fails closed.
9. **Budgeted retention.** Observation/evaluation payloads are bounded inline;
   oversized reports use the existing content-addressed ArtifactStore. The
   slice reports retention/storage pressure but does not silently delete
   authoritative history.

## Schema changes

Migration `022_observability_evaluation.sql` adds:

- `run_observations` for bounded operation observations and source metadata;
- `trace_spans` for parent/child timing spans;
- `cost_ledger` for measured/estimated usage and deterministic cost entries;
- `metric_samples` for fixed-dimension durable samples;
- `eval_datasets` with bounded versioned case JSON;
- `eval_runs` and `eval_results` for immutable replay outcomes and run status.

All tables use workspace-scoped identity and idempotency constraints. JSON
payloads are bounded by contract and contain canonical hashes. Evaluation
cases reference recorded agent runs rather than embedding raw prompts in
reports. Large reports may be linked to the existing artifact plane.

## Reference scan and reuse decisions

Repositories/files searched:

- Orloj `telemetry/metrics.go`, `telemetry/spans.go`, `store/eval_stores.go`,
  `controllers/eval_run_controller.go`, `runtime/eval_scorer.go`, and eval
  resource contracts;
- ClawMem `src/eval/types.ts`, `metrics.ts`, `replay.ts`, `report.ts`,
  `gold.ts`, and `retrieval-gate.ts`;
- agentmemory `src/eval/schemas.ts`, `metrics-store.ts`, `quality.ts`,
  `replay/`, diagnostics, and evaluation runners;
- FornixDB `fornixdb/evals.py`, `budget.py`, and eval/budget tests;
- current Fornix model-call, retrieval, agent-run, tool-run, artifact, auth,
  and scheduler paths.

Closest behaviors:

- Orloj: fixed metric dimensions, separate duration/token counters, durable
  dataset/run lifecycle, and bounded scorer inputs;
- ClawMem and FornixDB: versioned gold evidence, hit/reciprocal-rank and
  abstention gates, unresolved-reference reporting, and no-observation-change
  eval replay;
- agentmemory: durable function metrics, timeline/replay separation, quality
  validators, and diagnostics that expose estimated versus observed behavior.

Adaptation:

- Reimplement the contracts and stores in Go with Postgres as the authority.
- Replace Prometheus/OTel collectors with bounded durable rows and a
  workspace-authorized read-only JSON metrics endpoint; this avoids new
  infrastructure and prevents high-cardinality labels.
- Evaluate recorded Fornix agent histories and durable retrieval/tool
  references instead of mirroring the live pipeline or re-running effects.
- Reconcile model cost from `model_calls`, then record attribution entries for
  tool, retrieval, artifact, retry, and duplicate work.

License/provenance:

- Orloj and agentmemory are Apache-2.0; ClawMem and FornixDB are MIT.
- The implementation is independent Go code; no reference source is copied.
- Kronaxis Fabric is BSL 1.1 and is not used as source code.
- Fornix remains MIT-licensed. Any future copied source must retain its
  upstream notice and attribution.

## Cost, storage, and latency budget

- One bounded observation and one cost entry are the normal upper bound per
  durable terminal operation; duplicate delivery reuses the idempotent row.
- Observation evidence is capped at 16 KiB, metadata at 32 fixed entries, and
  report/result inline JSON at 64 KiB before artifact linking.
- Metrics queries aggregate only fixed dimensions and bounded time windows;
  no request-path scan may be unbounded.
- Target local overhead is under 5 ms p95 for one accounting insert in a warm
  transaction, excluding the existing model/tool/retrieval work.
- Cost is calculated from measured model-call usage where present, otherwise a
  clearly marked deterministic estimate; missing pricing remains unknown.
- Evaluation default batch size is 100 cases, with a hard request cap of 1,000.
- Retention and report artifact operations reuse Task 14 bounded batch and
  content-addressed semantics.

## Acceptance tests

- Fresh and existing databases apply migration 022 without prior checksum
  changes.
- Duplicate observations and ledger entries return one durable row; payload
  hash conflicts fail closed.
- Model usage/cost entries reconcile with terminal model-call records, and
  measured and estimated usage remain distinguishable.
- Fixed dimensions reject raw prompts, credentials, request IDs, arbitrary
  user text, and excessive cardinality.
- Metrics reads are authenticated and workspace isolated.
- Recorded fake-provider runs replay to the same state/context/termination
  hash; replay never calls a remote provider or external tool.
- Expected context hashes, evidence references, termination reasons, cost
  budgets, and abstention gates evaluate deterministically.
- Dry-run evaluation writes no results; bounded runs are resumable and
  idempotent; oversized reports use ArtifactStore without raw prompt leakage.
- Retrieval quality is a separate deterministic read-only layer: gold evidence
  hashes resolve against authoritative workspace evidence, recorded ranked
  context is scored with hit@k/MRR/precision/recall/nDCG and rank drift, and
  baseline gates cover quality, context instability, latency, SQL work, and
  cost. Migration 023 adds only bounded quality fields to append-only result
  history.
- Concurrent inserts, crash rollback, duplicate delivery, missing usage,
  redaction, and workspace isolation are covered.
- Existing unit, integration, race, build, vet, CI, and smoke checks remain
  green. Qualification records latency, SQL work, storage, attribution
  accuracy, replay throughput, evaluation throughput, and limitations.
