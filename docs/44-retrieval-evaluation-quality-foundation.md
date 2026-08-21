# Retrieval evaluation quality foundation

Status: active Task 16 design and implementation contract.

## Objective

Make retrieval quality measurable without allowing evaluation to become a
second authority, an external-effect path, or a source of prompt leakage.
Evaluation consumes immutable Fornix evidence records, recorded context
packs/traces, and durable observations. It never calls a provider, executes a
tool, mutates retrieval history, or treats telemetry as gold truth.

## Reference scan and reuse decision

Repositories/files searched:

- `reference_repos/ClawMem/src/eval/{types,gold,metrics,replay,report}.ts`
- `reference_repos/ClawMem/src/retrieval-gate.ts` and evaluation tests
- `reference_repos/fornixdb/fornixdb/evals.py`, `budget.py`, and eval tests
- `reference_repos/agentmemory/src/eval/{schemas,metrics-store,quality,validator}.ts`
- `reference_repos/agentmemory/src/functions/{replay,diagnostics}.ts` and
  `eval/runner/score.ts`
- `reference_repos/orloj/resources/eval_types.go`, `store/eval_stores.go`,
  `runtime/eval_scorer.go`, eval controller, telemetry, and eval migration

Closest implementations and adaptation:

- ClawMem's strict parsing/resolution is adapted as fail-closed authoritative
  evidence resolution: any missing, stale, contradictory, or cross-workspace
  gold reference makes the case untrusted rather than inflating recall.
- FornixDB's hit@k/MRR, explicit abstention cases, and rank-drift warning are
  adapted to immutable evidence hashes. Its local disk-budget machinery is
  adapted as hard evaluation batch/report budgets, not as a new storage layer.
- agentmemory's adapter-independent score rows and replay boundary are adapted
  as pure Go scoring functions over recorded ranked evidence. No provider,
  tool, or learned judge is introduced.
- Orloj's durable dataset/run/result lifecycle and metric vocabulary are kept;
  Fornix stores quality metrics in the existing append-only eval result and
  preserves Postgres as authority.

No source code is copied. The referenced projects are Apache-2.0 (Orloj and
agentmemory) or MIT (ClawMem and FornixDB); Fornix remains MIT and needs no
third-party source attribution beyond the existing reference matrix.

## Invariants

- Every dataset, run, case, evidence resolution, result, and comparison is
  workspace-scoped. A reference is never resolved outside the requested
  workspace.
- Gold evidence is identified by immutable evidence hash. A hash must resolve
  to exactly one integrity-verified authoritative evidence record in the same
  workspace. Missing, stale, duplicate-conflicting, superseded, or contradicted
  records fail closed; historical rows are not rewritten.
- Observed ranking is the ordered, deduplicated list of evidence hashes in the
  recorded context pack. Ties are already made deterministic by the retrieval
  compiler; the scorer applies a stable hash/source-reference tie-break if a
  caller supplies an unsorted list.
- `hit@k` is the fraction of cases with at least one gold hash in the first
  `k` distinct results. Reciprocal rank is the inverse rank of the first gold
  hash. Precision@k uses `min(k, distinct retrieved count)` as its denominator;
  recall uses the number of resolved gold hashes. Binary nDCG@k uses the
  standard `1/log2(rank+1)` discount and an ideal list of all gold hashes.
- Rank drift compares the first relevant rank with a prior run. A missing
  relevant item is assigned `k+1`; the normalized drift is the mean absolute
  rank delta divided by `max(k,1)`. This is a warning/error signal separate
  from hit@k so silent rank degradation is visible.
- Context-hash match, abstention correctness, latency, SQL work, and cost are
  measured independently. Unknown cost/usage is never treated as zero or
  exact. A configured gate requiring an unknown value fails closed.
- Metrics, regressions, reports, and gate decisions are deterministic functions
  of canonical inputs. Case and reference ordering is sorted by stable IDs and
  hashes; arbitrary map iteration and wall-clock values are excluded from
  hashes.
- Evaluation is read-only with respect to remote systems and authoritative
  retrieval/evidence history. Oversized reports may use the existing
  transactional artifact path only during durable run finalization.

## Contracts and schema

Task 16 extends `EvalResult` with a bounded retrieval-quality payload,
resolved evidence hashes, and regression findings. `EvalRun` records the
optional baseline run used for comparison and its aggregate regression gates.
Migration `023_retrieval_evaluation_quality.sql` is additive and uses
`IF NOT EXISTS`/defaults so fresh and already-migrated databases converge.
The existing append-only trigger protects the new history fields through the
existing eval-result row; no authoritative event or evidence row is replaced.

## Gate policy and budgets

The scorer supports deterministic `>=`, `<=`, `==`, `>`, and `<` gates for
quality, context stability, rank drift, latency, SQL work, and cost. A baseline
comparison can require minimum quality, maximum relative cost/latency increase,
and maximum context-hash instability. Batch size is capped by the existing
`MaxEvalCases`; per-result metric JSON and report output remain bounded by the
existing inline/artifact thresholds.

## Database work and cost budget

Gold resolution is one indexed workspace/hash lookup per distinct reference,
with stable ordering and no graph traversal unless the caller separately asks
for it. Scoring is in-memory O(cases × (gold + ranked)) and allocates only
bounded slices. Persistence adds one idempotent eval-result write per case and
one run update; no model or vector query is required. The target is under 2 ms
of CPU for a 100-case, 20-result offline batch excluding database round trips,
and no additional storage for duplicate results.

## Acceptance tests

- Exact evidence resolution succeeds only for one healthy same-workspace hash;
  missing, stale, contradictory, duplicate-conflicting, and cross-workspace
  references fail closed.
- Identical ranked inputs produce identical hit@k, MRR, precision, recall,
  nDCG, rank-drift, abstention, regression, report, and result hashes.
- Duplicate retrieved hashes do not inflate metrics; deterministic ties do not
  change ranks.
- Context hash and abstention gates are stable and truncation remains visible.
- Cost/latency/SQL regressions fail at configured thresholds, while unknown
  cost is distinguishable from measured or estimated cost.
- Replays perform no model/tool calls; duplicate result writes are idempotent.
- Concurrent workspace-scoped scoring cannot read or persist another
  workspace's evidence or evaluation results.
- Existing Go tests, race checks, builds, CI, and smoke checks remain green.

## Remaining limitations

This slice scores recorded retrieval surfaces; it does not yet launch a full
retrieval query for each dataset case or provide a public eval CLI. It uses
binary evidence relevance, not graded labels, and rank drift needs a prior
recorded run. Provider-specific latency/cost attribution remains dependent on
the observations already persisted by the caller.
