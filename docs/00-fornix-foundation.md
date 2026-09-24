# Fornix foundation

Status: active architecture and development contract.

## Purpose

Fornix is an efficiency-first AI work control plane for long-running,
multi-agent production-system operations. The control plane owns state,
evidence, scheduling, policy, and cost controls across typed adapters. AI
models are used for interpretation, synthesis, and ambiguity—not for work that
SQL, exact lookup, or deterministic routing can perform.

## Product definition

The technical foundation serves a product outcome:

> **Fornix is verifiable AI work infrastructure for long-running
> production-system operations.**

Teams should be able to delegate serious production-system operations to AI
without losing control of scope, cost, evidence, approval, or recovery. The
first product wedge is safe autonomous repository maintenance—dependency
upgrades, security remediation, migrations, refactors, CI repair, and related
work that teams currently keep manual because the result is difficult to verify
or recover. The same control plane is intended to support databases, APIs,
ticketing systems, cloud resources, and other typed adapters.

The user-facing result is a **Verified Change Packet**. Its durable foundation
is a future first-class **Work Receipt** linking source manifests, retrieval
context, model/tool effects, evidence, artifacts, validation, cost, and replay
history. The current alpha provides the control and retrieval substrate for
that outcome; the reference workflow is a bounded, read-only showcase rather
than a claim that unattended repository changes are complete.

The product responsibilities are:

```text
Admit  →  Execute  →  Prove  →  Improve
```

- **Admit:** identity, workspace, source, policy, approval, and budget.
- **Execute:** durable ownership, fencing, checkpoints, retries, cancellation,
  model calls, and policy-controlled tools.
- **Prove:** immutable evidence, provenance, artifacts, validation, cost, and
  replayable history.
- **Improve:** offline evaluation, quality gates, cost attribution, and
  regression detection.

The public [product vision](01-product-vision.md) is the narrative contract;
this document is the engineering contract that makes it possible.

The canonical public explanation of the universal product boundary is the
[Universal AI work control plane](68-universal-work-control-plane.md). This
foundation note defines the invariants that every future adapter must
preserve. Repository maintenance is the first concrete adapter, not the
authority model or the product ceiling.

Every adapter follows the same high-level path:

```text
intent
  → workspace and actor scope
  → typed capability and policy admission
  → durable execution, fencing, and checkpoints
  → evidence, artifacts, cost, and external-effect recording
  → verification, Work Receipt, and replay
```

An adapter owns domain validation and side effects for its resource type. It
must not create a second operation authority, bypass workspace policy, or turn
an untyped payload into an implicitly authorized effect.

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
- A deterministic retrieval planner and bounded context compiler. Requests run
  through workspace-scoped structured SQL, lexical search, bounded symbol
  graph expansion, and caller-supplied vector search only when justified;
  every item carries an evidence hash and hard item/byte/token budgets.
- A durable workspace-scoped provenance/evidence substrate. Control events are
  transactionally mirrored into immutable evidence records with computed raw
  hashes; typed supersession/contradiction edges support bounded traversal and
  deterministic gist/detail/raw disclosure without replacing event history.
- A typed model gateway with explicit provider registration, deterministic fake
  and Ollama capabilities, opt-in OpenAI-compatible chat, bounded retries and
  budgets, pre-content fallback, credential-safe evidence, and a durable
  workspace-scoped model-call ledger.
- A Postgres-backed agent-run scheduler with deterministic queue ordering,
  workspace/run worker leases, monotonic fences, expiry/takeover, retry and
  approval resumption, cancellation exclusion, and atomic checkpoint commits.
- Workspace-scoped identity, RBAC, authorization, API-key lifecycle, and
  credential-reference metadata. Production authentication is bound to one
  workspace per key; explicit development mode retains the local compatibility
  key. Authorization decisions are audited and authenticated actors propagate
  into typed mutation events.
- A deterministic structured-argv tool boundary with explicit registration,
  deny-by-default workspace/actor/task/session policy, durable one-shot
  approvals, bounded local execution, task-fence admission, idempotent run
  records, and typed lifecycle events.
- A deterministic bounded agent loop at `POST /v1/agent/run`. Runs persist
  context compilation, model/tool ordering, budgets, approvals, retries,
  cancellation, task fencing, state-version checkpoints, and run-scoped
  replay through PostgreSQL. The fake provider makes the path testable offline;
  remote model and process effects remain explicitly at-least-once.
- A Postgres-only workspace-scoped content-addressed artifact plane. Raw bytes
  are immutable ordered chunks keyed by SHA-256, references/provenance are
  append-only, model-call response evidence is linked transactionally, and
  gist/detail/raw disclosure plus archive/tombstone retention are bounded.
- Artifact-backed output integration for oversized tool stdout/stderr/results,
  evidence raw payloads, and agent output/history. Source rows retain bounded
  compatibility markers while transactional links preserve actor, causation,
  correlation, fencing, idempotency, and provenance metadata. Operators have
  bounded dry-run backfill, two-phase retention, integrity verification, and
  storage metrics APIs.
- Durable workspace-scoped observations, trace spans, cost ledger entries,
  fixed-dimension metric samples, and offline replay evaluations. Model usage
  distinguishes measured, estimated, and unknown values; the read-only metrics
  endpoint is authorized per workspace, and replay never invokes providers or
  external tools. Oversized evaluation reports use the artifact plane.
- A deterministic retrieval-quality scorer resolves gold evidence against
  immutable Postgres evidence, calculates hit@k/MRR/precision/recall/nDCG/rank
  drift/context/abstention/latency/SQL/cost metrics, and persists bounded
  per-case quality plus baseline regression findings. It is read-only over
  retrieval history and never invokes remote effects.
- Append-only retrieval-surface capture records the redacted plan/trace,
  evidence references, context hash, budgets, and bounded measurements for
  normal retrieval requests. Authenticated operators can register datasets,
  list surfaces, run bounded durable or dry-run evaluations, compare a
  baseline, and read deterministic reports. The offline `fornix-eval` CLI uses
  recorded surfaces only.
- An authenticated operator CLI/API/MCP surface for workspace bootstrap,
  identity/role/API-key lifecycle, bounded ingest bookkeeping, task/run
  inspection, disclosure, metrics, and the deterministic fake-provider
  reference workflow.
- Durable workspace-scoped repository ingestion with mounted-root discovery,
  stable manifest identity, append-only source snapshots, bounded
  checkpointing, deterministic chunks/symbols, supersession/removal lineage,
  optional embedding gates, and CLI/HTTP/MCP submit/status/resume/cancel
  operations. The reference workflow consumes this job rather than manually
  posting fixture chunks.
- Workspace-scoped declarative validation policy packs with immutable hashes,
  tightening-only budgets, mandatory safety validators, approval and re-index
  controls, lifecycle audit, and exact policy propagation through verified
  change admission.
- Domain-neutral operation contracts with explicit system/resource/connector
  references, versioned capabilities, effect classes, execution profiles,
  evidence requirements, external-effect semantics, deterministic plans and
  results, and optional hash-only Work Receipt links. Repository packages
  remain the first adapter.
- An explicit process-local connector/capability registry with immutable
  definition hashes, fail-closed health/schema/workspace/credential/approval
  admission, deterministic bounded retries, and a shared conformance suite.
  The repository inspection adapter is read-only and hash/evidence based; the
  registry now hands typed requests to the first durable generic operation
  authority, but the registry itself remains process-local. The first
  non-repository reference adapters now add bounded HTTP read/list/
  idempotent-submit and read-only SQL describe/query/explain capabilities;
  both retain the same typed input, evidence, budget, and workspace boundary.
- A universal operation-admission/effect boundary with immutable policy
  snapshots, deterministic effect classification, workspace/actor/resource/
  connector/credential/evidence/budget checks, exact approval bindings,
  append-only approval history, and fenced external-effect recovery states.
  It records at-least-once boundaries without claiming exactly-once remote
  execution; connectors still own domain execution and verification.

## Current gaps

- OAuth/SSO, external KMS/secret-manager resolution, production role-separated
  Postgres row-level-security enforcement across every historical surface, and
  automated key/credential rotation policy.
- Typed event integration for every mutation path.
- Durable generic operation persistence, compatibility links, lifecycle
  idempotency, fenced leases, attempts, effect reservations, callbacks, and
  replay are implemented as the Issue #39 foundation. Deterministic queue
  claims and an adapter-owned heartbeat/expiry worker boundary are now
  qualified; final authorization of every compatibility link and full adapter
  integration remain qualification work; the connector registry and repository
  adapter are still process-local admission/execution seams.
- Universal admission is implemented as an alpha Postgres foundation in
  migration 036: policy snapshots, deterministic decisions, approval history,
  quota-window accounting, effect-state history, and recovery-required
  semantics are durable and replay-safe. Final connector authorization,
  signed callbacks, secret-manager resolution, and production egress
  enforcement remain qualification work under [Issue #40](https://github.com/Kshitij-M/fornix/issues/40).
- The HTTP/API and read-only SQL reference adapters are implemented as an
  alpha qualification slice under [Issue #43](https://github.com/Kshitij-M/fornix/issues/43).
  They are deliberately narrow: configured HTTP targets and read-only,
  allowlisted SQL only. Cloud, ticketing, business-system adapters, durable
  workflow integration, and production egress/credential qualification remain
  open.
- A background evaluation scheduler, general dataset import pipeline, and
  multi-tenant administrative UX. The current operator API/CLI is intentionally
  bounded and requires pre-registered redacted surfaces and authoritative
  evidence.
- Repository ingestion is currently operator-driven rather than a background
  queue. Source bytes are re-read from an explicit configured mount, and symbol
  extraction is conservative regex-based indexing rather than full parser or
  tree-sitter coverage.
- Full migration of every historical inline prompt/tool/evidence payload to
  artifact references, raw prompt capture policy, and a general memory
  compiler. Task 14 provides bounded producer-specific backfill for oversized
  tool/evidence/agent outputs, but does not automatically rewrite all history.
- The original agent loop remains a single-run bounded orchestrator plus a
  single-node pull worker, and does not provide a multi-agent graph executor.
  Loop 27 adds the first domain-neutral durable workflow runtime with typed
  model/tool/connector/approval/wait/validation/compensation steps, but its
  distributed scheduler, operator API, and adapter-specific qualification are
  still follow-on work. The current tool executor is a bounded local-process
  seam and does not claim kernel-level network/filesystem isolation.
- External object storage, resumable uploads, background garbage collection,
  physical partitioning, tiered/cold artifact compaction, and scheduled
  retention execution. The current Postgres-only artifact plane establishes
  the identity, provenance, output-link, and deletion-safety contract first.
- Backups, restore drills, capacity benchmarks, metric retention/compaction,
  and operational backpressure. Task 15 adds bounded durable accounting and a
  JSON metrics snapshot, but not a Prometheus/OTel collector or HA metrics
  service.

## Development order

1. Establish typed contracts, migrations, event history, and test harness.
2. Add checkpointed projections and deterministic task lifecycle state.
3. Add deterministic retrieval planning and bounded context compilation.
4. Add the bounded tool registry, policy, approval, and execution seam.
5. Add the bounded agent loop and durable model/tool orchestration.
6. Add provenance graphs, selective unfolding, lifecycle consolidation, and
   optional learned routing behind measured evaluation.
7. Generalize the operation authority, policy/effect boundary, connectors, and
   workflow runtime through the [universal transformation roadmap](68-universal-work-control-plane.md).

Do not begin with a full autonomous swarm or a universal vector-search path.
The deterministic substrate must be observable and benchmarked first.

## Success metrics

Every optimization reports task quality, abstention correctness, coordination
tokens, injected context tokens, stage hit rates, avoided model calls, p50/p95
latency, query count, duplicate work, lease recovery, and evidence recovery.
