# Fornix production-readiness qualification

Status: alpha single-node control and retrieval substrate; not yet the complete
safe autonomous repository-maintenance product.

Fornix is runnable and testable, but it is not yet a production-grade,
multi-tenant harness for huge projects. The current system has a durable
Postgres control database, typed event history, deterministic projections,
task coordination, retrieval, and code indexing.

The product direction is **verifiable AI work infrastructure for long-running
repository operations**. The goal is to let teams delegate serious repository
work to AI without losing control of scope, cost, evidence, approval, or
recovery. The current qualification proves the substrate behind that goal; it
does not qualify unattended changes to important repositories.

The reference workflow is therefore a showcase of the path from admission to
replay, not a finished change-management product. The first Work Receipt
foundation now makes the result of that bounded operation an immutable,
workspace-scoped verification contract; a complete Verified Change Packet
still requires safe patch application and reviewer-facing change validation.

## Verified capabilities

- Numbered, embedded, checksum-validated migrations.
- Liveness/readiness endpoints, request IDs, body limits, timeouts, and
  graceful shutdown.
- Concurrent task claiming and completion.
- Workspace-scoped idempotency, append-only event history, replay, and
  monotonic checkpoints.
- Transactional projection updates with rebuild, duplicate protection, crash
  rollback tests, concurrency tests, and workspace isolation.
- Durable workspace-scoped projection consumer leases with monotonically
  increasing fencing tokens, expiry/takeover, stale-owner rejection, and
  checkpoint authorization.
- Durable workspace-scoped task execution leases with monotonically increasing
  fences, expiry/takeover, stale-worker rejection, dependency-aware ordering,
  bounded retry/dead-letter transitions, cancellation, and atomic lifecycle
  events.
- Deterministic staged retrieval with repeatable-read snapshots, workspace
  isolation, hard item/byte/token budgets, bounded graph expansion, gated
  vector search, evidence hashes, provenance, stable ordering, and context
  hashes.
- Immutable workspace-scoped evidence records with computed raw hashes,
  append-only typed provenance edges, supersession/contradiction metadata,
  bounded deterministic traversal, and gist/detail/raw disclosure budgets.
- Typed model gateway with explicit provider registry, deterministic fake,
  Ollama embedding compatibility, opt-in OpenAI-compatible chat, stable failure
  classification, bounded retries/budgets, pre-content fallback, redacted
  evidence, and durable idempotent model-call metadata.
- Explicit deterministic tool registry with structured-argv local execution,
  deny-by-default scoped policy, durable approval decisions, bounded timeout,
  output, argument, and environment budgets, task-fence admission, idempotent
  tool-run metadata, and typed lifecycle events.
- Deterministic bounded agent loop with durable run checkpoints, persisted
  context hashes, model/tool sequencing, hard turn/token/byte/time/cost/tool
  budgets, durable cancellation/approval/retry states, task-fence admission,
  idempotent run creation, and run-scoped event replay.
- Postgres-backed agent-run queue selection with deterministic ordering,
  workspace/run worker leases, monotonic fences, heartbeats, expiry/takeover,
  cancellation exclusion, automatic due-retry/approved-approval resumption,
  and atomic lease-renewed checkpoint commits.
- Docker-backed Go/Python checks, Postgres integration tests, CI, and smoke
  tests.
- Workspace-scoped identities, deterministic RBAC, fail-closed authorization,
  API-key hashing/expiry/revocation/rotation, credential-reference lifecycle,
  append-only authorization audit, and authenticated actor propagation.
- Workspace-scoped content-addressed artifacts with deterministic chunking,
  immutable raw-byte enforcement, concurrent per-workspace deduplication,
  append-only references/provenance, model-call response linking, bounded
  gist/detail/raw disclosure, integrity verification, and retention tombstones.
- Transactional artifact-backed tool, evidence, and agent output integration
  with bounded compatibility markers, idempotent source links, task-fence
  admission, dry-run/resumable backfill, two-phase archive/delete sweeps,
  corruption reports, and storage/deduplication metrics.
- Durable workspace-scoped observations, trace spans, cost ledger entries,
  fixed-dimension metrics, model/tool/retrieval/agent/artifact/approval/retry/
  scheduler instrumentation, and authenticated read-only metrics snapshots.
  Offline evaluation replays durable checkpoints and history only, supports
  bounded dry runs, deterministic quality gates, and artifact-backed reports.
- Deterministic retrieval-quality evaluation resolves gold hashes against
  integrity-checked workspace evidence and records hit@k, reciprocal rank,
  precision, recall, nDCG, rank drift, context-hash, abstention, latency, SQL,
  cost, and baseline-regression results without external effects.
- Normal retrieval requests can append a redacted workspace-scoped retrieval
  surface. Authenticated operators can register datasets, page through
  recordings, run bounded durable or dry-run evaluations, compare baselines,
  and read metrics/gates/reports. The offline `fornix-eval` CLI is deterministic
  and consumes recorded surfaces only.
- Immutable Work Receipts bind completed task/run identity to bounded steps,
  source/evidence/artifact hashes, cost classification, replay state, and
  verification outcomes. Finalization is idempotent, transactional, and
  fail-closed on missing, stale, contradictory, or cross-workspace references.
  Gist/detail/raw disclosure preserves the canonical receipt hash.
- The authenticated Go operator CLI, `/v1/operator/*` HTTP routes, and MCP
  compatibility shim now share workspace bootstrap, identity/role/API-key
  lifecycle, bounded ingest metadata, task/run inspection, disclosure, metrics,
  and reference-workflow semantics.
- Approval-gated repository change packets are now typed, workspace-scoped,
  idempotent, content-addressed, and persisted in migration 029. The planner
  rejects traversal and symlink escapes, the application boundary uses
  structured filesystem APIs with source/post-state hashes, and successful
  applications produce a derived Work Receipt. Dry-run, duplicate delivery,
  crash rollback, approval-hash, artifact, and live Postgres concurrency tests
  cover the first vertical slice.

## Production gaps

### Product-level gap

- The current alpha does not yet provide the complete flagship workflow for
  unattended repository maintenance. The reference path is bounded and
  read-only, while the change path is an explicit local-mount vertical slice;
  automatic agent-to-patch synthesis, multi-file transactional filesystem
  semantics, deterministic repository validation, and a reviewer-facing
  change UI remain product work.

- No OAuth/SSO, external KMS/secret-manager provider, or Postgres row-level
  security policy. The operator identity/API-key surface is intentionally
  bounded; local compatibility still requires explicit development mode.
- Not every mutation path emits typed events yet.
- Not every historical inline prompt/tool/evidence payload has been migrated to
  artifact references, and there is no general memory compiler yet. Task 14
  backfill is producer-specific, bounded, and operator-triggered. The artifact
  plane is Postgres-only and does not yet provide external object storage or
  resumable uploads.
- The agent loop is currently a single-run bounded orchestrator with a
  single-node pull worker. It does not provide multi-agent sub-run graphs or a
  general sandbox provider. The current local process seam cannot enforce
  complete network/filesystem isolation on every host. Remote model calls and
  external tool processes remain at-least-once at their network/process
  boundaries even when provider or run idempotency keys are supplied.
- Evidence raw bytes remain bounded inline for backward compatibility; model
  response evidence now has a transactional artifact reference. Tool/agent
  output migration, object-backed cold tiers, and a retention compactor remain
  follow-up work.
- No backup/restore drill, high-availability plan, capacity benchmark, metric
  exporter/collector, metric retention compactor, or operational backpressure
  policy. The Task 15 endpoint is a bounded Postgres snapshot, not a
  Prometheus/OTel replacement.
- No background evaluation scheduler, general historical import pipeline, or
  full multi-tenant administration UX. Recorded surfaces require binary gold
  evidence labels and the current CLI/API intentionally uses redacted hashes
  rather than raw prompts or rendered context. The reference workflow now
  consumes a durable bounded repository ingest job; automatic ingest scheduling
  and full parser-quality indexing remain future work.
- The projection runtime is an internal pull API; no background subscriber or
  public replay API is provided yet.
- Lease transitions are current coordination state rather than an append-only
  audit stream; operational metrics and lease-history retention are deferred.
- Repository application is an external filesystem effect. Temp-file writes
  protect individual files, but a crash during a multi-operation packet can
  leave a partial tree; Fornix records `recovery_required` and does not claim
  automatic rollback or exactly-once application. The current boundary does
  not include a general patch parser, VCS commit/push integration, or a
  host-independent sandbox/network policy.

## Qualification commands

```sh
make check
make build
make smoke
make smoke-projection
make smoke-leases
make smoke-tasks
make smoke-retrieval
make smoke-provenance
make smoke-model
make smoke-tools
make smoke-agent
make smoke-scheduler
make smoke-identity
make smoke-artifacts
make smoke-artifact-output
make smoke-observability
make smoke-retrieval-quality
make smoke-retrieval-evaluation
make smoke-reference-workflow
make smoke-reference-openai
make smoke-ingestion
make smoke-changes
```

Postgres-backed results and measured latency/storage/replay throughput are
recorded in the loop completion notes under `docs/`.
