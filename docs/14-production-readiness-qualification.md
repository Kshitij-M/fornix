# Fornix production-readiness qualification

Status: alpha single-node control and retrieval substrate with a fake-first
multi-domain reference workflow; not yet the complete safe autonomous
production-system operations product.

Fornix is runnable and testable, but it is not yet a production-grade,
multi-tenant harness for huge projects. The current system has a durable
Postgres control database, typed event history, deterministic projections,
task coordination, retrieval, and code indexing.

The product direction is **verifiable AI work infrastructure for long-running
production-system operations**. The goal is to let teams delegate serious
work to AI without losing control of scope, cost, evidence, approval, or
recovery. Repository maintenance is the first alpha-qualified local adapter.
The current
qualification proves the substrate behind that goal; it does not qualify
unattended changes to important production systems.

The reference workflow is therefore a showcase of the path from admission to
replay, not a finished change-management product. The first Work Receipt
foundation now makes the result of that bounded operation an immutable,
workspace-scoped verification contract; a complete Verified Change Packet
still requires safe patch application and reviewer-facing change validation.

## Universal transformation status

The repository-first alpha is the first alpha-qualified local adapter for a broader
product: a control plane for verifiable AI work against production systems.
The universal contracts, process-local connector registry, and first generic
operation authority establish the adapter boundary, but the alpha is not yet
a universal execution platform.

The next dependency-ordered work is tracked by [Issue #38](https://github.com/Kshitij-M/fornix/issues/38):

1. [#39 — Durable generic operation authority](https://github.com/Kshitij-M/fornix/issues/39) (foundation implemented; final qualification and adapter integration remain).
2. [#46 — Universal policy, approvals, and external effects](https://github.com/Kshitij-M/fornix/issues/46) (alpha admission/effect foundation implemented; connector qualification remains).
3. [#43 — Bounded HTTP/API and read-only SQL connectors](https://github.com/Kshitij-M/fornix/issues/43) (bounded reference adapters implemented; production qualification remains).
4. [#44 — Durable multi-step workflow runtime](https://github.com/Kshitij-M/fornix/issues/44) (alpha foundation implemented; operator surface and qualification remain).
5. [#42 — Multi-domain reference workflow](https://github.com/Kshitij-M/fornix/issues/42) (fake-first incident workflow implemented; live connector and production qualification remain).
6. [#40 — Universal production qualification](https://github.com/Kshitij-M/fornix/issues/40).

Issues [#41](https://github.com/Kshitij-M/fornix/issues/41) and
[#45](https://github.com/Kshitij-M/fornix/issues/45) define the current
domain-neutral contract and registry foundation. Their presence in the
architecture does not make HTTP, SQL, cloud, ticketing, or general workflow
operations available today. The [universal work-control-plane overview](68-universal-work-control-plane.md)
contains the current/planned domain matrix and the precise category boundary.

## Verified capabilities

- Numbered, embedded, checksum-validated migrations.
- A Postgres row-level-security foundation for the generic operation/admission
  authority with transaction-local workspace context, fail-closed policies,
  and an opt-in non-owner/NOBYPASSRLS qualification smoke. The development
  Compose role remains a table-owning compatibility role; production cannot
  claim database-enforced tenant isolation until role separation is deployed
  and the qualification smoke passes.
- An opt-in bounded generic-operation capacity harness that measures concurrent
  idempotent create/replay and fenced lease work, p50/p95/max latency,
  Postgres counters, and relation growth. This is local qualification
  evidence, not an HA, soak, or production SLO claim.
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
- Domain-neutral connector execution is split at the external boundary:
  trusted read/observation capabilities can persist results, while effectful
  work must first create a durable reservation and use the fenced reconciliation
  API. Dispatch, acknowledgement, verification, compensation, and recovery
  states are append-only and idempotent; this still does not qualify any live
  provider connector or exactly-once external execution.
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
- The generic operation authority is now available through authenticated HTTP
  and CLI routes for typed operation creation, inspection, fenced lease
  acquisition/takeover, legal transitions, and read-only replay. The adapter
  supplies the actor from the authenticated principal, reuses the Postgres
  operation store, and rejects stale fences and cross-workspace requests.
  This is an operator/qualification surface, not a claim that every connector
  is production-qualified.
- Built-in connector capabilities are now admitted through explicit,
  workspace-scoped trust snapshots that pin connector identity and definition
  hashes. Credential-bearing HTTP adapters can use a bounded lease resolver;
  lease values remain outside durable state. These are fail-closed seams, not
  a claim that the local profile is an external secret manager or signed
  supply-chain catalog. Shared destination policy now also normalizes and
  authorizes connector schemes, hosts, paths, and redirect budgets; HTTP keeps
  its adapter-specific DNS/private-network and exact-host controls as defense
  in depth. This is not yet a central egress proxy or signed policy catalog.
- The first non-repository reference workflow is now implemented as a
  fake-first incident path. It durably captures typed incident delivery,
  duplicate/conflict semantics, runbook and diagnostic steps, model/tool
  evidence, approval or rejection, a fenced remediation record, verification,
  Work Receipt linkage, CLI/HTTP/MCP access, crash recovery, and inert replay.
  This is a control-plane qualification workflow; it does not enable live
  monitoring verification, cloud/database/ticketing effects, or exactly-once
  remote execution.
- Approval-gated repository change packets are now typed, workspace-scoped,
  idempotent, content-addressed, and persisted in migration 029. The planner
  rejects traversal and symlink escapes, the application boundary uses
  structured filesystem APIs with source/post-state hashes, and successful
  applications produce a derived Work Receipt. Dry-run, duplicate delivery,
  crash rollback, approval-hash, artifact, and live Postgres concurrency tests
  cover the first vertical slice.
- Workspace-scoped declarative validation policy packs are now typed,
  content-hashed, immutable, and auditable. Active/default lifecycle bindings,
  exact validator resolution, tightening-only budgets, mandatory safety floors,
  approval/re-index controls, fail-closed workspace checks, and policy
  propagation through changes, validation, handoffs, receipts, events, and
  accounting are covered by Postgres integration tests and authenticated
  policy HTTP/CLI/MCP surfaces.
- The single-package local operator path is implemented as a native `fornix`
  CLI with a private profile, owner-only local credential references,
  deterministic fake-provider defaults, managed Docker/Compose lifecycle,
  pinned PostgreSQL/pgvector digest, automatic workspace bootstrap, loopback
  binding, readiness checks, repository mount validation, and redacted
  diagnostics. A disposable Docker smoke verifies start, the reference
  workflow, replay, duplicate-run idempotency, and service isolation.
- Release qualification now includes a reviewable macOS/Linux amd64/arm64
  archive contract, checksum verification, archive SBOM configuration,
  third-party notices, GitHub checksum attestation, an independent archive
  verifier, and a clean-room local installer smoke. A public GitHub release
  and a `get.fornix.dev` DNS alias are release-owner operations; until they
  exist, the raw GitHub installer URL is the honest distribution fallback.

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
- A destructive logical backup/restore drill exists, but production backup
  scheduling, WAL/PITR, HA/failover, restore-owner procedures, deployment-
  specific RPO/RTO evidence, and capacity benchmarks remain open. The Task 15
  endpoint is a bounded Postgres snapshot, not a Prometheus/OTel replacement.
- No background evaluation scheduler, general historical import pipeline, or
  full multi-tenant administration UX. Recorded surfaces require binary gold
  evidence labels and the current CLI/API intentionally uses redacted hashes
  rather than raw prompts or rendered context. The reference workflow now
  consumes a durable bounded repository ingest job; automatic ingest scheduling
  and full parser-quality indexing remain future work.
- Public release distribution is qualified for the first alpha through
  `v0.11.0-alpha.3`: all four canonical archives, checksums, SBOMs, notices,
  installer behavior, and multi-architecture GHCR images were verified from
  public assets. Homebrew/deb/rpm adapters do not yet exist. Docker remains
  an explicit macOS/Linux prerequisite. The local profile uses owner-only
  files rather than an OS keychain or external secret manager, and the managed
  runtime is single-node without automatic backup/restore or high availability.
- Policy packs are declarative and limited to the built-in validator catalog.
  They are not a general policy programming language, do not execute arbitrary
  code, and do not yet provide organization-wide policy distribution,
  delegated policy administration, or policy-as-code review workflows.
- Domain-neutral operation contracts and the first Postgres-backed generic
  operation authority are now available for typed system, resource, connector,
  capability, effect, execution, evidence, plan, result, and external-effect
  references. The authority provides durable operation identity, lifecycle
  history, fenced leases, attempts, external-effect reservations, callbacks,
  compatibility links, and read-only replay. A process-local
  connector/capability registry, fail-closed admission seam, bounded executor,
  and read-only repository adapter conformance slice are also qualified.
  Universal policy is now a tested alpha admission/effect foundation, but
  external connectors, link authorization, signed callbacks, host-independent
  egress controls, secret-manager resolution, and non-repository production
  execution remain outside this qualification.
- The generic operation HTTP/CLI surface is intentionally narrow: it does not
  expose a universal operation list endpoint, connector-specific payload
  submission, external credential resolution, or a background operation
  worker. Those concerns remain in the connector, workflow, policy, and Issue
  #40 qualification layers.
- Shared egress policy is currently a typed in-process admission contract. It
  is not yet durably versioned on every generic effect, enforced by a central
  network boundary, or implemented by every future connector. Production
  qualification must add signed policy/catalog distribution, DNS-rebinding
  controls independent of individual adapters, and adversarial confused-deputy
  tests.
- The generic operation authority now has an authenticated execution route for
  trusted read-only and observation capabilities. It persists deterministic
  plans and hash-only results transactionally, deduplicates committed result
  retries, and rejects effectful capabilities until durable effect admission
  and reservation are connected. This is the first registry-to-authority
  vertical slice, not a background universal executor or exactly-once remote
  execution claim.
- The Issue #43 connector slice now provides a fake-first HTTP/API adapter
  (`read`, `list`, and approval-gated `submit_idempotent`) and a read-only SQL
  adapter (`describe`, `query_readonly`, and `explain_readonly`). Both enforce
  workspace-bound typed inputs, configured targets, hard response/row/byte/
  timeout budgets, redacted hash-only results, and explicit external-effect
  semantics. The SQL cost limit is a deterministic result-size estimate, not
  a database planner or provider billing measurement. These adapters are
  qualified for unit/conformance and clean-migration tests only; public
  network egress, external credentials, provider-specific verification,
  high-availability operation, and multi-domain workflow execution remain
  unqualified.
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
make smoke-policy
make smoke-package
make smoke-universal-operation
make smoke-universal-trust
make smoke-universal-egress
make smoke-universal-execution
make smoke-universal-effects
```

For release output produced by GoReleaser, run:

```sh
make release-check
```

Postgres-backed results and measured latency/storage/replay throughput are
recorded in the loop completion notes under `docs/`.
