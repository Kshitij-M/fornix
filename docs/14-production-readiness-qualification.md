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
- A bounded, local-only support bundle with an explicit redaction allowlist,
  owner-only exclusive file creation, and no profile mutation during capture;
  this is a diagnostic primitive, not alerting or hosted support operations.
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
- Generic workflow retry state is durably scheduled on the operation queue and
  direct step starts recheck eligibility against PostgreSQL time. The runtime
  now advances due retries without hot-looping early attempts, preserves the
  earliest deadline across multiple waiting steps, and keeps explicit approval
  and recovery waits blocking. Offline contract/store checks pass; the new
  runtime/database-clock integration cases still require the disposable
  PostgreSQL qualification target.
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
- Retrieved context stays in a non-privileged user-role message, and a durable
  run's exact declared tool catalog constrains model-requested tools before
  execution. Unregistered tools are rejected before model invocation; registry,
  policy, approval, fencing, and effect controls remain independently required.
  Focused offline checks pass, while catalog persistence/mutation tests still
  require the disposable PostgreSQL qualification target.
- Tool descriptions and JSON argument schemas shown to models are now derived
  from registered capability metadata and the bounded structured-argv
  executor contract. Requester-supplied mismatches fail before reservation;
  persisted definition fingerprints fail closed on registry drift. This does
  not make tool descriptions policy, or eliminate prompt injection.
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
- Generic operations now have a bounded Postgres queue-claim API and CLI
  command. Claims are ordered deterministically, workspace-scoped, fenced,
  expiry-recoverable, and exclude uncertain external-effect work. This is a
  worker-claim primitive, not a background scheduler or provider dispatcher.
- Capability `RateLimitPerMinute` is now enforced transactionally from durable
  workspace/connector/capability admission history across the operation API,
  background read-only worker, generic workflow reads, and incident
  observations. Migration 079 and CI qualification are added, but the
  database-backed tests and contention measurements have not yet been run in
  this local environment; the limit is not provider-account-global.
- Generic operation claims now have an adapter-owned worker-consumption
  boundary. The worker supplies heartbeats, cancellation, bounded polling,
  fail-closed lease-loss behavior, and expiry-based recovery without becoming
  a universal provider dispatcher. Adapter handlers must persist their own
  plan/result/transition and use the independent external-effect authority;
  the worker does not claim exactly-once remote execution.
- Generic operation scheduling now also has explicit workspace round-robin
  supervision, a durable `MaxActive` workspace lease quota, and declared
  resource serialization with monotonic resource fences. The supervisor is
  process-local policy over an explicit workspace set; production fairness,
  weighted priorities, pool saturation, and load/soak evidence remain open.
- Built-in connector capabilities are now admitted through explicit,
  workspace-scoped trust snapshots that pin connector identity and definition
  hashes. Credential-bearing HTTP adapters can use a bounded lease resolver;
  lease values remain outside durable state. These are fail-closed seams, not
  a claim that the local profile is an external secret manager or signed
  supply-chain catalog. Shared destination policy now also normalizes and
  authorizes connector schemes, hosts, paths, and redirect budgets; HTTP keeps
  its adapter-specific DNS/private-network and exact-host controls as defense
  in depth. This is not yet a central egress proxy or signed policy catalog.
- Effectful connector execution now has an explicit authority envelope. Durable
  admission records the verified schema-catalog hash/revision and managed
  credential source version/expiry; effect reservations persist the same facts,
  and result/receipt authority links inherit them transactionally. The HTTP
  adapter can execute with the exact admitted credential lease rather than
  acquiring a replacement fence. Stale, missing, mismatched, expired, or
  revoked authority facts fail closed. This still does not claim exactly-once
  execution of a remote provider.
- Task 85 binds a hash-only egress policy, destination policy, and explicit
  network-boundary mode to generic effect reservations, authority links,
  domain-effect links, and federation poll attempts. Model, embedding,
  connector, change, tool, and federation paths preserve the same envelope
  where their controlled transport can describe it. Strict production mode
  rejects missing external-boundary facts before dispatch; fake/read-only
  development remains offline. These hashes prove local policy composition,
  not a hosted firewall, proxy, DNS, workload identity, provider behavior, or
  exactly-once remote execution.
- The first non-repository reference workflow is now implemented as a
  fake-first incident path. It durably captures typed incident delivery,
  duplicate/conflict semantics, runbook and diagnostic steps, model/tool
  evidence, approval or rejection, a fenced remediation record, verification,
  Work Receipt linkage, CLI/HTTP/MCP access, crash recovery, and inert replay.
  This is a control-plane qualification workflow; it does not enable live
  monitoring verification, cloud/database/ticketing effects, or exactly-once
  remote execution.
- Task 94 adds deterministic fake-first qualification fixtures for incident
  response, data pipeline operations, customer support, and infrastructure
  maintenance. They build generic operation plans, preserve explicit
  approval/effect classes, and replay to stable hashes without external
  calls. These fixtures prove contract portability only; each production
  connector still requires independent authentication, idempotency,
  verification, outage, egress, and recovery qualification.
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

- Task 70 adds a provider-neutral, redacted adapter-conformance report and an
  opt-in PostgreSQL topology qualification. The report exercises the existing
  connector conformance suite with deterministic case names and bounded error
  codes; effectful capabilities are blocked unless an operator explicitly
  enables external effects. The topology probe verifies writable-primary
  prerequisites, transaction-local workspace context, pool hygiene, and
  bounded acquisition latency. Neither surface claims hosted credential,
  failover, PITR, certificate-rotation, or production-SLO evidence that was
  not actually supplied by a deployment.

- Task 71 adds a common, hash-stable `QualificationReport` and offline CLI
  validator for deployment evidence. Passed backup/restore, PITR, failover,
  partition, and identity-rotation records must carry their required hashes
  and measurements; unknown fields, raw payloads, invalid fingerprints, and
  unexecuted drills fail closed. This improves evidence handoff but still does
  not execute or certify the deployment-owned provider, HA, PITR, RPO/RTO, or
  load/soak drills.

- Task 72 adds a deterministic built-in adapter qualification matrix covering
  local HTTP, SQL, repository, and fake incident read fixtures. Matrix entries
  are sorted, workspace-bound, hash-stable, and aggregated monotonically;
  effectful capabilities are blocked by default. The matrix improves repeatable
  connector coverage but does not certify live provider behavior, credentials,
  topology, failover, PITR, or external effects.

- Task 73 composes the existing Postgres queue claim, fenced operation worker,
  workspace supervisor, and durable identity lookup into the server runtime.
  The server worker claims only persisted read/observation plans, refreshes an
  explicit active-workspace inventory, records hash-only results, and leaves
  effectful or unknown plans for the existing authority-aware dispatcher.
  `FORNIX_WORKER_ENABLED` remains the explicit runtime switch; this does not
  certify deployment fairness, live providers, HA, PITR, or external effects.

## Production gaps

### Product-level gap

- The current alpha does not yet provide the complete flagship workflow for
  unattended repository maintenance. The reference path is bounded and
  read-only, while the change path is an explicit local-mount vertical slice;
  automatic agent-to-patch synthesis, multi-file transactional filesystem
  semantics, deterministic repository validation, and a reviewer-facing
  change UI remain product work.

- No OAuth/SSO or vendor-specific external KMS/secret-manager adapter is
  shipped. The workspace-scoped managed resolver, controlled HTTP manager
  protocol, source-version-bound credential leases, and redacted failure
  boundary now exist, but deployment workload identity/mTLS, zeroization,
  rotation-lag, and hosted-manager evidence are still required before this can
  be called production credential integration. Postgres row-level-security
  policies exist for the generic operation/admission/effect tables, but
  production role separation and deployment-wide enforcement are still
  required before this can be claimed as complete tenant isolation. The
  operator identity/API-key surface is intentionally bounded; local
  compatibility still requires explicit development mode.
- The workspace federation poller is now explicitly injectable and opt-in.
  Poll recovery can be fenced across takeover, and a bounded recorded response
  can be reconciled without contacting the peer or secret manager. Historical
  global federation rows have only a redacted quarantine/disposition path;
  ownership is never inferred. Task 68 adds a deployment-shaped mTLS/workload-
  identity test authority, certificate chain/name/pin validation, bounded
  rotation/revocation observations, partitioned retention tombstones, and an
  opt-in federation capacity qualification. Task 69 adds an environment-gated
  redacted authority probe and a fenced, workspace-paginated retention owner.
  A real secret-manager/workload-identity deployment, provider-specific live
  conformance, certificate revocation protocol, scheduled partition policy,
  and production load/soak evidence remain deployment qualification gates.
- Not every mutation path emits typed events yet.
- Not every historical inline prompt/tool/evidence payload has been migrated to
  artifact references, and there is no general memory compiler yet. Task 14
  backfill is producer-specific, bounded, and operator-triggered. The artifact
  plane is Postgres-only and does not yet provide external object storage or
  resumable uploads.
- The agent loop is currently a single-run bounded orchestrator with a
  single-node pull worker. It does not provide multi-agent sub-run graphs or a
  qualified isolated sandbox. Tool execution now has an explicit backend and
  capability registry, but only the same-host `local-process` provider is
  implemented. It cannot enforce network/filesystem isolation or hard CPU,
  memory, PID, and scratch limits. OCI, gVisor, and microVM profiles fail
  closed. A typed exact-attempt reconciliation and atomic result-finalization
  path now exists, but no non-local provider is shipped to supply
  crash-reconcilable observations. Remote model calls and external tool
  processes remain at-least-once at their network/process boundaries even when
  provider or run idempotency keys are supplied.
- Tool success finalization now has a database-only callback in the same
  transaction as the verified generic effect, reconciled domain link, and
  operation result. Interrupted nonterminal dispatch states can be normalized
  under a fresh effect-recovery lease before inspecting the exact sandbox
  attempt. Postgres rollback/fence/replay tests are present, but they have not
  been executed in this local environment because no disposable pgvector
  database is available. The local-process backend still cannot recover an
  attempt after host/control-plane loss; OCI remains unavailable.
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
  submission, external credential resolution, or a server-wired background
  worker supervisor. An adapter-owned worker package now supplies the bounded
  claim/heartbeat/release contract, while deployment composition and handler
  registration remain in the connector, workflow, policy, and Issue #40
  qualification layers.
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
- Task 54 adds a fail-closed signed trust/schema catalog reload path during
  server composition and before generic operation admission, readiness status,
  live operation/task-fence validation for authority-bound dispatch, and an
  effectful-adapter conformance guard. This does not qualify process restart,
  live provider verification, signer-rotation ceremony, or migration of every
  effectful path to one shared durable dispatcher.
- Task 55 now routes the generic HTTP effect path and fake incident remediation
  through the shared durable dispatcher. Admission, attempts, effect
  reservations, dispatching state, live fence validation, uncertain outcomes,
  concurrent duplicate delivery, and reserved-state crash recovery are
  qualified. Task 57 extends that composition seam to server model calls,
  structured tools, and repository-change application, while preserving the
  specialized ledgers as detail authorities. Task 58 extends fenced ownership
  to direct agent-run HTTP mutations, fixes active-workspace scheduler
  enumeration, and adds versioned append-only domain-link recovery history.
  Migration 055 records explicit `recovery_required` for uncertain model/tool
  outcomes. Task 59 extends the same authority to embedding calls through
  migration 056, scoped vector replay, the generic child-effect dispatcher,
  bounded workspace backfill, and migration 057 immutable target attachments.
  Repository-ingestion chunk attachment is transactionally qualified;
  provider-specific reconciliation, memo/symbol attachment, process restart,
  and live-provider verification remain unqualified. No exactly-once remote
  execution claim is made.
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
- The SQL connector v2 no longer accepts executable caller-authored SQL.
  Typed requests are compiled to parameterized reads over exact allowlisted
  persistent base tables; referenced columns must use supported PostgreSQL
  built-in scalar types. The PostgreSQL adapter pins `search_path`, opens a
  read-only transaction, and rechecks relation/column metadata before each
  query. Offline compiler, connector, and conformance tests pass. The
  PostgreSQL-backed qualification case was not run locally because
  `FORNIX_TEST_PG_DSN` is unset; arbitrary database credentials, grants, RLS,
  planner cost, and live isolation remain deployment qualification
  requirements. See [Loop 120](268-loop-120-completion.md).
- The projection runtime is an internal pull API; no background subscriber or
  public replay API is provided yet.
- Lease transitions are current coordination state rather than an append-only
  audit stream; operational metrics and lease-history retention are deferred.
- Repository application is an external filesystem effect. Temp-file writes
  are now rooted at a pinned `os.Root` handle, which prevents a symlink swap
  from redirecting an operation outside the opened repository root. It is not
  a kernel sandbox, does not prevent mount traversal or redirection within the
  root, and does not make a packet atomic. A crash during a multi-operation
  packet can leave a partial tree; rename may also leave both names if
  interrupted between no-overwrite link and unlink. Fornix records
  `recovery_required` and does not claim automatic rollback or exactly-once
  application. The current boundary does not include a general patch parser,
  VCS commit/push integration, or host-independent sandbox/network policy.
- Task 80 adds a durable, signed deployment trust snapshot and binds new
  authorized qualification imports to its exact revision/hash. Startup can
  require a current snapshot for every workspace, but this remains a
  repository-owned trust record; HSM/KMS custody, distribution transport,
  release signature verification, and deployment topology evidence remain
  outside the repository qualification.
- Task 81 adds an immutable release/evidence index and deterministic
  qualification gate. Releases bind to the exact trust snapshot used at
  registration; evidence links reference accepted signed imports by hash and
  cannot copy or replace raw evidence. The gate is read-only and fail-closed
  for missing, failed, stale, or unresolved evidence. It does not execute
  deployment operations or prove backup, HA/PITR, provider, sandbox, or
  external-effect behavior; those remain deployment-owned qualifications.
- Task 82 adds hash-only release verification and a deterministic admission
  decision bound to the exact release, trust snapshot, and qualification gate
  hash. Production can require this decision at startup through explicit
  release configuration. It still does not verify or execute a registry,
  image, binary, deployment target, HA topology, provider, or external effect;
  those claims require deployment-owned verifiers and evidence.
- Task 83 makes that admission decision consumable by the domain-neutral
  operation authority. A generic effectful operation may carry a bounded
  release/artifact/gate/snapshot/decision-hash reference, and effect
  reservation revalidates it in the same Postgres transaction. Production can
  require the reference with `FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS`.
  This closes the operation-to-qualification binding without creating a second
  authority or claiming live deployment truth, provider exactly-once behavior,
  registry verification, or rollout execution.
- Task 84 adds the corresponding adapter-composition gate. Strict startup
  inventories every registered workspace and rejects effectful connector
  capabilities that do not implement the authority-aware execution and effect
  description seams, or that do not advertise idempotency and verification.
  The server's model, embedding, tool, and change adapters are listed in a
  stable hash-only manifest and receive the exact deployment-admission
  reference before child-operation creation. The reference is revalidated by
  Postgres in the same transaction as effect reservation. This proves local
  composition and fail-closed wiring; it does not qualify a hosted provider,
  network sandbox, credential manager, or deployment target.
- Task 85 adds the corresponding managed-credential and controlled-egress
  envelope. The exact redacted boundary identity is carried from provider or
  connector transport construction into the durable reservation and every
  specialized link. Duplicate reservations with changed boundary facts fail
  closed; historical rows remain readable; strict federation polling uses the
  same requirement. The offline operator surfaces are
  `fornix qualification adapter-conformance` and
  `fornix qualification external-boundary`, with
  `make qualification-external-boundary` for CI. Deployment-owned secret
  manager/KMS, mTLS/workload identity, DNS/rebinding, proxy/firewall, live
  provider, HA/PITR, and load/soak evidence remain open.
- Task 86 adds the signed, hash-only deployment observation envelope for those
  remaining external-boundary facts. A qualification report can now carry
  typed observations for credential resolution, workload identity, mTLS,
  DNS/rebinding, proxy/firewall enforcement, provider idempotency, and
  external recovery. External-effect evidence links derive the exact boundary
  and evidence-set hashes from the signed import; release-admission references
  carry those hashes; and strict operation reservations reject a missing or
  mismatched boundary proof before dispatch. The repository still does not
  collect these observations, contact a secret manager, operate a proxy,
  resolve deployment DNS, call a live provider, or claim exactly-once remote
  execution. `make qualification-external-boundary` remains offline and
  reports the observation vocabulary and its deployment-owned limitation.
- Task 87 adds the deployment-owned publisher boundary. The offline
  `qualification boundary-sign` and `qualification boundary-validate` commands
  validate and sign only the existing bounded, redacted observation bundle;
  external-effect mode requires exactly one measured, passed, non-expired
  observation at an explicit validation time. The publisher never contacts a
  secret manager, DNS, proxy, certificate authority, provider, or tool, and it
  does not authorize an effect. Deployment automation remains responsible for
  collecting observations and importing the resulting signed bundle.
- Task 88 adds durable evidence-link lifecycle operations. Operators can
  revoke an active link or explicitly replace it with a new accepted import;
  the predecessor and every lifecycle event remain auditable. Gate evaluation
  includes historical links for explanation but admits only active links and
  reports deterministic `evidence_revoked` or `evidence_superseded` reasons.
  The new path is workspace-scoped, actor-bound, idempotent, transactional,
  and still does not mutate signed imports or claim that a deployment check
  was truthful.
- Task 89 adds advisory, hash-only readiness snapshots and structured incident
  annotations. A snapshot records the exact release, trust snapshot, active
  evidence IDs, gate hash, bounded diagnostics, and evaluation time from one
  PostgreSQL transaction. Snapshots are append-only and hash-stable; operator
  annotations are closed-vocabulary, actor-bound, idempotent, workspace-scoped
  evidence that cannot change admission. No signed bundle, deployment payload,
  secret, URL, or log is copied into this surface.
- Task 90 adds immutable workspace/deployment freshness policies and a
  read-only snapshot comparison. Operators can evaluate explicit `as_of`
  freshness, inspect deterministic evidence/diagnostic drift, and retain a
  stable review hash without changing release admission. Policy publication is
  append-only, idempotent, actor-bound, and RLS-protected; comparison never
  calls a provider, tool, deployment system, or model.
- Task 91 adds a hash-only qualification retention overlay. Retention policies
  are immutable and workspace/deployment-scoped; new snapshots, incident
  annotations, and freshness policies register metadata transactionally, while
  a bounded sync repairs only missing overlay rows. Read-only plans classify
  external-archival candidates and a recovery report detects missing/dangling
  metadata, authority-hash mismatches, and broken event links. This does not
  delete qualification history, perform external archival, or change release
  admission.
- Task 92 adds a bounded deployment-owned evidence refresh. Operators supply
  references to accepted signed imports at an explicit `as_of`; Fornix rejects
  stale or expired observations, supersedes active hash-only links in one
  transaction, and records a deterministic report. It never contacts a
  deployment, extends expiry, runs a provider/backup/HA operation, or claims
  remote exactly-once behavior.
- Task 93 adds a durable, workspace-scoped qualification refresh schedule with
  monotonic fences, expiry takeover, bounded retry/dead-letter transitions,
  append-only attempts/events, and recovery-drill validation from accepted
  signed imports. A stale worker cannot start a new refresh or advance the
  schedule. This remains a deployment-owned handoff: Fornix does not run cron,
  backups, PITR, HA/failover, provider calls, secret resolution, or live drills.
- Loop 112 adds a signed qualification gate for future non-local sandbox
  providers. The proof binds the exact provider/runtime/configuration, image,
  capabilities, numeric tested budget envelope, signer, target, and expiry to
  durable tool request and attempt hashes. Runtime-provider implementations
  must revalidate at launch. This does not ship an OCI/gVisor/microVM provider
  or qualify actual host isolation; only the limited local-process executor is
  currently available. Old uncertain non-local attempts without a qualification
  hash remain recovery-required and require deployment-owned inspection.
- Loops 113–116 add the path-free attempt contract, an authenticated bounded
  Unix-socket runner protocol, workspace/tool catalog foundations, a sealed
  OCI plan, and a durable cleanup queue with a fenced worker. The cleanup
  worker is composed only when a deployment explicitly injects a runner; the
  default local package injects none. No Moby Engine adapter, default runner
  process, or OCI provider is shipped, so OCI still fails closed. Protocol
  and fake-worker tests do not prove daemon behavior, operating-system
  isolation, hard resource enforcement, network denial, or restart recovery.
  Those require the official Moby client, a supported disposable runtime,
  PostgreSQL-backed qualification, and separate Linux and macOS evidence.
- Loop 117 binds typed OS/architecture/variant identity through non-local
  sandbox profiles, signed qualification evidence v2, runner protocol v2,
  admission, and the sealed OCI plan. It preserves local-process profile
  serialization and rejects old unbound evidence for new admission. This is
  still contract-level proof: there is no Moby adapter to inspect a real local
  image or prove no-pull behavior, and OCI remains unavailable.
- Loop 118 adds a pure inspection-only image-ID/platform verifier with stable
  redacted failures and no pull/tag API. Its fake-inspector tests do not prove
  that a future Moby adapter invokes it.
- Loop 119 propagates agent-run worker fences to the generic effect boundary.
  The current implementation validates live operation, task, agent-run, and
  effect authority while committing the unique dispatch intent in one short
  Postgres transaction. A takeover that wins before that transaction prevents
  dispatch and leaves the reservation resumable; a takeover after commit does
  not revoke an already-authorized one-shot external delivery. External calls
  remain at-least-once and uncertain. The full offline Go test and race suites
  and vet pass after this change; the takeover integration test is gated by
  `FORNIX_TEST_PG_DSN` and was not executed locally. The explicit
  Postgres-backed CI target is wired but its remote result has not been
  observed. This does not add runtime isolation; OCI remains unavailable
  pending the Moby lifecycle and live host qualification.
- Loop 120 replaces executable caller-authored SQL with a versioned
  structured read contract and a PostgreSQL implementation. The connector's
  disclosed row/byte budgets are checked after the driver materializes rows;
  they do not cap scan work or guarantee a hard allocation ceiling for one
  large database cell. Live PostgreSQL, least-privilege role, and server
  registration qualification remain separate requirements. See
  [Loop 120](268-loop-120-completion.md).
- Loop 121 adds an engine-independent OCI attempt lifecycle coordinator over
  an injected Engine interface, with deterministic recovery and cleanup
  decisions. Fake-Engine unit tests plus the full offline test, race, and vet
  suites pass. This is not a Moby adapter or runtime provider: no actual
  Docker daemon, cross-process runner ownership, transport-level output bound,
  or Linux/macOS isolation behavior has been qualified. See
  [Loop 121](270-loop-121-completion.md).

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
make smoke-universal-authority-conformance
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
make smoke-universal-schema
make smoke-universal-effect-authority
make smoke-universal-federation
make qualification-trust-distribution
FORNIX_TEST_PG_DSN='postgres://...' make qualification-deployment-evidence
FORNIX_TEST_PG_DSN='postgres://...' make qualification-deployment-refresh-scheduler
FORNIX_TEST_PG_DSN='postgres://...' make qualification-release-admission
make qualification-effect-conformance
make qualification-external-boundary
# deployment-owned, offline-only bundle validation/signing
fornix qualification boundary-validate --file deployment-boundary.json --external-effect true --as-of 2026-09-27T12:00:00Z
fornix qualification boundary-sign --file deployment-boundary.json --output signed-boundary.json --key-file "$FORNIX_QUALIFICATION_KEY_FILE" --key-id deployment-key --external-effect true --as-of 2026-09-27T12:00:00Z
```

For release output produced by GoReleaser, run:

```sh
make release-check
```

Postgres-backed results and measured latency/storage/replay throughput are
recorded in the loop completion notes under `docs/`.
