# Universal production roadmap status

Status: current public roadmap and qualification handoff; not a production
readiness declaration.

## Local verification snapshot — 2026-09-30

The offline Go unit suite (`GOPROXY=off go test ./...`), full race suite
(`GOPROXY=off go test -race ./...`), `go vet ./...`, and `make docs-check`
passed. The Go commands used temporary build caches that were removed after
each run. A passing package result does not mean its environment-gated tests
ran: `FORNIX_TEST_PG_DSN` was absent, so PostgreSQL-backed tests were skipped.
The Docker daemon socket denied access, and the Moby client module is not in
the local module cache. No live PostgreSQL or container-runtime qualification
was completed by this snapshot.

Loop 119 now propagates agent-run worker fences into generic effect authority
and atomically validates live fences while committing a unique dispatch
intent. Takeover before this transaction prevents invocation and leaves the
effect reservation resumable. A takeover after commit does not revoke an
already-authorized one-shot external delivery; external execution remains
at-least-once. The full offline test and race suites plus vet pass. The
stale-takeover integration case remains unexecuted locally because no
PostgreSQL test DSN is available. A DSN-required Make target is wired into the
Postgres CI job, but its remote result has not yet been observed.
See
[`265-agent-run-effect-dispatch-fence-foundation.md`](265-agent-run-effect-dispatch-fence-foundation.md)
and [`266-loop-119-agent-run-dispatch-fencing-completion.md`](266-loop-119-agent-run-dispatch-fencing-completion.md).

Loop 120 replaces the SQL reference adapter's executable caller-authored query
with a versioned structured read contract. It compiles identifiers and fixed
operators deterministically, binds values as parameters, validates the
allowlisted persistent relation and scalar columns, and applies a read-only,
repeatable-read transaction with bounded result disclosure. Offline unit/race,
vet, format, documentation, and connector-smoke checks passed in the recorded
snapshot; PostgreSQL-backed cases were skipped because no test DSN was
available. This does not bound database scan/planner work or qualify a live
database role. See
[`267-sql-read-only-query-contract-foundation.md`](267-sql-read-only-query-contract-foundation.md)
and [`268-loop-120-completion.md`](268-loop-120-completion.md).

Loop 121 is now scoped as an engine-independent OCI attempt lifecycle
coordinator over an injected Engine interface. The feature note makes clear
that this is a testable lifecycle decision layer, not the official Moby SDK
adapter, a shipped OCI provider, or operating-system isolation evidence. The
current environment has no accessible Docker daemon and the Moby SDK is not
available in the local module cache. The coordinator and fake-Engine tests are
implemented; targeted unit, race, vet, formatting, documentation, and
reference-connector smoke checks passed. The full offline Go test, race, and
vet suites also pass. Loopback and PostgreSQL smoke cases were skipped by the
sandbox, and live service qualification is still unverified. There is still
no real Engine adapter, durable cross-process runtime ownership, or
host-isolation qualification. See
[`269-oci-attempt-lifecycle-foundation.md`](269-oci-attempt-lifecycle-foundation.md)
and [`270-loop-121-completion.md`](270-loop-121-completion.md).

Loop 113 adds path-free request/response contracts; later Task 114 slices add
the authenticated host transport, workspace mount catalog, and trusted tool
catalog foundation. These remain building blocks, not a shipped Docker client
or OCI runtime provider. Request/profile/result hashes bind identity but do
not themselves authorize execution: the eventual provider must verify the
original durable tool request, derive its effective profile from trusted
policy, and pin the exact request hash to one runtime attempt before start.
See [`250-sandbox-runner-protocol-foundation.md`](250-sandbox-runner-protocol-foundation.md),
[`251-loop-113-completion.md`](251-loop-113-completion.md), and
[`252-host-sandbox-runner-foundation.md`](252-host-sandbox-runner-foundation.md).

Task 114's current slice adds fail-closed rejection of request/inherited
environment values for the offline runner profile and migration 081's durable,
workspace-scoped cleanup-intent queue. Fenced claim/renew/retry/completion and
append-only event history are implemented; cleanup is enqueued only when the
tool result, verified effect, and reconciled link agree on the same result
hash. Loop 116 adds a bounded cleanup worker and authenticated runner protocol,
but only explicit dependency injection starts it; the ordinary local package
still has no runner, and no OCI provider is wired into `fornix start`.
Earlier worker-integration verification was limited to unit, race, and
portable offline checks. The current verification snapshot above supersedes
those narrower results. PostgreSQL-backed tests remain unexecuted locally;
this is code/test progress, not runtime or production qualification. See
[`253-sandbox-cleanup-intent-foundation.md`](253-sandbox-cleanup-intent-foundation.md).

Loop 112 adds a trusted-signature gate for non-local sandbox providers,
including exact runtime/capability identity, workspace-pinned trust where
configured, tested numeric budgets, expiry, and attempt binding. The gate is
not a runtime: no OCI/gVisor/microVM provider is shipped or deployment-qualified.
See [`248-sandbox-backend-qualification-foundation.md`](248-sandbox-backend-qualification-foundation.md)
and [`249-loop-112-completion.md`](249-loop-112-completion.md).

Task 94 adds four offline, fake-first reference scenarios—incident response,
data pipeline, customer support, and infrastructure maintenance—through the
same generic operation/effect/replay seams. This expands qualification
coverage beyond repository work without claiming that any live provider or
external system is already supported. See
[`213-multi-domain-reference-workflows-foundation.md`](213-multi-domain-reference-workflows-foundation.md)
and [`214-loop-94-completion.md`](214-loop-94-completion.md).

This document is the honest handoff for the transformation tracked by
[Issue #38](https://github.com/Kshitij-M/fornix) and qualified through
[Issue #40](https://github.com/Kshitij-M/fornix/issues/40). It distinguishes
what the control plane proves today from what a deployment must still prove
before Fornix can safely govern arbitrary production systems.

Loop 111 makes local HTTP tests capability-aware: they run against real
`httptest` servers where loopback is available and explicitly skip otherwise.
The current full Go command exits successfully with skips, while PostgreSQL
integration tests remain unqualified without `FORNIX_TEST_PG_DSN`. See
[`247-loop-111-completion.md`](247-loop-111-completion.md); this changes test
portability, not the production readiness boundary.

Loop 110 anchors repository change operations to a checked `os.Root` for
precondition checks, mutation, and post-state hashing. Adversarial tests cover
an outside-symlink swap after checking and a concurrent create that must not
be overwritten. The broader change test also exposed and fixed a mismatch
where the observed hash included absent paths after rename/delete but the
expected tree hash did not. This narrows filesystem escape and verification
risk; it does not supply a kernel sandbox or multi-file transaction.
See [`245-root-anchored-change-io-foundation.md`](245-root-anchored-change-io-foundation.md)
and [`246-loop-110-completion.md`](246-loop-110-completion.md).

Loop 109 adds a typed, fenced recovery-finalization path for a tool attempt
whose external outcome is uncertain. It can publish a provider-attested,
hash-bound completed observation in one Postgres transaction, but the shipped
default registry has no non-local runtime that can produce such observations.
The new database integration tests are present but were not executed locally:
`FORNIX_TEST_PG_DSN` is unset and this machine has no pgvector extension. This
is implementation progress, not sandbox or production qualification; see
[`243-tool-sandbox-recovery-foundation.md`](243-tool-sandbox-recovery-foundation.md)
and [`244-loop-109-completion.md`](244-loop-109-completion.md).

## What Fornix is now

Fornix is a domain-neutral, Postgres-authoritative work control plane with a
repository adapter and a fake-first incident workflow. The generic path now
has:

```text
typed intent
  → workspace/actor/capability admission
  → durable operation identity and idempotency
  → deterministic plan/queue claim and monotonic fence
  → adapter-owned heartbeat and bounded execution boundary
  → evidence/artifact/result/verification references
  → replayable state history and Work Receipt seams
```

The worker boundary added in Loop 43 is intentionally adapter-owned. It does
not silently choose a provider, resolve a secret, execute a remote effect, or
turn a process-local callback into a second authority.

## Historical implementation work: durable capability rate admission

Loop 97a now consumes the existing signed `RateLimitPerMinute` capability
field in PostgreSQL admission and routes the server worker, generic workflow
reads, and incident workflow observations through the same durable decision
before adapter invocation. The scope is workspace/connector/capability name
over a rolling minute; it is not provider-account-global. Migration 079,
concurrency/duplicate/rollback tests, a generic workflow non-invocation test,
CI wiring, and the feature/measurement note are present. Full database-backed
qualification and workload measurements remain pending because this local
run has no disposable Postgres DSN. Details: [`219-capability-rate-admission-foundation.md`](219-capability-rate-admission-foundation.md)
and [`220-loop-97-rate-admission-completion.md`](220-loop-97-rate-admission-completion.md).

Loop 98 closes a separate workflow scheduling correctness gap: a retry's
`next_retry_at` is now carried into the operation queue projection, and the
fenced `StartStep` path rejects attempts before the persisted deadline using
the PostgreSQL clock. Offline contract coverage and a DSN-gated end-to-end
queue/direct-start test are added. The integration test is included in CI, but
has not run locally: the sandbox denies PostgreSQL's required System V shared
memory and does not permit access to the Docker daemon. See
[`221-workflow-retry-deadline-foundation.md`](221-workflow-retry-deadline-foundation.md)
and [`222-loop-98-completion.md`](222-loop-98-completion.md).

Loop 99 connects that queue deadline to capability admission. A saturated
workspace/connector/capability window now records its exact next eligibility
time in immutable admission history; generic read-only workflows enter
`awaiting_retry`, and a crash after the denial but before workflow completion
can recover that proven no-connector-call result under the current workflow
fence. Migration 080 is backward-compatible and avoids scanning historical
admission rows. Unit tests pass locally; database migration, concurrency, and
crash-recovery qualification is included in CI but cannot be claimed locally
without the disposable Postgres endpoint. See
[`223-capability-rate-retry-scheduling-foundation.md`](223-capability-rate-retry-scheduling-foundation.md)
and [`224-loop-99-completion.md`](224-loop-99-completion.md).

Loop 100 closes the corresponding runtime-resumption gap: `awaiting_retry` is
now eligible for deterministic runtime advancement, while PostgreSQL remains
the due-time authority. Multiple pending steps retain the earliest queue
deadline; run status and wait details remain aligned with the pending step;
recovery and explicit approval/human/external waits cannot be bypassed by a
timer retry. Offline unit, race, vet, formatting, and documentation checks pass.
The new due/early/multiple-step PostgreSQL runtime tests are wired into the
qualification target but were skipped locally because no disposable DSN or
listening PostgreSQL instance is available. Do not treat Loop 100 as
database-qualified until that target passes. See
[`225-workflow-due-retry-advancement-foundation.md`](225-workflow-due-retry-advancement-foundation.md)
and [`226-loop-100-completion.md`](226-loop-100-completion.md).

## Verified on the current branch

- Generic operation identity, plans, lifecycle transitions, attempts, results,
  callbacks, effect reservations, recovery state, replay, and idempotency are
  durable in Postgres.
- Claims are bounded, deterministic, workspace-scoped, `SKIP LOCKED`, and
  protected by monotonic operation fencing. Active leases are excluded and
  expired leases are recoverable.
- Declared operation resources are serialized transactionally with independent
  monotonic resource fences. Workspace-wide active-operation quotas are
  available through `ClaimReadyWithOptions`, and the explicit-workspace
  supervisor provides bounded round-robin turns with cancellation.
- The adapter-owned worker keeps a lease alive, cancels on lease loss, reports
  failures without hot-looping, releases successful claims, and preserves
  expiry-based takeover.
- Generic operation HTTP/CLI paths, CI integration, a real HTTP/CLI smoke, Go
  tests, race tests, migration checks, and documentation checks are green.
- Migration 046 installs fail-closed RLS policies on every current table with
  `workspace_id`; the role-separated qualification helper transfers table
  ownership to a migration role, verifies a non-owner `NOBYPASSRLS` runtime
  role, and exercises the scoped API-key authentication function. Local
  development remains owner-role compatibility mode, so deployment-specific
  application-context and pool-hygiene evidence is still required.
- Migration 047 adds durable workspace-scoped credential leases, monotonic
  fences, revocation epochs, append-only lease events, and exact-fence
  validation. Secret bytes remain behind an injected resolver; the local
  environment path is still development-only.
- Connector admission can require detached Ed25519 trust snapshots with
  signer verification, expiry, and strictly increasing signed revisions.
  Unsigned snapshots remain an explicit development compatibility mode.
- Connector and model HTTP traffic now share a controlled egress client that
  enforces destination, DNS, redirect, private-network, proxy, timeout, and
  request/response byte policy at the transport boundary. Local tests cover
  private DNS answers, redirect denial, oversized bodies, and uninspectable
  transports.
- Migration 048 adds durable workspace-scoped trust signers, append-only
  signer events, and signed policy history. `TrustCatalogStore` verifies
  detached signatures, expiry, active signer status, policy hashes, and
  monotonic revisions before returning a policy to the process registry.
- Migration 049 adds append-only cross-authority links. Admission, operation
  result, and Work Receipt writes now bind operation/capability/policy/trust
  hashes, credential and operation/task fences, effect reservations, and
  evidence/artifact/result/receipt references in their source transaction.
  Bounded `authority-links` inspection is available over the operation store
  and HTTP API. Fresh-Postgres integration tests cover duplicate delivery,
  stale-fence rejection, workspace isolation, and rollback recovery.
- Migration 050 adds opaque managed-source version and source-expiry facts to
  credential leases. `ManagedSecretResolver` carries workspace identity into
  an injected secret authority; the bounded HTTP manager protocol is built on
  controlled egress, rejects redirects and oversized/malformed responses, and
  never places secret bytes in durable state. Lease acquisition and renewal
  bind to the exact source version and expiry. Local profile resolution remains
  an explicit development compatibility path.
- Migration 051 adds an append-only, workspace-scoped signed schema catalog.
  Catalog entries bind connector and capability definition hashes to bounded
  input/output schema versions and hashes. Ed25519 verification, signer status,
  expiry, monotonic revision checks, duplicate-safe Postgres publication/load,
  and opt-in admission enforcement are qualified. Raw schema documents and
  executable validators are intentionally out of scope.
- Migration 052 binds the verified schema-catalog identity and managed
  credential source facts to effect reservations and cross-authority links.
  Effectful admission now establishes the durable authority envelope before
  reservation; exact credential-lease materialization preserves the admitted
  fence; result and receipt links inherit the same facts; and stale, missing,
  mismatched, expired, or revoked facts fail closed. The local HTTP adapter is
  the first authority-aware effectful adapter; other adapters still require
  conformance work.
- Migration 054 binds generic operation/effect identities to typed,
  workspace-scoped domain ledgers. Task 57 adds migration 055 so model calls
  and tool runs preserve `recovery_required` when an external outcome may be
  unknown. The state is non-terminal and duplicate delivery fails closed until
  a bounded domain-specific reconciler resolves it.

## Production gates still open

These are not documentation-only gaps. Each gate needs implementation,
deployment evidence, failure-injection coverage, and an operator runbook.

### P0 — authority and security

1. **Deployment database qualification.** The current migration and
   role-separated qualification cover every current workspace-scoped table,
   a non-owner `NOBYPASSRLS` runtime role, grants, scoped authentication, and
   credential leases. Remaining work is deployment-specific pool hygiene,
   unset-context fail-closed evidence, migration rollout/rollback, and
   qualification against the intended hosted topology.
2. **Managed credential integration.** The workspace-scoped resolver,
   controlled HTTP manager seam, source-version binding, source-expiry
   bounding, rotation, revocation, audit metadata, and redacted failure path
   now exist. A deployment must still inject and qualify its production
   secret-manager/KMS adapter, workload identity or mTLS authentication,
   short-lived leases, zeroization, rotation lag, and failure evidence. Task
   85 additionally binds the exact hash-only egress, destination, and
   network-boundary envelope to model, connector, embedding, and federation-
   poll reservations; deployment still owns the secret-manager, KMS,
   mTLS/workload-identity, and hosted-boundary qualification.
3. **Trust distribution and schema admission.** The registry now verifies
  signed connector/capability snapshots and signed schema catalogs, rejecting
  unsigned, expired, downgraded, tampered, revoked, or schema-mismatched
  admission facts. Durable signer, policy, and schema-catalog history now
  exists in Postgres with revocation-aware loading, and the effect authority
  slice carries the verified catalog identity into reservations and result/
  receipt links. Production still needs signer rotation ceremony,
  deployment-wide catalog distribution/lag handling, startup catalog reload,
  adapter-wide conformance, and signed release/image artifacts at startup and
  operation admission.
4. **Central outbound policy enforcement.** A shared controlled HTTP boundary
   now enforces destination, DNS, redirect, private-network, proxy, credential,
   and byte/time budgets for the HTTP connector and model providers. Production
   still needs deployment-specific proxy/DNS policy, resolver caching and
   rebinding evidence, certificate policy, and live connector conformance.
   Task 85 makes the normalized policy and destination hashes durable and
   requires them in strict production effect reservations; a local hash is not
   hosted network proof.

### P0 — execution correctness and isolation

5. **Worker deployment composition and fairness qualification.** The generic
   supervisor, workspace quota, bounded backpressure primitive, declared
   resource serialization, starvation-resistant ordering, and cancellation
   boundary now exist. Remaining work is deployment wiring, explicit durable
   workspace inventory, weighted priorities/resource budgets, connection-pool
   qualification, and load/soak evidence. Keep external-effect
   reconciliation separate from ordinary queue claims.
6. **Complete mutation linkage.** The generic admission/result/receipt path now
  has transactional hash-and-reference linkage and bounded inspection, and
  effect reservations carry the same schema and managed-credential authority
  facts as admission. The remaining qualification is adapter-by-adapter
  coverage for session, artifact, cost, and live external-effect references,
  plus deployment proof that every production adapter supplies the trust,
  exact credential-lease, egress, and verification authorities before
  execution. Task 57 extends the common dispatcher and typed link to the
  server-composed model, tool, and repository-change boundaries. Remaining
  work includes embedding-provider coverage and atomic specialized-ledger/link
  composition where current stores expose separate transaction seams. Task
  74A now binds agent-run owner and fence facts into model-call and tool-run
  reservation, attempt, start, and completion transactions; stale run workers
  fail closed even when a taskless run has no task fence. This remains a
  repository-owned qualification slice, not deployment proof.
7. **Sandbox and boundary qualification.** Provide an explicit backend matrix
   for filesystem, process, network, SQL, HTTP, MCP, and container execution.
   State exactly what local process execution can and cannot isolate; qualify
   the stronger backend before unattended effectful work. Loop 109 adds the
   exact-attempt reconciliation contract and atomic finalization path, but no
   non-local sandbox provider is shipped, and its Postgres recovery test has
   not yet run against the supported database topology. Loop 110 pins change
   operations to a root handle, but OS sandboxing and independent concurrent
   repository-writer isolation remain open.

### P1 — durability and operations

8. **Backup, restore, and availability.** Add scheduled backups, WAL/PITR,
   restore-owner procedures, HA/failover design, migration forward/rollback
   rules, and deployment-specific RPO/RTO evidence. Recompute replay, artifact,
   provenance, and receipt hashes after restore.
9. **Retention and storage lifecycle.** Add event partitioning, artifact cold
   tiers or an explicitly bounded Postgres-only scale envelope, retention
   compaction, tombstones, reference safety, corruption alerts, and storage
   growth/restore benchmarks.
10. **Load and soak qualification.** Measure p50/p95/p99 latency, lock waits,
    WAL, pool saturation, duplicate work, workspace fairness, failure recovery,
    and replay throughput across single-node and intended production
    topologies. Local smoke numbers are not production SLOs.
11. **Security and support operations.** Add adversarial prompt-injection,
    retrieval contamination, SQL/path/shell injection, secret leakage,
    privilege-escalation, callback forgery, and API/MCP abuse suites. Add
    bounded support bundles, alerts, incident runbooks, and privacy-safe
    diagnostics.

### P1 — ecosystem and product qualification

12. **Live connector conformance.** Qualify HTTP/API, SQL, ticketing,
    monitoring, cloud, deployment, and other adapters independently. A fake
    connector proves control-plane semantics; it does not prove a provider's
    idempotency, verification, rate limits, or outage behavior.
13. **Workflow and multi-agent scale.** Extend the bounded workflow runtime
    with multi-step fairness, sub-run ownership, human approval, callback
    authenticity, compensation, and deterministic replay at the intended
    concurrency. Do not make “multi-agent” a bypass around the same authority.
14. **Release and compatibility policy.** Publish signed multi-platform
    binaries/images, SBOM and notices, API/contract compatibility rules,
    migration support windows, connector conformance requirements, and an
    upgrade/rollback guide.

## Dependency order

The next implementation slice should qualify the authority envelope across
all effectful adapters while the resolver, durable lease, authority links,
supervisor, and schema catalog primitives are tested against the role-separated
database boundary. The sequence is:

```text
database/credential authority
  → managed secret resolver qualification
  → signed schema/catalog admission and outbound enforcement
  → effectful-adapter authority linkage and deployment distribution
  → worker supervisor, fairness, quotas, and resource serialization
  → complete evidence/artifact/receipt linkage
  → sandbox and live connector qualification
  → backup/HA/retention/load/security operations
  → ecosystem release and production support
```

Do not close Issue #38 or call Fornix production-ready until every applicable
gate has executable evidence, a measured operating envelope, and a recovery
runbook. “Universal” describes the contract and adapter model; it is not a
claim that every production system is already supported.

## Tasks 54–57 status

Task 54 added the first mandatory composition guard for universal effect
authority: effectful capabilities can be required to use the authority-aware
adapter seam, operation fences can be revalidated immediately before dispatch,
and the server has a durable signed trust/schema catalog reload path with
operator-visible readiness status. The fake incident effect adapter is covered
by the same authority-aware conformance rule.

Task 55 adds the first shared durable effect dispatcher. The generic HTTP
effect path and the approval-gated incident remediation path now reserve the
effect before adapter dispatch, revalidate live authority before and after the
provider call, and classify duplicate, stale, crash, and uncertain outcomes.
Migration 053 adds workspace-scoped provider identity uniqueness. Task 56 adds
migration 054 and the typed, append-only domain-effect link authority. The
generic HTTP and fake incident paths now bind their specialized domain identity
to the generic effect; the universal transformation remains in progress.

This is a qualification slice, not a production-readiness declaration. Task
57 composes server model, tool, and repository-change effects through the same
child-operation, fenced dispatcher, and typed-link seam. Task 58 extends
fenced ownership to direct agent-run HTTP mutations, fixes the multi-workspace
scheduler inventory path, requires a production agent loop to carry an
explicit run lease, and adds versioned append-only domain-link recovery
transitions. Specialized ledgers remain authoritative and preserve explicit
`recovery_required` when a provider/process/filesystem outcome may be unknown.
Task 59 now applies that boundary to embeddings: the typed gateway, migration
056 ledger, generic child effect, domain link, vector replay, scope
propagation, stale-call recovery, task-fence validation, and bounded workspace
backfill are implemented. Migration 057 now adds immutable embedding-target
attachments and composes them transactionally with repository-ingestion chunk
projection/checkpoint batches, memo/symbol writes, RAG chunk upserts, and
backfill updates. The ledger/gateway and attachment paths are locally
qualified; typed provider reconciliation and bounded query-vector retention
are now implemented as a fail-closed foundation. Task 61 now composes the
generic effect, domain link, embedding ledger, recovery audit, and typed event
through one transaction-local coordinator under the existing effect-recovery
fence. It also adds a hash-only query-use ledger and adaptive preflight for
legacy memo, symbol, and RAG routes. Task 62 now gives legacy memo and RAG
ranking an explicit bounded request reference time, stable composite-score
ordering, and filter-consistent preflight. Provider reconciliation and full
production cost accuracy remain open. Process-level crash/restart
qualification, live provider verification, signer-rotation ceremony, and
production catalog distribution also remain open. Task 63 now adds the
reusable transaction-local workspace boundary to legacy memo, RAG, chunk,
symbol, backfill, and session routes, and the role-separated qualification
executes a real authenticated retrieval request as a non-owner NOBYPASSRLS
runtime role. Legacy global federation remains explicitly unscoped, and Task
64 quarantined the historical compatibility surfaces behind both an explicit
non-production flag and the dedicated `legacy:global_admin` capability. The
first HTTP effectful adapter also preserves the durable reserved effect
identity in its result. The global tables still require a workspace-scoped
replacement and migration proof before they can be considered universally
safe. Task 65 adds workspace-scoped coordination messages and router
observations with append-only events, workspace/idempotency uniqueness,
bounded read-after-sequence pagination, deterministic recommendations, and
runtime-role RLS qualification. Historical federation remains quarantined;
no global row is assigned to a workspace by inference. Task 66 adds the
workspace-scoped federation peer projection, append-only peer commands,
monotonic peer leases, bounded poll attempts, managed credential references,
controlled egress, deterministic remote-message import, and explicit recovery
for unknown remote outcomes. Ordinary peer registration/listing now uses the
workspace API and strict decoding rejects the historical raw-token payload.
The packaged fake-incident effectful adapter also preserves the generic
dispatcher effect identity. Historical global federation remains quarantined;
it is not silently migrated. Task 67 now adds explicit server dependency
injection for a managed credential authority, a process-boundary token-source
adapter, fenced recovery-attempt takeover, response-hash-checked local
reconciliation, and redacted historical federation quarantine with RLS and
bounded pagination. The new production poll path still never falls back to a
raw bearer token or ambient provider secret.

Task 68 adds a deployment-shaped credential authority test double, explicit
mTLS certificate-chain/name/pin validation, redacted rotation/revocation
observations, partitioned retention tombstones for operational federation
rows, backup fingerprints for federation authority state, and a bounded
disposable federation capacity qualification. It does not claim a hosted
secret-manager integration, certificate revocation service, HA/PITR, or
production SLO.

Task 69 adds an opt-in, provider-neutral authority qualification probe over
the existing `SecretManager`/`TokenSource` seams and a bounded retention owner
that acquires the `federation.retention` consumer lease per workspace. The
owner supplies its exact fence inside the retention transaction, so takeover
cannot leave a stale process able to delete operational rows or append a
retention event. The live probe emits only redacted source-version/timing
metadata and is skipped unless an operator supplies explicit deployment
environment metadata.

Task 70 adds a redacted, hash-stable adapter-conformance report over the
existing connector registry and a bounded PostgreSQL topology qualification.
Read-only or recorded adapter checks can be replayed without exposing raw
errors; effectful checks require an explicit operator opt-in. The topology
probe measures writable-primary, WAL/archive, workspace transaction-context,
pool-hygiene, and bounded acquisition prerequisites without claiming a
failover or PITR drill it did not execute.

Task 71 adds the common `QualificationReport` contract and an in-memory
builder that adapts connector conformance into the same redacted evidence
shape as authority, certificate, topology, retention, load, backup, PITR,
failover, partition, and identity-rotation observations. The native CLI can
validate deployment-produced reports offline, rejecting unknown fields,
unbounded input, duplicate cases, invalid hashes, and passed drills without
evidence. This creates a comparable evidence handoff without making Fornix
the authority for a deployment's raw secrets or recovery system.

Task 72 adds a bounded deterministic adapter matrix over the built-in HTTP,
SQL, repository, and fake incident read seams. It sorts explicitly named
entries, preserves workspace and capability hashes, aggregates outcomes
monotonically, and blocks effectful capabilities unless the existing
authority-aware opt-in is supplied. The matrix is covered by a local fixture
test and CI target; it does not claim live provider idempotency, credential,
database-topology, failover, PITR, or external-effect evidence.

Task 73 composes the durable generic operation queue into the server runtime
for the safe read/observation subset. The server refreshes an explicit active
workspace inventory, schedules bounded turns with the existing supervisor,
re-resolves active identity permissions, advances operations under the exact
Postgres fence, and records hash-only results. SQL claim filtering excludes
unplanned, unknown-effect, and effectful plans; those remain with the
authority-aware dispatcher and connector-owned execution path.

Task 74A closes the remaining repository-owned agent-run effect binding gap.
The loop copies its worker lease into model and tool requests, migration 065
persists the binding, and the model/tool ledgers validate the exact owner and
fence in the same transaction as every durable effect transition. Duplicate
requests cannot be rebound to another run, and stale results cannot advance a
ledger. External execution remains explicitly at-least-once; this does not
replace deployment-owned provider reconciliation or live connector drills.

Task 74B closes the generic dispatcher outcome-link gap for the common
authority path. Verified effects now append one reconciled link transition;
uncertain or missing-effect outcomes append recovery-required transitions; and
duplicate dispatch returns the durable link state without invoking again. The
initial reservation/link binding remains a separate transaction seam, and
specialized ledgers still need adapter-specific composition coverage.

Task 75 adds the portable qualification runner and evidence bundle. Check
registration, execution order, cancellation, report size, redaction, manifest
hashing, and bundle merge are deterministic and bounded. The native CLI and CI
wrapper generate and validate one ephemeral offline bundle without contacting
Postgres, providers, tools, brokers, or deployments. This validates evidence
composition; it does not create deployment proof.

Task 76 adds the authority-aware effect qualification observation seam. A
dispatcher-backed probe can now return a bounded, hash-only observation proving
reservation identity, domain-link identity, reconciled result, Work Receipt
linkage, duplicate suppression, stale-fence rejection, workspace isolation, and
replay stability. The common runner records whether a check is offline or
external, blocks external checks by default, and permits them only through an
explicit caller option. This composes existing PostgreSQL dispatcher tests into
portable evidence without pretending that a local fake is deployment proof.

Task 77 adds the explicit disposable-PostgreSQL bridge. It composes the real
operation, admission, effect, domain-link, Work Receipt, and authority-link
stores around a deterministic fake invoker; proves duplicate suppression,
receipt-link hashing, rollback before receipt commit, stale-fence rejection,
workspace isolation, and replay stability; and emits only the Task 76
hash-only observation. The command is opt-in and requires a caller-confirmed
disposable DSN. Append-only authority history is deliberately preserved rather
than bypassed or deleted, so the caller disposes the ephemeral database after
the run. It is repository evidence, not hosted production certification.

Task 78 adds the signed deployment-evidence importer. Signed bundles bind
workspace, target, manifest, runner, commit, environment-name names, and a
hash-only observation to an Ed25519 subject. Offline validation, external
trust-key binding, deterministic merge, duplicate suppression, strict scope
checks, bounded key handling, restrictive atomic writes, and CLI/CI coverage
are implemented. Private keys remain deployment-owned and are never persisted
or represented by Fornix. The embedded public key proves integrity only; it
does not authorize a signer or certify deployment truth. The `import` command
is intentionally a validation boundary and does not create an implicit
database row.

Task 79 adds the deployment-owned qualification trust catalog and authorized
import boundary. Migration 066 stores workspace/deployment-scoped public
signers, append-only lifecycle events, immutable accepted signed bytes, source
hashes, import provenance, and import audit history with transaction-local
workspace RLS. Registration, atomic rotation, revocation, validity windows,
strict signature/scope verification, dry-run validation, duplicate replay,
conflict detection, bounded disclosure, and separate read/import/admin
authorization are implemented. Embedded public keys still cannot authorize
themselves; only the durable catalog can admit an import. The existing offline
`qualification import` command remains non-mutating, while
`qualification import-authorized` uses the authenticated durable route.

This remains repository-owned qualification evidence, not deployment proof.
Private-key custody, HSM/KMS ceremonies, trust distribution and lag policy,
startup snapshot conformance, live provider evidence, backup/restore, and
production topology qualification remain open.

Task 80 adds a signed, revisioned deployment trust snapshot. The Task 79
catalog authorizes snapshot publishers; the snapshot distributes a bounded
public signer set for new qualification imports. Migration 067 preserves the
exact snapshot bytes, append-only publication/revocation events, monotonic
workspace/deployment revisions, and the import revision/hash binding. Snapshot
publication, current-load, revocation, bounded disclosure, CLI operations,
RBAC routing, and deterministic contract/store/server tests are implemented.
When explicitly required, startup/readiness loads a current snapshot for each
workspace under `FORNIX_QUALIFICATION_DEPLOYMENT_ID`; missing, expired,
revoked, malformed, or publisher-untrusted snapshots keep readiness false.
Historical Task 79 imports remain replayable and are never retroactively
rewritten. This is still a repository-owned authority slice: deployment
signing ceremonies, private-key custody, trust distribution transport, and
hosted topology evidence remain deployment-owned.

Task 81 adds the durable release/evidence index and read-only qualification
gate. An immutable release captures the exact trust snapshot used at
registration. Evidence links reference accepted Task 79 imports by release,
kind, and hash; raw signed bytes remain in the import authority. Duplicate
registration/link requests are idempotent, later trust snapshots cannot be
linked to an older release, and the gate fails closed for missing, failed,
stale, or unresolved evidence. The HTTP and CLI surfaces are workspace/RBAC
scoped. The gate is an auditable admission projection, not a deployment
executor and not proof that a backup, HA topology, provider, or external
effect actually occurred.

Task 82 adds hash-only release verification and the deterministic startup/
admission binding. Deployment-owned verification facts bind an artifact or
image identity and attestation hash to the immutable release, exact trust
snapshot, and Task 81 gate hash in one transaction. The read-only admission
decision is exposed through HTTP/CLI, preserves expiry/revocation history, and
can be required by production readiness with an explicit release ID. This
proves Fornix's binding and fail-closed decision logic; it does not verify a
registry, pull an image, execute a rollout, or certify external deployment
truth.

Task 83 makes that decision consumable by the domain-neutral operation
authority. A generic effectful operation can carry a bounded, workspace-scoped
reference containing release, artifact, gate, trust-snapshot, and decision
hashes. The operation store re-evaluates the reference inside the same
Postgres transaction that reserves an external effect; missing, stale,
revoked, expired, mismatched, and cross-workspace facts fail closed. The
operation request remains the durable identity, so replay and duplicate
delivery preserve the original reference without copying deployment payloads.
`FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS` enables strict consumption and
is automatic in production. This is still a control-plane boundary: it does
not execute deployments, verify registries, or claim exactly-once external
execution.

Task 84 closes the next composition gap. Connector capabilities now have a
deterministic, all-workspace authority-conformance inventory that requires the
authority-aware execution seam, an effect descriptor, provider idempotency and
verification metadata, and an external-effect execution profile. The server
also validates a static manifest for every dynamic effect path—model complete,
model stream, embedding generation/reconciliation, tools, and change
application—and passes the current hash-only deployment-admission reference
into each child operation. Strict startup fails closed if a registered
effectful connector is incomplete or if its required release authority is not
available. The process-local manifest is not a second authority and does not
qualify a live provider or claim exactly-once execution.

Task 85 closes the managed-credential and controlled-egress composition gap.
The typed external-boundary envelope contains only the exact egress-policy,
destination-policy, network-boundary mode, and network-boundary hash. It is
copied into generic effect reservations, operation authority links,
domain-effect links, and federation poll attempts. Provider and connector
transports derive it from the policy they actually use; the dispatcher checks
that admission, credential lease/source facts, deployment admission, fences,
and boundary identity agree before and after the external call. Strict
production configuration fails closed before a missing envelope can dispatch.
The new `external-boundary` qualification command is deterministic and
offline. It proves repository-owned composition only; hosted secret-manager,
proxy/firewall, DNS-rebinding, workload identity, live-provider, and remote
exactly-once behavior remain deployment-owned gates.

Task 86 adds the first typed deployment-owned evidence bridge for those
remaining external-boundary facts. Signed qualification reports can carry
bounded, hash-only observations for credential resolution, workload identity,
mTLS, DNS/rebinding, proxy/firewall enforcement, provider idempotency, and
external recovery. External-effect links derive the exact boundary hash,
evidence-set hash, and time-bounded expiry from the signed import. Release
admission and operation references preserve the same facts and fail closed on
missing, mismatched, expired, or ambiguous proof. This remains an evidence
consumption boundary: Fornix still does not collect deployment observations,
contact a secret manager, operate a proxy, resolve deployment DNS, call a live
provider, or claim exactly-once remote execution.

Task 87 adds the deployment-owned publisher boundary for that evidence. The
offline publisher validates only the existing bounded, redacted bundle and
signs it with an operator-held Ed25519 key. External-effect publication is
strictly one measured, passed, non-expired observation at an explicit `as_of`
time. `boundary-sign` and `boundary-validate` never contact external systems,
never persist private keys, and never authorize an operation; deployment
automation remains responsible for collecting and importing the evidence.

Task 88 adds the durable lifecycle for linked evidence. Revocation and
explicit replacement are workspace-scoped, actor-bound, idempotent, and
transactional. The active-only uniqueness boundary prevents two current proofs
for one release/kind; predecessor links and append-only lifecycle events remain
auditable. Gate evaluation now exposes historical links while admitting only
active links and reports deterministic revocation/supersession reasons.

This is still a repository-owned readiness gate. It does not mutate signed
imports, independently attest deployment truth, or perform live secret,
network, provider, HA, PITR, failover, or load checks.

Task 89 adds the operator observation layer for that gate. Readiness snapshots
capture the exact release hash, trust revision/hash, active evidence IDs, gate
hash, bounded missing/blocked reasons, and evaluation time in one transactional
PostgreSQL record. Their stable hashes exclude observation timestamps and actor
metadata, so unchanged facts replay identically while changed facts create new
append-only snapshots. Structured incident annotations link a snapshot to a
bounded disposition and optional evidence hash without accepting raw logs or
deployment payloads. The surface is advisory: operation admission continues to
re-evaluate the authoritative release/evidence rows and fails closed.

Task 90 adds explicit freshness policy and incident review qualification.
Policies are immutable, monotonically versioned, workspace/deployment-scoped,
and limited to bounded review age plus an operator `require_ready` hint. A
read-only review compares two snapshots against the current policy at an
explicit `as_of` time, reporting stale/future observations, ready-state drift,
gate changes, evidence additions/removals, and blocked-reason changes. The
review hash is deterministic and advisory; it cannot authorize, revoke, or
extend any release or external effect.

Task 91 adds qualification retention metadata and recovery evidence. Immutable
retention policies define bounded archival horizons and latest-record/incident
protection. Snapshot, annotation, and freshness-policy creation registers
hash-only metadata in the same transaction; a bounded cursor sync repairs only
missing overlay rows. Read-only retention plans and recovery reports are
deterministic at an explicit `as_of`, preserve source hashes, and fail closed
on missing/dangling metadata or broken references. Fornix still does not purge
authority history or operate an external archive.

## Remaining sandbox-runtime milestone

The separately managed host runner remains incomplete. Its architecture and
qualification matrix are documented in
[`252-host-sandbox-runner-foundation.md`](252-host-sandbox-runner-foundation.md).
Implemented foundations now include a host-only workspace mount catalog,
authenticated Unix-socket transport, bounded request cancellation/drain,
socket-path ownership checks, a trusted tool catalog, fixed-policy OCI plan
builder, durable Postgres cleanup authority and fenced worker, exact local
image inspection contract, and the engine-independent attempt lifecycle
coordinator in Loop 121. The coordinator's fake Engine validates lifecycle
decisions only; it is not an Engine implementation.

Still missing are the official Moby client adapter, a separately managed
runner process, default installation/startup composition, engine-backed
stream-to-sink integration and lifecycle recovery, and a qualified OCI provider. The
cleanup worker and runner protocol are composed only when explicitly
injected; the normal local package injects no runner and does not consume the
queue. Keep Docker daemon authority out of the Fornix control-server
container. The adapter must bind the exact attempt identity and sealed policy,
apply fixed resource/security controls, and recover by inspection only. Keep
the provider unavailable until signed evidence is produced on disposable,
supported targets. The next engineering milestone is to implement and
qualify that adapter using the officially supported Moby client modules;
this environment lacks those modules and an accessible Docker daemon.

The current host denies Docker daemon access and has no disposable PostgreSQL
DSN. Therefore this branch can run Go unit/race tests and static/doc checks,
but cannot claim live OCI, PostgreSQL RLS/recovery, or container isolation
qualification. Preserve these as explicit environment gates rather than
marking skipped checks passed.

Issue #40 remains open after those slices. Disposable-Postgres migration,
concurrency, RLS, and crash-recovery evidence; adversarial connector and
prompt-injection review; live-provider/credential conformance; backup/restore;
HA/PITR/failover; and topology-specific load/soak measurements are still
required before production readiness can be claimed. See
[`217-generic-effect-verification-foundation.md`](217-generic-effect-verification-foundation.md),
[`218-loop-96-completion.md`](218-loop-96-completion.md),
[`225-workflow-due-retry-advancement-foundation.md`](225-workflow-due-retry-advancement-foundation.md),
[`227-support-bundle-redaction-foundation.md`](227-support-bundle-redaction-foundation.md),
[`159-deployment-credential-retention-load-qualification-foundation.md`](159-deployment-credential-retention-load-qualification-foundation.md),
[`163-live-adapter-postgres-topology-qualification-foundation.md`](163-live-adapter-postgres-topology-qualification-foundation.md),
and [`165-live-provider-recovery-qualification-foundation.md`](165-live-provider-recovery-qualification-foundation.md).

The latest tool recovery slice has the same qualification boundary: contract
and local provider-selection tests pass, but its transaction rollback,
concurrent finalization, stale-fence, workspace-isolation, and redaction
assertions remain unverified until run against disposable Postgres. The default
local-process backend cannot reconcile a process after host/control-plane
failure, and there is no installed OCI, gVisor, or microVM provider.

Loop 115 closes one additional control-plane crash window: specialized tool
result finalization now shares the generic verified-effect/link/operation
transaction, and recovery can normalize an interrupted dispatching,
dispatched, or acknowledged effect plus linked tool run under a fresh effect
lease before asking the exact attempt-aware sandbox to reconcile. Conflicting
result hashes fail closed. Focused Go package suites pass, but all PostgreSQL
rollback, concurrency, and fencing cases are DSN-gated and did not execute
locally; the installed PostgreSQL lacks pgvector. This does not ship the Moby
client, OCI provider, or process-restart qualification. See
[255-tool-effect-finalization-recovery-foundation.md](255-tool-effect-finalization-recovery-foundation.md)
and [256-loop-115-completion.md](256-loop-115-completion.md).

Loop 116 adds a versioned private cleanup command/observation, a bounded
fenced consumer that renews its lease during runner calls, and optional server
composition behind explicit dependency injection. Focused contract,
transport, worker, and server package tests pass. The official Moby module
download could not complete because this environment cannot resolve
`proxy.golang.org`; there is still no OCI runtime, no runner in the default
package, and no live Postgres or Engine qualification. See
[`257-moby-runner-implementation-note.md`](257-moby-runner-implementation-note.md)
and [`258-loop-116-cleanup-consumer-slice.md`](258-loop-116-cleanup-consumer-slice.md).

Loop 117 adds explicit image OS/architecture/variant binding to non-local
sandbox profiles and signed qualification, versions the runner protocol, and
preserves the prior local-profile serialization hash shape. The image field is
defined as the exact local Docker image ID/config digest; no pull is permitted.
The contract, qualification drift, hash, protocol, and OCI-plan tests pass, but
the Engine adapter/image inspection still does not exist. No Moby lifecycle,
live Docker, or PostgreSQL runtime-role qualification is claimed. See
[`261-sandbox-image-platform-identity-foundation.md`](261-sandbox-image-platform-identity-foundation.md)
and [`262-loop-117-platform-bound-sandbox-qualification-completion.md`](262-loop-117-platform-bound-sandbox-qualification-completion.md).

Loop 118 adds an inspection-only boundary and plan verifier for exact local
image ID and platform, with redacted failures and no pull/tag API. This closes
the pure policy comparison only; there is still no Moby implementation to call
it and no container create/start/reconcile/cleanup lifecycle. See
[`264-loop-118-local-image-identity-verifier-completion.md`](264-loop-118-local-image-identity-verifier-completion.md).
