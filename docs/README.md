# Fornix documentation

Status: active public documentation map.

This is the public documentation map for Fornix. It is written for people who
want to use the alpha, operate it locally, review its design, or contribute to
the repository.

The product direction is **verifiable AI work infrastructure for long-running
production-system operations**. Fornix is intended to let teams delegate
serious work to AI without losing control of scope, cost, evidence, approval,
or recovery. Repository maintenance is the first adapter and qualification
workflow, not the product boundary. The current implementation is the durable
control and retrieval substrate behind that outcome; the [product vision](01-product-vision.md)
explains the distinction.

The universal transformation is the next product phase. It preserves the
repository adapter while generalizing the operation authority, policy/effect
boundary, connectors, and workflow runtime. Read the [universal work control
plane overview](68-universal-work-control-plane.md) before the chronological
foundation notes when evaluating the product direction.

## Choose a starting point

| If you want to know… | Read… |
| --- | --- |
| What Fornix is and why it exists | [`README.md`](../README.md) |
| What works today and what is not production-qualified | [`02-what-works-today.md`](02-what-works-today.md) |
| The meaning of workspace, operation, capability, fence, effect, and replay | [`03-concepts.md`](03-concepts.md) |
| How the universal control plane works across domains | [`68-universal-work-control-plane.md`](68-universal-work-control-plane.md) |
| What problem Fornix will own and how the product should feel | [`01-product-vision.md`](01-product-vision.md) |
| How to run, test, and smoke the service | [`DEVELOPMENT.md`](../DEVELOPMENT.md) |
| How to install, start, run, and operate the local package | [`63-fornix-local-operations.md`](63-fornix-local-operations.md) |
| How to contribute a change | [`CONTRIBUTING.md`](../CONTRIBUTING.md) |
| How to report security concerns | [`SECURITY.md`](../SECURITY.md) |
| How the project handles community conduct | [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md) |
| Which design rules are non-negotiable | [`00-fornix-foundation.md`](00-fornix-foundation.md) |
| How generic production-system operations are represented | [`66-domain-neutral-harness-foundation.md`](66-domain-neutral-harness-foundation.md) |
| How connectors and capabilities are explicitly registered and admitted | [`67-connector-capability-foundation.md`](67-connector-capability-foundation.md) |
| Which routes and request rules exist | [`53-http-api-reference.md`](53-http-api-reference.md) |
| What is actually qualified today | [`14-production-readiness-qualification.md`](14-production-readiness-qualification.md) |
| How to operate a generic operation and reconcile an external effect | [`89-universal-operator-runbook.md`](89-universal-operator-runbook.md) |
| How to run qualification commands and interpret evidence | [`90-qualification-runbook.md`](90-qualification-runbook.md) |
| How to operate a generic workflow across domains | [`215-generic-workflow-service-foundation.md`](215-generic-workflow-service-foundation.md) |
| How legacy global federation/router APIs are contained | [`91-legacy-global-surface-containment-foundation.md`](91-legacy-global-surface-containment-foundation.md) |
| How documentation should be written | [`52-documentation-guide.md`](52-documentation-guide.md) |
| Which reference projects informed the design | [`13-reference-reuse-matrix.md`](13-reference-reuse-matrix.md) |

## How to read the architecture record

The numbered notes are chronological and intentionally preserve the project’s
engineering history through Loop 120 of the universal-operation work:

- A **foundation** note explains the problem, invariants, authority boundary,
  schema/API shape, research and licensing decisions, cost budget, and planned
  acceptance tests for one slice.
- A **completion** note records what was implemented, how it was qualified,
  measured local results, and what remains limited or deferred.
- The cross-cutting foundation and qualification notes are the best current
  summaries. A completion note is the more reliable source when a historical
  foundation intention differs from the current implementation.

The chronological implementation notes cover the original repository
substrate and continue through Loop 120 of the universal-operation work,
alongside the packaging and transformation delivery records below. These
numbers record engineering slices; they are not a product-completeness
milestone. Fornix still requires real runtime isolation and broader production
qualification before it can be described as a complete production-system
operations harness.

The latest loopback test-preflight result is documented in
[`247-loop-111-completion.md`](247-loop-111-completion.md). Loop 112 adds the
signed admission contract and Loop 113 adds a path-free runner protocol, but
no authenticated runner or real isolated runtime is shipped or qualified; see
[`249-loop-112-completion.md`](249-loop-112-completion.md) and
[`251-loop-113-completion.md`](251-loop-113-completion.md). The full suite
previously exited successfully in this sandbox only with environment-dependent
test skips; that does not qualify skipped loopback or PostgreSQL paths.

Task 114 established the host-runner boundary design and acceptance matrix in
[`252-host-sandbox-runner-foundation.md`](252-host-sandbox-runner-foundation.md).
Subsequent slices add a host-only workspace mount catalog, authenticated
Unix-socket protocol, durable cleanup worker, platform-bound image identity,
and a pure local-image identity verifier. Those pieces are not wired to a
shipped Engine lifecycle: there is no Moby client, OCI provider, or live
isolation qualification. The request contract rejects caller-supplied and
inherited environment values until restart-safe credential redaction exists.
See the latest generic effect fence note,
[`265-agent-run-effect-dispatch-fence-foundation.md`](265-agent-run-effect-dispatch-fence-foundation.md),
and completion evidence in
[`266-loop-119-agent-run-dispatch-fencing-completion.md`](266-loop-119-agent-run-dispatch-fencing-completion.md).
Durable cleanup authority was introduced in
[`253-sandbox-cleanup-intent-foundation.md`](253-sandbox-cleanup-intent-foundation.md).
Loop 115 closes the tool-result/effect transaction gap and makes known
interrupted dispatch states recoverable without re-execution; this remains
unverified against PostgreSQL locally and does not provide an OCI runtime. See
[`255-tool-effect-finalization-recovery-foundation.md`](255-tool-effect-finalization-recovery-foundation.md)
and [`256-loop-115-completion.md`](256-loop-115-completion.md).
Loop 116 adds a bounded cleanup command/observation protocol, a fenced cleanup
worker, and optional server composition behind an explicitly injected runner.
It does not implement the Moby runtime or start a cleanup runner in the normal
local package. See [`257-moby-runner-implementation-note.md`](257-moby-runner-implementation-note.md)
and [`258-loop-116-cleanup-consumer-slice.md`](258-loop-116-cleanup-consumer-slice.md).

The current in-progress slice is the engine-independent OCI attempt lifecycle
coordinator described in
[`269-oci-attempt-lifecycle-foundation.md`](269-oci-attempt-lifecycle-foundation.md).
It is intended to make exact-attempt create/start/reconcile/cleanup decisions
testable before an official Moby SDK adapter exists; it is not itself a Moby
runtime or proof of operating-system isolation.

## Universal transformation roadmap

The historical loops built the durable control-plane substrate. The next
product phase generalizes that substrate beyond repository operations. The
canonical top-down explanation is [Universal AI work control plane](68-universal-work-control-plane.md);
the implementation sequence is tracked by [Issue #38](https://github.com/Kshitij-M/fornix/issues/38).

| Issue | Workstream | Current boundary |
| --- | --- | --- |
| [#41](https://github.com/Kshitij-M/fornix/issues/41) | Domain-neutral contracts and adapter boundary | Universal typed vocabulary; repository remains the first adapter |
| [#45](https://github.com/Kshitij-M/fornix/issues/45) | Connector and capability registry | Explicit process-local registration and fail-closed admission |
| [#39](https://github.com/Kshitij-M/fornix/issues/39) | Durable generic operation authority | Postgres-backed foundation for common operation identity, lifecycle, attempts, effects, leases, replay, and links; final qualification remains |
| [#46](https://github.com/Kshitij-M/fornix/issues/46) | Universal policy and external-effect admission | Alpha Postgres foundation for deterministic policy, approval, quota, and at-least-once effect recovery |
| [#43](https://github.com/Kshitij-M/fornix/issues/43) | HTTP/API and read-only SQL connectors | Alpha bounded reference adapters: HTTP read/list/idempotent-submit and SQL describe/query/explain; production qualification remains |
| [#44](https://github.com/Kshitij-M/fornix/issues/44) | Durable multi-step workflow runtime | Alpha foundation implemented; generic durable steps, waits, fences, replay, and bounded read-only fan-out |
| [#42](https://github.com/Kshitij-M/fornix/issues/42) | Multi-domain reference workflow | Fake-first incident workflow implemented; live connector and production qualification remain |
| [#40](https://github.com/Kshitij-M/fornix/issues/40) | Universal production qualification | In progress: authority/API qualification slice; security, scale, recovery, backup/restore, and operational evidence remain |

These issues are a dependency-ordered roadmap, not a claim that all listed
connectors or workflows are available in the alpha. Supporting issues
[#23–#30](https://github.com/Kshitij-M/fornix/issues?q=is%3Aissue+is%3Aopen)
remain open and should be re-scoped as the universal authority and
qualification work progresses. The Issue #46 design and delivery record is
[`70-operation-admission-effects-foundation.md`](70-operation-admission-effects-foundation.md)
and [`71-loop-25-completion.md`](71-loop-25-completion.md).

## Implementation loops

| Loop | Capability | Design note | Completion note |
| ---: | --- | --- | --- |
| 1 | Working baseline | — | [`15-loop-1-baseline-completion.md`](15-loop-1-baseline-completion.md) |
| 2 | Typed events and state deltas | [`16-event-state-delta-foundation.md`](16-event-state-delta-foundation.md) | [`17-loop-2-completion.md`](17-loop-2-completion.md) |
| 3 | Checkpointed projections | [`18-projection-subscription-foundation.md`](18-projection-subscription-foundation.md) | [`19-loop-3-completion.md`](19-loop-3-completion.md) |
| 4 | Consumer leases and fencing | [`20-consumer-lease-fencing-foundation.md`](20-consumer-lease-fencing-foundation.md) | [`21-loop-4-completion.md`](21-loop-4-completion.md) |
| 5 | Task execution and recovery | [`22-task-execution-foundation.md`](22-task-execution-foundation.md) | [`23-loop-5-completion.md`](23-loop-5-completion.md) |
| 6 | Retrieval and bounded context | [`24-retrieval-context-foundation.md`](24-retrieval-context-foundation.md) | [`25-loop-6-completion.md`](25-loop-6-completion.md) |
| 7 | Provenance and disclosure | [`26-provenance-disclosure-foundation.md`](26-provenance-disclosure-foundation.md) | [`27-loop-7-completion.md`](27-loop-7-completion.md) |
| 8 | Model gateway and providers | [`28-model-gateway-foundation.md`](28-model-gateway-foundation.md) | [`29-loop-8-completion.md`](29-loop-8-completion.md) |
| 9 | Tool policy and execution | [`30-tool-runtime-foundation.md`](30-tool-runtime-foundation.md) | [`31-loop-9-completion.md`](31-loop-9-completion.md) |
| 10 | Bounded agent loop | [`32-agent-loop-foundation.md`](32-agent-loop-foundation.md) | [`33-loop-10-completion.md`](33-loop-10-completion.md) |
| 11 | Agent-run scheduler | [`34-agent-run-scheduler-foundation.md`](34-agent-run-scheduler-foundation.md) | [`35-loop-11-completion.md`](35-loop-11-completion.md) |
| 12 | Identity, RBAC, and credentials | [`36-identity-rbac-credential-foundation.md`](36-identity-rbac-credential-foundation.md) | [`37-loop-12-completion.md`](37-loop-12-completion.md) |
| 13 | Content-addressed artifacts | [`38-artifact-storage-foundation.md`](38-artifact-storage-foundation.md) | [`39-loop-13-completion.md`](39-loop-13-completion.md) |
| 14 | Artifact-backed outputs and retention | [`40-artifact-output-integration-foundation.md`](40-artifact-output-integration-foundation.md) | [`41-loop-14-completion.md`](41-loop-14-completion.md) |
| 15 | Observability and replay evaluation | [`42-observability-evaluation-foundation.md`](42-observability-evaluation-foundation.md) | [`43-loop-15-completion.md`](43-loop-15-completion.md) |
| 16 | Retrieval quality and regression gates | [`44-retrieval-evaluation-quality-foundation.md`](44-retrieval-evaluation-quality-foundation.md) | [`45-loop-16-completion.md`](45-loop-16-completion.md) |
| 17 | Retrieval-surface capture and operator evaluation | [`46-retrieval-surface-capture-foundation.md`](46-retrieval-surface-capture-foundation.md) | [`47-loop-17-completion.md`](47-loop-17-completion.md) |
| 18 | Operator control and reference workflow | [`48-operator-reference-workflow-foundation.md`](48-operator-reference-workflow-foundation.md) | [`49-loop-18-completion.md`](49-loop-18-completion.md) |
| 19 | Resumable repository ingestion | [`50-repository-ingestion-foundation.md`](50-repository-ingestion-foundation.md) | [`51-loop-19-completion.md`](51-loop-19-completion.md) |
| 20 | Work Receipts and Verified Change Packet foundation | [`54-work-receipt-foundation.md`](54-work-receipt-foundation.md) | [`55-loop-20-completion.md`](55-loop-20-completion.md) |
| 21 | Approval-gated repository change artifacts and application | [`56-repository-change-foundation.md`](56-repository-change-foundation.md) | [`57-loop-21-completion.md`](57-loop-21-completion.md) |
| 22 | Deterministic post-change validation and re-index handoff | [`58-validation-foundation.md`](58-validation-foundation.md) | [`59-loop-22-completion.md`](59-loop-22-completion.md) |
| 23 | Workspace validation policy packs and verified change admission | [`60-validation-policy-packs-foundation.md`](60-validation-policy-packs-foundation.md) | [`61-loop-23-completion.md`](61-loop-23-completion.md) |
| 24 | Single-package local runtime and release qualification | [`64-fornix-local-release-qualification-foundation.md`](64-fornix-local-release-qualification-foundation.md) | [`65-loop-24-completion.md`](65-loop-24-completion.md) |
| 25 | Universal operation admission and external effects | [`70-operation-admission-effects-foundation.md`](70-operation-admission-effects-foundation.md) | [`71-loop-25-completion.md`](71-loop-25-completion.md) |
| 26 | Bounded universal reference connectors | [`72-reference-connectors-foundation.md`](72-reference-connectors-foundation.md) | [`73-loop-26-completion.md`](73-loop-26-completion.md) |
| 120 | Structured read-only SQL connector boundary | [`267-sql-read-only-query-contract-foundation.md`](267-sql-read-only-query-contract-foundation.md) | [`268-loop-120-completion.md`](268-loop-120-completion.md) |
| 121 | Engine-independent OCI attempt lifecycle coordinator | [`269-oci-attempt-lifecycle-foundation.md`](269-oci-attempt-lifecycle-foundation.md) | [`270-loop-121-completion.md`](270-loop-121-completion.md) |
| 27 | Durable multi-step workflow runtime | [`74-workflow-runtime-foundation.md`](74-workflow-runtime-foundation.md) | [`75-loop-27-completion.md`](75-loop-27-completion.md) |
| 28 | Multi-domain incident reference workflow | [`76-multi-domain-reference-workflow-foundation.md`](76-multi-domain-reference-workflow-foundation.md) | [`77-loop-28-completion.md`](77-loop-28-completion.md) |
| 29 | Universal production qualification: operation authority and API surface | [`78-universal-production-qualification-foundation.md`](78-universal-production-qualification-foundation.md) | [`79-loop-29-completion.md`](79-loop-29-completion.md) |
| 30 | Universal credential leases and capability trust admission | [`80-credential-egress-trust-foundation.md`](80-credential-egress-trust-foundation.md) | [`81-loop-30-completion.md`](81-loop-30-completion.md) |
| 31 | Shared destination and egress policy | [`82-egress-policy-foundation.md`](82-egress-policy-foundation.md) | [`83-loop-31-completion.md`](83-loop-31-completion.md) |
| 32 | Durable generic connector execution | [`84-generic-execution-foundation.md`](84-generic-execution-foundation.md) | [`85-loop-32-completion.md`](85-loop-32-completion.md) |
| 33 | External-effect reservation and reconciliation API | [`86-external-effect-reconciliation-foundation.md`](86-external-effect-reconciliation-foundation.md) | [`87-loop-33-completion.md`](87-loop-33-completion.md) |
| 34 | Legacy global surface containment | [`91-legacy-global-surface-containment-foundation.md`](91-legacy-global-surface-containment-foundation.md) | [`92-loop-34-completion.md`](92-loop-34-completion.md) |
| 35 | Terminal operation admission hardening | [`93-terminal-operation-admission-foundation.md`](93-terminal-operation-admission-foundation.md) | [`94-loop-35-completion.md`](94-loop-35-completion.md) |
| 36 | Operation result authority binding | [`95-operation-result-authority-foundation.md`](95-operation-result-authority-foundation.md) | [`96-loop-36-completion.md`](96-loop-36-completion.md) |
| 37 | Snapshot and cursor-based operation replay | [`97-snapshot-replay-foundation.md`](97-snapshot-replay-foundation.md) | [`98-loop-37-completion.md`](98-loop-37-completion.md) |
| 38 | Backup and restore qualification harness | [`99-backup-restore-qualification-foundation.md`](99-backup-restore-qualification-foundation.md) | [`100-loop-38-completion.md`](100-loop-38-completion.md) |
| 39 | Independent external-effect recovery ownership | [`101-external-effect-recovery-foundation.md`](101-external-effect-recovery-foundation.md) | [`102-loop-39-completion.md`](102-loop-39-completion.md) |
| 40 | Postgres workspace-isolation foundation | [`103-postgres-workspace-isolation-foundation.md`](103-postgres-workspace-isolation-foundation.md) | [`104-loop-40-completion.md`](104-loop-40-completion.md) |
| 41 | Generic operation capacity qualification | [`105-capacity-qualification-foundation.md`](105-capacity-qualification-foundation.md) | [`106-loop-41-completion.md`](106-loop-41-completion.md) |
| 42 | Generic operation queue and worker claims | [`107-generic-operation-queue-foundation.md`](107-generic-operation-queue-foundation.md) | [`108-loop-42-completion.md`](108-loop-42-completion.md) |
| 43 | Adapter-owned generic operation worker | [`109-generic-operation-worker-foundation.md`](109-generic-operation-worker-foundation.md) | [`110-loop-43-completion.md`](110-loop-43-completion.md) |
| 44 | Universal operation fairness, quotas, and resource serialization | [`112-operation-supervisor-resource-serialization-foundation.md`](112-operation-supervisor-resource-serialization-foundation.md) | [`113-loop-44-completion.md`](113-loop-44-completion.md) |
| 45 | Production role-separated PostgreSQL isolation and credential authority | [`114-production-role-separated-isolation-foundation.md`](114-production-role-separated-isolation-foundation.md) | [`115-loop-45-completion.md`](115-loop-45-completion.md) |
| 46 | Durable credential lease authority and redacted model evidence | — | [`116-credential-lease-authority-completion.md`](116-credential-lease-authority-completion.md) |
| 47 | Signed connector and capability trust admission | — | [`117-signed-trust-admission-completion.md`](117-signed-trust-admission-completion.md) |
| 48 | Central outbound policy and egress boundary | [`118-central-outbound-egress-foundation.md`](118-central-outbound-egress-foundation.md) | [`119-loop-48-completion.md`](119-loop-48-completion.md) |
| 49 | Managed credential and durable trust catalog | [`120-managed-credential-trust-catalog-foundation.md`](120-managed-credential-trust-catalog-foundation.md) | [`121-loop-49-completion.md`](121-loop-49-completion.md) |
| 50 | Transactional cross-authority operation linkage | [`122-authority-linkage-foundation.md`](122-authority-linkage-foundation.md) | [`123-loop-50-completion.md`](123-loop-50-completion.md) |
| 51 | Managed credential resolution and source-version authority | [`124-managed-credential-resolution-foundation.md`](124-managed-credential-resolution-foundation.md) | [`125-loop-51-completion.md`](125-loop-51-completion.md) |
| 52 | Signed capability-schema admission and catalog distribution | [`126-signed-capability-schema-foundation.md`](126-signed-capability-schema-foundation.md) | [`127-loop-52-completion.md`](127-loop-52-completion.md) |
| 53 | Effectful adapter authority and exact credential-fence linkage | [`128-effectful-adapter-authority-foundation.md`](128-effectful-adapter-authority-foundation.md) | [`129-loop-53-completion.md`](129-loop-53-completion.md) |
| 54 | Universal effect-authority conformance and startup qualification | [`130-universal-effect-authority-conformance-foundation.md`](130-universal-effect-authority-conformance-foundation.md) | [`131-loop-54-completion.md`](131-loop-54-completion.md) |
| 55 | Durable effect dispatcher and effectful-path conformance | [`132-durable-effect-dispatcher-foundation.md`](132-durable-effect-dispatcher-foundation.md) | [`133-loop-55-completion.md`](133-loop-55-completion.md) |
| 56 | Unified domain-effect ledger bindings | [`134-unified-domain-effect-ledger-foundation.md`](134-unified-domain-effect-ledger-foundation.md) | [`135-loop-56-completion.md`](135-loop-56-completion.md) |
| 57 | Universal domain-effect dispatch | [`136-universal-domain-effect-dispatch-foundation.md`](136-universal-domain-effect-dispatch-foundation.md) | [`137-loop-57-completion.md`](137-loop-57-completion.md) |
| 58 | Agent-run ownership and recovery authority | [`138-agent-run-recovery-authority-foundation.md`](138-agent-run-recovery-authority-foundation.md) | [`139-loop-58-completion.md`](139-loop-58-completion.md) |
| 59 | Scoped embedding-call authority and provider boundary | [`140-embedding-call-authority-foundation.md`](140-embedding-call-authority-foundation.md) | [`141-loop-59-completion.md`](141-loop-59-completion.md) |
| 60 | Embedding target attachment and query lifecycle foundation | [`142-embedding-target-attachment-foundation.md`](142-embedding-target-attachment-foundation.md), [`143-embedding-reconciliation-and-query-lifecycle-foundation.md`](143-embedding-reconciliation-and-query-lifecycle-foundation.md) | [`144-loop-60-completion.md`](144-loop-60-completion.md) |
| 61 | Atomic embedding recovery and adaptive retrieval | [`145-embedding-recovery-adaptive-retrieval-foundation.md`](145-embedding-recovery-adaptive-retrieval-foundation.md) | [`146-loop-61-completion.md`](146-loop-61-completion.md) |
| 62 | Deterministic retrieval consistency and embedding cost qualification | [`147-deterministic-retrieval-consistency-foundation.md`](147-deterministic-retrieval-consistency-foundation.md) | [`148-loop-62-completion.md`](148-loop-62-completion.md) |
| 63 | Workspace transaction boundary and pool hygiene | [`149-workspace-transaction-boundary-foundation.md`](149-workspace-transaction-boundary-foundation.md) | [`150-loop-63-completion.md`](150-loop-63-completion.md) |
| 64 | Legacy global-surface containment and HTTP effect qualification | [`151-legacy-surface-and-http-effect-qualification-foundation.md`](151-legacy-surface-and-http-effect-qualification-foundation.md) | [`152-loop-64-completion.md`](152-loop-64-completion.md) |
| 65 | Workspace-scoped coordination and router authority | [`153-workspace-scoped-coordination-router-foundation.md`](153-workspace-scoped-coordination-router-foundation.md) | [`154-loop-65-completion.md`](154-loop-65-completion.md) |
| 66 | Workspace federation authority and effect identity | [`155-workspace-federation-authority-foundation.md`](155-workspace-federation-authority-foundation.md) | [`156-loop-66-completion.md`](156-loop-66-completion.md) |
| 67 | Production federation injection, reconciliation, and historical quarantine | [`157-production-federation-injection-reconciliation-foundation.md`](157-production-federation-injection-reconciliation-foundation.md) | [`158-loop-67-completion.md`](158-loop-67-completion.md) |
| 68 | Deployment credential, certificate, retention, backup, and load qualification | [`159-deployment-credential-retention-load-qualification-foundation.md`](159-deployment-credential-retention-load-qualification-foundation.md) | [`160-loop-68-completion.md`](160-loop-68-completion.md) |
| 69 | Live authority conformance and fenced retention ownership | [`161-live-authority-retention-owner-qualification-foundation.md`](161-live-authority-retention-owner-qualification-foundation.md) | [`162-loop-69-completion.md`](162-loop-69-completion.md) |
| 70 | Live adapter conformance and PostgreSQL topology qualification | [`163-live-adapter-postgres-topology-qualification-foundation.md`](163-live-adapter-postgres-topology-qualification-foundation.md) | [`164-loop-70-completion.md`](164-loop-70-completion.md) |
| 71 | Live provider and recovery qualification evidence | [`165-live-provider-recovery-qualification-foundation.md`](165-live-provider-recovery-qualification-foundation.md) | [`166-loop-71-completion.md`](166-loop-71-completion.md) |
| 72 | Deterministic built-in adapter qualification matrix | [`167-adapter-qualification-matrix-foundation.md`](167-adapter-qualification-matrix-foundation.md) | [`168-loop-72-completion.md`](168-loop-72-completion.md) |
| 73 | Server-composed generic read/observation operation worker | [`169-server-operation-worker-foundation.md`](169-server-operation-worker-foundation.md) | [`170-loop-73-completion.md`](170-loop-73-completion.md) |
| 74A | Agent-run fenced model and tool effects | [`171-agent-run-effect-fencing-foundation.md`](171-agent-run-effect-fencing-foundation.md) | [`172-loop-74a-completion.md`](172-loop-74a-completion.md) |
| 74B | Generic effect and domain-link outcome finalization | [`173-effect-link-finalization-foundation.md`](173-effect-link-finalization-foundation.md) | [`174-loop-74b-completion.md`](174-loop-74b-completion.md) |
| 75 | Portable qualification runner and evidence bundle | [`175-portable-qualification-runner-foundation.md`](175-portable-qualification-runner-foundation.md) | [`176-loop-75-completion.md`](176-loop-75-completion.md) |
| 76 | Authority-aware effect qualification observation | [`177-effect-authority-qualification-foundation.md`](177-effect-authority-qualification-foundation.md) | [`178-loop-76-completion.md`](178-loop-76-completion.md) |
| 77 | Disposable PostgreSQL effect-authority qualification probe | [`179-effect-authority-postgres-probe-foundation.md`](179-effect-authority-postgres-probe-foundation.md) | [`180-loop-77-completion.md`](180-loop-77-completion.md) |
| 78 | Signed deployment-evidence import and qualification bundle | [`181-signed-qualification-import-foundation.md`](181-signed-qualification-import-foundation.md) | [`182-loop-78-completion.md`](182-loop-78-completion.md) |
| 79 | Deployment-owned qualification trust catalog and authorized import | [`183-qualification-trust-catalog-foundation.md`](183-qualification-trust-catalog-foundation.md) | [`184-loop-79-completion.md`](184-loop-79-completion.md) |
| 80 | Deployment qualification trust distribution and startup conformance | [`185-qualification-trust-distribution-foundation.md`](185-qualification-trust-distribution-foundation.md) | [`186-loop-80-completion.md`](186-loop-80-completion.md) |
| 81 | Deployment release/evidence index and deterministic qualification gate | [`187-deployment-evidence-gate-foundation.md`](187-deployment-evidence-gate-foundation.md) | [`188-loop-81-completion.md`](188-loop-81-completion.md) |
| 82 | Release verification and startup/admission binding | [`189-release-admission-verification-foundation.md`](189-release-admission-verification-foundation.md) | [`190-loop-82-completion.md`](190-loop-82-completion.md) |
| 83 | Generic effect admission-reference consumption | [`191-deployment-admission-consumption-foundation.md`](191-deployment-admission-consumption-foundation.md) | [`192-loop-83-completion.md`](192-loop-83-completion.md) |
| 84 | Adapter authority conformance and strict startup inventory | [`193-adapter-authority-conformance-foundation.md`](193-adapter-authority-conformance-foundation.md) | [`194-loop-84-completion.md`](194-loop-84-completion.md) |
| 85 | Managed-credential and controlled-egress conformance | [`195-managed-credential-egress-conformance-foundation.md`](195-managed-credential-egress-conformance-foundation.md) | [`196-loop-85-completion.md`](196-loop-85-completion.md) |
| 86 | Deployment-owned boundary and live-provider evidence | [`197-deployment-boundary-provider-evidence-foundation.md`](197-deployment-boundary-provider-evidence-foundation.md) | [`198-loop-86-completion.md`](198-loop-86-completion.md) |
| 87 | Deployment observation publisher boundary | [`199-deployment-observation-publisher-foundation.md`](199-deployment-observation-publisher-foundation.md) | [`200-loop-87-completion.md`](200-loop-87-completion.md) |
| 88 | Deployment evidence freshness and lifecycle | [`201-deployment-evidence-lifecycle-foundation.md`](201-deployment-evidence-lifecycle-foundation.md) | [`202-loop-88-completion.md`](202-loop-88-completion.md) |
| 89 | Readiness snapshots and operator incident evidence | [`203-readiness-snapshot-incident-evidence-foundation.md`](203-readiness-snapshot-incident-evidence-foundation.md) | [`204-loop-89-completion.md`](204-loop-89-completion.md) |
| 90 | Readiness freshness policy and incident review | [`205-readiness-freshness-review-foundation.md`](205-readiness-freshness-review-foundation.md) | [`206-loop-90-completion.md`](206-loop-90-completion.md) |
| 91 | Qualification retention metadata and recovery evidence | [`207-qualification-retention-recovery-foundation.md`](207-qualification-retention-recovery-foundation.md) | [`208-loop-91-completion.md`](208-loop-91-completion.md) |
| Runtime-role RLS pooled-connection hygiene | [`259-runtime-role-rls-pool-hygiene-foundation.md`](259-runtime-role-rls-pool-hygiene-foundation.md) | — |
| 92 | Deployment-backed qualification evidence refresh | [`209-deployment-qualification-refresh-foundation.md`](209-deployment-qualification-refresh-foundation.md) | [`210-loop-92-completion.md`](210-loop-92-completion.md) |
| 93 | Durable qualification refresh scheduling and recovery handoff | [`211-qualification-refresh-scheduler-foundation.md`](211-qualification-refresh-scheduler-foundation.md) | [`212-loop-93-completion.md`](212-loop-93-completion.md) |
| 94 | Multi-domain reference workflows and qualification | [`213-multi-domain-reference-workflows-foundation.md`](213-multi-domain-reference-workflows-foundation.md) | [`214-loop-94-completion.md`](214-loop-94-completion.md) |
| 95 | Generic workflow service, explicit leases, adapters, and operator surface | [`215-generic-workflow-service-foundation.md`](215-generic-workflow-service-foundation.md) | [`216-loop-95-completion.md`](216-loop-95-completion.md) |
| 96 | Generic effect verification and fenced reconciliation | [`217-generic-effect-verification-foundation.md`](217-generic-effect-verification-foundation.md) | [`218-loop-96-completion.md`](218-loop-96-completion.md) |
| 97a | Durable workspace-scoped capability rate admission | [`219-capability-rate-admission-foundation.md`](219-capability-rate-admission-foundation.md) | [`220-loop-97-rate-admission-completion.md`](220-loop-97-rate-admission-completion.md) |
| 98 | Workflow retry deadline and queue eligibility | [`221-workflow-retry-deadline-foundation.md`](221-workflow-retry-deadline-foundation.md) | [`222-loop-98-completion.md`](222-loop-98-completion.md) |
| 99 | Capability rate denial retry scheduling and crash recovery | [`223-capability-rate-retry-scheduling-foundation.md`](223-capability-rate-retry-scheduling-foundation.md) | [`224-loop-99-completion.md`](224-loop-99-completion.md) |
| 100 | Due workflow retry advancement and multi-step queue deadlines | [`225-workflow-due-retry-advancement-foundation.md`](225-workflow-due-retry-advancement-foundation.md) | [`226-loop-100-completion.md`](226-loop-100-completion.md) |
| 101 | Privacy-safe local support bundles | [`227-support-bundle-redaction-foundation.md`](227-support-bundle-redaction-foundation.md) | [`228-loop-101-completion.md`](228-loop-101-completion.md) |
| 102 | Agent-loop untrusted context and per-run tool trust boundary | [`229-agent-loop-trust-boundary-foundation.md`](229-agent-loop-trust-boundary-foundation.md) | [`230-loop-102-completion.md`](230-loop-102-completion.md) |
| 103 | Registry-authoritative agent tool schemas and provider tool-call semantics | [`231-agent-tool-schema-authority-foundation.md`](231-agent-tool-schema-authority-foundation.md) | [`232-loop-103-completion.md`](232-loop-103-completion.md) |
| 104 | Agent tool resume authorization, path containment, and provider call identity | [`233-agent-tool-resume-and-path-containment-foundation.md`](233-agent-tool-resume-and-path-containment-foundation.md) | [`234-loop-104-completion.md`](234-loop-104-completion.md) |
| 105 | Atomic effect-result finalization and credential lease fencing | [`235-effect-result-and-credential-lease-atomicity-foundation.md`](235-effect-result-and-credential-lease-atomicity-foundation.md) | [`236-loop-105-completion.md`](236-loop-105-completion.md) |
| 106 | Managed model credential composition and final egress fencing | [`237-managed-model-credential-composition-foundation.md`](237-managed-model-credential-composition-foundation.md) | [`238-loop-106-completion.md`](238-loop-106-completion.md) |
| 107 | Tiered sandbox capability contract and fail-closed backend selection | [`239-tiered-sandbox-foundation.md`](239-tiered-sandbox-foundation.md) | [`240-loop-107-completion.md`](240-loop-107-completion.md) |
| 108 | Fenced sandbox attempt identity and crash-reconciliation provider boundary | [`241-sandbox-effect-identity-bridge.md`](241-sandbox-effect-identity-bridge.md) | [`242-loop-108-completion.md`](242-loop-108-completion.md) |
| 109 | Fenced sandbox recovery finalization and operator API | [`243-tool-sandbox-recovery-foundation.md`](243-tool-sandbox-recovery-foundation.md) | [`244-loop-109-completion.md`](244-loop-109-completion.md) |
| 110 | Root-anchored repository change I/O | [`245-root-anchored-change-io-foundation.md`](245-root-anchored-change-io-foundation.md) | [`246-loop-110-completion.md`](246-loop-110-completion.md) |
| 111 | Local test preflight portability and explicit environment skips | — | [`247-loop-111-completion.md`](247-loop-111-completion.md) |
| 112 | Signed qualification for non-local sandbox backends | [`248-sandbox-backend-qualification-foundation.md`](248-sandbox-backend-qualification-foundation.md) | [`249-loop-112-completion.md`](249-loop-112-completion.md) |
| 113 | Path-free, hash-bound host sandbox-runner protocol contracts | [`250-sandbox-runner-protocol-foundation.md`](250-sandbox-runner-protocol-foundation.md) | [`251-loop-113-completion.md`](251-loop-113-completion.md) |
| 114 | Host runner and OCI provider qualification (in progress) | [`252-host-sandbox-runner-foundation.md`](252-host-sandbox-runner-foundation.md), [`253-sandbox-cleanup-intent-foundation.md`](253-sandbox-cleanup-intent-foundation.md) | — |
| 114 review | Engine adapter identity and unresolved safety gates | [`260-engine-adapter-review-findings.md`](260-engine-adapter-review-findings.md), [`261-sandbox-image-platform-identity-foundation.md`](261-sandbox-image-platform-identity-foundation.md) | — |
| 117 | Platform-bound sandbox qualification contract | [`261-sandbox-image-platform-identity-foundation.md`](261-sandbox-image-platform-identity-foundation.md) | [`262-loop-117-platform-bound-sandbox-qualification-completion.md`](262-loop-117-platform-bound-sandbox-qualification-completion.md) |
| 118 | Local sandbox image ID/platform pre-create verifier | [`261-sandbox-image-platform-identity-foundation.md`](261-sandbox-image-platform-identity-foundation.md) | [`264-loop-118-local-image-identity-verifier-completion.md`](264-loop-118-local-image-identity-verifier-completion.md) |
| 115 | Atomic tool-result/effect finalization and interrupted-dispatch recovery | [`255-tool-effect-finalization-recovery-foundation.md`](255-tool-effect-finalization-recovery-foundation.md) | [`256-loop-115-completion.md`](256-loop-115-completion.md) |
| 116 | Authenticated sandbox cleanup protocol and fenced consumer (partial) | [`257-moby-runner-implementation-note.md`](257-moby-runner-implementation-note.md) | [`258-loop-116-cleanup-consumer-slice.md`](258-loop-116-cleanup-consumer-slice.md) |
| — | Universal production roadmap status | [`111-universal-production-roadmap-status.md`](111-universal-production-roadmap-status.md) | — |

Tasks 61–91 are implemented as foundation and qualification slices. Full
production qualification, live provider reconciliation, federation replacement,
complete adapter conformance, and production operating evidence remain open;
see [`156-loop-66-completion.md`](156-loop-66-completion.md).

## Local package and managed runtime

The current release-blocking workstream is [`62-packaging-distribution-foundation.md`](62-packaging-distribution-foundation.md), implemented by the single-package `fornix` CLI and managed local runtime. [`63-fornix-local-operations.md`](63-fornix-local-operations.md) is the operator-facing guide for the implemented alpha path. [`64-fornix-local-release-qualification-foundation.md`](64-fornix-local-release-qualification-foundation.md) records the release artifact, installer, and clean-room qualification contract; [`65-loop-24-completion.md`](65-loop-24-completion.md) records the delivered checks and remaining release-owner work.
It is deliberately separate from the numbered implementation loops: users
need a verified installation and first-run path before the remaining product
work can be evaluated by the community. Public release hosting, signed
provenance, native package managers, and production deployment qualification
remain open release work.

## Cross-cutting decisions

### Authority and determinism

[`00-fornix-foundation.md`](00-fornix-foundation.md) defines the core boundary:
Postgres owns control state, event history, checkpoints, and rebuild inputs.
Retrieval, projections, embeddings, graphs, metrics, and reports are derived
or inspectable outputs. Identical scoped inputs and recorded dependencies
should produce stable ordering, hashes, and decisions within the documented
limits.

### Qualification and limitations

[`14-production-readiness-qualification.md`](14-production-readiness-qualification.md)
is the current honest summary of verified behavior and production gaps. It
should be read before using Fornix for sensitive or high-availability work.

### API and operator workflows

[`53-http-api-reference.md`](53-http-api-reference.md) documents route families,
workspace authentication, idempotency, disclosure, and external-effect
semantics. [`DEVELOPMENT.md`](../DEVELOPMENT.md) contains copyable Docker,
CLI, database-test, and smoke commands.

### Research and licensing

[`13-reference-reuse-matrix.md`](13-reference-reuse-matrix.md) records which
reference implementations were studied, which patterns were independently
reimplemented, and where license notices would be required if code were ever
copied. Fornix itself is MIT-licensed; this does not change the license of any
third-party repository.

## Documentation standards

[`52-documentation-guide.md`](52-documentation-guide.md) is the active writing
contract. In particular, public docs should explain the user problem, safe
default path, authority, workspace/security boundary, failure semantics, hard
limits, evidence, and remaining limitations. Claims should identify whether a
result is measured locally, a design target, or not yet qualified.

## Current documentation boundaries

The repository documents the current alpha implementation and its local
qualification evidence. It does not yet provide a complete production
deployment guide, capacity model for huge repositories, backup/restore runbook,
HA topology, OAuth/SSO integration guide, or external secret-manager guide.
Those are documentation gaps because the corresponding operational features
are also not implemented or qualified; this index does not imply otherwise.
