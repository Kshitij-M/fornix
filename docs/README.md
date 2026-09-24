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
| How legacy global federation/router APIs are contained | [`91-legacy-global-surface-containment-foundation.md`](91-legacy-global-surface-containment-foundation.md) |
| How documentation should be written | [`52-documentation-guide.md`](52-documentation-guide.md) |
| Which reference projects informed the design | [`13-reference-reuse-matrix.md`](13-reference-reuse-matrix.md) |

## How to read the architecture record

The numbered notes are chronological and intentionally preserve the project’s
engineering history:

- A **foundation** note explains the problem, invariants, authority boundary,
  schema/API shape, research and licensing decisions, cost budget, and planned
  acceptance tests for one slice.
- A **completion** note records what was implemented, how it was qualified,
  measured local results, and what remains limited or deferred.
- The cross-cutting foundation and qualification notes are the best current
  summaries. A completion note is the more reliable source when a historical
  foundation intention differs from the current implementation.

The project has 39 numbered repository/control-plane and universal-operation
implementation loops plus the packaging and transformation delivery records
below. Those loops build the control-plane substrate and reference adapters;
they are not a claim that the complete production-system operations product is
finished. Repository maintenance remains the first qualified adapter, and the
pairs below are the detailed engineering record for each implementation loop.

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
| — | Universal production roadmap status | [`111-universal-production-roadmap-status.md`](111-universal-production-roadmap-status.md) | — |

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
