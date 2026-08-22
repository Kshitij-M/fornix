# Fornix documentation

Status: active public documentation map.

This is the public documentation map for Fornix. It is written for people who
want to use the alpha, operate it locally, review its design, or contribute to
the repository.

The product direction is **verifiable AI work infrastructure for long-running
repository operations**. Fornix is intended to let teams delegate serious
repository work to AI without losing control of scope, cost, evidence,
approval, or recovery. The current implementation is the durable control and
retrieval substrate behind that outcome; the [product vision](01-product-vision.md)
explains the distinction.

## Choose a starting point

| If you want to know… | Read… |
| --- | --- |
| What Fornix is and why it exists | [`README.md`](../README.md) |
| What problem Fornix will own and how the product should feel | [`01-product-vision.md`](01-product-vision.md) |
| How to run, test, and smoke the service | [`DEVELOPMENT.md`](../DEVELOPMENT.md) |
| How to contribute a change | [`CONTRIBUTING.md`](../CONTRIBUTING.md) |
| How to report security concerns | [`SECURITY.md`](../SECURITY.md) |
| How the project handles community conduct | [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md) |
| Which design rules are non-negotiable | [`00-fornix-foundation.md`](00-fornix-foundation.md) |
| Which routes and request rules exist | [`53-http-api-reference.md`](53-http-api-reference.md) |
| What is actually qualified today | [`14-production-readiness-qualification.md`](14-production-readiness-qualification.md) |
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

The project currently has 21 completed implementation loops. Those loops build
the control-plane substrate; they are not 19 claims that the complete
repository-maintenance product is finished. The pairs below are the detailed
engineering record for each one.

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
