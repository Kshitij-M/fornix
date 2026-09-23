# Universal AI work control plane

Status: canonical product and architecture overview for the universal
transformation; the current implementation is alpha and the repository
adapter is the first qualified adapter.

## The category

Fornix is the control plane that turns AI intent into bounded, durable,
evidence-backed operations against systems that matter.

Use Fornix when an answer is not enough. The operator also needs to know what
the AI was allowed to do, which resources it could see, what happened, what it
cost, which evidence supports the result, whether the work can resume after a
failure, and whether another operator can replay the decision without
repeating external effects.

The product promise is:

> **Delegate serious work to AI without losing the ability to bound,
> understand, recover, verify, and replay it.**

Fornix is designed for production-system operations, not for one resource
type. Repositories are the first adapter because they make the control-plane
problem concrete and locally testable. The same authority model is intended
to govern bounded work against APIs, databases, cloud resources, ticketing
systems, and internal business systems as those adapters are implemented and
qualified.

## The problem

Most AI integrations stop at a model response or a transient agent trace.
That is insufficient for work that changes or interrogates an important
system. Long-running work can lose its place, repeat an action, cross a tenant
boundary, exceed a budget, use stale context, or produce a result that cannot
be verified later.

Production operators need durable answers to a common set of questions:

- What operation was requested, by whom, and against which scope?
- Which capability and policy revision admitted it?
- Which model, tool, connector, or human approval participated?
- Which external effects were attempted, acknowledged, verified, or left
  requiring recovery?
- Which evidence, artifacts, and source versions support the result?
- What did the work cost, and which usage was measured versus estimated?
- Can the operation resume or replay deterministically after a crash?

Fornix makes those questions part of the execution contract rather than an
after-the-fact logging exercise.

## The universal lifecycle

The domain-neutral lifecycle is:

```text
Intent
  → scope and authenticated actor
  → typed operation and capability selection
  → policy, budget, credential, and approval admission
  → durable ownership, execution, and checkpoints
  → evidence, artifact, cost, and external-effect recording
  → verification and Work Receipt
  → replay, evaluation, and improvement
```

The lifecycle is intentionally split into authorities and adapters:

```text
                         ┌─ repository adapter
                         ├─ HTTP/API adapter
Universal operation ─────┼─ read-only SQL adapter
control plane             ├─ cloud/resource adapter
                         └─ ticketing/business-system adapter

Every adapter uses the same scope, policy, execution, evidence, and receipt
boundary. An adapter owns its domain semantics; it does not become a second
control-plane authority.
```

The current alpha implements the durable control and retrieval substrate,
domain-neutral contracts, and a process-local connector/capability registry.
It does not yet implement the full durable generic operation authority or the
future adapters shown above. See the status matrix below and the
[production qualification](14-production-readiness-qualification.md) before
using the system for sensitive work.

## What Fornix owns

Fornix owns the cross-domain facts that must remain stable regardless of the
underlying system:

- workspace and actor scope;
- typed operation, resource, connector, and capability identity;
- idempotency, causation, correlation, request, and provenance references;
- policy and approval decisions with immutable revisions and hashes;
- task ownership, fencing, checkpoints, retries, cancellation, and recovery
  state;
- bounded model, tool, connector, time, token, byte, SQL, storage, and cost
  budgets;
- immutable evidence, artifacts, source references, and Work Receipts;
- external-effect status, verification, compensation, and explicit
  at-least-once semantics;
- replay and offline evaluation over recorded dependencies.

An adapter owns domain-specific validation and execution. For example, a
repository adapter understands normalized paths and source manifests; an HTTP
adapter will understand endpoints and response bounds; a SQL adapter will
understand read-only statements and row limits. None of those adapters may
silently bypass Fornix identity, policy, fencing, evidence, or receipt rules.

## Current and planned domain coverage

| Surface | Status | Boundary |
| --- | --- | --- |
| Typed universal operation vocabulary | Implemented alpha foundation | Contracts for systems, resources, connectors, capabilities, effects, plans, results, evidence, and receipts |
| Events, checkpoints, leases, fencing, retrieval, evidence, artifacts, model/tool boundaries, and evaluation | Implemented alpha foundation | Postgres-backed control-plane substrate; see qualification limits |
| Connector and capability registry | Implemented alpha foundation | Process-local, explicit, fail-closed admission; not durable operation authority |
| Repository ingestion and read-only inspection | First qualified adapter | Explicit local mounts, bounded indexing, evidence, artifacts, and replay |
| Repository change and validation | First write-boundary vertical slice | Approval-gated local filesystem effects with recovery-required semantics |
| Durable generic operation authority | Roadmap: Issue [#39](https://github.com/Kshitij-M/fornix/issues/39) | One generic operation identity, lifecycle, attempts, effects, and compatibility links |
| Universal policy, approvals, and external effects | Roadmap: Issue [#46](https://github.com/Kshitij-M/fornix/issues/46) | Cross-domain admission and explicit remote-effect semantics |
| Bounded HTTP/API connector | Roadmap: Issue [#43](https://github.com/Kshitij-M/fornix/issues/43) | Read/list/idempotent-submit capabilities with egress, response, and retry bounds |
| Read-only SQL connector | Roadmap: Issue [#43](https://github.com/Kshitij-M/fornix/issues/43) | Describe/query/explain capabilities with prepared statements and write rejection |
| Durable multi-step workflow runtime | Roadmap: Issue [#44](https://github.com/Kshitij-M/fornix/issues/44) | Typed model, tool, connector, approval, human, validation, callback, and compensation steps |
| Multi-domain incident reference workflow | Roadmap: Issue [#42](https://github.com/Kshitij-M/fornix/issues/42) | Fake-first workflow spanning at least two non-repository domains |
| Universal production qualification | Roadmap: Issue [#40](https://github.com/Kshitij-M/fornix/issues/40) | Isolation, trust, egress, quotas, recovery, backup/restore, load, and support evidence |

This matrix is a product-status statement, not a promise that planned
connectors are already available. Commands and APIs are documented as
supported only when they exist in the current code and pass the corresponding
qualification checks.

## How Fornix relates to adjacent tools

Fornix is complementary to model providers and agent clients. It is not a
replacement for every layer of an AI application.

| Layer | Primary responsibility | Fornix’s relationship |
| --- | --- | --- |
| Model SDK | Serialize requests and call a provider | Fornix can use providers through a bounded model gateway and records usage, cost, identity, and failure semantics |
| Agent framework | Compose prompts, turns, and tool calls | Fornix provides durable scope, policy, recovery, evidence, cost controls, and replay around bounded work |
| Workflow engine | Persist and resume a process | Fornix adds AI-specific context, model/tool/connector boundaries, approvals, provenance, and external-effect accounting |
| Task queue | Dispatch work to workers | Fornix adds durable operation authority, fencing, idempotency, verification, receipts, and replayable history |
| Connector system | Access one external system | Fornix standardizes capability admission, budgets, evidence, approvals, and result contracts across connectors |

Teams may continue to use their preferred model SDK, chat interface, agent
client, or workflow integration. The boundary Fornix contributes is the
verifiable work record and the rules around the work—not ownership of the
conversation surface.

## Product roadmap

The universal transformation is tracked by [Issue #38](https://github.com/Kshitij-M/fornix/issues/38).
Its implementation sequence is:

1. [#41 — Domain-neutral contracts and adapter boundary](https://github.com/Kshitij-M/fornix/issues/41).
2. [#45 — Connector and capability registry](https://github.com/Kshitij-M/fornix/issues/45).
3. [#39 — Durable generic operation authority](https://github.com/Kshitij-M/fornix/issues/39).
4. [#46 — Universal policy, approvals, and external effects](https://github.com/Kshitij-M/fornix/issues/46).
5. [#43 — Bounded HTTP/API and read-only SQL connectors](https://github.com/Kshitij-M/fornix/issues/43).
6. [#44 — Durable multi-step workflow runtime](https://github.com/Kshitij-M/fornix/issues/44).
7. [#42 — Multi-domain incident investigation and controlled remediation](https://github.com/Kshitij-M/fornix/issues/42).
8. [#40 — Universal execution-plane production qualification](https://github.com/Kshitij-M/fornix/issues/40).

Issues [#23–#30](https://github.com/Kshitij-M/fornix/issues?q=is%3Aissue+is%3Aopen)
remain supporting production work. They should be closed or re-scoped as the
generic authority and qualification work exposes their dependencies.

## The honest starting point

The current supported experience is still the repository adapter. It is the
smallest complete path for demonstrating the universal contract:

```sh
make build
./bin/fornix doctor
./bin/fornix start --repo .
./bin/fornix demo --repo .
```

The default provider is deterministic and offline. The resulting evidence,
artifacts, validation, replay hashes, and Work Receipt demonstrate the
control-plane behavior without requiring a remote model. The alpha should not
be described as a complete multi-domain production platform until Issues
#39–#40 and the adapter qualification work are complete.

## Non-goals and safety boundaries

Fornix does not claim exactly-once execution at a remote provider, network,
process, or filesystem boundary. It does not currently provide OAuth/SSO,
external secret-manager integration, high availability, backup/restore
qualification, a host-independent sandbox, or a general multi-agent graph.
Those are explicit qualification and roadmap concerns, not assumptions hidden
behind the universal language.

Read the [development contract](00-fornix-foundation.md),
[documentation guide](52-documentation-guide.md),
[reference reuse matrix](13-reference-reuse-matrix.md), and
[production qualification](14-production-readiness-qualification.md) before
extending the control plane.
