# Multi-domain incident workflow foundation

Status: feature note for Issue [#42](https://github.com/Kshitij-M/fornix/issues/42); the fake-first, bounded reference workflow is implemented on the feature branch and is not a claim that arbitrary incident remediation is production-safe.

## Product purpose

Repository maintenance is Fornix's first adapter, not its product boundary.
This slice proves the broader promise with a production-shaped operation that
crosses a monitoring system, a repository runbook, a model boundary, bounded
diagnostics, an approval boundary, and an external remediation boundary:

> Delegate serious operational work to AI without losing the ability to bound,
> understand, approve, recover, verify, and replay it.

The workflow is deliberately fake-first. It demonstrates universal control
semantics without requiring a live monitoring vendor, deployment platform,
model credential, or uncontrolled network access.

## Scope and non-goals

The vertical slice will provide:

- a typed, workspace-scoped incident event and incident resource;
- append-only event capture with duplicate delivery and conflicting-payload
  rejection;
- deterministic fake incident-read and fake remediation connectors behind the
  existing capability registry;
- one durable workflow plan that uses read-only, model, tool, approval,
  effect, verification, evidence, artifact, and replay boundaries;
- an explicit approval pause before remediation;
- HTTP, CLI, and MCP entry points with equivalent request semantics;
- a single bounded Work Receipt linking the incident and remediation proof.

It will not enable real production remediation by default, accept arbitrary
webhook signatures without an injected verifier, expose unbounded network
access, or make an exactly-once claim about an external effect.

## Invariants

1. **Workspace isolation is structural.** Incident IDs, source systems,
   event identities, evidence, artifacts, operations, workflow runs, actors,
   approvals, and receipts must share one workspace. Cross-workspace input or
   lookup fails closed.
2. **Event identity is content-bound.** The natural identity is
   `(workspace, source_system, external_event_id)`. A duplicate with the same
   payload hash returns the original incident and does not append a second
   authoritative effect. A changed payload for that identity is a conflict,
   not an overwrite.
3. **Raw input is preserved once.** The bounded event payload is stored as
   immutable evidence/content-addressed bytes; incident and workflow rows
   carry only references and hashes.
4. **The generic authority is reused.** The incident workflow creates one
   generic operation and one workflow run. It does not create a parallel
   scheduler, lease, event history, or approval authority.
5. **Read before effect.** Incident read, runbook retrieval, investigation,
   diagnostics, and verification are read-only/observational. Remediation is
   an explicit effectful capability and cannot run while approval is pending.
6. **Approval is bound to intent.** Approval resumes the exact workspace,
   workflow, step attempt, operation hash, and plan hash. A changed request,
   stale worker, or wrong workspace cannot unlock remediation.
7. **External delivery is at-least-once.** The fake remediation connector has
   an idempotency key and deterministic state verification. The control plane
   records uncertainty and never converts an external call into an
   exactly-once guarantee.
8. **Replay is inert.** Replay reads incident, operation, workflow,
   evidence, artifact, receipt, and transition history. It never invokes a
   connector, model, tool, network, approval callback, or remediation handler.
9. **Budgets are hard.** Event size, context/evidence disclosure, workflow
   steps, fan-out, tokens, output bytes, wall time, retry count, and cost are
   bounded before and after each recorded step.
10. **Proof is additive.** A Work Receipt references authoritative incident,
    workflow, evidence, artifact, operation, approval, effect, and replay
    hashes. It does not replace their histories.

## Durable schema

Migration `039_incident_workflow.sql` adds:

- `incident_records`: the current workspace-scoped incident projection,
  natural source identity, payload/evidence hash, severity, status, and
  current workflow/receipt references;
- `incident_event_deliveries`: append-only delivery history with unique
  idempotency identity and event hash;
- `incident_idempotency`: request identity mapped to the canonical incident
  and payload hash.
- `incident_approvals`: append-only, workspace-scoped approval decisions bound
  to one workflow step, operation hash, plan hash, actor, and evidence hash.

The tables will use PostgreSQL uniqueness, workspace-qualified foreign keys,
append-only triggers where history is authoritative, and row-count checks in
the store. Workflow transitions, operation leases/fences, evidence, artifacts,
and Work Receipts remain owned by their existing stores.

## Reuse and licensing decisions

- Reuse Fornix's operation authority, connector registry, admission/effect
  contracts, workflow runtime, event store, EvidenceStore, ArtifactStore,
  WorkReceiptStore, identity middleware, and workspace checks.
- Reimplement the useful architectural patterns studied in Orloj webhook and
  approval lifecycles, DeepSeek Harness capability seams, agentmemory replay
  and cleanup discipline, ClawMem recorded action flow, OpenTelemetry-style
  bounded event envelopes, and Temporal-style durable checkpoints.
- Do not copy reference source. Orloj and agentmemory are Apache-2.0,
  ClawMem/FornixDB are MIT, and Kronaxis-fabric is BSL 1.1 and is excluded.
  Fornix remains MIT; no new runtime dependency or infrastructure is planned.

## Cost and stability budget

- One incoming event performs one bounded transaction: evidence/content
  insertion or reuse, incident identity/delivery deduplication, and one
  `incident.received` event.
- One workflow step performs the existing fenced Postgres checkpoint work;
  fake connector/model/tool work is deterministic and local.
- The workflow is capped at 10 steps, two read-only parallel workers, 64 KiB
  event payload, 32 KiB report disclosure, 100k model tokens, 16 MiB output,
  two retries, and an explicit zero-cost offline default.
- Storage grows with immutable event deliveries, evidence/artifacts, workflow
  transitions, and receipt references. Current projections are rebuildable;
  no raw payload is copied into workflow or receipt rows.
- Measurements will report ingest/step/approval/replay latency, SQL work,
  artifact/evidence bytes, duplicate external work, and replay throughput.
  Local measurements are observations, not production capacity claims.

## Acceptance tests and implementation status

- fake event validation and bounded signed-event verifier seam;
- duplicate delivery returns one incident/effect; conflicting payload fails;
- incident and workflow data cannot cross workspaces;
- raw event, diagnostic, report, and remediation evidence retain hashes and
  provenance without credentials, prompts, or arbitrary raw metrics in logs;
- the workflow pauses before remediation and resumes only after the exact
  approval decision;
- stale operation fences and wrong plan/step identities fail closed;
- crash before and after each major transaction boundary is recoverable;
- fake remediation is idempotent and verification detects an injected failure;
- cancellation and retry preserve durable state;
- report, evidence, artifact, receipt, workflow, and replay hashes are stable;
- replay performs no external calls and produces the same terminal state;
- CLI, HTTP, and MCP requests have equivalent semantics;
- existing tests, race checks, migrations, CI, package checks, and smokes stay
  green.

The implementation adds the typed incident contracts, migration 039, durable
incident store, fake incident/remediation connectors, workflow service,
approval persistence, CLI/HTTP/MCP routes, and the multi-domain smoke. The
Postgres integration suite covers duplicate and conflicting delivery,
workspace isolation, approval and rejection, crash-before-checkpoint recovery,
replay stability, and duplicate approval semantics. Parent workflow tests
cover generic cancellation, stale fencing, retry, wait/resume, and replay
boundaries.

## Deliberate limitations

This is a qualification workflow, not a catalog of incident integrations. A
real adapter still needs signed ingress verification, secret-manager-backed
credentials, egress policy, domain-specific authorization, provider
idempotency, post-effect reconciliation, alert storm controls, distributed
scheduling, and operational SLO qualification. Those remain within the
universal production qualification roadmap.
