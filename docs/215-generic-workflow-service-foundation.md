# Generic durable workflow service and multi-domain qualification foundation

Status: implemented as a bounded generic workflow/API/CLI foundation (Task 95).

## Problem

Fornix already has a durable workflow runtime, generic operation authority,
connector registry, effect dispatcher, evidence/artifact stores, and Work
Receipts. The public workflow surface is still incident-specific, however.
That makes the universal architecture difficult to use and impossible to
qualify end to end across domains.

This task adds one domain-neutral workflow service and two additional
fake-first domain adapters. The service owns request normalization, workflow
creation, lease-bound advancement, waiting-state decisions, cancellation,
replay, and receipt disclosure. Domain adapters own only capability schemas,
bounded observation/result hashes, effect descriptions, and verification
classification.

## Non-goals

- No domain-specific workflow tables, queues, schedulers, or lease systems.
- No live ticketing, data-pipeline, cloud, deployment, monitoring, or support
  provider integration.
- No claim of exactly-once external execution.
- No bypass around operation admission, effect reservation, task fencing,
  workspace authorization, or receipt integrity validation.
- No raw prompts, credentials, customer text, provider payloads, or unbounded
  output in generic workflow state.

## Authorities and reuse

The service must compose existing authorities in this order:

```text
authenticated actor/workspace
  → adapter registry and capability/schema admission
  → OperationStore identity/idempotency
  → WorkflowStore plan/checkpoint/lease/fence
  → ConnectorExecutor observation or EffectDispatcher reservation
  → EvidenceStore/ArtifactStore references
  → WorkReceiptStore verification and disclosure
```

The service must not persist a second copy of operation or workflow history.
The workflow projection remains a convenience view; append-only operation,
event, effect, evidence, artifact, and receipt histories remain authoritative.

## Contracts and invariants

- Every request, target, actor, capability, policy, operation, workflow, effect,
  evidence, artifact, and receipt reference is workspace-scoped.
- Logical request/plan hashes exclude delivery IDs but include domain,
  capability definition, schema, target, actor, policy, and input hashes.
- Duplicate create/advance/approval/cancel/replay requests return the existing
  durable result when the idempotency identity matches; conflicting reuse
  fails closed.
- A workflow lease is required for mutation. Stale, expired, released, or
  cross-workspace leases cannot advance, cancel, approve, or finalize.
- Task-bound workflows must carry the current task owner and fence into every
  step mutation and effect dispatch.
- Effectful steps cannot execute without a durable effect reservation and the
  exact live operation/effect/task authority envelope. The server-composed
  path now injects the existing admission and effect-dispatch authorities;
  offline or unconfigured services still fail closed.
- Unknown external outcomes become `recovery_required`; replay never marks an
  unknown outcome successful.
- Receipts are verified only after authoritative references resolve, hashes
  match, terminal/recovery state is explicit, and replay performs zero external
  calls.
- Read-only replay is bounded and cannot invoke models, tools, connectors,
  brokers, or external systems.

## Domain adapter coverage

The fake qualification adapters must have materially different schemas and
capability names, while sharing the same service:

| Domain | Observation | Validation | Effect boundary | Verification |
| --- | --- | --- | --- | --- |
| data pipeline | source/window snapshot | schema, watermark, quality hashes | bounded publish/backfill | output watermark/result hash |
| customer support | case snapshot | policy/classification hash | approval-gated response update | case status/result hash |

The existing incident workflow remains a compatibility path. The new generic
service must not special-case incident concepts.

## Crash and recovery semantics

- Crash before workflow checkpoint commit leaves the prior projection and
  lease state unchanged.
- Crash after a committed checkpoint is replayable from that checkpoint and
  from sequence zero.
- A worker losing its lease cancels the in-process step and cannot commit a
  result later.
- A started external effect whose outcome is unknown remains unresolved until
  the adapter's verification/reconciliation boundary records an authoritative
  result.
- Receipt finalization and its typed authority links are atomic within the
  existing receipt transaction; no orphan authoritative link is accepted.

## API and CLI

Authenticated HTTP routes:

```text
POST /v1/workflows
GET  /v1/workflows/{id}
POST /v1/workflows/{id}/lease
POST /v1/workflows/{id}/renew
POST /v1/workflows/{id}/release
POST /v1/workflows/{id}/advance
POST /v1/workflows/{id}/approve
POST /v1/workflows/{id}/verify
POST /v1/workflows/{id}/cancel
POST /v1/workflows/{id}/replay
GET  /v1/workflows/{id}/receipt
POST /v1/workflows/{id}/receipt
```

Lease ownership is explicit. `advance`, `approve`, `verify`, and `cancel` require the
authenticated actor's `X-Operation-Fence`; the service never silently acquires
or refreshes a lease. The CLI mirrors these operations with bounded JSON
output and explicit workspace/actor context. Read routes require workflow or
receipt read permission; lifecycle mutations require operation execution
permission; approval requires the approval permission. Development
compatibility mode must remain explicit.

`verify` is the only workflow path that can resolve an external-effect wait.
It requires `X-Operation-Fence`, `X-Effect-Fence`, the exact effect/link
versions observed by the recovery worker, and a stable idempotency key. The
registered capability verifier receives normalized hashes and bounded provider
identifiers. It cannot submit a success payload. The local effect state and
domain-effect link are reconciled atomically; the workflow checkpoint is then
advanced under its own fence. A duplicate after final local commit reads the
durable proof and does not invoke the verifier again, but still requires the
current effect lease.

The generic offline executor runs read-only and observation capabilities from
the registered connector catalog. The server-composed executor injects the
existing fenced admission/effect-dispatch boundary for effectful capabilities.
An unconfigured service turns those steps into an explicit
`awaiting_external` wait; no generic fallback marks them succeeded.

## Storage and cost budget

No migration is expected for the first vertical slice. Existing workflow,
operation, event, effect, evidence, artifact, and receipt tables are reused.
Each request adds bounded rows already accounted for by those authorities.
Offline fake-domain tests must create no persistent artifacts or containers.
Database qualification must report transaction count, lock waits, row/WAL
growth, receipt size, and replay throughput against an explicitly disposable
DSN.

## Licensing and research

The implementation reuses Fornix interfaces and independently reproduces
architecture patterns from Orloj, DeepSeek Harness, ClawMem, agentmemory,
and FornixDB. No reference source is copied. Kronaxis source remains excluded
because its BSL 1.1 license is incompatible with copying into this MIT repo.

## Acceptance tests

### Offline

- Generic create/plan/trace hashes are deterministic.
- Data-pipeline and customer-support capabilities have distinct schemas and
  definitions.
- Cross-workspace and unauthorized operations fail closed.
- Effectful execution without authority is rejected.
- Unknown outcomes become `recovery_required`.
- Replay makes zero adapter/model/tool/external calls.
- Reports and receipts contain hashes/references, not raw secrets or payloads.

### Postgres-backed

- Duplicate create and mutation delivery produces one durable effect.
- Concurrent lease claim has one owner; stale workers cannot commit.
- Approval pauses and resumes without duplicate effect reservations.
- Cancellation prevents future steps.
- Crash-before-commit and crash-after-commit are recoverable.
- Operation, workflow, effect, evidence, artifact, and receipt links are
  atomic and workspace-isolated.
- Replay from zero and checkpoint produce identical hashes.
- Receipt disclosure is bounded and hash-stable.

### Qualification

- Add a deterministic multi-domain qualification matrix and Make/CI target.
- Keep fake workflow evidence separate from live provider, credential, egress,
  backup/restore, HA, load, and security qualification.

## Implemented files and remaining boundary

- `internal/contracts/workflow_control.go` defines authenticated workflow
  create, advance, resume, cancel, and replay envelopes.
- `internal/workflows/generic/` owns the service, explicit lease lifecycle,
  connector-backed read/observation execution, deterministic offline fallback,
  replay, and operation-backed receipt finalization.
- `internal/adapters/fakedomains/` registers distinct data-pipeline and
  customer-support capability schemas for offline qualification.
- `internal/server/workflows.go` and `cmd/fornix/cli.go` expose bounded HTTP
  and operator surfaces.

This slice deliberately does not claim that a generic effectful adapter is
production-ready. The generic step executor is connected to
`effectdispatch.Dispatcher` and authoritative workflow-step effect links, and
Task 96 now supplies the explicit verifier and reconciliation/resume path for
acknowledged or uncertain external outcomes. The remaining universal boundary
is qualification against disposable Postgres plus live adapter/provider
evidence. No non-test external write should be enabled until those boundaries
are qualified.
