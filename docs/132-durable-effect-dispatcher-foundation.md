# Fornix durable effect dispatcher foundation

Status: implementation note for Task 55. This note describes the boundary
implemented in this loop; it is not a claim that arbitrary remote systems are
exactly-once.

## Problem and decision

Fornix already records generic operation admission, fenced operation leases,
attempts, external-effect reservations, credential authority facts, and
append-only effect-state transitions. Several callers could still compose
those primitives in different orders, however. That creates a dangerous
possibility: an adapter can be called before the effect is visible to the
control plane, or a stale worker can finalize a result after a lease takeover.

Task 55 adds one domain-neutral dispatch seam. Effectful work must follow:

```text
authenticated intent
  -> durable admission
  -> operation attempt reservation
  -> external-effect reservation
  -> reserved -> dispatching transition
  -> live operation/task/credential/catalog validation
  -> adapter/provider dispatch
  -> dispatched/acknowledged/verification or recovery transition
  -> durable result/reconciliation
```

The dispatcher owns ordering and fail-closed behavior. The adapter owns the
external protocol and never receives credentials from the dispatcher. The
callback is deliberately provider-neutral so HTTP connectors, incident
actions, tools, model calls, repository mutations, and future business-domain
adapters can share the same authority boundary without importing one another.

## Invariants

1. Workspace, operation, actor, request hash, and idempotency identity are
   normalized before any durable write or external call.
2. Admission is durable before an effect can be reserved. A denied or
   approval-pending decision never reaches the adapter.
3. An effect reservation is immutable. A duplicate request with the same
   identity replays the existing reservation/state and does not invoke the
   adapter a second time.
4. The operation lease owner and fence, task owner and fence, credential lease
   facts, and signed schema/trust catalog facts are references to authority;
   they do not grant authority by themselves. The live Postgres checks run
   immediately before dispatch and again before final result finalization.
5. A stale worker fails closed. It cannot reserve a new effect, transition an
   effect, record a result, or move the operation checkpoint.
6. Provider request IDs and provider idempotency support are recorded as facts.
   The dispatcher never claims exactly-once delivery for a remote system.
7. If the process can have dispatched an effect but cannot prove the outcome,
   the effect remains recoverable/unknown. It is not automatically retried.
8. Raw prompts, credentials, authorization headers, and external payloads are
   not part of the dispatcher contract or its durable records. Only bounded
   hashes, references, redacted evidence, and provider identifiers may cross
   the boundary.
9. All transitions are idempotent by workspace-scoped command key and retain
   the append-only history needed for replay and reconciliation.

## Crash semantics

| Crash point | Durable state | Recovery rule |
| --- | --- | --- |
| Before admission commit | no admission/effect | safe to retry the same idempotency key |
| After admission, before reservation | admission only | reserve using the same operation/attempt identity |
| After reservation, before `dispatching` | `reserved` | no adapter call; a worker may continue the reservation |
| After `dispatching`, before adapter return | `dispatching` | do not blind-retry; inspect provider idempotency/status or move to recovery |
| After provider return, before reconciliation | `dispatching` or `dispatched` | reconcile with provider ID/evidence; at-least-once remains explicit |
| After adapter reply, during local finalization | acknowledged state or transaction rollback | final effect, domain link, operation result, operation transition, and authority link commit together; a rollback never exposes partial terminal success |
| After finalization commit, before result response | terminal effect and immutable operation result | duplicate delivery reads and returns the same durable result without re-invoking the adapter |

The dispatcher does not hold a SQL transaction across an external call. This
is intentional: a database transaction cannot make a remote provider atomic.
The terminal local result is nevertheless one Postgres commit. If the process
loses the adapter result before that transaction commits, an acknowledged
effect is not re-dispatched to synthesize a replacement result; duplicate
delivery fails closed and requires provider/domain reconciliation.

## Schema and reuse decision

Migration `053_effect_dispatch_identity.sql` adds the workspace-scoped unique
provider identity `(boundary, idempotency_key)` needed to prevent two
concurrent delivery attempts from creating different local effect rows for
the same provider boundary. The existing operation/effect tables and
migrations provide the remaining durable records:

- `operations`, `operation_leases`, `operation_attempts`
- `operation_admission_decisions`, `operation_approvals`, and authority links
- `operation_effects`, `operation_effect_state`, and append-only transitions
- credential lease/source facts and signed trust/schema catalog facts

The dispatcher is a service-layer composition over those authorities. It does
not introduce a second effect ledger, an in-memory lock, or a broker.

The generic HTTP connector operation path and approval-gated incident
remediation now use the dispatcher. Tool runs, repository-change applications,
and billable model calls retain their domain ledgers today; those ledgers
reserve before their external boundary and are fenced, but they are not yet
cross-linked to a generic `operation_effects` row. That distinction is
intentional and documented: a domain ledger is an equivalent local admission
boundary only when its provider/repository identity, uncertainty, and recovery
semantics are explicit. A later loop must add cross-references or route those
paths through this dispatcher before Fornix claims one universal effect ledger.

## Reuse and licensing

The design reuses Fornix’s existing Postgres stores and typed contracts. It is
architecturally informed by Orloj’s explicit execution engine and provider
identity, DeepSeek Harness’s prepared-call and no-fallback-after-content rule,
agentmemory’s bounded retry/checkpoint discipline, and OpenBao’s durable
lease/reload concepts. These references are patterns only; no incompatible
source is copied. Kronaxis-fabric source is not copied because it is BSL 1.1.
Fornix remains MIT-licensed, and any future dependency must be reviewed for
license compatibility before adoption.

## Cost and performance budget

The dispatcher adds bounded Postgres work per effect: one durable admission
lookup/insert, one attempt reservation, one effect reservation, one preflight
authority validation, and bounded effect-state transitions. It must not add a
network hop or a background service. Expected cost is extra database round
trips and rows proportional to effect count; provider cost is unchanged.

Qualification must measure p50/p95 dispatch latency, SQL statement count and
pool wait time, effect/transition row growth, duplicate-hit rate, recovery
latency, and the percentage of effects requiring manual/provider reconciliation.
No performance number in this note is a production SLO.

## Acceptance tests

- admission and reservation precede the adapter callback;
- duplicate delivery invokes the callback once and replays durable state;
- approval-pending and denied work never invokes the callback;
- stale operation, task, credential, or catalog authority fails closed;
- a failure before reservation leaves no effect reservation;
- a failure after reservation is visible and recoverable;
- a failure after possible provider dispatch is never silently retried;
- successful reconciliation is idempotent and replayable;
- operation/task workspace isolation fails closed;
- HTTP connector and incident adapter use the same dispatcher contract;
- MCP reaches the same protected HTTP operation path;
- raw credentials and payloads are absent from events, errors, and evidence.
