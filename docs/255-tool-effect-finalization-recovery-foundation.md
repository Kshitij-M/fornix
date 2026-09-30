# Tool effect finalization and recovery foundation

Status: Implemented locally; PostgreSQL qualification pending.

## Status

Design note for closing the tool/effect commit gap identified during production
qualification. This is intentionally narrower than adding another sandbox
backend. The Moby/OCI runtime remains disabled until its runtime lifecycle and
cleanup worker are implemented and qualified.

## Problem

Fornix persists tool execution in two authorities: `tool_runs` owns the
specialized result, evidence, artifact links, lifecycle event, and cleanup
intent; the generic effect dispatcher owns the operation result, external
effect state, and domain-effect link. If the generic transaction commits and
the process stops before `tool_runs` is finalized, the durable rows disagree.
The provider/process must not be invoked again merely to repair this local
commit gap.

There is a second boundary: after a sandbox may have started, live authority
revalidation can fail. The effect may be durable but the tool run can remain
`running`. Recovery must therefore classify durable states, retain fences, and
consume an exact sandbox observation without repeating the external action.

## Invariants

1. A successful built-in tool execution becomes terminal in one Postgres
   transaction that also commits its verified generic effect, reconciled
   domain link, generic operation result, evidence/artifact references,
   terminal tool event, observations, and cleanup intent.
2. The local finalization callback performs database work only. It cannot call
   a sandbox, connector, model, or other external boundary.
3. The callback is run only for a verified effect. A verification-pending or
   unknown outcome cannot be represented as a successful tool result.
4. Task, agent-run, operation, and effect fences are checked at the commit
   boundary. A stale worker cannot publish either half of the result.
5. Recovery consumes a persisted, identity-bound observation. It may move
   forward from compatible dispatch/acknowledgement/recovery states, but never
   backwards, and never changes an already-finalized hash.
6. Duplicate delivery is idempotent: one stable tool-run identity and one
   result hash; conflicts fail closed. External execution remains at-least-once
   where the sandbox/provider offers no idempotency primitive.
7. A crash before commit leaves all finalization records unchanged. A crash
   after commit is read-only replay; cleanup remains separately retryable.
8. All rows, events, artifacts, and cleanup intents remain workspace-scoped;
   secrets are not added to evidence or diagnostic messages.

## Proposed change

- Add an optional internal transaction-finalizer callback to
  `effectdispatch.ChildRequest` and `effectdispatch.Request`.
- Forward it through `effectdispatch.Runtime` and invoke it inside the same
  workspace transaction as the verified effect, domain-link, and generic
  operation-result writes.
- Add a `ToolRunStore` transaction method that locks the expected tool run,
  verifies its stable request identity and worker fences, then reuses
  `finishToolResultTx` to write the canonical result, event, artifacts,
  observations/cost, and cleanup intent.
- Wire only the tool adapter to this callback. Model and embedding ownership
  remain unchanged.
- Extend sandbox recovery to accept explicitly enumerated interrupted
  dispatch/acknowledgement states and normalize them to recovery-required
  before inspection. Recovery is transactionally fenced and must not repeat
  the external effect. A legacy verified-effect/reconciled-link split with a
  nonterminal tool run is intentionally not auto-repaired because the original
  response body may no longer be available; it requires operator investigation.
- If post-invocation authority/fence validation fails, preserve a recoverable
  unknown outcome and expose it to the existing sandbox recovery route; do not
  report success or run a second process.

No schema migration is expected for atomic success finalization. If the audit
finds that an uncertain `running` tool run cannot be represented or selected by
the recovery route, any schema change must be narrowly justified in a follow-up
note before implementation.

## Crash and retry semantics

| Boundary | Durable state | Allowed next action |
|---|---|---|
| Before external invocation | Reserved/dispatching | Existing fenced dispatch rules |
| External invocation may have started; no proof | Unknown/recovery-required | Inspect/reconcile exact sandbox attempt; never blind retry |
| Provider returned a valid result; before final transaction | Prior nonterminal state | Reconcile persisted attempt result under fresh valid authority |
| During final transaction | Transaction rolls back | Retry local finalization only if the same authoritative result is available |
| After final transaction commits | Verified + reconciled + tool terminal | Read-only duplicate replay; cleanup worker may retry cleanup |
| Hash or identity disagreement | Conflicting durable evidence | Fail closed and retain for operator investigation |

## Licensing and reuse

This change reuses Fornix's own dispatcher, tool store, event, artifact, and
sandbox-recovery abstractions. It adds no copied third-party implementation,
new runtime dependency, or licensing obligation.

## Cost and operational impact

The expected success-path database cost is one additional tool-run row lock
and the existing result/artifact/event/observation writes moved into the
dispatcher’s final transaction; it should reduce cross-transaction repair
work, not add an extra round trip. Recovery remains a bounded local transaction.
No model calls, container images, or new services are introduced by this
slice. A real OCI backend and its cleanup consumer have separate image,
storage, CPU, and operational budgets.

## Acceptance tests

- Successful tool finalization commits the tool result, generic operation
  result, verified effect, reconciled link, artifact references, terminal event,
  observations, and cleanup intent atomically.
- Injected failure at each finalization write rolls back all final records;
  retry with the exact same result succeeds once.
- Duplicate invocation after commit does not call the sandbox again and
  returns the canonical terminal tool run.
- Task/agent/effect fence loss rejects finalization and cannot write a partial
  success.
- Unknown and pre-finalization crash states reconcile only from the exact
  sandbox attempt identity and matching result hash.
- Already-terminal replay with matching evidence is read-only; mismatched
  observation or result hashes fail closed.
- Existing tool, effect-dispatch, recovery, workspace-isolation, cleanup, and
  smoke suites remain green.

## Limits remaining after this slice

This does not implement a container runtime manager, Moby Engine adapter,
cleanup queue worker, durable host/container reconciliation loop, production
deployment qualification, or a general external-effect reconciler. Those
remain explicit production-readiness gates.
