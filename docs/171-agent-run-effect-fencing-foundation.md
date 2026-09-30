# Task 74A — Agent-run fenced model and tool effects

Status: proposed implementation slice

## Why this slice exists

Fornix already leases agent runs to workers and protects the durable loop
checkpoint with an agent-run fencing token. Model calls and tool runs, however,
currently carry task ownership when a task exists but do not carry the agent-run
lease when a run is taskless. That leaves a narrow but important stale-worker
window: a worker that loses its run lease can finish an in-flight model or tool
record after a replacement worker has taken over.

This slice closes that mutation boundary without changing the external provider
or tool execution model. The durable Postgres record becomes explicitly bound
to the agent-run owner and fencing token. A stale worker may still have an
already-started external call in flight—exactly-once external execution is not
possible—but it cannot commit the resulting model/tool effect or advance its
durable lifecycle after losing the lease.

## Invariants

1. A run-bound model call or tool run belongs to exactly one workspace and one
   agent run.
2. The persisted owner ID and fencing token are immutable identity attributes
   of that effect attempt.
3. A bound effect can be started, retried, or finalized only while the same
   workspace-scoped agent-run lease is valid.
4. A stale owner fails closed; it cannot change status, usage, output, event
   history, or checkpoint state.
5. Agent-run binding is optional only for legacy standalone model/tool API
   calls. New agent-loop production calls always carry the binding.
6. Workspace and actor metadata remain explicit and are never inferred from a
   request ID.
7. Existing authoritative model/tool history is append-only and compatible;
   migration of old rows does not invent an agent-run owner.
8. Duplicate request identities remain idempotent. A duplicate with a
   different agent-run binding is rejected rather than silently reattached.

## Transaction and crash semantics

The store validates the agent-run lease in the same transaction that creates or
mutates the model/tool record. The run row is locked before the validation so
lease takeover and effect mutation serialize in Postgres. Commit makes the
effect transition and its fencing proof durable together; rollback leaves both
unchanged. A crash before commit is safe to retry. A crash after commit is
replayable and the idempotency key returns the existing effect.

The provider or external tool remains an at-least-once boundary. If a lease
expires while remote work is executing, the external work may still happen,
but its stale result cannot become authoritative. Recovery must reconcile from
the durable effect record rather than assume exactly-once remote execution.

## Schema changes

Migration 065 adds nullable agent-run reference, owner, and fence columns to
`model_calls` and `tool_runs`, with pairing and non-negative-fence checks and
workspace-scoped lookup indexes. Existing rows remain valid and unbound. No
new infrastructure or authority is introduced.

## Reuse and licensing

The implementation reuses Fornix's existing `AgentRunLease`, lease validation,
transaction helpers, model-call ledger, tool-run ledger, event store, and
agent-loop worker context. Orloj's claim/fence semantics and DeepSeek
Harness's explicit execution boundaries informed the design; no reference
source is copied. The repository and this slice remain MIT-licensed. Kronaxis
source is not used because its repository is BSL 1.1.

## Cost and storage budget

The feature adds three small scalar/reference fields per model-call and tool-run
row plus indexes. It adds no payload duplication, provider calls, broker, or
background service. Indexes are scoped to workspace/run lookups and should be
removed or consolidated if production measurements show they are redundant.
The expected request-path cost is one lease validation query within the existing
transaction for bound effects; no extra round trip is required.

## Acceptance tests

- Fresh and existing schemas migrate cleanly.
- Agent-loop model and tool requests carry the current run fence.
- A stale run worker cannot start, retry, or finish a bound effect.
- Lease takeover permits the new owner to continue without accepting the old
  owner's result.
- Duplicate bound submissions return one durable effect and reject conflicting
  bindings.
- Crash/rollback leaves effect and lease state unchanged; post-commit replay is
  deterministic.
- Taskless standalone model/tool calls remain backward compatible.
- Workspace mismatches fail closed.
- Existing unit, race, integration, smoke, build, and migration checks remain
  green.

## Explicit limitation

This slice prevents stale durable mutation; it does not cancel or reverse an
external model/provider or tool process that already crossed the execution
boundary. The universal roadmap still requires deployment-owned connector
recovery drills, sandbox policy qualification, and live provider/tool
conformance before a production readiness claim.
