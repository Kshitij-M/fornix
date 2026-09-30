# Agent-run fencing at the generic effect dispatch boundary

Status: feature note; implementation evidence and qualification limits are in
the Loop 119 completion note.

## Problem

Fornix already carries an agent-run owner and monotonic fence on model and tool
requests, and the specialized model/tool stores validate those values when
recording effects. The generic effect dispatcher performs the final live
operation and task lease check immediately before invoking a provider or tool,
but its authority envelope currently drops the agent-run fence. A run can be
taken over after a specialized request is prepared; the generic dispatch
boundary must independently fail closed rather than relying on an earlier
check.

## Invariants

- An agent-run authority tuple is either absent (a standalone effect) or
  complete: workspace, run ID, owner ID, and a positive fence.
- The tuple must match the live, unexpired workspace-scoped lease in Postgres
  in the transaction that commits the effect's first dispatch intent.
- Validation never acquires or renews ownership. A stale owner cannot regain
  authority by presenting its old tuple.
- A stale agent-run fence must prevent dispatch intent and invoker execution.
- Authority validation and the one-shot durable dispatch permit are atomic. If
  takeover commits first, no permit is created and the reservation stays
  `reserved`; if dispatch intent commits first, a later lease takeover does
  not revoke an external effect already authorized for delivery.
- A committed `dispatching` state is an external-boundary uncertainty marker,
  not proof that the provider/tool has started or completed. Duplicates do not
  invoke again; recovery must reconcile the external outcome.
- Workspace scope comes from the containing effect authority; run IDs cannot
  be used to cross workspace boundaries.
- No credential material or prompt/tool payload is added to the authority.
- Agent-run fields are omitted from serialized authority when not applicable,
  preserving standalone-effect JSON and stable hashes.

## Design and reuse

Reuse the existing `AgentRunLease` validator and Postgres lease table. Thread
the existing owner/fence from the tool request through the child dispatcher
into `contracts.EffectAuthority`, then validate all live fences and commit the
dispatch intent in one short workspace transaction. Reuse the existing
authority validator and effect transition store. Do not hold a Postgres
transaction open during model, connector, or process execution; do not
introduce a second lease, another authority table, or new infrastructure.

The authority is an ephemeral reference to live lease facts; the specialized
tool/model records remain the durable record of which run/fence produced an
effect. This additive, `omitempty` tuple needs no schema migration and does
not change hashes for effects that are not bound to an agent run.

## Cost and failure behavior

The check adds one indexed lease-row lock/read to the existing final authority
transaction for agent-run-bound effects. Standalone effects perform no added
agent-run query. Missing, incomplete, expired, or stale tuples fail closed
before a dispatch intent is committed; the existing reservation remains
replayable under a current owner rather than being stranded in `dispatching`.
The dispatch-intent commit is the authorization linearization point. A takeover
after that point cannot recall an already-issued one-shot permit, and Fornix
does not claim exactly-once or revocable execution across an external provider
or process boundary. That boundary remains at-least-once/uncertain by design.

The implementation reuses the existing validator and therefore introduces no
copied third-party code or licensing change.

## Acceptance tests

- Contract normalization rejects partial and out-of-range run fences and
  accepts a complete tuple.
- Adding/removing the optional tuple changes the authority hash only for
  agent-run-bound effects; absent fields remain omitted from JSON.
- A valid current agent-run owner reaches the generic dispatcher.
- Force takeover after reservation but before the atomic authority/intent
  transaction; prove the stale owner is rejected, the invoker count remains
  zero, and the effect state remains `reserved`.
- Resume that same reservation with the current owner and prove one successful
  invocation and deterministic terminal state.
- Verify dispatch intent committed before a later takeover remains a single
  in-flight external effect and is never invoked again by duplicate delivery.
- The DSN-required `make qualification-agent-run-effect-dispatch-postgres`
  target runs this acceptance test in the Postgres-backed CI job.
- Existing standalone effect hashes, tests, and smokes remain unchanged.
- The focused unit and dispatch integration tests pass; the integration case
  is reported as skipped (not passed) when no disposable PostgreSQL test DSN
  is configured.
