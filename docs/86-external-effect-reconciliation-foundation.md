# External-effect reconciliation foundation

Status: implemented on the universal production-qualification branch.

## Why this boundary exists

Fornix is a control plane for work that may cross a remote system boundary.
The database can make the reservation, lease, state transition, evidence
reference, and recovery decision durable. It cannot make an HTTP request,
payment, ticket mutation, deployment, or other remote side effect exactly
once. The public API therefore needs an explicit reconciliation surface for
the uncertain interval between a connector dispatch and a provider response.

This slice exposes the existing Postgres authority without moving connector
execution into the server transaction:

1. reserve an external effect before dispatch;
2. read its current state by workspace and effect identity;
3. record dispatch, acknowledgement, verification, compensation, or recovery
   transitions with a lease fence and idempotency key.

## Invariants

- The workspace, operation, attempt, step, and effect identities must agree.
- A caller must authenticate into the operation workspace and be the actor
  bound to that operation; a caller cannot impersonate another operation actor.
- The operation lease owner is the authenticated actor. A stale owner or fence
  fails closed before any effect state or history is changed.
- Task-bound operations retain their existing task-owner and live task-fence
  validation. The operation store remains the task-fence authority.
- Effect state is a projection of append-only transition history. No endpoint
  overwrites the authoritative transition log.
- State transitions follow the explicit finite-state graph. Terminal
  `verified` and `compensated` states cannot be changed.
- Duplicate idempotency keys return the committed state only when their
  command hash is identical; a different command is a conflict.
- Provider request IDs and response, verification, and compensation values
  are references or SHA-256 hashes. Raw payloads, credentials, headers, and
  arbitrary provider text are not accepted by this surface.
- External delivery remains at-least-once or unknown. The API never claims
  exactly-once provider execution and never retries a remote call implicitly.

## Schema and reuse decision

No new migration is required. Migration 035 owns immutable operation attempts
and effect reservations; migration 036 owns the current effect projection and
append-only transition history. Reusing these tables avoids a second source of
truth and keeps recovery queries and replay hashes compatible with existing
operation history.

The HTTP and CLI layers use `OperationStore.ReserveEffect`,
`AdmissionStore.GetEffectState`, and `AdmissionStore.UpdateEffect` directly.
They do not duplicate state-machine or fencing logic.

## Crash and concurrency semantics

- A crash before reservation commit leaves no authoritative effect.
- A crash after reservation commit leaves `reserved`; a worker can inspect it
  and reconcile it explicitly.
- A crash after a transition commit leaves the new state and append-only
  history durable. Repeating the same idempotency command is a read-only
  duplicate.
- A crash after a provider dispatch but before acknowledgement is represented
  as `dispatched` or `recovery_required`; the provider must be queried or
  compensated by a domain connector according to its contract.
- Concurrent transitions serialize on the effect state row. A stale fence,
  invalid transition, or terminal state fails without a partial write.

## Cost and operational budget

The new surface adds one bounded Postgres transaction for reservation and one
for each reconciliation command. Reads are indexed by `(workspace_id,
effect_id)`. No provider call, broker, queue, object store, or new process is
introduced. The caller controls the retry and polling cadence; Fornix does not
create hidden remote traffic.

## Licensing and reuse

The implementation reuses Fornix’s own MIT-licensed contracts and stores. No
source is copied from Kronaxis Fabric, whose BSL 1.1 license is incompatible
with this repository’s MIT distribution.

## Acceptance tests

- reserve and read an effect through the authenticated API;
- reconcile a valid state sequence and return a stable state projection;
- repeat a transition idempotently and reject a same-key command conflict;
- reject cross-workspace effects, non-actors, stale operation fences, and
  invalid state transitions;
- roll back a transition at the injected pre-commit failure point;
- verify that raw secrets and arbitrary provider payloads cannot enter the
  request or response contract;
- verify that CLI commands use the same API semantics and bounded fields.

Remaining qualification: a generic worker/connector still needs domain-owned
verification and compensation implementations, provider callback handling,
tenant defense-in-depth, backup/restore drills, and load/failure-injection
evidence before Fornix can be called production-ready.
