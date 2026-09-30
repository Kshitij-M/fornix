# Generic operation queue and worker-claim foundation

Status: implementation foundation for universal production qualification;
adapter execution, external-effect dispatch, and distributed scheduler
qualification remain separate boundaries.

## Problem

The generic operation authority already persists operation identity, retries,
leases, fences, and replayable transitions. A worker still needs a durable way
to select due work without a broker or an in-memory queue. Manual lease calls
are sufficient for an operator demo but are not a complete control-plane
substrate for long-running work.

## Invariants

- Postgres remains the only queue authority; no broker or process-local queue
  is introduced.
- Selection is workspace-scoped, bounded, deterministic, and ordered by due
  time, creation time, and operation ID.
- The claim transaction locks selected operation rows, acquires the existing
  monotonic operation lease fence, and commits all claims atomically.
- An active, unexpired lease is never claimed by another worker.
- An expired or released lease can be taken over only with a higher fence.
- The queue does not dispatch provider calls or external effects. In
  particular, `awaiting_external` remains outside generic queue selection;
  effect recovery uses the independent effect lease and reconciliation API.
- Workspace scope is applied both through explicit predicates and the
  transaction-local RLS context introduced by migration 044.
- A worker crash before commit claims nothing; a crash after commit leaves a
  durable lease that expires and can be recovered.

## Schema and reuse decision

No new migration is required. The existing `operations.next_retry_at`, status,
creation order, and `operation_leases` table already contain the authority
needed for a queue claim. The implementation reuses `OperationStore` lease
fencing and the PostgreSQL `FOR UPDATE SKIP LOCKED` pattern used by the
reference schedulers, adapted to Fornix's workspace and append-only history
rules. No reference source is copied; Fornix remains MIT licensed and no
Kronaxis Fabric code is used.

## Cost and fairness budget

Each claim batch performs one bounded indexed candidate query plus one lease
mutation per selected operation in the same transaction. The queue limit is
clamped to 64. The per-workspace API is intentionally explicit: cross-workspace
fairness requires an authenticated scheduler policy and is not inferred from a
single workspace claim call.

## Acceptance tests

- due operations are returned in stable order and never exceed the batch limit;
- active leases are excluded and expired leases are taken over with a higher
  fence;
- concurrent claimers produce one active owner per operation;
- a stale owner cannot renew, release, or advance a claimed operation;
- a failed transaction leaves no committed lease claim;
- `awaiting_external` is not claimed by the generic queue;
- workspace data cannot cross a claim request;
- duplicate claim delivery is represented by the same durable lease/fence;
- unit, Postgres integration, race, CI, smoke, and documentation checks remain
  green.

The authenticated HTTP route `POST /v1/operations/claims` and
`fornix operation claim` expose the same store authority. They return typed
operation/lease metadata only and require `operation:execute` in the caller's
workspace.

## Remaining limitations

This is a durable claim primitive, not a complete scheduler. It does not add a
background worker loop, global fairness coordinator, provider dispatch,
external-effect verification, HA/failover, autoscaling, or a broker. Those
must be qualified at deployment and adapter boundaries.
