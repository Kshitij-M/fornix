# Terminal operation admission foundation

Status: implemented; long-history replay and external-effect recovery remain open qualification work.

## Problem

An operation can reach a terminal state while a stale or duplicate delivery
still attempts to acquire a lease, reserve an attempt/effect, record a
callback, or start connector execution. Without an authority-level guard,
those deliveries can create cost, duplicate work, or a new external boundary
after the operation is complete.

## Invariants

- `succeeded`, `failed`, `cancelled`, `dead_letter`, and `abstained` are
  immutable operation states.
- New leases and new attempt/effect/callback records are rejected after
  terminality.
- Generic connector execution is rejected before connector lookup/admission
  when the operation is terminal and has no committed result.
- A committed duplicate may still read and return its original immutable
  result, attempt, effect, or callback; duplicate delivery is not treated as
  new work.
- An already-reserved external effect remains independently reconcilable after
  its parent operation becomes terminal. Recovery must not be blocked by the
  parent projection's terminal state.
- Existing fencing, workspace, actor, and idempotency checks remain in force.

## Implementation decision

The guard is placed in the Postgres transaction after duplicate lookup and
before any new authoritative admission or lease mutation. This preserves
idempotent replay while making terminality authoritative rather than relying
on HTTP callers or adapters to remember the rule. The connector HTTP path
performs the same early check to avoid unnecessary registry work.

The implementation reuses the existing operation status contract and
`contracts.IsTerminalOperationStatus`; it does not introduce a second state
machine or copy reference-repository execution code.

## Schema, licensing, and cost

No migration is required. The change uses the existing status/check constraints
and adds no rows or indexes. Safe terminal rejection adds one status branch
under an already locked operation read. It prevents post-terminal connector,
lease, and database work. All code is original Fornix code under the repository
MIT license; no BSL reference source is copied.

## Acceptance tests

- terminal operations reject lease acquisition;
- terminal operations reject new attempt, effect, and callback admission;
- a duplicate committed result remains readable without a live lease;
- connector execution stops before adapter invocation for terminal operations;
- stale workers remain rejected by fencing;
- effect state for a previously reserved effect can still be reconciled after
  parent terminality;
- all existing unit, integration, race, smoke, and documentation checks pass.
