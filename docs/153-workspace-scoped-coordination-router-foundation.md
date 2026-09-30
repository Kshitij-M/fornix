# Workspace-scoped coordination and router authority foundation

Status: implemented; this note defines the Task65 vertical slice and its
qualification boundary.

## Problem

The original coordination and router-learning tables predate Fornix’s
workspace authority model. Their rows have no tenant key, and their handlers
use direct global SQL. Keeping those routes available behind a permission is
containment, not universal safety: one privileged compatibility caller can
still observe or mutate data that cannot be attributed to a workspace.

## Scope of this slice

This slice introduces workspace-scoped durable authorities for:

- coordination messages used by local operators and workers;
- router observations used to derive deterministic model recommendations.

The existing federation peer and remote-polling path remains quarantined. It
currently stores a raw bearer token and performs outbound work without a
workspace-bound managed credential lease. It will be replaced only after the
credential-reference and controlled-egress boundary is integrated.

## Invariants

1. Every new coordination message and router observation has one explicit
   workspace identity and cannot be read or written from another workspace.
2. Postgres is the authority; application predicates and transaction-local
   RLS context are both required.
3. Duplicate submissions with the same workspace/idempotency key return the
   original record without appending a second effect or event.
4. Reusing an idempotency key with a different request hash fails closed.
5. Coordination reads use a bounded, monotonic workspace sequence and stable
   tie-breaking; they never read the historical global table.
6. Router recommendations are derived only from the requesting workspace’s
   observations and have deterministic score/order rules.
7. Historical global rows are preserved for audit but are never copied into a
   workspace without an explicit ownership proof.
8. Actor, request, causation, correlation, and provenance metadata are
   propagated without credentials or raw bearer tokens.
9. Raw coordination bodies are bounded and preserved as authoritative payload
   only in the new workspace-scoped record/event; recommendation responses
   disclose aggregates, not raw observations.
10. The quarantined federation poller remains disabled until its replacement
    can resolve a managed credential lease immediately before egress.

## Schema changes

Migration 061 adds workspace-scoped coordination and router-observation tables
under `fornix`, with workspace/idempotency uniqueness, bounded JSON metadata,
indexes for read-after-sequence and recommendation windows, and RLS policies.
The migration is additive and does not mutate, reassign, or delete rows from
`public.coord_messages`, `fabric.coord_messages`, `fabric.federation_peers`, or
`fabric.router_observations`.

## Reuse and licensing

The store reuses Fornix’s `EventStore`, `WithWorkspaceTx`, idempotency
contracts, observability metadata, and RLS qualification patterns. It copies
no reference source. Kronaxis Fabric source remains excluded because its BSL
1.1 license is not compatible with direct reuse; the repository’s own code is
MIT-licensed.

## Cost and efficiency budget

Coordination writes perform one bounded workspace transaction, one unique-key
arbitration, and one append-only event. Reads are bounded by a caller limit
and indexed by `(workspace_id, sequence)`. Router recommendations scan only a
bounded recent window for one workspace and aggregate in Postgres; they do
not invoke a model, embedding provider, broker, or remote service. The new
tables add one durable row per accepted message/observation and no duplicate
global copy.

## Failure semantics

If the domain row commits but the event cannot commit, the transaction rolls
back. If duplicate delivery arrives after commit, the existing row and event
are returned. A connection crash before commit leaves neither authority; a
crash after commit is replay-safe. Cross-workspace reads fail through explicit
predicates and RLS. Federation remains unavailable rather than silently
falling back to raw-token polling.

## Acceptance tests

- fresh and existing databases apply migration 061 cleanly;
- duplicate coordination and router submissions are idempotent;
- conflicting idempotency keys fail closed;
- concurrent writers preserve one effect and deterministic sequence order;
- read-after-sequence is bounded and workspace isolated;
- event replay reproduces the durable record identity;
- router recommendations are deterministic and cannot use another workspace;
- RLS blocks direct cross-workspace access under NOBYPASSRLS;
- historical global rows remain untouched;
- the legacy federation surface and raw-token poller remain unavailable;
- offline, race, full Postgres, qualification, docs, package, and smoke checks
  remain green.

## Follow-up boundary

The next federation slice must add a workspace-scoped peer record containing a
credential reference (never a bearer token), lease/fence validation, bounded
controlled egress, provider idempotency semantics, and recovery for unknown
remote outcomes before enabling remote polling.
