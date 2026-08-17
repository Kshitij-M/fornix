# Loop 2 feature note: typed control-plane events and state deltas

Status: implemented
Date: 2026-08-16

## Problem

The current service mutates task and session state directly. That gives callers a current status, but not a durable history of what changed, who changed it, why it changed, or how a consumer can resume after a crash. The coordination table is a compatibility channel, but its free-form body is not a typed state protocol.

This slice adds the smallest durable substrate needed to make one mutation path event-sourced without rewriting the server or changing the existing task API shape.

## Scope

The vertical slice covers task completion only:

```text
POST /v1/task/:id/complete
  → transactionally update task/session state
  → append one typed control event with state deltas
  → reserve/resolve an optional idempotency key
  → commit both or neither
```

The event store also exposes deterministic read-after-sequence, bounded replay, and monotonic consumer checkpoints for the next projection/subscription slice. It does not yet project all historical events back into every legacy table.

## Contracts

`internal/contracts` defines:

- `EventEnvelope`: event ID, global sequence, type, schema version, occurrence/recording times, scope, actor, task/session references, causation/correlation IDs, idempotency key, raw payload, deltas, artifacts, and provenance.
- `StateDelta`: deterministic `set`, `add`, `remove`, or `delete` operation over a typed path, with JSON value and evidence references.
- `ArtifactReference`: content/address/type metadata; the event stores references, not large artifact bodies.
- `Provenance`: source event and artifact references plus optional source paths.

The default workspace is `default` for backward-compatible starter calls. Workspace is stored explicitly in the event row and is never inferred from a natural-language field.

## Invariants

1. An event row is append-only. No API in this slice updates or deletes it.
2. Every committed event has a unique event ID, positive schema version, valid JSON payload, and a strictly increasing database sequence in the global append order.
3. An idempotency key is scoped by workspace. Repeating the same key and request hash returns the original event; reusing the key for a different request fails closed.
4. Task/session mutation and event append share one Postgres transaction. A rollback leaves no task change, event, or idempotency reservation.
5. Consumer checkpoints are monotonic. A stale checkpoint cannot move a consumer backwards.
6. Reads are ordered by sequence, never by wall-clock timestamps.
7. Raw payload JSON and evidence references remain recoverable. Compaction and embeddings are future projections, not authority.
8. No event payload contains the bearer token or other secret material.

## Storage

Migration `002_control_events.sql` adds the tables; migration `003_control_events_append_only.sql` adds the database guard:

- `fornix.control_events`: append-only event envelope, raw JSON payload, typed JSONB projections, and global `BIGSERIAL` sequence.
- `fornix.idempotency_records`: workspace/key reservation, request hash, and resolved event sequence.
- `fornix.control_checkpoints`: workspace/consumer cursor with monotonic advancement.
- `fornix.control_events` rejects `UPDATE` and `DELETE` at the database boundary; migrations remain immutable and checksum-validated.

The global sequence is the deterministic ordering key for this first slice. Workspace-scoped filtering keeps consumers from seeing other workspaces while avoiding a second counter/locking protocol. Sequence allocation may contain gaps after rolled-back transactions; readers treat gaps as normal and continue from `sequence > cursor`.

## Reference reuse

- Orloj `eventbus/memory_bus.go`: reused the typed event/filter vocabulary and since-ID semantics conceptually; not copied because memory bus history is bounded and non-authoritative.
- Orloj `store/session_store.go` and `session_checkpoint_store.go`: reused the transactional shape, ordered event sequence, idempotency-key handling, lease/fence validation, and checkpoint verification ideas; reimplemented for pgx/Postgres and Fornix contracts.
- Orloj migration runner pattern already adopted in Loop 1: numbered embedded migrations, advisory lock, checksums, and commit-before-record.
- Task completion remains the compatibility API; this slice adds the event record underneath it.

No source code is copied from Orloj. Orloj is Apache-2.0. Fornix is released under MIT with copyright permission confirmed by the project owner.

## Cost and performance budget

- No model, embedding, reranker, broker, or new service is introduced.
- One task completion adds one event insert, one idempotency operation only when a key is supplied, and one checkpoint-independent append sequence allocation inside the existing transaction.
- The integration target is p95 local append overhead below 10 ms excluding the existing task transaction and network latency.
- Event payloads are bounded by the existing HTTP body limit; large artifacts must be referenced rather than embedded.

## Acceptance tests

- Fresh and existing databases apply migration 002 and preserve migration checksums.
- Concurrent identical appends with one idempotency key produce one event.
- Reusing a key with a different request returns an idempotency conflict and changes nothing.
- A rolled-back transaction leaves no event or idempotency record.
- Read-after-sequence and replay return the same ordered event IDs/sequences.
- Checkpoints advance monotonically and survive a new store instance.
- Task completion returns the same event identity for a duplicate request.
- Existing v0.5–v0.10 smokes and all Go/Python/CI-equivalent checks remain green.
