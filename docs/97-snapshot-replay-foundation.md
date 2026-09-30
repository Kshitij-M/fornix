# Snapshot and cursor-based operation replay foundation

Status: implemented; large-history capacity qualification remains open.

## Invariants

- Operation projection and transition history are read from one Postgres
  repeatable-read, read-only snapshot.
- Replay is bounded by a maximum page size and can continue from the returned
  `next_from_version` cursor.
- Every page validates version continuity, previous-state hash, state hash,
  lifecycle transition validity, event existence, event workspace, event type,
  and event payload identity.
- A completed replay page must agree with the snapshot's current operation
  state hash; a partial page reports `has_more=true` and is not misrepresented
  as a full replay.
- Replay is read-only and never invokes a model, tool, connector, broker, or
  external callback.

## API shape

The existing replay request remains compatible:

```json
{"from_version": 0, "limit": 256}
```

The response now includes `current_state_version`, `current_state_hash`,
`next_from_version`, `has_more`, and `complete`. A caller continues by sending
`from_version=next_from_version`. The existing `state_version` and
`state_hash` identify the end of the returned verified page.

## Efficiency decision

Replay from sequence zero validates a bounded prefix. Replay from a non-zero
checkpoint reads only the checkpoint row plus the requested page, making
continuation cost proportional to the page rather than the complete history.
The checkpoint row remains authoritative append-only history; callers should
retain its hash in an evidence or operator record when a stronger external
checkpoint contract is required.

## Cost, storage, and licensing

The implementation adds no schema, WAL, or durable storage. Each page uses one
read-only transaction and at most `limit+1` transition rows plus one checkpoint
row. The extra event payload checks are bounded by the page. Existing Fornix
MIT code and Postgres authority are reused; no reference source is copied.

## Acceptance tests

- full replay from zero remains deterministic;
- a small page returns a continuation cursor and does not fail merely because
  the history is longer than the page;
- continuation reaches the same terminal state hash as full replay;
- replay from a checkpoint is deterministic;
- event payload tampering or missing event linkage fails closed;
- concurrent mutation is observed through one consistent snapshot;
- replay has no external effects and respects workspace scope.
