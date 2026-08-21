# Loop 10 completion — deterministic bounded agent loop

Status: completed vertical slice
Date: 2026-08-19

## Delivered

- Typed agent-run, turn, state, decision, model-step, tool-step, checkpoint,
  failure, budget, and pending-tool contracts in
  `internal/contracts/agent.go`.
- Migration 015 for durable workspace-scoped `fornix.agent_runs`, followed by
  migration 016 for an indexed run-id replay predicate. Existing migration
  checksums remain immutable.
- Atomic Postgres reserve/transition/event/checkpoint writes with
  state-version compare-and-swap, task-fence validation, idempotent creation,
  crash injection coverage, and run-scoped replay.
- Deterministic context compilation before the first model admission. The
  context content hash and bounded evidence rendering are persisted in the
  run history, preventing silent retrieval changes across retries.
- Sequential model-order tool execution through the existing model gateway and
  structured-argv tool executor. Approval, retry, cancellation, terminal
  states, task fencing, and at-least-once external boundaries are explicit.
- HTTP lifecycle endpoints:
  `POST /v1/agent/run`, `GET /v1/agent/run/{id}`,
  `POST /v1/agent/run/{id}/advance`, `/cancel`, `/external/wait`,
  `/external/complete`, and `/replay`.
- Unit, Postgres transaction, concurrency, replay, workspace, crash-boundary,
  and HTTP smoke coverage; CI runs the new integration tests and v0.19 smoke.

## Qualification evidence

Observed in the Docker-backed development database on 2026-08-19:

| Check | Result |
|---|---|
| `go test ./...` | Green; all packages and Postgres integration tests passed |
| `go test -race ./...` | Green after fixing shared model retry-slice normalization |
| agent-loop unit slice | Green; deterministic context/model/tool/cancel tests |
| agent-run Postgres slice | Green; atomic crash rollback, CAS concurrency, replay isolation |
| v0.19 HTTP smoke | Green; fake provider, durable context hash, duplicate run, replay |
| Migration 015 existing checksum | Preserved; migration 016 applied for the replay index |

The successful fake-provider HTTP path measured approximately 20 ms of
application timestamp time and 30 ms wall time on the local Docker/Postgres
setup, including context snapshot, three run events, and one durable
model-call ledger effect. A three-event replay measured approximately 10 ms
wall time and returned a 3.2 KiB JSON response. The in-memory deterministic
loop tests completed below the millisecond scale. These are development
observations, not capacity claims; remote provider and process latency is
excluded from the control-plane estimate.

## Work and storage model

The normal no-tool run performs one repeatable-read retrieval transaction, one
reserve transaction, one context checkpoint transaction, one model-call ledger
transaction, and one terminal checkpoint transaction. Each checkpoint writes
one append-only control event/evidence mirror and replaces only the bounded
`agent_runs` checkpoint projection; authoritative event/evidence history is not
updated or deleted. A tool turn adds the existing tool-run reserve/start/finish
ledger transactions and one ordered agent checkpoint.

On the same local database, observed relation sizes were approximately 128 KiB
for `agent_runs`, 12 MiB for the accumulated `control_events`, 4.9 MiB for
the accumulated evidence mirror, 424 KiB for `model_calls`, and 272 KiB for
`tool_runs`; these include prior loop smokes and are not per-run allocations.
Run history is capped at 256 messages and 4 MiB, pending tool calls at 64, and
the API budget caps turns, model/tool calls, context bytes, output tokens, wall
time, retries, and cumulative cost. Storage therefore grows with event and
evidence history plus the bounded JSON checkpoint, rather than unbounded
transcripts. The new replay index makes run-scoped replay use workspace,
run-id, and sequence ordering; replay response size remains bounded by its
requested limit.

## Determinism and failure boundaries

- Repeating the same request with the fake provider yields the same context
  hash, model request identity, history hash, terminal state hash, and ordered
  event shape (timestamps and database sequences are operational metadata).
- A crash before transaction commit leaves the previous checkpoint and event
  sequence unchanged. A crash after a committed model/tool ledger effect is
  safe to resume through the same deterministic idempotency key.
- Duplicate workers lose the state-version race; duplicate run submissions
  read the existing `(workspace_id, idempotency_key)` row.
- A task fence is checked before model/tool admission and again inside the
  checkpoint transaction. Expiry during an external call fails closed for
  authoritative state, while the external call remains at-least-once.
- No exactly-once claim is made for arbitrary remote providers or local process
  execution. Provider-side idempotency is used where supported; otherwise the
  durable ledger can only make the Fornix-side effect deterministic.

## Remaining limitations

This is a production-quality control-plane vertical slice, not the complete
harness product. Loop 11 adds the single-node Postgres pull worker, but the
system still needs multi-agent/sub-run orchestration, streamed structured
tool-call assembly, general sandbox backends, identity/RBAC, large artifact
storage, retention and backups, metrics/exporters, and capacity/load
qualification. External completion is a bounded integration handoff, not a
claim of exactly-once delivery from the upstream integration.
