# Loop 11 completion — durable agent-run scheduler and recovery

Status: completed vertical slice
Date: 2026-08-20

## Delivered

- Migration 017 adds deterministic scheduler priority/wake metadata to
  `fornix.agent_runs` and creates the append-preserving
  `fornix.agent_run_worker_leases` table with expiry and owner indexes.
- `ClaimNextAgentRun` uses Postgres `FOR UPDATE SKIP LOCKED`, workspace scope,
  priority/due/created/ID ordering, and one active lease per run. Expired and
  released rows are taken over with a strictly higher fence.
- Heartbeat and release APIs fail closed for stale, expired, released, or
  cross-workspace tokens. `CommitOwned` locks and validates the lease, renews
  it, appends the typed event, and updates the run checkpoint in one
  transaction.
- The orchestrator accepts a worker lease context and routes every scheduler
  transition through the stronger owned commit boundary. The worker polls
  Postgres, heartbeats while model/tool work runs, releases after a bounded
  run, and automatically resumes due retries and approved tool approvals.
- The HTTP service starts one pull worker. It does not add a broker or a
  second authority. `awaiting_external` remains gated by explicit durable
  external completion rather than speculative polling.
- Tests cover ordering, concurrent claims, workspace isolation, expiry,
  takeover, stale workers, crash rollback, atomic renewal, approval gates,
  cancellation, due-retry resumption, blocked model execution, duplicate
  delivery, and scheduler latency. v0.20 smoke coverage and CI commands are
  included.

## Qualification evidence

Verified against the Docker-backed development database on 2026-08-20:

| Check | Result |
|---|---|
| `go test ./... -count=1` | Green |
| `go test -race ./... -count=1` | Green |
| `go vet ./...` | Green |
| `go build` service and watcher | Green |
| v0.10–v0.19 smoke chain | Green with the Docker DSN `host.docker.internal:55433` |
| v0.20 scheduler smoke | Green |
| migration 015/016 checksums | Preserved |
| migration 017 | Applied; checksum `beafb8f4a57ca05c401e83d20276e189c54d8413012244dac96c660d9597eee6` |

The scheduler latency test measured 20 claim samples at approximately p50
2.73 ms, p95 4.39 ms, and maximum 5.76 ms on the local Docker/Postgres
development setup. This includes the transactional queue selection, run lock,
lease insert/takeover, and scheduling-attempt update. Release is one indexed
lease update. An owned checkpoint adds one lease-row lock/renewal to the
existing run-row lock, event append, and checkpoint update transaction.

## Work, cost, and storage model

The worker has no correctness-critical in-memory queue. One empty poll is one
short indexed read transaction; one claimed run adds a primary-key lease
insert or takeover and one scalar scheduler update. Heartbeats are bounded by
the lease renewal interval (normally one per third of the TTL), not by model
tokens or tool output. A normal worker-owned phase uses the existing model/tool
ledger work plus one atomic lease-protected checkpoint. The local scheduler
overhead is therefore a few Postgres statements per claim/phase and no model
call when a run is terminal, awaiting unresolved approval, awaiting external
completion, or not yet due.

Observed relation sizes after the qualification smokes were approximately:

| Relation | Total relation size |
|---|---:|
| `agent_run_worker_leases` | 208 KiB |
| `agent_runs` | 208 KiB |
| `control_events` | 16 MiB |
| `evidence_records` | 8.6 MiB |
| `model_calls` | 472 KiB |
| `tool_runs` | 296 KiB |

The new lease row and four scheduler columns add bounded scalar storage per
run; append-only control events and existing model/tool evidence remain the
dominant growth sources. Queue metadata is operational state, not a duplicate
event history. The worker never stores prompts, credentials, or model output
outside the existing bounded checkpoint and ledgers.

## Determinism and failure boundaries

- Selection is strict priority then due time, creation time, workspace ID, and
  run ID. The workspace ID is only a final tie-breaker, so all-workspace polls
  do not starve older due work merely because of workspace naming.
- A lease takeover increments the fence. A stale worker returning from a
  blocked model call cannot commit its result after takeover; the authoritative
  run remains unchanged.
- A crash before an owned checkpoint commits neither the state transition nor
  the lease renewal. A crash after commit leaves a replayable event and a
  reclaimable or released lease state.
- Duplicate queue delivery is safe because only one lease/fence can commit a
  state-version transition; model/tool duplicate semantics remain those of
  their durable ledgers and external at-least-once boundaries.
- Cancellation is terminal and excluded from queue selection. A cancellation
  racing an in-flight external call wins the state-version/fence boundary;
  the external call itself cannot be retroactively cancelled without provider
  support.

## Remaining limitations

This is a single-node pull worker, not a distributed notification service,
supervisor, autoscaler, HA scheduler, or global fairness coordinator. Polling
cadence and worker count are deployment policy. There is no multi-agent
sub-run graph, identity/RBAC layer, complete sandbox backend, large artifact
store, retention/backup drill, metrics exporter, or capacity qualification.
External model and process effects remain at-least-once, and unresolved
external waits require an explicit completion call by design.
