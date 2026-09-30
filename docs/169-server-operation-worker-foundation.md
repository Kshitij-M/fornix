# Server-composed generic operation worker foundation

Status: Task 73 repository-owned implementation slice. This note covers
durable worker composition; it is not live-provider, HA, PITR, or production
SLO qualification.

## Problem and scope

Fornix already has a fenced generic operation queue, an adapter-owned worker,
and a fair workspace supervisor. The server did not compose those pieces,
which meant a generic operation required a caller or connector-specific host
to advance it. This slice adds an explicitly configured server worker for
read-only and observation operations. Effectful work remains behind the
existing admission, effect reservation, dispatcher, verification, and
connector-owned execution boundaries.

## Invariants

1. Postgres remains authoritative for operation state, leases, fences,
   transitions, results, and replay. The process supervisor owns only polling
   and fairness.
2. A server worker claims only operations with a persisted plan whose every
   step is `read_only` or `observation`. Created, unplanned, unknown-effect,
   and effectful operations are excluded in SQL and cannot be executed by this
   worker.
3. The worker revalidates workspace, actor identity, connector trust/schema,
   capability health, operation plan hash, and authorization before execution.
4. Every transition and result uses the exact operation owner/fence and task
   owner/fence. Lease loss cancels the adapter context; stale completion fails
   closed.
5. Provider/tool credentials and raw adapter payloads never enter worker
   telemetry or operation records. Results contain bounded hashes and typed
   evidence references only.
6. Workspace inventory is explicit, bounded, active-only, and refreshed. The
   supervisor rotates fairly across the current inventory and never scans
   global operation state without a workspace.
7. The worker is enabled only through the existing `FORNIX_WORKER_ENABLED`
   switch. It adds no broker, cache, database, or infrastructure dependency.

## Reuse and licensing

The implementation reuses `OperationStore.ClaimReadyWithOptions`, monotonic
operation leases, `operationworker.Worker`, `operationsupervisor.Supervisor`,
`connector.Registry`, `connector.Executor`, and `AuthStore`. Orloj’s
controller/worker separation and DeepSeek Harness’s explicit lease/crash
ownership are architectural inputs only; no source is copied. Kronaxis source
is excluded because its repository is BSL 1.1. Fornix remains MIT licensed.

## Cost and operational budget

The worker adds one bounded workspace-inventory query and one bounded queue
claim per polling round, plus the existing adapter and result transactions.
Defaults are four concurrent workspace turns, eight claims per turn, four
active leases per workspace, and a two-second poll interval; every value is
bounded by configuration. No raw payload, artifact, migration, or background
storage is introduced by the worker itself.

## Acceptance tests

- Read-only and observation plans are claimable; effectful and unknown plans
  remain unclaimed.
- The server worker executes a planned read operation exactly once under
  concurrent delivery and records a durable result.
- A stale worker cannot record a result after lease takeover.
- Cross-workspace operation claims and actor identities fail closed.
- Revoked or disabled identities cannot be reauthorized for background work.
- Workspace inventory refresh preserves bounded round-robin fairness and
  cancellation.
- Worker shutdown stops polling without mutating unclaimed operations.
- Existing API, agent-worker, replay, race, smoke, and documentation checks
  remain green.
