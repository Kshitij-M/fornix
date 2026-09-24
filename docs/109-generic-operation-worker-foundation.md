# Generic operation worker foundation

Status: implemented as an adapter-owned, Postgres-backed claim-consumption
slice. It is not a provider dispatcher and it does not claim exactly-once
execution for external systems.

## Why this boundary exists

The generic operation queue can now select due work and assign a monotonic
workspace-scoped fence. A claim must still be consumed by a worker process
without turning a process-local callback into a second source of authority.
This slice supplies that runtime boundary:

```text
Postgres claim -> fenced adapter handler -> durable adapter-owned mutation
              -> lease release, or lease expiry for recovery
```

The worker deliberately does not invent capability plans, provider credentials,
approval decisions, result records, or external-effect transitions. An adapter
handler receives the complete claim, including the operation request and fence,
and remains responsible for using the existing operation store and effect
authority for its domain.

## Invariants

- Postgres is the only queue and lease authority; the worker has no durable
  in-memory queue.
- A worker handles only claims returned for its owner and workspace. The
  operation fence is passed unchanged to the handler.
- A heartbeat renews the same owner/fence while a bounded handler is running.
  A failed renewal cancels the handler context and the worker does not release
  the lease as if the work had completed.
- Successful handler completion releases the lease. Handler failure leaves the
  lease to expire so a recovery worker can take it over without a hot retry
  loop. The handler must persist an appropriate retry, terminal, verification,
  or recovery transition before returning success.
- Release and renewal failures are surfaced; they are never hidden behind a
  successful callback result.
- A handler may perform external work only through its adapter's explicit
  effect reservation and reconciliation contract. The worker itself performs
  no external call and cannot provide exactly-once semantics.
- Claims are bounded by a maximum batch size and a bounded lease TTL. Polling
  is cancellable and deterministic; fairness across workspaces remains an
  authenticated scheduler policy rather than an implicit global scan.

## Crash and fencing semantics

If the process crashes before a handler commits its authoritative mutation,
the claim remains leased until expiry and can then be taken over with a higher
fence. If it crashes after a handler commits but before lease release, the
durable operation idempotency key and result/transition authority make the
recovery callback safe to replay; the next worker must still use the new fence.

If a heartbeat loses the lease while an adapter is running, the handler is
cancelled and the worker reports the fencing failure. The adapter must treat a
cancelled context as a possible at-least-once boundary and reconcile any
uncertain external effect before retrying.

## Reuse and licensing

The implementation reuses Fornix's `OperationStore.ClaimReady`, monotonic
operation leases, `SKIP LOCKED` ordering, and explicit external-effect
reconciliation. The shape is informed by the worker/lease patterns in the
reference repositories, but no reference source is copied. Fornix remains
MIT-licensed; no Kronaxis Fabric code is used.

## Cost and operational budget

Each poll performs one bounded claim transaction. While work is active, the
worker adds one small lease-renewal transaction per heartbeat interval and one
release transaction after successful handling. The handler's model, database,
network, and artifact budgets remain domain-owned. A deployment must size the
poll interval, lease TTL, connection pool, and workspace fairness policy from
measured workload data; this slice does not establish production SLOs.

## Acceptance tests

- two workers cannot handle one active operation concurrently;
- a heartbeat preserves a claim through a handler longer than one heartbeat
  interval;
- a stale or expired fence cancels/blocks completion and is recoverable by a
  higher-fence owner;
- successful handling releases the lease and a duplicate poll finds no active
  work;
- handler failure leaves the claim recoverable after expiry rather than
  immediately hot-looping;
- cancellation stops polling and handler work without a false success;
- workspace scope is preserved by the claim and handler contract;
- the operator smoke exercises queue claim, renewal, release, takeover, and
  stale-fence rejection;
- unit, Postgres integration, race, CI, smoke, and documentation checks pass.

## Remaining limitations

There is no generic provider dispatcher, global fairness coordinator, resource
serialization scheduler, autoscaling, HA/failover controller, or external
secret manager here. Those remain deployment and adapter-specific work. A
worker callback that performs a remote effect must use the independent effect
lease and verification path; this package cannot convert at-least-once remote
delivery into exactly-once execution.
