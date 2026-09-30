# Universal operation supervisor, quotas, and resource serialization

Status: implemented as a bounded scheduling and resource-coordination
foundation; it is not a production-readiness declaration.

This slice advances the universal transformation tracked by
[Issue #38](https://github.com/Kshitij-M/fornix/issues/38) and the execution
qualification work in
[Issue #40](https://github.com/Kshitij-M/fornix/issues/40). It addresses the
queue boundary shared by repository work, incidents, HTTP/API actions, SQL
read operations, deployment workflows, and future domain adapters.

## Problem

The generic operation worker already had durable claims, heartbeats, and
monotonic fences, but a deployment still needed explicit policy for two
production concerns:

1. multiple operations must not mutate the same declared resource concurrently;
2. a busy workspace must not consume unbounded worker capacity or starve other
   workspaces.

Process-local maps cannot safely enforce either rule across replicas. A
resource lock or quota that is not backed by the same Postgres transaction as
the operation lease can disagree with the authoritative operation state after
a crash.

## Invariants

- Postgres remains the only authority for operation identity, operation leases,
  resource leases, fences, queue eligibility, and recovery.
- Resource serialization is explicit. An adapter declares
  `OperationResource{ResourceKind, ResourceID}` at operation creation. An
  operation with no declared resource does not acquire an implicit target lock;
  this preserves existing queue semantics and avoids guessing whether two
  domain targets are safely interchangeable.
- Resource keys are workspace-local, normalized as `kind:id`, de-duplicated,
  and sorted before locking. Sorted advisory transaction locks prevent a
  multi-resource operation from deadlocking another operation that declares
  the same resources in a different order.
- The current resource row has an independent monotonic resource fence, while
  every operation mutation still requires the operation owner and operation
  fence. The resource fence is coordination evidence, not a replacement for
  the generic operation authority.
- A resource conflict fails closed for that queue item. The tentative
  operation lease is released in the same claim transaction and the operation
  remains eligible for a later poll; it is not hot-looped.
- Renewal validates the complete expected resource set, locks resource keys,
  and extends operation and resource leases within one transaction. A missing,
  expired, released, or foreign resource fence rejects renewal.
- Release marks the operation and all matching resource rows released in one
  transaction and appends resource ownership history. History is append-only.
- Resource rows are workspace-scoped and protected by RLS policy. The same
  resource key in two workspaces is independent.
- The supervisor receives an explicit workspace list. It never scans a global
  catalog or infers tenant scope from worker configuration.
- Supervisor fairness is deterministic round-robin. `MaxConcurrent` bounds
  process-local turns; the durable `MaxActive` queue option bounds active
  operation leases per workspace with a Postgres advisory workspace lock.
- Worker failures are returned as per-workspace outcomes and do not stop an
  unrelated workspace from receiving its turn. Durable claim failure and
  recovery remain visible through the worker result and the operation lease.

## Schema and migration

Migration `045_operation_resource_leases.sql` adds:

- `fornix.operation_resource_leases`, one current row per
  `(workspace_id, resource_key)`, linked to the operation with a cascading
  foreign key;
- `fornix.operation_resource_lease_history`, append-only ownership history
  for acquisition, takeover, release, and future bounded renewal records;
- workspace indexes, bounded identity/fence checks, append-only mutation
  protection, and workspace RLS policies.

The migration is additive and embedded in the existing ordered migration
runner. It preserves all prior operation rows and leaves existing migration
checksums immutable.

## Scheduling model

```text
explicit workspace set
        ↓ deterministic round-robin
bounded supervisor turns
        ↓ per-workspace worker
Postgres ClaimReadyWithOptions
        ↓ workspace MaxActive quota + SKIP LOCKED
operation lease + declared resource leases
        ↓ adapter handler with operation fence
renew / durable mutation / release or expiry takeover
```

`internal/operationsupervisor` is intentionally a process-level policy
package. It does not own a queue, persist a process scheduler cursor, execute
providers, resolve credentials, or bypass effect reconciliation. A replica
restart resets only the round-robin cursor; Postgres leases and fences preserve
correctness. A deployment that requires cross-restart fairness must provide a
durable workspace inventory and an operational scheduling policy around this
bounded primitive.

## Reuse and licensing

The implementation reuses Fornix's existing operation identity, transactional
lease, `SKIP LOCKED`, event, idempotency, RLS, and effect-recovery contracts.
The scheduling shape is informed by the reference repositories' lease,
checkpoint, controller, and cleanup patterns, but no reference source was
copied. In particular, no Kronaxis Fabric source was copied because that
project is BSL 1.1. Fornix remains MIT-licensed.

## Cost and efficiency budget

- A claim without a workspace quota keeps the existing bounded queue query and
  one claim transaction.
- A quota-enabled claim adds one transaction advisory lock and one indexed
  active-lease count per bounded claim transaction. It prevents excess work
  rather than creating a process-local backlog.
- A declared resource adds one current-row lookup/update and one append-only
  history insert on acquisition/takeover/release. Resource renewals update the
  current row but do not write a history row on every heartbeat, limiting WAL
  growth.
- Resource keys are sorted in memory and capped by existing operation reference
  bounds. The supervisor launches at most `MaxConcurrent` bounded turns and
  does not allocate a global work queue.
- Exact p50/p95/p99 numbers remain deployment-specific. Qualification must run
  the capacity, lock-wait, WAL, pool-saturation, fairness, and recovery
  harnesses against the intended Postgres topology.

## Acceptance tests

- fresh migration creates both resource tables and existing migrations remain
  checksum-clean;
- two operations with the same declared resource cannot be claimed together;
- release permits the next operation and advances the resource fence;
- stale operation/resource ownership cannot renew or release another owner;
- resource renewal keeps the lease alive through a bounded worker handler;
- a missing resource row fails renewal closed;
- the same resource key is usable concurrently in separate workspaces;
- `MaxActive=1` prevents a workspace from holding two active operation leases
  and admits the next deterministic operation after release/terminalization;
- concurrent queue claims have one owner and expiry takeover advances the
  operation fence;
- supervisor rounds are deterministic, deduplicated, bounded, cancellable,
  and starvation-resistant across the explicit workspace set;
- worker errors are retained per workspace while unrelated turns continue;
- unit, Postgres integration, race, CI, and smoke checks remain green.

## Remaining limitations

This slice does not provide autoscaling, a durable global scheduler cursor,
resource discovery, resource-level rate limits, weighted priorities, a
distributed semaphore outside Postgres, or a sandbox. `MaxActive` is a
workspace operation-lease quota, not a complete CPU/memory/network budget.
Adapters must declare resources correctly; Fornix cannot infer safe
serialization boundaries from arbitrary domain payloads. External calls still
have at-least-once semantics and must use effect reservation and reconciliation.
