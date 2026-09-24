# Capacity and operational qualification foundation

Status: implementation foundation for the universal production-qualification
track. This slice produces bounded local evidence; it is not a distributed
scalability claim or an SLO for an unqualified deployment.

## Problem

Fornix's generic authority is designed to coordinate long-running work, but
correctness tests alone do not show how database work, relation growth, or
contention behave under a repeatable workload. Operators need a safe way to
measure the control-plane path before choosing queue depth, worker counts,
retention windows, or service budgets.

## Qualification invariants

- The workload is explicitly bounded by operation count and worker count.
- Every operation is scoped to a unique disposable workspace and carries a
  stable idempotency key.
- Re-delivery of the same request returns the original durable operation and
  does not add a second authoritative operation.
- Lease ownership uses the existing monotonic operation fence; stale ownership
  is not relaxed for the benchmark.
- Event history and operation rows remain authoritative; measurements are
  observations, not replacements for those records.
- Every measurement identifies whether it is a local sample, database counter,
  or derived percentile. It must not be presented as a production guarantee.
- No model, tool, connector, provider, broker, or external effect is invoked.

## Workload and measurements

The opt-in qualification test runs concurrent create → duplicate replay →
lease acquire → lease release cycles. It reports:

- operation and lease p50/p95/max latency;
- committed/rolled-back transaction and buffer-hit/read deltas from
  `pg_stat_database` for the target database;
- relation-size deltas for generic operation tables and control events;
- operation count, worker count, duplicate-hit count, and elapsed time.

The test defaults to a small local workload and hard-clamps caller-provided
limits. An optional p95 threshold can fail the run. The threshold is a
qualification budget chosen by the operator, not a universal performance
promise.

## Safety and cleanup

The harness requires a dedicated `FORNIX_CAPACITY_PG_DSN`. It creates one
unique workspace and measures before and after state. Authoritative operation
and control-event history is intentionally retained for replay; because those
rows are append-only, the qualification database must be disposable and must
be retired through the database lifecycle rather than row deletion. It must
never run against a production database or a workspace containing user data.
The relation-size delta is captured before the test returns so the report
reflects the workload's actual storage impact.

## Reuse, licensing, and cost

The implementation reuses the existing operation store, event store,
workspace contracts, and Postgres counters. No reference repository code is
copied and no new infrastructure is introduced; the repository remains MIT
licensed. Database cost is bounded by three transactions per operation plus
the bounded measurement queries. Storage cost is reported from PostgreSQL's
relation sizes rather than inferred from payload length.

## Acceptance tests

- the harness is skipped unless an explicit disposable DSN is provided;
- fresh and existing databases migrate cleanly through migration 044;
- concurrent workers create exactly the requested number of operations;
- duplicate delivery returns the original operation and does not add history;
- lease fences remain monotonic and leases are released;
- p50/p95/max and database/storage deltas are emitted without secrets;
- caller limits are clamped to documented bounds;
- an optional p95 threshold fails deterministically;
- the unique workspace keeps all authoritative history isolated; the harness
  does not delete append-only records;
- unit, race, integration, smoke, migration, and documentation checks remain
  green.

## Remaining limitations

This does not qualify HA/failover, network latency, remote providers,
background dispatch, multi-region operation, autovacuum behavior over long
retention windows, connection-pool saturation, or the full legacy table set.
Those require deployment-specific load, soak, backup, failover, and support
evidence.
