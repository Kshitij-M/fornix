# Loop 41 completion: generic operation capacity qualification

Status: implemented as bounded local evidence; HA, soak, failover, and
deployment-specific SLO qualification remain open.

Delivered:

- an opt-in Postgres-backed capacity test for the generic operation authority;
- bounded concurrent create, duplicate replay, fenced lease acquisition, and
  lease release workload generation;
- p50/p95/max operation and lease latency measurements;
- transaction, rollback, buffer, authoritative-row, event-row, and relation
  size measurements;
- deterministic optional p95 budget enforcement;
- unique disposable-workspace isolation that preserves append-only operation
  and control-event history for replay;
- `make qualification-capacity`, development guidance, and public runbook
  documentation.
- CI runs a smaller bounded workload and provisions a disposable non-owner
  PostgreSQL role/database for the RLS qualification.

Measured local qualification on the local Postgres 17 container with 128
operations and four workers:

- create latency: p50 2.24 ms, p95 3.51 ms, max 11.26 ms;
- lease latency: p50 1.24 ms, p95 1.70 ms, max 3.59 ms;
- 128 operations and 128 control events, with duplicate delivery returning the
  original operation for every request;
- observed database deltas: 132 committed transactions, 10,849 buffer hits,
  zero rollbacks, and 622,592 bytes across the measured generic relations;
- no model, tool, connector, broker, provider, or external network call.

These numbers are a repeatable local baseline, not a production capacity
promise. The harness does not qualify connection-pool saturation, autovacuum,
long-retention growth, network latency, HA/failover, multi-region operation,
background dispatch, or every legacy table.
