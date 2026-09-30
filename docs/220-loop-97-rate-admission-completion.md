# Loop 97a: Durable capability rate admission

Status: implementation slice added; full Postgres qualification still requires
the CI or an explicitly disposable local database.

## What changed

Fornix now consumes the signed `RateLimitPerMinute` capability field in the
durable operation-admission path. Admission counts committed, non-denied
decisions in a rolling 60-second window scoped to workspace, connector name,
and capability name. Connector or capability version changes do not reset
that workspace bucket. Duplicate idempotency keys return their original
decision without reserving a second slot, denied requests do not consume
capacity, and pending approvals do.

The reservation is serialized in PostgreSQL with a capability-scoped
transaction advisory lock and indexed decision history. The existing
workspace/actor operation and cost quotas remain in place. A denied operation
admission is returned as HTTP 429. No in-memory limiter or new service is
introduced.

The server's background read-only operation worker, generic workflow
read/observation steps, and incident-workflow connector observations all use
the same durable admission authority before invoking a connector. Rate-limited
generic workflow steps fail with the stable rate-limit reason and are marked
retryable; the connector is not called. Generic workflow automatic
rescheduling is not implemented in this slice, so operators must submit a
fresh operation/workflow after the window expires. Missing durable admission
or effect authority fails closed. Effectful workflow steps continue to use
their fenced effect-dispatch path.

## Tests and commands

Added coverage includes pure policy boundary/hash/redaction checks, persistent
scope and duplicate tests, different-actor concurrency, pending-approval
accounting, rollback recovery, and a Postgres-backed generic workflow test
which proves the denied read is not invoked. The Postgres qualification also
checks that the background worker and incident workflow write durable
admission rows before connector execution. The CI qualification job invokes
these database-backed scenarios against its disposable PostgreSQL service.

Run the non-database checks with:

```sh
make qualification-capability-rate-admission
```

Run the store and workflow integration tests with:

```sh
FORNIX_TEST_PG_DSN='postgres://…' make qualification-capability-rate-admission-postgres
```

The supplied database must be disposable: migrations are applied by the test
suite.

## Performance and operational impact

The implementation adds one capability advisory-lock SQL command (and thus
one database round trip) plus one additional indexed scan to each new
admission. Actor quota count and cost sum are computed in one scan, while the
capability count uses the new partial expression index. It creates no counter
row. Exact latency, lock wait, WAL, and index-size measurements have not been produced
because this local environment has no configured disposable PostgreSQL DSN;
those numbers must come from CI or a local qualification database rather than
being inferred from source.

The capability lock serializes only admissions sharing the same workspace,
connector name, and capability name. It can become a throughput bottleneck for
a high-volume capability and should be load-tested before setting large limits.
Limits are not provider-account-global: sharing one upstream credential across
multiple workspaces still requires upstream limits or a later account-scoped
authority.

## Remaining qualification

- Run the new Postgres tests against the CI PostgreSQL version and confirm
  migration 079 applies to both fresh and already-migrated databases.
- Generic workflows expose a retryable failure but do not yet have an
  automatic scheduler that waits for the capability window and replays the
  connector step.
- The server background worker records an admission denial as a failed
  operation result; it does not currently defer that operation automatically
  until the rate window rolls over.
- Record p50/p95/p99 admission latency, advisory-lock wait under contention,
  index size, and throughput at representative workspace/capability limits.
- Run the full test, race, smoke, vet, and documentation checks after the
  repository's other in-progress changes are integrated.
- Continue Issue #40 production hardening; this slice does not establish
  production readiness by itself.
