# Loop 4 completion: workspace-scoped consumer leases and fencing

Status: complete

## Delivered

- Added typed `contracts.ConsumerLease` and `ConsumerLeaseResult` contracts
  with explicit workspace, consumer, owner, expiry, release, and monotonic
  fence fields.
- Added migration `006_consumer_leases.sql` with one current lease row per
  `(workspace_id, consumer_id)`, positive-fence checks, non-empty identity
  checks, and an expiry lookup index.
- Added transactional `EventStore` acquire, renew, release, read, validate,
  and lease-authorized checkpoint APIs. Acquisition is idempotent for the
  current owner, rejects active competitors, and increments the fence on
  expiry or release takeover.
- Integrated ownership into `internal/projection.Runner`. Both bounded batch
  application and rebuild validate the exact owner/fence token before derived
  writes; projection effects and checkpoint advancement remain one Postgres
  transaction.
- Added tests for one-owner concurrency, stale tokens, expiry/takeover,
  renewal/release rollback, projection crash boundaries, duplicate delivery,
  replay/hash equivalence, stale checkpoints, and workspace isolation.
- Added the v0.13 Docker smoke, Makefile command, CI integration step, and
  architecture/qualification updates.
- Fixed an existing concurrent-append race by cloning events at the store
  boundary before normalization. Duplicate callers can now safely submit the
  same event value concurrently.

## Qualification

The local Docker/Postgres qualification passed:

- Existing database: migrations `001_initial` through
  `006_consumer_leases` remained checksum-valid and the lease table was
  present.
- Fresh disposable database: all six migrations applied cleanly and created
  `fornix.consumer_leases`; the database was removed after verification.
- `make check`, `make build`, `make python-check`, full `go test -race ./...`,
  and the complete `make smoke` sequence passed.
- Consumer lease acquire/reuse latency, 20 samples: p50 `1.084 ms`, p95
  `1.163 ms`, max `1.220 ms`.
- Event append latency, 20 samples: p50 `717 µs`, p95 `979 µs`, max
  `1.193 ms`.
- Projection batches of 10 events: p50 `7.024 ms`, p95 `10.581 ms`, max
  `10.581 ms`.
- Rebuild of 100 events: `57.4 ms`, approximately `1,744 events/s`.
- Empty lease table plus expiry index occupied `80 KiB` in the local
  PostgreSQL database. Growth is one current row per known workspace/consumer
  pair; no event or artifact bodies are duplicated.

## Database work and cost

Lease acquisition uses one short Postgres transaction with an insert-if-needed,
one locked current-row read, and an update only for takeover. Renewal and
release validate the locked row and update it in one transaction. A projection
batch adds the lease-row lock/validation to the existing checkpoint lock,
ordered event read, projection writes, and checkpoint advance; the checkpoint
boundary performs a defensive second fence validation in the same transaction.
There is no broker, cache, worker service, model call, embedding, or new
infrastructure.

## Remaining limitations

- The lease substrate protects projection consumers; task execution still
  needs dependency-aware scheduling, worker leases/fences, retry budgets,
  cancellation, and dead-letter policy.
- Lease state is current coordination metadata, not an append-only lease audit
  stream. Metrics/exporter and history retention are deferred.
- The projection runtime remains an explicit bounded pull API. It does not
  start a background subscriber or expose public replay endpoints.
- Callers must renew between bounded batches when processing can exceed the
  configured TTL. The default is 30 seconds and the maximum is 10 minutes.
