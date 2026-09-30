# Loop 67 completion: production federation injection and recovery controls

Status: implemented and qualified on the disposable PostgreSQL path. This is
not a production-readiness declaration; deployment-specific secret-manager,
certificate, HA, retention, and load evidence remain required.

## Delivered

- Added non-secret federation/credential-manager configuration:
  `FORNIX_FEDERATION_POLL_ENABLED`, bounded manager URL, private-network
  switch, token-variable name, and timeout.
- Added `server.NewWithDependencies` and `ServerDependencies`. `server.New`
  remains safe and source-compatible; polling is unavailable unless the
  deployment explicitly injects a lease authority or managed secret manager.
- A supplied `SecretManager` is composed into `ManagedSecretResolver` and the
  Postgres `CredentialLeaseStore`; the exact lease validator is wired into the
  federation poller. Hosted deployments can inject mTLS/workload-identity
  resolvers without changing the universal connector contracts.
- Added `credentials.EnvTokenSource` as an explicit, redacted process adapter
  for the packaged binary. It stores only an environment-variable name and
  never places the token in `config.Config`, events, errors, or evidence.
- Fixed same-owner peer-lease reacquisition so its lifecycle event identity
  includes the bounded lease expiry and does not collide with a previous
  renewal event.
- Added fenced `ReclaimPollAttempt` for expired-peer takeover. A new owner
  must have a strictly higher fence; stale workers cannot reclaim, import, or
  finalize the attempt.
- Added `ReconcilePoll` and `Poller.Reconcile`. Reconciliation hashes and
  decodes a bounded response envelope in memory, checks the recorded response
  hash/provider request identity, performs no remote or credential-manager
  call, and commits idempotent coordination imports plus attempt completion.
- Added migration `063_federation_quarantine.sql` for redacted,
  workspace-scoped historical disposition records with RLS and bounded
  pagination. The operation reads no legacy bearer-token column, does not
  infer workspace ownership, and never mutates old global rows.
- Added authenticated API routes:
  - `POST /v1/federation/poll/reconcile`
  - `POST`/`GET /v1/federation/legacy-quarantine`
- Added unit, concurrency, takeover, duplicate reconciliation, pagination,
  redaction, configuration, and disposable-Postgres migration tests.
- Updated API docs, environment example, smoke output, qualification runbook,
  and documentation index.

## Qualification evidence

The fresh tmpfs PostgreSQL/pgvector run applied all migrations, including 063,
and passed:

```text
FORNIX_TEST_PG_DSN=<disposable-tmpfs-dsn> go test ./... -count=1
```

The final full run passed every package, including the new store and poller
tests. The focused federation suite also passed after testing fresh migration,
workspace isolation, duplicate delivery, stale fences, lease takeover,
reconciliation without external calls, and legacy quarantine pagination.

Measured local qualification cost was approximately 20 seconds for the full
Go suite on a fresh disposable database and approximately 2 seconds for the
focused federation/server packages. These are development measurements, not
deployment SLOs.

## Database and storage impact

- One append-only metadata table, two indexes, and one RLS policy were added.
- A normal poll remains one indexed peer lease/attempt flow plus the bounded
  coordination import transaction. Reconciliation uses one local transaction
  after bounded attempt/peer reads and performs no network work.
- Quarantine performs one bounded historical metadata scan and at most one
  insert per selected legacy row plus one event. It persists IDs, hashes,
  disposition, reason, and audit metadata; it never stores raw URLs, tokens,
  or response bodies.
- Manager acquisition adds one bounded manager request and one short lease
  transaction. The database lock is not held during secret-manager I/O.
- The temporary database used tmpfs and was destroyed after qualification;
  no persistent volume or build cache was added.

## Remaining limitations

- A remote call remains at-least-once. Reconciliation makes the local effect
  deterministic; it does not claim exactly-once external execution.
- The packaged environment token source is suitable for explicit local/simple
  deployments. Hosted production should inject workload identity or mTLS and
  a deployment-specific SecretManager/TokenSource.
- Certificate pinning, provider-specific live conformance, manager rotation
  lag/zeroization evidence, backup/restore, HA failover, retention/partition
  policy, and load/soak qualification remain Issue #40 gates.
- The quarantine operation records a redacted disposition for historical
  `fornix.federation_peers` rows but intentionally does not migrate them into
  workspace peer ownership. An operator must create a new workspace peer with
  a managed credential reference after independently verifying ownership.

## Next qualification slice

Task 68 should qualify the injected manager and federation adapter against a
deployment-shaped mTLS/workload-identity test double, add certificate and
rotation/revocation evidence, and measure retention/load behavior under
concurrent workspace pollers. Keep the external-effect reconciliation
boundary separate from ordinary queue scheduling.
