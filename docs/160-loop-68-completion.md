# Loop 68 completion: deployment credentials, retention, backup, and federation load

Status: implemented and qualified on the disposable local path. This is not a
production-readiness declaration; hosted secret-manager authentication,
certificate revocation infrastructure, HA/PITR, scheduled retention, and
topology-specific load/soak evidence remain deployment-owned.

## Delivered

- Added public certificate-policy and redacted deployment-authority contracts.
- Added an in-memory deployment authority test double implementing the existing
  `SecretManager` and `TokenSource` seams. It supports explicit source-version
  rotation, revocation epochs, expiry, workspace scope, and bounded
  rotation/revocation observations without logging or persisting secret bytes.
- Added mTLS client construction with TLS 1.3 minimum, explicit client key-pair
  loading, server chain/name verification, certificate and SPKI pinning, and
  revoked-fingerprint rejection. Certificate failures occur before transport
  dispatch.
- Added migration `064_federation_retention.sql`. Operational federation poll
  attempts and historical quarantine records now have explicit retention class,
  deadline, state, and tombstone metadata. Hash-only retention tombstones are
  stored in a range-partitioned Postgres parent with a default partition and
  workspace RLS. Authoritative peer commands and control events are untouched.
- Added bounded, deterministic, workspace-scoped federation retention sweeps
  with dry-run, idempotency, active-lease protection, recovery protection,
  append-only tombstones, and no external calls.
- Added a disposable federation capacity qualification that reports lease/poll
  p50/p95/p99 latency, transaction/buffer deltas, relation growth, and waiting
  locks. It is opt-in through `FORNIX_FEDERATION_CAPACITY_PG_DSN`.
- Extended the backup/restore fingerprint with federation peer, poll, and
  retention-tombstone hashes and emitted a redacted replay identity hash.
- Updated the qualification runbook, production-readiness summary, roadmap,
  documentation index, Make targets, and shell checks.

## Verification

Offline focused verification passed:

```text
go test ./internal/contracts ./internal/credentials ./internal/store \
  -run 'Test(DeploymentAuthority|Certificate|FederationRetention|FederationCapacityQualification)' -count=1
sh -n scripts/qualification/backup-restore.sh scripts/qualification/federation-capacity.sh
git diff --check
```

A fresh disposable PostgreSQL/pgvector migration run passed the federation
authority suite, including migration 064, retention dry-run/commit/tombstone
behavior, workspace boundaries, peer fencing, takeover, duplicate delivery,
and quarantine coverage. The bounded capacity qualification then measured 32
operations with four workers: lease p50/p95/p99 of 1.85/7.43/9.19 ms, poll
p50/p95/p99 of 1.71/2.58/3.50 ms, zero waiting locks, 38 committed
transactions, and 180,224 bytes of relation growth. These are local
qualification observations, not deployment SLOs.

The backup/restore drill was run inside the matching PostgreSQL 17 image and
passed with a 623,352-byte backup, one-second backup duration, zero-second
restore duration at this scale, matching source/restore fingerprint, and
replay identity hash
`89a3ba9e9c90aa1bf2218ede6e0b2cbc07f831612f1bd33761a2629ad87629e7`.
Temporary database storage, backup bytes, and the temporary pgvector image
were removed after qualification.

## Cost and storage impact

- Certificate validation is process-local and bounded by an eight-certificate
  chain and 32 pins per category.
- Authority observations are capped in the test double and contain only
  workspace/provider/version/epoch/timing metadata.
- Retention scans use deadline/state indexes and commit at most the configured
  batch size per source table. The tombstone relation adds one small metadata
  row per expiration and is partition-ready by expiration time.
- Federation capacity qualification is bounded to 512 operations and 16
  workers. It writes only to a disposable workspace and reports database
  counters and relation-size deltas rather than retaining raw payloads.

## Remaining limitations

- The mTLS implementation validates injected certificate material but does not
  provide a cloud workload-identity plugin, OCSP/CRL service, or automatic
  certificate rotation controller.
- The in-memory authority is a conformance test double, not a secret manager;
  production deployments must inject a provider-specific implementation with
  zeroization and identity-bound authentication.
- Retention is an explicit operator sweep, not a background scheduler. Peer
  command and control-event history remain append-only and require a separate
  deployment retention policy if they grow beyond the Postgres envelope.
- The backup script verifies logical restore fingerprints and replay identity;
  WAL/PITR schedules, encryption at rest, HA/failover, restore ownership, and
  measured RPO/RTO remain deployment responsibilities.
- Capacity numbers are local qualification observations, not service SLOs.
  Live adapter/provider conformance and long-duration soak testing remain
  open production gates.

## Recommended next task

Task 69 should qualify a real deployment credential/certificate authority and
the intended PostgreSQL topology, then add live adapter conformance, scheduled
retention ownership, HA/PITR drills, and long-running failure-injection/soak
evidence. The universal roadmap must remain open until those deployment facts
are measured rather than inferred from local test doubles.
