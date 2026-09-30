# Loop 69 completion: live authority probe and fenced retention ownership

Status: implemented and qualified on the offline/default path. A real hosted
authority and production PostgreSQL topology remain deployment-owned gates;
this completion note does not convert local evidence into a production SLO.

## Delivered

- Added `AuthorityProbe` over the existing provider-neutral
  `SecretManager`/`TokenSource` seams. It validates workspace/provider/
  reference/purpose metadata, bounds timeout, resolves at most once, clears
  returned bytes, and emits only source version, expiry, timing, outcome, and
  stable redacted hash metadata.
- Added an environment-gated live qualification test and
  `make qualification-credential-authority`. The token is read only from the
  named process environment variable; it is not a command argument, file,
  report field, or log field. The default path skips without a live endpoint.
- Added optional owner/fence fields to `FederationRetentionRequest`. When
  present, `FederationStore.RetentionSweep` validates the exact
  `federation.retention` consumer lease inside the mutation transaction before
  selecting, tombstoning, deleting, or appending its event.
- Added `federation.RetentionOwner`, which pages workspaces deterministically,
  acquires one bounded consumer lease at a time, skips held workspaces without
  a hot retry loop, and releases each fence after the sweep.
- Added opt-in server ownership through
  `FORNIX_FEDERATION_RETENTION_ENABLED`, with bounded interval, batch-size, and
  workspace-limit configuration. It is disabled by default and uses the
  existing Postgres authority; no scheduler, broker, or new service was added.
- Added unit coverage for authority redaction/bounds, provider failures,
  deterministic probe hashes, owner pagination, workspace actor isolation,
  held leases, stalled cursors, and fenced requests. Added a Postgres
  integration test proving an expired-owner takeover rejects the old fence.
- Updated the qualification runbook, `.env.example`, production-readiness
  summary, roadmap status, documentation index, and Make/package checks.

## Verification

The following completed successfully on the current worktree:

```text
go test ./... -count=1 -timeout=20m
go vet ./...
go build -trimpath ./cmd/fornix ./cmd/fornix-eval ./cmd/fornix-watcher
python3 scripts/check_docs.py
sh -n scripts/qualification/credential-authority.sh \
  scripts/qualification/federation-capacity.sh \
  scripts/qualification/backup-restore.sh
git diff --check
```

The live authority test is intentionally skipped unless all explicit
`FORNIX_LIVE_AUTHORITY_*` metadata is supplied. No provider key was used or
printed during this qualification.

A fresh PostgreSQL 17/pgvector disposable container with a tmpfs data
directory also passed:

```text
FORNIX_TEST_PG_DSN=... go test ./internal/store \\
  -run 'TestFederation(Retention|RetentionOwnerFence)' -count=1
ok   github.com/omaveda/fornix/internal/store  0.758s
```

The temporary container and image were removed immediately after the run;
Docker build cache remained at zero and no persistent volume was modified.

## Cost, latency, and storage impact

- The authority probe performs at most one bounded authority read and an
  optional token-source check. It stores no request, response, secret, token,
  or report payload.
- Probe latency is measured in milliseconds in the returned redacted report;
  it is not treated as a model/provider SLO.
- Retention ownership adds one consumer-lease acquisition/release pair per
  eligible workspace and reuses the existing indexed, bounded retention
  transaction. A held lease is one failed attempt, not an unbounded retry.
- No migration or new durable relation was required for this slice. Existing
  retention tombstones and append-only events remain the only durable output.
- Workspace pages are capped at 1000 per pass and source rows at 500 per
  transaction by existing contract bounds.

## Remaining limitations

- The live qualification validates Fornix's adapter boundary, not a vendor's
  identity, rotation, revocation, audit, or HA guarantees. A deployment must
  supply a real workload-identity/mTLS authority and retain its evidence.
- Certificate revocation protocol, automatic certificate rotation, and
  topology-specific pool behavior remain deployment gates.
- The retention owner is an opt-in in-process loop. PostgreSQL partition
  creation/retirement, HA/PITR, failover, RPO/RTO, and long-duration soak
  evidence still require deployment runbooks and a real topology.
- External authority reads and provider/connector effects remain at-least-once
  at their external boundary; local Fornix state remains fenced and
  idempotent.

## Next task

Task 70 should qualify live connector/provider authority conformance and the
intended PostgreSQL HA/PITR topology, including certificate rotation/revocation
drills, partition maintenance, failover, RPO/RTO, and long-duration
failure-injection/soak evidence.
