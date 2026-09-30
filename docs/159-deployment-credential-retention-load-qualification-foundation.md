# Deployment credential, retention, and load qualification foundation

Status: implementation feature note for Task 68. This note defines a
deployment-shaped qualification boundary; it is not a claim that one local
process provides high availability, PITR, or a universal production SLO.

## Problem and scope

Fornix now has workspace-scoped credential leases, controlled outbound HTTP,
fenced federation polling, response reconciliation, and a redacted quarantine
path. A serious deployment still needs evidence that the credential authority
can authenticate the process, that certificate rotation fails closed, that
operational federation history has an explicit storage envelope, and that
backup/restore and load qualification preserve the same hashes and fences.

This slice adds provider-neutral in-memory deployment test doubles, certificate
validation and pinning policy, observable rotation/revocation facts, bounded
federation retention operations, and qualification commands. It does not
pretend to implement a cloud KMS, an HA controller, or a network-level
certificate revocation service.

## Invariants

1. Secret and private-key bytes exist only in memory at the final authority or
   transport boundary. They are never placed in events, SQL, metrics, errors,
   reports, or qualification output.
2. A deployment authority must validate workspace, provider purpose, source
   version, expiry, and revocation before returning a lease. Rotation produces
   a new opaque source version; an old version cannot be silently upgraded.
3. A certificate must have a matching private key, a valid chain, a bounded
   validity window, and an allowed server name. When pins are configured, the
   peer certificate's SHA-256 SPKI fingerprint must match one of them.
4. Certificate policy is fail-closed. Expired, not-yet-valid, malformed,
   revoked, mismatched, or unpinned certificates are rejected before a remote
   request is sent.
5. Rotation and revocation observations use bounded dimensions and stable
   hashes. They identify timing and state, never secret values or arbitrary
   request text.
6. Federation peer command history and control events remain authoritative.
   Retention may expire only explicitly operational poll/quarantine rows after
   their deadline; each expiration leaves an append-only tombstone hash and
   audit event.
7. Retention sweeps are workspace-scoped, bounded, deterministic, idempotent,
   and dry-run capable. Rows in recovery, active lease, or before their
   deadline are never expired.
8. Load qualification uses a disposable database and a unique workspace. It
   reports measured p50/p95/p99 latency, transaction/database counters, lock
   and storage deltas where available, duplicate work, and replay hashes. It
   is evidence for sizing, not a production promise.

## Schema and API changes

Migration 064 adds retention metadata to federation poll attempts and legacy
quarantine records, plus a bounded tombstone table. Authoritative peer command
history is not physically deleted. Expiration removes only operational row
payloads selected by the explicit retention API while the tombstone preserves
workspace, source identity, row hash, expiration time, and reason.

Typed contracts cover certificate policy, deployment authority observations, and
federation retention requests/results. The store exposes dry-run and commit
paths with batch limits and exact workspace scope. The test authority supports
resolve, rotation, revocation, and token issuance while sharing the same
redaction and clear-on-release behavior as a hosted adapter.

## Reuse and licensing

The design reuses Fornix's existing `SecretManager`, `TokenSource`, controlled
egress client, credential lease, workspace transaction, append-only event, and
backup fingerprint seams. Orloj's injected secret-manager/controller boundary,
agentmemory's bounded lease/checkpoint recovery, and ClawMem's replay-without-
external-effects discipline informed the contracts. No reference source is
copied; Kronaxis is excluded because its repository is BSL 1.1. Fornix remains
MIT licensed.

## Cost and operational budget

Certificate validation is process-local and linear in the configured chain and
pin set. Manager test-double calls have bounded response sizes and short-lived
leases. Retention scans are index-backed and capped by batch size; a sweep
commits one bounded transaction. Qualification runs are opt-in, use a
disposable database, and report storage/WAL/counter deltas without retaining
raw payloads.

## Acceptance tests

- Valid mTLS material produces a client with the required TLS policy.
- Missing, malformed, expired, not-yet-valid, mismatched, revoked, and
  unpinned certificates fail before transport.
- Authority rotation changes source version and revokes old leases; revocation
  propagation is measurable and bounded.
- Token and secret material never appear in errors, events, SQL, or metrics.
- Retention dry-runs have no mutations; committed sweeps are bounded,
  idempotent, workspace-isolated, and preserve tombstone hashes.
- Recovery-required and actively leased poll attempts cannot expire.
- Backup/restore fingerprints include federation authority and retention facts,
  and replay/hash verification remains deterministic.
- Concurrent disposable poll/retention qualification preserves fences and
  reports p50/p95/p99, database work, storage, and recovery measurements.
- Existing tests, race checks, builds, CI, docs, and smokes remain green.

## Explicit remaining deployment gates

The slice does not certify a particular cloud secret manager, workload identity
provider, certificate revocation protocol, PostgreSQL HA/PITR topology, backup
schedule, or multi-region failover. Those require deployment-owned integration
tests, credentials, network policy, and retained operational evidence.
