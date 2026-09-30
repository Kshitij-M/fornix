# Live authority and fenced retention-owner qualification

Status: implementation feature note for Task 69. This note defines the next
deployment qualification boundary; it does not claim that a local process is
an HA deployment or that a provider-specific secret manager has been certified.

## Problem and scope

Task 68 proved the local certificate policy, deployment-authority test double,
federation retention semantics, backup fingerprinting, and disposable load
qualification. Two operational seams were still implicit:

1. A deployment needs a repeatable, provider-neutral probe that can qualify an
   injected live `SecretManager`/`TokenSource` without persisting secret bytes.
2. Retention cannot be left to an unowned operator process. A scheduled owner
   must acquire a workspace-scoped fencing lease, sweep only that workspace,
   and lose the ability to mutate rows when another owner takes over.

This slice adds a bounded authority-conformance runner and a fenced,
workspace-paginated retention owner. It deliberately does not implement a
cloud secret manager, a certificate revocation service, a cron daemon, or a
new infrastructure service.

## Invariants

1. Authority probes accept only validated workspace/provider/reference/purpose
   metadata. Secret and token bytes are measured and cleared in memory; they
   never enter reports, logs, events, SQL, or errors.
2. A live authority probe is opt-in, bounded by one request timeout and one
   response/secret size limit, and reports only source version, expiry state,
   latency, and redacted hashes.
3. A successful authority resolution must have a non-empty opaque version and
   a future expiry (or an explicitly documented non-expiring deployment policy).
   Missing, expired, malformed, denied, and cross-workspace responses fail
   closed.
4. Retention ownership uses the existing Postgres consumer-lease table. Every
   committed sweep performed by the owner carries the exact workspace,
   consumer, owner, and fencing token into the same transaction that deletes
   operational rows and appends tombstones/events.
5. A stale or expired retention owner cannot sweep, append a retention event,
   or advance operational state. Takeover increments the fence monotonically.
6. Workspace pagination is deterministic and bounded. A failed workspace does
   not cause another workspace's lease or data to be reused.
7. Dry-run remains read-only. Retention protects recovery-required rows,
   active leases, authoritative history, and all rows outside the requested
   workspace.
8. Repeated probes and retention runs are idempotent at the local durable
   boundary. Remote secret resolution remains an external read and is not
   advertised as exactly-once.

## API and schema decisions

The authority probe is an in-process qualification API over the existing
`SecretManager` and `TokenSource` interfaces. It returns a redacted report and
supports an environment-gated live qualification command. No migration is
needed for probe output because deployment reports are intentionally not
durable application state.

`FederationRetentionRequest` gains optional `owner_id` and `fence` fields. The
fields are required together when supplied and are checked against the
workspace consumer lease inside the retention transaction. Existing explicit
operator calls remain compatible; the scheduled owner always supplies them.

The retention owner uses consumer ID `federation.retention`, bounded workspace
pagination, a bounded batch size, and a single lease per workspace. It does
not hold a database transaction while iterating workspaces.

## Reuse and licensing

The implementation reuses Fornix's `SecretManager`, `TokenSource`, controlled
egress, certificate policy, consumer leases, `OperatorStore.ListWorkspaces`,
`FederationStore.RetentionSweep`, append-only events, and existing redaction
rules. It takes architectural guidance from Orloj's injected provider and
controller boundaries, agentmemory's lease/checkpoint ownership, ClawMem's
bounded replay discipline, and FornixDB's retention tiers. No reference source
is copied. Kronaxis remains excluded because its repository is BSL 1.1.
Fornix remains MIT licensed.

## Cost and operational budget

The authority probe performs at most one bounded resolution and one optional
token acquisition per invocation. It stores no probe payload. The retention
owner performs at most the configured number of workspaces and the configured
row batch per workspace; lease acquisition and sweep use existing indexed
Postgres operations. A held lease is skipped, not retried in a hot loop.

## Acceptance tests

- Live-probe metadata is validated and secret/token bytes are absent from
  reports and errors.
- A missing, expired, unversioned, cross-workspace, oversized, or denied
  authority response fails closed.
- A valid injected authority produces a stable redacted report with measured
  latency and source-version metadata.
- Retention owner pagination is deterministic and bounded.
- One workspace owner can sweep; a concurrent owner is fenced; an expired
  owner cannot sweep after takeover.
- Workspace A's lease and rows cannot authorize a sweep for workspace B.
- Dry-run has no mutation, and successful sweeps preserve tombstone hashes and
  authoritative history.
- Existing migrations, tests, race checks, smokes, and qualification scripts
  remain green.
- Live qualification remains opt-in and emits no credential material.

## Explicit non-goals

This slice does not certify a cloud workload-identity provider, OCSP/CRL,
automatic certificate rotation, PostgreSQL failover, PITR, or a production
RPO/RTO. Those require deployment-owned credentials, topology, and retained
evidence described in the qualification runbook.
