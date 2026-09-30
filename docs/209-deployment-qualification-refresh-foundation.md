# Deployment qualification refresh foundation

Status: implemented (Task 92); see [210-loop-92-completion.md](210-loop-92-completion.md)

## Purpose

Fornix can already register immutable deployment releases, verify signed
qualification imports, and link hash-only evidence to a release. This feature
adds a bounded operator refresh operation for that evidence. A refresh is a
transactional replacement of selected deployment-owned evidence links at an
explicit observation time. It does not run a deployment, contact a provider,
perform a backup or restore, or make a release-admission decision.

The deployment remains the authority for observations. Fornix stores only the
accepted signed bundle through the existing trust-import path and stores
hash-only release links and refresh history. This keeps the universal control
plane useful for any production system without making Fornix a deployment
orchestrator.

## Invariants

- Every refresh is scoped to exactly one workspace, deployment, and immutable
  release.
- The request names bounded import identities and closed evidence kinds; it
  cannot carry raw reports, credentials, endpoints, or provider payloads.
- Candidate imports must be accepted, signature-verified, target-compatible,
  and bound to the release's current trust snapshot.
- Expired external-boundary evidence is rejected at the requested `as_of`
  time. Expiry is never extended by a refresh.
- Existing evidence links are never overwritten. A successful replacement
  marks the predecessor superseded and appends a new link and lifecycle
  events in the same transaction.
- The refresh run, item report, evidence replacements, supersession events,
  and deterministic result hash commit atomically.
- Duplicate idempotency keys return the original result only when the request
  identity matches; conflicting reuse fails closed.
- Dry runs validate and plan the exact same candidate set but commit no rows.
- The gate remains read-only and advisory. A refresh does not authorize
  production execution.

## Failure, crash, and concurrency semantics

The store takes the existing workspace/deployment advisory transaction lock.
Concurrent refreshes therefore serialize within one deployment scope. A crash
before commit leaves the release links, predecessor lifecycle, refresh run,
and events unchanged. A crash after commit is an idempotent replay: the
refresh result and replacement links are returned from durable identity.

The operation is operator-driven in this slice. A future scheduler may invoke
the same API, but no scheduler or external provider integration is implied.
Evidence is fail-closed when the import is missing, cross-scoped, stale,
revoked/superseded, malformed, contradictory with the release, or expired at
`as_of`.

## Schema and migration strategy

Migration 076 adds append-only workspace/deployment-scoped refresh runs,
bounded item rows, and refresh events. Rows contain identity and provenance
hashes only. The migration is additive and compatible with migrations 066–075.
RLS, append-only triggers, scope constraints, idempotency uniqueness, and
bounded JSON metadata follow the existing qualification schema conventions.

## API and CLI

The authenticated API exposes a release-scoped refresh endpoint with dry-run,
explicit `as_of`, bounded items, and status/list reads. The CLI exposes the
same operation using a bounded JSON item file so a refresh of several evidence
kinds remains one atomic request. Both surfaces propagate the authenticated
actor, request, idempotency, causation, and correlation metadata.

Qualification administration is required for mutation; qualification read is
required for plans and reports. No secret material is accepted or disclosed.

## Reuse and licensing

The implementation reuses Fornix's deployment evidence store, trust-import
verification, signed qualification contracts, gate evaluation, workspace
transaction helper, RBAC mapping, and existing CLI/API conventions. No source
is copied from Kronaxis Fabric or any BSL-licensed repository. The Fornix
implementation remains covered by the repository's MIT license.

## Cost and storage budget

The refresh stores one small run row, at most six item rows, and append-only
hash metadata per committed operation. It reads the existing bounded signed
import and does not duplicate signed bytes. It performs one serialized
workspace/deployment transaction and one deterministic gate evaluation. Dry
runs perform reads and rollback only. No network, model, broker, object store,
or new infrastructure is introduced.

## Acceptance tests

- Contracts normalize closed kinds, bounded items, explicit time, actor scope,
  and deterministic request/result hashes.
- Fresh and existing databases apply migration 076 cleanly.
- A refresh rejects missing, cross-deployment, stale, revoked, superseded,
  malformed, and expired evidence.
- A successful refresh supersedes links without mutating prior history.
- Duplicate requests return one durable refresh and one set of effects;
  conflicting idempotency reuse fails closed.
- Dry-run refresh creates no durable rows or lifecycle events.
- Concurrent refreshes preserve one active link per kind and integrity.
- Crash/rollback boundaries leave no partial replacement or orphan run.
- Workspace and RBAC boundaries fail closed.
- The API and CLI disclose only bounded hashes and metadata.
- Existing tests, race checks, builds, smokes, and replay remain green.

## Explicit limitations

This feature does not execute or independently verify live deployments,
Postgres backups, PITR, failover, DNS, mTLS, workload identity, provider
idempotency, or remote exactly-once behavior. Those remain deployment-owned
observations and must be supplied through the existing signed import boundary.
