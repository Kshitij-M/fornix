# Task 80 — Deployment qualification trust distribution and startup conformance

Status: feature note; implementation follows.

## Problem and boundary

Task 79 made a workspace/deployment signer catalog authoritative for accepting
signed qualification evidence. That catalog is durable, but it is not yet a
portable deployment trust decision: a deployment needs to distribute a
bounded, revisioned set of accepted qualification signers and prove which
set was loaded when a new evidence bundle was accepted.

This slice adds a signed trust snapshot. The existing qualification signer
catalog remains the authority for authorizing snapshot publishers. A snapshot
is a bounded, immutable description of the signer set that a deployment wants
the runtime to accept. The process cache is only a loaded view; PostgreSQL is
the authority for published snapshots, revocation, and import binding.

Private signing keys, HSM/KMS calls, workload identity, mTLS, distribution
transport, and deployment truth remain deployment-owned. No broker, object
store, Redis, external key service, or new infrastructure is introduced.

## Invariants

1. A snapshot is scoped to exactly one workspace and deployment identity. Its
   revision is a positive decimal counter and never moves backwards. A
   duplicate `(workspace, deployment, snapshot_hash)` publication is
   idempotent; a different payload at the same revision fails closed.
2. A snapshot publisher must be an active, valid signer in the durable Task 79
   catalog at publish time. The public key embedded in a snapshot is an
   integrity hint, never a trust decision.
3. Snapshot content is deterministic: normalized signer entries are sorted by
   deployment, key ID, and public-key hash. The snapshot hash covers workspace,
   deployment, revision, validity window, publisher key ID, and every entry.
4. A snapshot is usable only when its status is active, its validity window is
   current, its signature verifies against the currently trusted publisher,
   and its workspace/deployment matches the requested import.
5. New authorized qualification imports must bind the exact loaded snapshot
   revision and snapshot hash. Existing Task 79 imports without those columns
   remain auditable and can be replayed by identity, but they are never
   rewritten or retroactively attributed to a newer snapshot.
6. Revocation is fail-closed for future admission. Revoked snapshots and
   predecessor snapshots remain readable as history. Revoking a publisher in
   the Task 79 catalog invalidates snapshots signed by that publisher on the
   next load/import check.
7. Startup qualification is explicit. When
   `FORNIX_REQUIRE_QUALIFICATION_TRUST=true` (automatically true in production
   configuration), startup loads one current snapshot per workspace for the
   configured deployment identity. Missing, stale, revoked, malformed, or
   cross-scope snapshots keep readiness false and prevent authorized imports.
8. All snapshot rows, lifecycle events, imports, and disclosures preserve
   workspace, actor, request, causation, correlation, source, and hash facts.
   RLS and explicit predicates fail closed on cross-workspace access.
9. Source bytes are bounded and hash-addressed. No raw private key, bearer
   token, secret-manager response, prompt, or unrestricted provider output can
   enter the snapshot contract, event metadata, or API response.

## Contracts and schema

- Add `QualificationTrustSnapshot`, bounded signer entries, signing and
  verification helpers, publish requests, snapshot records, pages, and
  disclosure contracts.
- Add migration 067 with current snapshot rows, append-only lifecycle events,
  unique workspace/deployment/revision/hash identities, bounded signed bytes,
  source references, RLS, and Task 79 import columns for
  `trust_snapshot_revision` and `trust_snapshot_hash`.
- Add a Postgres store for publish, duplicate replay, current-load,
  revocation, bounded pagination, and explicit disclosure.
- New imports resolve the current snapshot inside the same transaction and
  store its revision/hash with the accepted record. Historical import rows are
  compatible because the new columns are nullable only for pre-Task-80 rows.

## Rotation, revocation, and startup semantics

The deployment signs a new snapshot with a key already trusted by the Task 79
catalog. Rotation is a new snapshot revision, not an overwrite. The snapshot
may carry a new active signer entry, but import admission still requires the
snapshot publisher itself to be currently trusted. A revoked publisher or
snapshot fails on load even if its signed bytes remain cryptographically
valid. Startup and readiness expose only bounded revision/hash metadata.

The normal development path does not require a snapshot. Production or an
explicit qualification deployment must set the requirement and a deployment
identity. This makes an incomplete trust rollout visible as an unavailable
service rather than silently falling back to the process-local or embedded
key.

## Reuse and licensing

Reuse Task 79's Ed25519 subject discipline, public-key-only contracts,
workspace transaction/RLS helpers, actor propagation, strict bounded JSON,
monotonic revision conventions from connector trust/schema catalogs, and
existing readiness/authority reporting. Keep qualification trust separate from
connector capability trust because their scopes and operational lifecycles are
different. No Kronaxis source is copied; Fornix remains MIT-licensed.

## Cost and storage budget

Snapshot publish/load is one bounded indexed read or transaction plus one
signature verification over a fixed-size hash. A snapshot carries at most 64
public signer entries and 128 KiB of signed JSON. New import rows add two hash/
revision columns and no duplicate evidence body. Snapshot history is append
only apart from a small current status flag; operational retention must never
delete a snapshot referenced by an accepted import.

## Acceptance tests

- Fresh and existing databases apply migration 067 without changing prior
  Task 79 import hashes or bytes.
- Valid snapshots publish once; exact replay is idempotent; same revision with
  different content, unsigned content, bad signatures, and downgraded
  revisions fail closed.
- Unknown, revoked, expired, superseded, cross-workspace, and
  cross-deployment publishers cannot publish or load snapshots.
- Snapshot entry order does not change the canonical snapshot hash.
- Snapshot revocation and publisher revocation invalidate future imports while
  preserving auditable history.
- New imports record the exact snapshot revision/hash; duplicate imports return
  the original record; historical imports remain readable and are not rewritten.
- Startup/readiness is unavailable when required trust is missing or stale and
  becomes ready only after a current valid snapshot is loaded for every
  workspace in scope.
- Concurrent publication has one monotonic outcome; crash/rollback before
  commit leaves no snapshot or import row; replay after commit is safe.
- API/CLI pagination, disclosure, RBAC, redaction, workspace isolation, race
  checks, builds, CI, smokes, and offline qualification remain green.

## Explicit limitations

This is a repository-owned trust-distribution authority, not a deployment
identity provider or HSM. It cannot prove who operated the signing ceremony,
that the deployment inventory is truthful, or that a remote distribution
channel delivered every revision. Production still needs a deployment-owned
rotation ceremony, secure publication channel, lag/rollback policy, and live
topology qualification.
