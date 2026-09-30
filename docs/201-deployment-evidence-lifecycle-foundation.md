# Task 88 feature note — deployment evidence freshness and lifecycle

Status: implementation design for the next repository-owned qualification
slice. This note defines how Fornix revokes or replaces a linked deployment
claim without rewriting the imported evidence or historical decisions.

## Problem and scope

Task 86 made external-boundary evidence time-bounded and Task 87 made signed
publication explicit. The remaining control-plane gap is lifecycle handling:
operators need to revoke a linked claim after an incident, replace it with a
new signed import, and understand exactly why a release is no longer ready.

This slice adds a mutable current-state projection for evidence links plus
append-only lifecycle events. The signed import bytes, trusted snapshots,
release identities, Work Receipts, and historical event rows remain immutable.

## Invariants

1. A deployment evidence link has one current status: `active`, `revoked`, or
   `superseded`. Only one active link exists for a workspace, deployment,
   release, and evidence kind.
2. Revocation and replacement are workspace-scoped, actor-bound, idempotent,
   and committed in one Postgres transaction with their lifecycle event.
3. A revoked or superseded link cannot satisfy a qualification gate. Gate
   output includes bounded deterministic reasons and preserves inactive link
   metadata for operator explanation.
4. Replacement requires an explicit predecessor link ID and a new accepted
   import. The predecessor is marked superseded; it is never deleted or
   overwritten with the new hashes.
5. Repeating the same lifecycle request returns the current durable state.
   Reusing an idempotency key with a different target, reason, or link fails
   closed.
6. Evidence freshness, trust-snapshot currency, boundary expiry, outcome,
   recovery state, and lifecycle status are evaluated in one transaction.
7. Cross-workspace IDs, missing actors, malformed reasons, stale link IDs,
   revoked predecessors, and attempts to mutate historical imports fail
   closed.
8. Readiness explanations contain only bounded identifiers, status codes, and
   hashes. They never expose signed bytes, credentials, provider payloads, or
   arbitrary operator text.

## Schema and API changes

- Migration `072` adds current lifecycle fields to deployment evidence links,
  replaces the historical kind uniqueness constraint with an active-only
  uniqueness index, and extends evidence events with request identity and
  lifecycle event kinds.
- Add typed link status, replacement, and revocation contracts. Revocation
  reason is bounded and normalized; it is audit metadata, not a source of
  authority.
- Add transactional `RevokeEvidence` and replacement-aware `LinkEvidence`
  store paths. Existing list APIs retain historical rows; gate evaluation
  consumes all rows but admits only active rows.
- Add authenticated HTTP and CLI operations for evidence revocation and
  explicit replacement. Existing release-gate output becomes the readiness
  explanation surface.
- No raw signed bytes are duplicated and no new infrastructure is introduced.

## Crash, concurrency, and replay semantics

Advisory locking serializes lifecycle changes per workspace/deployment. A
crash before commit leaves both the current link and lifecycle event unchanged;
a crash after commit is replay-safe because the idempotency key and current
status return the same result. Concurrent replacement or revocation requests
cannot create two active links or move a link back to active.

## Reuse and licensing

The implementation reuses the existing deployment evidence store, qualification
gate, signed-import authority, workspace transactions/RLS, actor normalization,
bounded pagination, and release-admission validation. No source is copied from
reference repositories. Orloj, ClawMem, agentmemory, and FornixDB informed the
append-only lifecycle, replay, disclosure, and retention model. Kronaxis is
excluded because its repository is BSL 1.1. Fornix remains MIT licensed.

## Cost and storage budget

Each current link gains bounded status/identity fields and each lifecycle
transition adds one small append-only event. Gate evaluation reads the
existing bounded release evidence set and performs constant-time status/hash
checks. No model, provider, broker, cache, or raw payload is added. Hosted
readiness and revocation latency still require PostgreSQL topology
measurement.

## Acceptance tests

- Fresh and existing databases migrate through `072` cleanly.
- Revocation is transactional, idempotent, workspace-scoped, and gate-blocking.
- Explicit replacement creates one active link, preserves the predecessor,
  and is deterministic under duplicate/concurrent delivery.
- A crash before lifecycle commit leaves state and events unchanged; replay
  after commit returns the same link and gate hashes.
- Stale, superseded, revoked, expired, untrusted, and cross-workspace evidence
  fail closed with deterministic readiness reasons.
- Historical signed imports, receipts, and event rows cannot be mutated.
- HTTP/CLI authorization, pagination, redaction, and audit actor propagation
  remain correct.
- Unit, race, vet, package, documentation, smoke, and PostgreSQL integration
  checks remain green when a disposable DSN is configured.
