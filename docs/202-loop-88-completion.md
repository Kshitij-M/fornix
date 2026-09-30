# Loop 88 completion — deployment evidence freshness and lifecycle

Status: implemented as a repository-owned current-state and append-only event
slice.

## Outcome

Fornix can now withdraw a linked deployment claim or replace it with an
explicitly identified successor without deleting the signed import, changing
the release identity, or hiding the prior decision. Readiness uses only the
active link for admission and retains inactive links for audit explanation.

## Delivered

- Added typed `active`, `revoked`, and `superseded` evidence-link states with
  bounded revocation reasons, predecessor/successor IDs, and timestamps.
- Added migration `072` with active-only kind uniqueness, lifecycle fields,
  bounded lifecycle indexes, and append-only `linked`, `superseded`, and
  `revoked` event identities.
- Made `LinkEvidence` replacement-aware. A replacement requires the exact
  active predecessor ID, transitions that predecessor before inserting the
  successor, and commits both changes and events atomically.
- Added transactional, idempotent `RevokeEvidence` with dry-run behavior,
  actor propagation, workspace locking, and fail-closed terminal-state rules.
- Updated gate evaluation to retain historical links, admit only active links,
  and report deterministic `evidence_revoked:<kind>` or
  `evidence_superseded:<kind>` readiness reasons.
- Added authenticated HTTP and CLI surfaces for `evidence-replace` and
  `evidence-revoke`; existing evidence pagination and gate disclosure remain
  bounded and workspace-scoped.
- Kept signed imports, raw signed bytes, releases, receipts, and historical
  event rows immutable. No credentials or provider payloads are stored.

## Verification

The focused contract/store/server/CLI tests passed after implementation. The
full repository gates are being run as isolated commands:

```text
go test ./...
go test -race ./...
go vet ./...
make check
make qualification-effect-conformance qualification-external-boundary
go build ./...
git diff --check
```

PostgreSQL-backed lifecycle, migration, RLS, concurrency, and crash tests run
when `FORNIX_TEST_PG_DSN` is configured. No disposable PostgreSQL DSN is
available in this local environment, so those checks are explicitly skipped.

## Cost and storage impact

Migration `072` adds five bounded lifecycle columns, one active-only index,
and four request-identity columns to the existing evidence-event table. Each
revocation or replacement adds one small append-only event; no raw evidence is
duplicated. Gate evaluation scans the already bounded evidence set and makes
constant-time status/hash decisions. Hosted PostgreSQL latency, WAL growth,
and index size still require deployment-specific measurement.

## Remaining limitations

- Revocation is a Fornix-side readiness decision; it does not revoke a remote
  credential, provider request, certificate, or deployment process.
- Signed imports remain accepted immutable evidence. Trust snapshot rotation,
  expiry, and operator lifecycle still determine whether a link is admissible.
- Deployment truth remains externally collected and signer-authorized; a valid
  signature is not an independent attestation of reality.
- Remote execution remains at-least-once. Provider idempotency and
  reconciliation evidence remain deployment/provider responsibilities.
- HA, PITR, failover, partition-maintenance, connector/provider conformance,
  and topology load/soak qualification remain open.

## Next handoff

Task 89 should add advisory, hash-only readiness snapshots and incident
annotations so operators can compare exactly which authority facts made a
release ready or blocked without copying raw deployment material.
