# Loop 64 completion: legacy surface containment and HTTP effect qualification

Status: implemented; qualification evidence recorded below.

## Delivered

- Added `legacy:global_admin` as a known, explicit permission for historical
  global compatibility routes.
- Extended legacy containment to `/v1/coord` and `/v1/coord/recent`; the
  quarantine now covers coordination, federation, and router surfaces.
- Required both the explicit non-production compatibility flag and the
  dedicated permission before any legacy handler can run.
- Kept unknown routes and unknown paths under the legacy families fail-closed.
- Preserved production rejection of
  `FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES` and safe-mode poller suppression.
- Preserved the HTTP adapter’s durable effect authority identity in effectful
  results instead of generating a second adapter-local identity.
- Added a Make smoke target covering global-surface authorization and HTTP
  authority identity behavior.
- Documented the containment boundary, no-migration decision, at-least-once
  external semantics, storage/cost impact, and remaining replacement work in
  [`151-legacy-surface-and-http-effect-qualification-foundation.md`](151-legacy-surface-and-http-effect-qualification-foundation.md).

## Verification

The focused authorization and connector tests pass. The complete offline Go
test suite and race suite pass. A fresh disposable PostgreSQL + pgvector
database passes the full Go suite; rerunning store/server tests against the
same database confirms existing-database migration compatibility. `make check`
passes, including vet, Python checks, documentation validation, and shell
syntax checks. The role-separated PostgreSQL qualification passes with a
non-superuser/NOBYPASSRLS runtime role, scoped API-key authentication, fenced
credential leases, and the real runtime-RLS retrieval route. The containment,
package, and installer smoke targets pass.

## Security interpretation

This loop closes a concrete authorization escape hatch but does not make the
historical global tables tenant-safe. The dedicated permission is deliberately
not implied by workspace read, workspace write, identity administration, or
ordinary connector access. Production must keep the compatibility flag off.

The HTTP result identity fix improves provenance joins, but remote execution
remains at-least-once. A lost provider response is still an uncertain outcome
and must use the existing recovery/reconciliation path; Fornix does not claim
exactly-once external execution.

## Measured impact

- Legacy safe mode adds only an in-process path/flag and permission decision;
  it starts no poller and performs no global-table query.
- The new permission adds no database table, index, or row.
- HTTP effect output adds no payload storage; it reuses the durable effect ID
  already reserved by the dispatcher.
- The disposable qualification database is temporary and uses no persistent
  Docker volume.

## Remaining work

Task 65 replaced ordinary coordination and router traffic with workspace-scoped
durable authorities without assigning historical rows by inference. The
remaining quarantined federation surface still needs workspace-scoped peer
records, managed credential references, controlled egress, and explicit
unknown-outcome recovery. Production qualification, live provider
verification, signer rotation, and operating evidence remain open; see
[`154-loop-65-completion.md`](154-loop-65-completion.md).
