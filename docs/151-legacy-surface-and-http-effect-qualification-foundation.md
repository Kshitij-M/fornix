# Legacy surface and HTTP effect qualification foundation

Status: implemented as a containment and qualification slice. Coordination
and router learning now have workspace-scoped replacement authorities; the
historical federation surface remains quarantined until its credential and
egress boundary is replaced.

## Purpose

Fornix is a universal, workspace-scoped work control plane. The repository
still contains compatibility routes created before that authority boundary
existed:

- `/v1/coord` and `/v1/coord/recent` historically read and wrote
  `public.coord_messages`; Task 65 now routes ordinary workspace traffic to
  the durable `fornix.workspace_coordination_messages` authority;
- `/v1/federation/*` reads and writes global federation state;
- `/v1/router/*` historically read and wrote global routing observations;
  Task 65 now routes ordinary workspace traffic to the durable
  `fornix.workspace_router_observations` authority.

Those tables have no authoritative workspace key. They must not become an
accidental cross-tenant API merely because a caller has a normal workspace
permission. This slice also qualifies the existing HTTP connector boundary,
which is the first concrete universal external-effect adapter.

## Invariants

1. Every historical global route is unavailable unless
   `FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES=true` is explicitly set in a
   non-production environment.
2. The opt-in is not a workspace authorization grant. It is a compatibility
   switch, and the caller must additionally hold `legacy:global_admin`.
3. Ordinary workspace read/write, identity-admin, and wildcard-free API keys
   cannot reach global tables through these routes.
4. Unknown paths under a legacy prefix fail closed rather than inheriting a
   nearby workspace permission.
5. Production configuration rejects the compatibility switch.
6. Historical rows are not assigned to a workspace without a forward-only
   migration and proof of ownership.
7. The default runtime does not start the federation poller.
8. HTTP effectful capabilities remain at-least-once. Provider idempotency is
   explicit; Fornix never claims exactly-once external execution.
9. HTTP requests are built only from a workspace-bound connector binding,
   bounded structured payload, controlled destination policy, and a fresh or
   explicitly admitted credential lease.
10. Credentials, raw authorization headers, and arbitrary response bodies do
    not cross durable result, event, evidence, or error boundaries.

## Implementation and reuse decisions

The change reuses the existing authenticated middleware, permission audit
store, connector registry, admission/operation dispatcher, credential lease
resolver, destination policy, and HTTP connector. No parallel authorization
or egress layer is introduced.

The new permission is intentionally narrow and explicit. It makes an unsafe
compatibility mode visible in identity configuration and audit records while
leaving normal workspace capabilities unchanged. The HTTP qualification uses
the existing strict `ExecuteWithAuthority` seam and does not add a broker,
worker, or external coordination service.

## Schema and migration decision

Migration 061 adds workspace-scoped coordination and router authorities. The
old global tables are not backfilled because ownership cannot safely be
guessed. Federation still requires a future migration with credential
references instead of raw peer tokens and a reviewed adoption process.

## Failure and crash semantics

If a legacy request is disabled or lacks `legacy:global_admin`, it fails before
the handler and before any SQL. If an effectful HTTP call loses transport
confirmation after dispatch, the operation remains at-least-once and must be
reconciled through the existing recovery path; it is never reported as
exactly-once success by inference. A stale operation or credential fence is
rejected before outbound execution.

## Cost, performance, and storage

Safe-mode legacy containment adds an in-process path and boolean check per
authenticated request and avoids the legacy poller’s database/network work.
The new permission adds no rows or indexes. HTTP qualification adds tests only;
runtime request cost remains the existing bounded connector, authorization,
credential-lease, and egress-policy work. No new infrastructure or storage is
required.

## Licensing and reuse

The implementation is original Fornix code under the repository MIT license.
It reuses architectural ideas already documented in the reference matrix but
copies no source from Kronaxis Fabric, whose BSL 1.1 license is incompatible
with direct source reuse here.

## Acceptance tests

- historical federation and any remaining global compatibility route fail
  closed by default;
- ordinary coordination and router routes require workspace-scoped
  authorization and never reach the historical global tables;
- remaining legacy routes require the explicit global-admin permission when
  compatibility mode is enabled;
- unknown paths under legacy prefixes do not inherit a workspace permission;
- production rejects the compatibility flag;
- the federation poller is absent in safe mode;
- HTTP connector requests remain bounded, workspace-bound, redacted, and
  protected by credential/effect authority;
- stale authority cannot dispatch an HTTP effect;
- provider idempotency and at-least-once behavior remain explicit;
- focused, race, full disposable-Postgres, qualification, documentation, and
  packaging checks remain green.

## Remaining work

This slice does not migrate historical global federation rows or enable the
raw-token federation poller. Task 66 must add workspace-scoped peer records,
managed credential references, bounded controlled egress, and an explicit
unknown-outcome recovery path before federation can be enabled. Additional
first-party effect adapters still need the same authority, verification, and
compensation qualification.
