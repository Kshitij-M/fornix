# Legacy global surface containment foundation

Status: implemented security containment; workspace-scoped replacement remains required.

## Why this exists

Fornix is becoming a universal, workspace-scoped work control plane. Some
early compatibility APIs were written before that boundary existed:

- federation coordination messages use `public.coord_messages` without an
  authoritative workspace column;
- federation peers use a global peer table and historically stored a bearer
  token directly;
- router observations use a global table and can influence recommendations
  across workspaces.

Those routes cannot honestly be presented as tenant-safe. This slice contains
the risk without silently assigning historical rows to a workspace.

## Invariants

1. An authenticated request never receives an implicit permission for an
   unknown or misspelled route.
2. Legacy federation and router routes are unavailable by default.
3. The legacy routes can be enabled only by an explicit configuration flag for
   controlled development or migration compatibility testing.
4. Production configuration rejects that flag.
5. Historical global rows remain auditable; they are not reassigned to an
   arbitrary workspace.
6. The federation poller does not run while the legacy surface is quarantined.
7. Existing workspace authorization still applies when compatibility mode is
   explicitly enabled; the opt-in does not make global data workspace-safe.

## Implementation and reuse decision

The implementation reuses the existing authenticated HTTP middleware and
permission matrix. The change is deliberately small: the middleware now
fails closed on an empty route permission, gates the legacy paths, and the
runtime skips their background poller unless enabled. No connector, event,
credential, or Postgres authority is duplicated.

The compatibility smoke scripts remain available but require
`FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES=true`. The normal production/runtime
manifest sets the flag to false. This makes the safety boundary visible in
both code and operator workflows.

## Schema and migration decision

This slice does not add a migration that invents workspace ownership for
existing global rows. The correct follow-up is a quarantining migration that
adds nullable workspace identity, scoped indexes, credential references, and a
forward-only adoption procedure. Until that migration and its backfill proof
exist, the APIs stay disabled by default.

## Cost, performance, and licensing

The containment adds one in-process path/flag check per authenticated request
and avoids starting the legacy poller in the default mode. It adds no query,
row, index, network, or storage cost in the safe path. The code uses Fornix's
existing MIT-licensed implementation and copies no reference-repository code.

## Acceptance tests

- unknown authenticated routes return a non-success response and never reach a
  handler;
- federation and router routes fail closed when the flag is absent;
- explicit non-production opt-in allows the compatibility routes to pass the
  reviewed permission matrix;
- production configuration rejects the opt-in;
- the poller is not started when the surface is quarantined;
- the compatibility smoke scripts are explicit about their opt-in;
- existing workspace, operation, effect, and full unit checks remain green.

## Remaining work

The containment is not tenant isolation. The universal roadmap still needs a
durable workspace-scoped replacement for coordination/federation and router
learning, database-enforced tenant defense in depth, credential references
instead of raw peer tokens, egress controls for peer polling, and migration
fixtures proving historical data cannot cross a workspace boundary.
