# Task 63 — Workspace transaction boundary and pool-hygiene foundation

Status: implemented qualification slice; broader deployment qualification remains open
Scope: universal transformation / database-enforced workspace authority
Migrations: none

## Why this task exists

Fornix's universal contract says that Postgres is the authority for workspace
scope, not merely a convention in application predicates. Migrations 044 and
046 install fail-closed row-level-security policies that read a transaction-
local `fornix.workspace_id` setting. The store package already starts scoped
transactions for most durable mutations.

The audit for Task 63 found that compatibility HTTP handlers for memo search,
RAG, chunk indexing, and symbol indexing still use the connection pool
directly. Their SQL predicates are workspace-aware, but a non-owner,
`NOBYPASSRLS` runtime role has no transaction-local context on those calls.
That is a deployment correctness defect: reads return empty results and writes
are rejected even for the authenticated workspace. It also means the code has
no direct test proving that a returned pooled connection cannot retain another
workspace's context.

This slice makes the boundary explicit and reusable without changing the
schema, weakening RLS, or introducing a second tenant authority.

## Invariants

1. Every workspace-scoped SQL statement runs inside a transaction whose local
   context is set before the first protected query.
2. The caller's explicit `workspace_id` predicates remain mandatory. RLS is
   defense in depth, not a replacement for application-level scope checks.
3. Context is installed with `set_config(..., true)` through
   `fornix.set_workspace_context(text)`, so commit or rollback clears it before
   the pooled connection can serve another request.
4. A transaction cannot be created without a non-empty, bounded workspace ID.
5. A transaction-local context failure rolls back and returns the connection
   without executing application SQL.
6. Cross-workspace reads and writes fail closed under a non-owner,
   `NOBYPASSRLS` role. The development owner/superuser path remains compatible
   but is not evidence of isolation.
7. Read-only compatibility routes may use the same transaction boundary; they
   must not create a new cache, process-global tenant variable, or connection
   session setting.
8. Transaction ownership is unambiguous: the helper starts, scopes, commits,
   or rolls back; the caller owns row iteration and must close rows before
   commit.
9. No credentials, prompts, raw provider payloads, or arbitrary user text are
   added to scope diagnostics. Qualification output contains only bounded
   status and timing facts.
10. The boundary is domain-neutral. Repository chunks and symbols are the
    first compatibility surfaces being migrated, not the definition of the
    universal harness.

## Reuse and licensing decisions

- Reuse `store.BeginWorkspaceTx` and the existing migration-defined
  `set_workspace_context` function rather than adding a pool wrapper or a
  process-global tenant variable.
- Reuse the current typed retrieval and embedding ledgers; this task changes
  only their SQL transaction boundary.
- Use reference repositories as architectural input for scoped stores,
  checkpoint discipline, and diagnostics. No reference source is copied.
- No Kronaxis Fabric source is copied because its BSL 1.1 license is
  incompatible with Fornix's MIT distribution.

## Schema and deployment impact

No migration is required. The implementation uses migrations 044 and 046 as
already-applied authority. Existing databases remain compatible. Deployment
operators must still transfer table ownership to a migration role and run the
application as a non-owner `NOBYPASSRLS` role before claiming database-enforced
tenant isolation.

The change is intentionally additive at the Go boundary. It does not change
RLS policy text, grants, role ownership, or the migration checksum history.

## Crash, pool, and concurrency semantics

A crash before commit rolls back the scoped transaction and returns no partial
mutation. A crash after commit leaves the existing authoritative row/history
semantics unchanged. Concurrent workspace requests may use the same pool, but
each transaction has its own local context and explicit predicates. A returned
connection must show an empty workspace setting before reuse; a subsequent
request must be able to install a different workspace without observing the
previous one.

## Cost and performance budget

Each scoped operation adds one small `SELECT fornix.set_workspace_context`
inside the existing transaction. No extra durable row, index, provider call,
or payload storage is introduced. Read/write latency impact should be measured
as one bounded SQL round trip per transaction. The pool-hygiene test uses a
single connection to make context contamination deterministic; it is a
correctness test, not a throughput benchmark.

## Acceptance tests

- A transaction cannot begin without a workspace scope.
- Context is set before the first protected statement.
- Commit and rollback clear the transaction-local setting on a reused pooled
  connection.
- Same-workspace retrieval/index writes work through the scoped boundary.
- Cross-workspace reads and writes fail closed under `NOBYPASSRLS`.
- Concurrent requests for two workspaces cannot observe each other's context.
- Crash/failure hooks leave no partial scoped mutation.
- Existing migrations remain checksum-compatible.
- Offline, race, disposable-Postgres, smoke, and documentation checks remain
  green.
