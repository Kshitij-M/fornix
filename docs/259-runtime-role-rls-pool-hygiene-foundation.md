# Runtime-role RLS and pooled-connection hygiene qualification

Status: implementation note for a database-backed qualification slice. This
does not prove a hosted deployment's complete tenant isolation or production
readiness.

## Problem

The generic control plane relies on transaction-local PostgreSQL workspace
context and row-level security. A unit test or owner-role query cannot prove
that an application role which does not own the tables, has no `BYPASSRLS`,
and reuses pooled connections remains isolated after commit, rollback, or an
unset context. The current role-separated qualification seeds two workspace
rows but probes them from separate `psql` processes; it does not exercise the
same application connection through those scope transitions.

## Intended qualification

Extend the existing disposable role-separated PostgreSQL qualification. With
the non-owner runtime role and one pooled connection, verify:

1. The connected role is non-superuser, does not have `BYPASSRLS`, is not the
   owner of the fixture table, and RLS is enabled there.
2. An unscoped read sees no fixture rows.
3. A transaction scoped to workspace A sees only A's fixture, then commit
   clears the context before that physical connection is reused.
4. A transaction scoped to workspace B sees only B's fixture, then commit
   clears the context.
5. A workspace-scoped transaction rolled back after reading its own fixture
   leaves no context on the reused connection.
6. Subsequent unscoped reads remain empty; one tenant's setting never grants
   visibility to the other tenant.

The qualification uses synthetic fixture identifiers and hashes only. It
prints no DSN, credential, row payload, or secret. The test is opt-in when run
alone, but the explicit role-separated qualification command must fail if its
DSNs or roles are absent and must execute the test against the pre-provisioned
disposable database.

## Invariants and failure behavior

- Postgres RLS is evaluated by the actual non-owner runtime role; table-owner
  behavior is not accepted as tenant-isolation evidence.
- Workspace context is transaction-local and set through the same helper used
  by application stores.
- Every assertion is made on one acquired physical connection with the pool
  limited to one connection, so connection reuse is deterministic.
- Missing workspace context fails closed by returning no tenant rows.
- A setting observed in workspace A or B never survives the commit/rollback
  boundary into an unscoped query.
- Setup fixtures are owned and deleted by the administrator qualification
  script. The runtime role performs read-only probes.
- A failed assertion exits the explicit qualification command non-zero; no
  skipped integration test is reported as a passed role-separated gate.

## Reuse, licensing, and cost

Reuse the existing `SetWorkspaceContext`, the disposable RLS fixture setup in
`scripts/qualification/role-separated-postgres.sh`, and the CI PostgreSQL
service. No third-party code is copied and no dependency or license changes.
The test adds a small bounded number of reads and short transactions to the
qualification job. It adds no production query, table, storage, or runtime
cost.

## Acceptance tests

- The non-owner role and table/RLS assertions pass in the role-separated CI
  qualification.
- On one connection, unscoped → workspace A → commit → unscoped → workspace B
  → commit → unscoped → workspace A → rollback → unscoped always observes the
  expected row counts.
- An explicit query for the other workspace remains empty while a tenant
  context is active.
- The qualification script fails closed for missing DSNs/roles and propagates
  a failed Go test.
- Existing Go tests, race checks, `make check`, and documentation validation
  remain green.
- Local results without a disposable non-owner PostgreSQL database are
  reported as unverified, not passed.

## Limitations

This qualifies the current RLS policy and transaction-context hygiene in the
provided PostgreSQL topology. It does not establish isolation across every
hosted proxy/pooler, prove administrator separation, qualify migration
rollbacks, or replace deployment-specific access reviews and failure drills.
