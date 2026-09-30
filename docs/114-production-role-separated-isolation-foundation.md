# Production role-separated PostgreSQL isolation

Status: implementation and qualification in progress. This note defines the
deployment contract; it is not a production-readiness declaration.

## Why this boundary exists

Fornix is a universal work control plane. A workspace is the durable tenant
boundary for operations, evidence, artifacts, retrieval, credentials,
observations, and their derived state. Application-level authorization is the
first boundary, but it must not be the only boundary protecting a shared
PostgreSQL authority. A compromised query, a missing predicate, or a future
adapter should fail closed rather than expose another workspace's rows.

PostgreSQL row-level security (RLS) is therefore a defense-in-depth boundary
for every table that carries `workspace_id`. It does not replace identity,
RBAC, policy admission, input validation, encryption, backup controls, or
connector-specific authorization.

## Migration layers

The workspace boundary is installed in layers:

| Migration | Responsibility | Qualification status |
| --- | --- | --- |
| `044_postgres_workspace_isolation.sql` | installs `fornix.set_workspace_context(text)` and RLS for the original generic operation/admission tables | qualified with the existing owner-role smoke; owner-role execution does not prove production isolation |
| `045_operation_resource_leases.sql` | adds resource lease tables and their workspace policies | covered by the generic isolation qualification |
| `046_workspace_rls_all_surfaces.sql` | installs the same fail-closed policy on every current base or partitioned table with a `workspace_id` column | policy coverage is migration-tested; complete application-role operation is still being qualified |
| `047_credential_leases.sql` | adds durable workspace-scoped credential leases, monotonic fences, revocation epochs, append-only lifecycle events, and RLS | covered by fresh migration and lease integration tests; managed KMS/secret-manager deployment remains open |

Migration `046` is deliberately catalog-driven so a new universal surface is
not silently omitted from the policy installation. Future migrations must
continue to add a `workspace_id` column to tenant-owned tables and must keep
the policy coverage test green.

## Transaction-local context

The policy compares each row's `workspace_id` with the transaction-local
setting `fornix.workspace_id`:

```sql
workspace_id = NULLIF(current_setting('fornix.workspace_id', true), '')
```

An unset or empty context matches no row. Both `USING` and `WITH CHECK` are
installed, so an unscoped runtime cannot read another workspace or insert and
update a row into one. The context is set by `fornix.set_workspace_context`
inside the transaction that performs the protected work. The function uses
`set_config(..., true)`, so the setting is local to that transaction.

Every workspace-scoped store transaction must therefore follow this order:

```text
BEGIN
  SELECT fornix.set_workspace_context($workspace)
  -- all protected reads and writes
COMMIT or ROLLBACK
```

The explicit `workspace_id` predicate remains mandatory in application SQL.
RLS is a second check, not a reason to remove predicates or authorization
checks. A pooled connection must never be configured with a session-level
workspace value; transaction-local scope prevents one request from leaking
into the next request using the same connection.

## Deployment roles

Production uses separate database roles:

1. The migration role owns the `fornix` tables and runs versioned migrations.
2. The runtime role is a non-owner, `NOSUPERUSER`, `NOBYPASSRLS` role. It
   receives only the schema usage, table DML, sequence, and
   `set_workspace_context(text)` privileges required by the service.
3. Runtime role membership must not inherit `SUPERUSER`, `BYPASSRLS`, or an
   owner role indirectly.
4. Role creation, passwords, rotation, and connection strings stay with the
   deployment secret system. The repository qualification script accepts role
   names and a pre-provisioned administrator DSN; it does not print or create
   secret material.

The local Compose database user owns the tables and consequently bypasses RLS
under PostgreSQL's owner rules. Local tests prove application behavior and
schema correctness, but they are not evidence of tenant isolation. The
role-separated qualification must run against a disposable database with the
non-owner runtime role.

## Coverage matrix

There are two separate things to qualify:

| Boundary | Question | Evidence |
| --- | --- | --- |
| Policy coverage | Does every current table with `workspace_id` have RLS enabled and a fail-closed policy? | migration `046`, catalog assertions, and the role-separation script |
| Application context coverage | Does every store transaction set the intended workspace before its first protected query? | store tests, representative runtime-role API tests, and code review of the store context helper adoption |
| Role separation | Can the runtime role neither bypass RLS nor mutate the migration catalog? | role attributes, ownership, grants, and negative privilege checks |
| Pool hygiene | Does a connection reused for another workspace start with no prior scope? | unset-context and alternating-workspace tests |

Policy coverage alone is insufficient. A runtime role with incomplete context
plumbing will fail closed for legitimate requests, which is preferable to a
cross-workspace read but still a release blocker. The qualification status in
this note must remain “in progress” until the application-context matrix and
representative HTTP/CLI workflow pass under the runtime role.

## Failure semantics

- Missing or empty workspace scope is an authorization failure, not a default
  workspace.
- A cross-workspace read returns no visible row or a stable not-found result;
  callers must not use error differences to discover another tenant.
- A cross-workspace write fails through RLS or the store's explicit workspace
  validation and must not create a partial event, artifact, lease, or
  projection effect.
- A failed context-setting call rolls the transaction back before it can reach
  a protected query.
- A database role that is owner, superuser, or `BYPASSRLS` is rejected by the
  role-separated qualification; development-mode ownership is explicit and
  never represented as production evidence.

## Operational commands

The verification helper is:

```sh
FORNIX_RLS_ADMIN_DSN='postgres://...' \
FORNIX_RLS_TEST_DSN='postgres://...' \
FORNIX_RLS_APP_ROLE='fornix_runtime' \
FORNIX_RLS_MIGRATION_ROLE='fornix_migrator' \
scripts/qualification/role-separated-postgres.sh
```

The helper verifies an already-provisioned database, exercises the non-owner
RLS smoke, and checks the scoped API-key authentication function without
printing the credential fixture. Do not place a real DSN, password, or token
in source, logs, CI output, or an issue. The helper does not replace a secret
manager or a production migration orchestrator.

## Remaining limitations

This foundation does not yet provide a hosted identity provider, managed KMS/
secret-manager adapter, encryption-at-rest policy, HA/failover, backup/PITR, connector
conformance, sandbox isolation, or a universal egress boundary. It also does
not make remote model or external-system calls exactly once. Those remain
separate production gates in
[`111-universal-production-roadmap-status.md`](111-universal-production-roadmap-status.md).
