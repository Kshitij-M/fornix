# Loop 45 completion: role-separated PostgreSQL isolation and credential boundary

Status: implemented on the qualification branch; deployment-specific
application-role rollout, external secret-manager integration, and signed
trust admission remain release gates. Durable lease authority is recorded in
[`116-credential-lease-authority-completion.md`](116-credential-lease-authority-completion.md).

## Why this loop matters

Fornix is a universal work control plane. Its Postgres authority contains
operations, tasks, evidence, artifacts, retrieval surfaces, credentials,
observations, approvals, workflows, and derived state. Application code must
scope every query, but a missing predicate or a compromised adapter must not
become a cross-workspace read or write. This loop makes that invariant
defensible at the database boundary and makes credential use explicit at the
provider boundary.

## Delivered

- Migration `046_workspace_rls_all_surfaces.sql` enables fail-closed RLS on
  every current base or partitioned `fornix` table carrying `workspace_id`.
  Both `USING` and `WITH CHECK` compare rows with the transaction-local
  `fornix.workspace_id` setting.
- `beginWorkspaceTx`, `workspaceQueryRow(s)`, `workspaceExec`, and the
  exported transaction-context boundary are used by the universal stores and
  retrieval snapshot before protected work. Explicit workspace predicates are
  retained.
- Event/projection, task, operation, validation, change, artifact, evidence,
  retrieval, ingestion, workflow, model-call, tool-run, evaluation,
  observability, policy, and scheduler paths now install tenant context at
  their workspace transaction boundary.
- API-key authentication now calls a narrowly scoped `SECURITY DEFINER`
  function. It resolves the opaque key identifier without requiring an
  unscoped runtime read of `api_keys`, returns only the redacted principal and
  normalized permissions, and records `last_used_at` inside the same
  transaction. The qualification script transfers the function to the
  migration role and grants `EXECUTE` only to the runtime role.
- `scripts/qualification/role-separated-postgres.sh` verifies role
  attributes, table ownership, schema/table/sequence/function privileges,
  RLS coverage, non-owner workspace filtering, and scoped API-key
  authentication. It uses a synthetic hash fixture and removes it before
  returning.
- OpenAI-compatible model calls can acquire, validate, and release a
  short-lived `credentials.Lease` bound to workspace, credential reference, and
  purpose. Migration 047 supplies the durable Postgres fence/revocation
  authority while an injected resolver owns secret bytes. Environment-backed
  credential resolution remains explicit development-only compatibility
  behavior.
- Authorization-denial audit writes now commit before returning the denial,
  preserving the durable audit invariant.

## Qualification commands

Fresh owner-role integration:

```sh
FORNIX_TEST_PG_DSN='postgres://USER:PASSWORD@HOST:PORT/DISPOSABLE_DATABASE?sslmode=disable' \
  go test ./internal/... -count=1
```

Role-separated database qualification:

```sh
FORNIX_RLS_ADMIN_DSN='postgres://...' \
FORNIX_RLS_TEST_DSN='postgres://...' \
FORNIX_RLS_APP_ROLE='fornix_runtime' \
FORNIX_RLS_MIGRATION_ROLE='fornix_migrator' \
  make qualification-role-separated-postgres
```

The admin and runtime DSNs are read from the environment and are never printed
by the helper. Role creation, passwords, rotation, network policy, and
migration scheduling remain deployment-owned responsibilities.

## Observed local results

- Fresh PostgreSQL 17/pgvector database: all `internal/...` integration tests
  passed after applying the full embedded migration set.
- Non-owner runtime qualification: all current workspace-bearing tables had
  RLS and the expected policy; unset context returned no rows; same-workspace
  insert succeeded; foreign reads were invisible; foreign writes failed.
- Scoped API-key qualification: the runtime role successfully invoked only
  the authentication function and received the expected workspace/identity/
  permission tuple; the synthetic fixture was then deleted by the admin role.
- Model lease unit test: the provider used the leased secret only for the
  outbound request and released the lease after completion.
- Authorization audit regression: denied requests now leave one durable
  denial record.
- The role-separated database adds one bounded context-setting statement per
  protected transaction. RLS adds an indexed workspace equality predicate;
  exact p95 depends on pool size, network, PostgreSQL statistics, and table
  cardinality and must be measured again on the target deployment.

## Invariants and crash behavior

- A runtime role is never a table owner, superuser, or `BYPASSRLS` role.
- No protected query occurs before transaction-local workspace context is set.
- Missing scope fails closed. Context is not stored at session level, so pooled
  connections cannot retain a previous workspace.
- A failed context setup, credential lease acquisition, or transactional
  mutation rolls back without a partial event, audit decision, artifact,
  lease, or projection write.
- External model execution remains at-least-once. A credential lease bounds
  and authorizes secret use; it cannot turn a remote provider call into an
  exactly-once effect.
- The authentication function returns no token, token hash, secret, prompt,
  or raw provider payload to durable Fornix history.

## Remaining limitations

This loop does not provide an external KMS/secret-manager implementation,
credential rotation orchestration, encryption-at-rest policy, HA/failover,
backup/PITR SLOs, signed capability catalogs, central egress enforcement, or
full production load/soak evidence. Global workspace-registry operations are
privileged control-plane paths and require deployment authorization. Signed
trust admission for connectors, capabilities, schemas, and policy catalogs is
the next boundary.
