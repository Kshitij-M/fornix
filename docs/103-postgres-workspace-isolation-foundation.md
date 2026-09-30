# Postgres workspace-isolation foundation

Status: implementation foundation for universal production qualification.

Audience: operators, database administrators, and maintainers deploying Fornix
as a multi-workspace control plane.

## Problem

Fornix validates workspace scope in typed contracts, authenticated handlers,
and SQL predicates. That is necessary, but it leaves a defense-in-depth gap:
an accidental query omission or an overly privileged database role could still
read or write another workspace. The generic operation and admission authority
is the highest-risk boundary because it owns leases, transitions, effects,
approvals, and replay inputs.

This slice adds database policies for that authority and makes its transaction
scope explicit. It does not pretend that enabling a policy on a table-owning
development role provides isolation; production must use a dedicated
non-owning, `NOBYPASSRLS` application role.

## Invariants

- The workspace context is transaction-local and must be set before any
  generic operation or admission query.
- A policy permits a row only when its `workspace_id` equals the transaction's
  `fornix.workspace_id` setting. An unset or empty context matches no row.
- Both reads and writes use the same policy; cross-workspace inserts and
  updates fail closed.
- The operation/admission store continues to include explicit workspace
  predicates. RLS is defense in depth, not a replacement for application
  authorization or typed contracts.
- RLS policies are forward-compatible and do not rewrite authoritative rows.
  Existing development roles retain compatibility because table owners bypass
  policies until deployment ownership is transferred deliberately.
- No secret, prompt, provider payload, or arbitrary diagnostic text enters the
  context setting or policy error.

## Schema and deployment contract

Migration 044 adds `fornix.set_workspace_context(text)` and a shared policy to
generic operation, lease, attempt, effect, callback, result, admission, and
approval tables. It does not create roles because role ownership and grants are
deployment-specific privileged operations.

Production deployment must:

1. run migrations with a separate owner/migrator role;
2. transfer table ownership to that role;
3. grant the application role only the required schema/table/sequence
   privileges plus `EXECUTE` on `fornix.set_workspace_context(text)`;
4. ensure the application role has `NOBYPASSRLS`; and
5. use the application role for runtime connections.

The local development Compose profile remains compatible with the existing
single user until this role split is qualified. The qualification runbook must
include a non-owner role test before a deployment claims database-enforced
tenant isolation.

## Reuse and licensing

The design follows PostgreSQL's native row-level security and transaction-local
configuration model. It reuses Fornix's existing workspace predicates and
operation transaction boundaries. No reference source is copied and no
additional service or network component is introduced. The repository remains
MIT licensed; Kronaxis Fabric source is not used.

## Cost and failure budget

Setting the context is one bounded `SELECT` per generic transaction. RLS adds a
simple equality predicate on already indexed workspace keys. The expected
database work is constant with respect to payload size; deployment
qualification must measure p50/p95 latency, query count, lock time, and plans
with the application role. Missing context is a deliberate fail-closed error.

## Acceptance tests

- every generic operation/admission transaction sets the expected context;
- an unset context cannot read or write protected rows under a non-owner role;
- a workspace context can read/write only its own rows;
- a cross-workspace operation, effect, approval, or replay request fails
  without changing authoritative history;
- owner-role development tests remain compatible while policies are installed;
- policy installation is idempotent on fresh and existing databases;
- role/grant verification reports whether the current runtime role bypasses
  RLS instead of silently claiming isolation;
- existing unit, Postgres integration, race, smoke, migration, and
  documentation checks remain green.

## Remaining limitations

This is a policy and application-context foundation, not proof that every
legacy table has RLS. Identity, retrieval, artifacts, tasks, and older global
compatibility surfaces require their own staged policy coverage. Production
role separation, managed Postgres privileges, failover, and adversarial
cross-tenant qualification remain deployment gates.
