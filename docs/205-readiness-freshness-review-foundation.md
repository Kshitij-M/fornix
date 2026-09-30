# Task 90 feature note — readiness freshness policy and incident review

Status: implemented as an advisory, repository-owned qualification slice.

## Problem and scope

Task 89 records exactly which authority facts produced a readiness snapshot.
Operators also need to answer whether a snapshot is still recent enough for a
review and what changed between two observations. A live freshness check must
not silently turn an advisory observation into an admission token, and a
comparison must not copy deployment logs or re-run deployment work.

This task adds versioned workspace/deployment freshness policies and a
read-only, deterministic snapshot comparison. Policies and comparisons are
control-plane metadata; release admission continues to re-evaluate current
release, trust, and evidence rows through the existing authoritative path.

## Invariants

1. Policies are workspace- and deployment-scoped, immutable after publication,
   monotonically versioned, idempotent, and hash-addressed.
2. A policy contains only a bounded maximum age and a boolean review hint. It
   cannot weaken release admission, extend signed evidence expiry, or authorize
   an external effect.
3. Freshness is evaluated against an explicit `as_of` time. A snapshot is fresh
   only when `as_of >= evaluated_at` and its age is no greater than the policy
   maximum. Future-dated observations fail closed as invalid.
4. A review reads both immutable snapshots and one policy in a single
   workspace-scoped transaction. It never calls a model, tool, provider, or
   deployment system.
5. Review ordering is deterministic. Evidence additions/removals and blocked
   reason additions/resolutions are sorted, bounded sets. Review hashes exclude
   wall-clock metadata but include policy and snapshot authority hashes.
6. Comparing snapshots from different workspaces, deployments, releases, or
   policy scopes fails closed. Missing snapshots and missing policies are not
   treated as fresh or equivalent.
7. Policy rows and policy events are append-only. Replacing a policy creates a
   new revision; historical policy decisions remain auditable.
8. HTTP and CLI surfaces are authenticated. Policy publication requires
   qualification-admin; policy inspection and snapshot review require
   qualification-read.

## Schema and transaction design

Migration `074` adds:

- `qualification_readiness_policies`, an immutable versioned policy table with
  bounded age, stable policy hash, idempotency, actor, and provenance;
- `qualification_readiness_policy_events`, an append-only publication stream;
- bounded indexes and workspace RLS policies.

Publishing locks one workspace/deployment scope, checks idempotency and the
current revision, and commits the policy plus publication event atomically.
Review uses the existing snapshot table and the current policy in one read
transaction. It writes no review row because comparison is a read-only
operator operation; the returned review hash is sufficient for a receipt,
incident annotation, or external report to reference it.

## Reuse and licensing

The implementation reuses Task 89 normalization, stable cursors, workspace
transactions, audit actors, RLS, qualification error mapping, and CLI HTTP
adapter. It reuses the existing release/evidence authority only through the
already-persisted snapshot facts. No reference-repository source is copied;
the repository remains MIT-licensed and BSL code is not reused.

## Cost and storage budget

Each policy publication adds one small bounded row and one append-only event.
Review performs two indexed snapshot reads and one policy read; it creates no
artifact, event, or raw payload. Policy history is bounded by operator
publication frequency and can be measured/retained independently. Review
responses are bounded to the configured evidence and diagnostic list limits.

## Acceptance tests

- Policy normalization rejects invalid scope, age, revisions, and unbounded
  input; hashes are stable under equivalent normalized input.
- Publication is idempotent, monotonic, append-only, and workspace-isolated.
- A snapshot is fresh only within the configured age and explicit `as_of`
  boundary; future and missing observations fail closed.
- Two identical snapshots produce an identical review hash regardless of list
  order or request metadata.
- Reviews deterministically report ready-state drift, gate-hash drift,
  evidence additions/removals, blocked-reason additions/resolutions, and
  stale-side diagnostics.
- Cross-workspace, cross-deployment, cross-release, and missing-policy review
  requests fail closed.
- Policy publication and review are correctly authorized over HTTP and CLI.
- Concurrent policy publishers preserve one revision per serialized scope;
  duplicate requests have one durable effect.
- Review is read-only and produces no durable mutation.
- Existing unit, race, vet, migration, RLS, smoke, offline qualification,
  build, and documentation checks remain green. PostgreSQL integration tests
  run when `FORNIX_TEST_PG_DSN` is configured.
