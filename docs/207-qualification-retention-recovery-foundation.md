# Task 91 feature note — qualification retention and recovery evidence

Status: implementation in progress.

## Problem and scope

Fornix now records immutable readiness snapshots, incident annotations, and
freshness-policy revisions. Those records are intentionally useful during a
long-lived incident, but operators also need a bounded way to answer two
operational questions:

1. Which qualification records are past the workspace's declared review
   retention horizon and may be considered for external archival?
2. Is the qualification history internally recoverable, or are there missing
   retention metadata rows, dangling references, or hash mismatches?

This task adds a repository-owned retention metadata and planning layer. It
does not delete, rewrite, or hide readiness history. External archival remains
an explicitly deployed operation and must preserve the record hashes and
provenance links returned by Fornix.

## Invariants

- Readiness snapshots, incident annotations, and freshness policies remain
  append-only authoritative history.
- Retention policy revisions and retention metadata are workspace- and
  deployment-scoped, immutable, and auditable.
- A record is eligible for external archival only when its retention deadline
  has elapsed and it is outside the configured latest-record protection window.
- Incident annotations remain protected while their referenced snapshot is
  retained; the default policy also protects all incident annotations.
- Retention planning is deterministic for a fixed `as_of`, policy revision,
  database state, and cursor. It returns hashes and bounded identifiers, never
  raw deployment payloads or logs.
- Recovery reports are read-only. They detect missing metadata, dangling
  references, workspace/deployment mismatches, and authority-hash mismatch;
  they never repair or mutate state implicitly.
- Missing retention metadata is a recoverable qualification condition, not a
  license to delete history. A bounded metadata sync may insert only missing
  metadata rows and is idempotent.
- No retention result grants, extends, or revokes release admission.
- Every write carries workspace, actor, request, idempotency, causation, and
  correlation context. Cross-workspace access fails closed through both
  application predicates and PostgreSQL RLS.

## Policy and metadata model

`QualificationRetentionPolicy` is an immutable revision for one workspace and
deployment. It contains bounded retention durations for snapshots,
annotations, and freshness policies, plus `keep_latest` protection counts and
an explicit `protect_incidents` flag. The policy hash excludes identity and
actor metadata, so a replay can compare the policy's authority facts.

`QualificationRetentionMetadata` is a separate append-only row keyed by
`record_kind` and `record_id`. It preserves the record's authority hash, the
policy revision used to calculate `retain_until`, and a deterministic metadata
hash. This avoids mutating historical rows merely to add operational metadata.
New readiness records register metadata in the same transaction as their
authoritative insert. A bounded sync covers records created before migration
075 or before a metadata registration failure was repaired.

## Planning and recovery semantics

Retention planning is always dry-run/read-only. It orders candidates by
`record_kind`, record ID, and authority hash, applies the current policy at an
explicit `as_of`, and returns at most the requested batch. It classifies rows
as `eligible`, `protected_latest`, `protected_incident`, `metadata_missing`,
or `not_due`. A caller may use the hashes to construct an external archival
manifest, but Fornix does not remove authoritative rows.

The recovery report checks bounded counts and hash-only facts for snapshots,
annotations, freshness policies, retention metadata, and append-only event
links. It reports issue codes and a stable report hash. It does not read raw
deployment content, execute external tools, or alter release admission.

## Crash, idempotency, and concurrency behavior

- Policy publication uses a workspace/deployment advisory transaction lock and
  an idempotency key. Replaying the same request returns the existing policy;
  reusing the key with different authority facts fails closed.
- Metadata registration occurs in the same transaction as the source record
  when possible. If the transaction crashes, neither authoritative source nor
  metadata is committed partially.
- Metadata sync uses bounded transactions, `ON CONFLICT DO NOTHING`, and a
  stable cursor. Repeated or concurrently overlapping pages create no second
  effect.
- Planning and recovery run in workspace-scoped read transactions. They are
  safe to repeat and return the same hash for the same `as_of` and database
  state.
- A stale or cross-workspace cursor is rejected; no query is allowed to widen
  the workspace scope.

## Schema and migration

Migration 075 adds:

- immutable `qualification_retention_policies` and append-only policy events;
- append-only `qualification_retention_metadata` rows and metadata events;
- workspace-scoped RLS, bounds, indexes, and append-only mutation triggers.

The migration is additive and does not rewrite migrations 073 or 074. Existing
rows remain readable; the recovery report explicitly identifies rows lacking
metadata until a bounded sync is run.

## Reuse and licensing

The implementation reuses Fornix's existing `ReadinessStore`, qualification
normalization/hash helpers, workspace transaction/RLS conventions, artifact
disclosure model, pagination limits, audit actor contract, and operator CLI.
No reference-repository source is copied. The repository remains MIT-licensed;
external deployment-owned retention and backup systems retain their own
licensing and operational responsibilities.

## Cost and storage budget

The new metadata row is hash-only and bounded; it does not duplicate source
payloads. Policy publication and metadata sync use one bounded transaction per
page. Planning and recovery use bounded indexed reads, with no artifact or
network work. The default sync/planning page is 100 rows and is capped at
1,000. Operators should run sync and recovery off the request path for large
workspaces.

## Acceptance tests

- Fresh and existing databases apply migration 075 cleanly.
- Policy publication is idempotent, monotonic, workspace-isolated, and
  immutable.
- New records receive metadata transactionally; a crash leaves no orphan
  metadata or authoritative row.
- Metadata sync is bounded, resumable, idempotent, and dry-run safe.
- Planning is deterministic, budget-bounded, and never deletes history.
- Latest-record and incident protection are enforced.
- Recovery detects missing metadata, dangling annotations/events, and hash
  mismatch without leaking raw content.
- Cross-workspace reads, writes, cursors, and policy references fail closed.
- Concurrent policy publication and metadata sync preserve one durable effect.
- Existing unit, race, documentation, smoke, and offline qualification gates
  remain green. PostgreSQL integration tests run when
  `FORNIX_TEST_PG_DSN` is supplied.
