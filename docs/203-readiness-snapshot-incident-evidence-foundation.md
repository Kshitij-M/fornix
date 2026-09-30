# Task 89 feature note — readiness snapshots and operator incident evidence

Status: implemented as an advisory, repository-owned observation slice.

## Problem and scope

Fornix can now evaluate a release gate and preserve the lifecycle of each
deployment-evidence link. Operators still need a compact, durable answer to a
different question: **which exact authority facts made this release ready or
blocked at a particular evaluation?** A live gate query is useful, but it is
not an auditable observation when evidence is later revoked, replaced, or
superseded.

This task adds an advisory, hash-only readiness snapshot and a bounded incident
annotation surface. A snapshot records the release identity, trust snapshot,
active evidence-link identities, gate hash, deterministic diagnostics, and
evaluation time. An annotation records a closed-vocabulary operator
disposition against a snapshot or its stable hash. Neither surface changes
release admission, mutates signed imports, or executes deployment work.

## Invariants

1. A snapshot is workspace-scoped and is created only from the authoritative
   release/evidence/trust rows inside one PostgreSQL transaction.
2. The snapshot hash covers the exact release hash, trust revision/hash,
   required kinds, active evidence IDs, gate hash, missing kinds, blocked
   reasons, and readiness bit. Evaluation timestamps and actor metadata are
   provenance, not authority identity, so repeated evaluation of unchanged
   facts is hash-stable.
3. Snapshot rows and annotation rows are append-only. No operation rewrites a
   prior readiness decision; a later gate result creates a new snapshot hash.
4. One idempotency key has one request identity and one durable effect. A
   replay with different scope or facts fails closed. The same canonical
   snapshot hash is deduplicated within a workspace/deployment/release scope.
5. An annotation can reference only a snapshot in the same workspace,
   deployment, and release. It cannot make a blocked snapshot ready.
6. Active evidence IDs are sorted and bounded. Inactive historical evidence is
   not treated as active evidence merely because it remains auditable.
7. Incident annotations contain no free-form deployment payload. They use
   bounded reason codes, dispositions, and optional evidence/snapshot hashes.
8. PostgreSQL remains the sole authority. HTTP and CLI are authenticated
   adapters over the store; they do not maintain local readiness state.

## Schema and transaction design

Migration `073` adds:

- `qualification_readiness_snapshots`, keyed by workspace and identity,
  storing canonical hashes, bounded JSON arrays, actor/provenance metadata,
  idempotency, and evaluation time;
- `qualification_readiness_snapshot_events`, an append-only capture/audit
  stream; and
- `qualification_incident_annotations`, an append-only, workspace-scoped
  operator evidence table with idempotency and snapshot linkage.

All JSON and text fields have database bounds. Hashes use the existing
lowercase SHA-256 contract. RLS policies follow the existing qualification
tables. The capture transaction locks the release scope, reads the release and
current gate facts, checks idempotency/hash conflicts, and commits the
snapshot plus event atomically. Dry-run rolls back and returns a deterministic
projected record. Annotation creation locks and verifies its referenced
snapshot in the same transaction.

## Disclosure and authorization

Read APIs disclose only snapshot and annotation contracts. There is no raw
payload flag because the records never contain raw deployment data. List APIs
use stable ID cursors and bounded page sizes. Qualification-read permission is
required for inspection; qualification-admin permission is required for
capture and annotation. Workspace identity comes from authenticated request
context, and a caller-supplied workspace cannot widen it.

## Reuse and licensing

The implementation reuses Fornix's `DeploymentEvidenceStore.EvaluateGate`,
qualification audit actors, cursor limits, workspace transaction helpers,
qualification error mapping, RBAC permissions, and CLI HTTP adapter. It does
not copy reference-repository source. The repository remains MIT-licensed;
only design patterns, not BSL source, are reused.

## Cost and storage budget

Each capture adds one small bounded snapshot row and one small append-only
event only when the gate hash is new. Replaying an unchanged snapshot is a
read and does not duplicate authority facts. An annotation adds one bounded
row. No raw evidence, signed bundle, artifact, or deployment log is copied.
Capture performs one existing release-scope gate read plus bounded indexed
lookups; list operations are cursor-paginated. Hosted PostgreSQL latency, WAL,
and index size must still be measured with the deployment's actual topology.

## Acceptance tests

- Contract normalization rejects cross-scope, oversized, unsupported, or
  malformed snapshot and annotation values.
- Identical gate facts produce the same snapshot hash despite different
  evaluation timestamps or list order.
- Snapshot capture is idempotent and conflicting request reuse fails closed.
- Dry-run capture creates no row or event.
- A gate change produces a new snapshot while the previous snapshot remains
  readable and unchanged.
- Annotation creation is idempotent, auditable, bounded, and cannot cross
  workspace/deployment/release or reference a missing snapshot.
- Concurrent captures preserve one canonical snapshot per hash and one effect
  per idempotency key.
- A crash before commit leaves no snapshot, event, or annotation; a replay
  after commit returns the committed identity.
- HTTP authorization, cursor pagination, redaction, and CLI behavior are
  covered.
- Existing unit, race, vet, migration, offline qualification, build, smoke,
  and documentation checks remain green. PostgreSQL integration tests execute
  when `FORNIX_TEST_PG_DSN` is configured.
