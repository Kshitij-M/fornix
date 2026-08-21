# Artifact-backed output integration foundation

Status: Task 14 design note — implementation follows this note.

## Objective

Move oversized tool, evidence, and agent output out of hot inline columns while
preserving the existing rows as bounded compatibility projections. The
authoritative bytes remain immutable, content-addressed artifacts in Postgres;
the source row and its artifact reference are committed in the same
transaction.

This is an additive migration. Existing inline data remains readable and is
eligible for deterministic backfill. No historical source record is rewritten,
deleted, or replaced by a summary.

## Invariants

1. **Workspace authority.** Every artifact, chunk, reference, source link, and
   lifecycle operation is keyed by `workspace_id`. Composite foreign keys and
   store predicates fail closed on cross-workspace access.
2. **Immutable identity.** An artifact is identified by
   `(workspace_id, sha256(raw_bytes))`. Raw bytes, byte size, chunk ordering,
   manifest, and content hash cannot be updated. Identical content in one
   workspace has one canonical artifact; different workspaces never deduplicate
   with one another.
3. **Source-row compatibility.** Tool/evidence/agent rows retain bounded inline
   fields. When a payload exceeds its inline threshold, the inline field is a
   deterministic marker/summary containing the artifact ID and content hash;
   the original byte size and authoritative hash remain durable. Existing small
   payloads continue to use the current inline representation.
4. **Transactional references.** Artifact creation, the append-only artifact
   reference, the source-row link, and the authoritative event/checkpoint
   mutation occur in one Postgres transaction. A rollback leaves neither an
   authoritative reference nor a newly-created artifact.
5. **Idempotency.** Each output role has a stable source identity and
   idempotency key, for example `tool-output:<run>:stdout`,
   `evidence-raw:<record>`, and `agent-output:<run>:history`. Repeated delivery
   returns the same artifact/reference and does not duplicate source effects.
6. **Fencing.** Task-bound tool and agent writes validate the current task
   owner/fence while holding the authoritative row lock before writing any
   artifact or reference. A stale worker fails closed.
7. **Provenance.** Actor, workspace, causation, correlation, source, role, and
   idempotency metadata are carried on the link. Raw evidence remains linked to
   its source record; artifact references are append-only and auditable.
8. **Retention safety.** Archive is a reversible status transition governed by
   explicit policy. Deletion is allowed only after its deadline, only for
   non-authoritative unreferenced artifacts, and leaves a tombstone/hash/status
   record. Authoritative references block deletion.
9. **Integrity.** Verification recomputes chunk hashes and the whole-artifact
   hash in deterministic chunk order. Corruption is reported and never repaired
   by overwriting authoritative bytes.
10. **Bounded operations.** Backfill, retention, and verification operate in
    bounded ordered batches. Dry runs perform no writes. Re-running a batch is
    safe and deterministic.

## Thresholds and representations

- Tool stdout, stderr, and the redacted result envelope remain inline only
  within the existing `MaxToolEvidenceBytes` compatibility bound (64 KiB).
  Oversized values are stored as separate redacted artifacts with roles
  `stdout`, `stderr`, and `result`; inline fields retain a compact deterministic
  reference marker.
- Evidence raw payloads remain inline up to `MaxEvidenceRawBytes` (4 MiB).
  Larger raw payloads are stored as an `evidence-raw` artifact; the source row
  retains a bounded marker, original byte size, and the artifact content hash.
- Agent `last_output` and serialized history use the existing hot-row limits
  as compatibility thresholds. Oversized output/history is artifact-backed and
  represented inline by a stable marker. Pending tools, state hashes, and
  checkpoints remain authoritative in their existing typed columns.
- Artifact manifests contain only bounded derived gist/detail/metadata. They
  never replace raw bytes and never contain credentials.

All output artifacts use the existing redaction boundary before persistence.
Credentials and secret references are not copied into artifacts, events,
errors, or audit data. The system does not claim that arbitrary child-process
output can be perfectly classified as a secret; the existing redaction policy
and explicit tool environment controls remain the boundary.

## Migration strategy

Migration `021_artifact_output_links.sql` adds nullable workspace-scoped
composite foreign-key columns:

- `tool_runs.stdout_artifact_id`, `stderr_artifact_id`, and
  `result_artifact_id`;
- `evidence_records.raw_artifact_id`; and
- `agent_runs.last_output_artifact_id` and `history_artifact_id`.

It adds indexes for source lookup and uses the existing artifact identity
constraints. The migration is safe on fresh and existing databases, does not
populate links implicitly, and leaves all old inline rows valid. The evidence
raw-size constraint is widened only as needed for an artifact-backed original;
the store still enforces the artifact maximum and bounded inline marker.

## Backfill semantics

Backfill enumerates one source kind at a time in stable `(source_id, role)`
order, with a caller-supplied cursor and a bounded batch size. It selects only
rows without a link whose inline payload exceeds the threshold, locks the source
row, creates/gets the canonical artifact, writes the source link, and commits
each batch transactionally. The operation returns examined, eligible, created,
linked, skipped, and next-cursor counts. Dry run executes the same ordered
selection and reports work without mutation. A source row already linked to an
artifact is skipped, making retries resumable and idempotent.

## Retention and integrity operations

Retention sweep evaluates archive candidates before delete candidates, ordered by
artifact ID. It respects `retain_until`, `archive_after`, `delete_after`,
`allow_delete`, status, and authoritative-reference existence. Dry run returns
the exact bounded candidate counts with no status/chunk mutation. A committed
sweep updates lifecycle state in one transaction per batch, records tombstone
metadata, and never deletes a referenced artifact. Verification is similarly
bounded and records `valid` or `corrupt`; corruption is surfaced in the report
and blocks destructive lifecycle action.

## Reuse and licensing

The implementation reuses Fornix's existing `ArtifactStore`, composite
workspace foreign keys, SHA-256/chunking, redacted evidence helpers, model-call
transaction pattern, task-fence validation, and append-only event history.
Reference research informed behavior, not code copying:

- FornixDB's hot/consolidated/cold disclosure and explicit non-destructive
  retention model;
- agentmemory's dry-run retention, bounded eviction, raw-versus-derived memory,
  supersession, diagnostics, and keyed concurrency patterns;
- Orloj's transactional resource updates, checkpoint ownership, and orphan-safe
  cleanup; and
- DeepSeek Harness's lossless session artifact boundary and crash-tail handling.

No reference source is copied. Fornix remains MIT-licensed. Kronaxis Fabric's
BSL 1.1 code is not reused.

## Cost and performance budget

The hot-row win is proportional to oversized payload bytes: those bytes move to
TOAST-backed artifact chunks and are read only during explicit disclosure.
Each new oversized output normally adds one artifact lookup, one or more chunk
inserts, one artifact reference, and one source-row update inside the existing
mutation transaction. Deduplication removes repeated chunk/storage cost within
a workspace. Backfill/retention/verification default to bounded batches of at
most 100 records and never perform unbounded scans in a request path.

The qualification run must report p50/p95/max write latency, SQL statement and
row work, relation/storage growth, deduplication ratio, backfill and retention
throughput, and integrity findings. The primary tradeoff is additional Postgres
write work at first persistence in exchange for lower hot-row size, stable
replay, and no object-store dependency.

## Acceptance tests

- Fresh and existing databases apply migration 021 cleanly.
- Oversized tool stdout/stderr/result, evidence raw, and agent output/history
  create one canonical artifact and preserve inline compatibility markers.
- Duplicate output delivery creates one artifact, one link per role, and one
  authoritative source effect.
- A forced failure before commit leaves no artifact or source reference.
- Stale task workers cannot create or link task-bound artifacts.
- Cross-workspace artifact/link/disclosure operations fail closed.
- Backfill is deterministic, bounded, dry-run safe, resumable, and idempotent.
- Retention dry runs are side-effect free; referenced artifacts cannot be
  deleted; tombstones and provenance remain auditable.
- Chunk or artifact corruption is detected deterministically and reported.
- Disclosure content hashes remain stable through inline, linked, and replayed
  output paths.
- Existing unit, integration, race, build, CI, and smoke checks remain green.

