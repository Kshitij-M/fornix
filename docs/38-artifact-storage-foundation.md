# Fornix artifact storage foundation

## Decision

Task 13 adds a Postgres-only, workspace-scoped content-addressed artifact
plane. It stores immutable raw bytes in bounded chunks, keeps a canonical
SHA-256 identity for each `(workspace_id, content_hash)` pair, and records
references and provenance separately from the bytes. Disclosure is a derived
view over the same hash: `gist`, `detail`, and `raw` never replace or mutate
the authoritative content.

The first integrated path is the model-call response evidence path. Existing
inline request/response evidence remains in place for compatibility and fast
ledger inspection; the artifact reference is the durable large-output pointer.
The same store API is usable by tool, evidence, and agent-output producers
without introducing another persistence authority.

## Invariants

- A workspace is part of artifact identity. The same bytes in two workspaces
  produce two canonical artifact rows and can never be disclosed across the
  workspace boundary.
- `content_hash` is the lowercase hexadecimal SHA-256 digest of the exact raw
  byte sequence. Callers cannot supply or override it.
- Raw bytes are split into deterministic, ordered chunks. Chunk indexes are
  contiguous, chunk sizes are bounded, and each chunk has its own SHA-256
  digest. The artifact digest is verified over the reconstructed byte stream.
- Artifact bytes, content hash, media type, size, and chunk layout are
  immutable. Lifecycle state, integrity state, and retention metadata are
  operational state and do not alter the artifact identity.
- A reference is append-only. It identifies the source kind/id, role,
  idempotency key, actor, and provenance context without copying or replacing
  the raw bytes.
- A provenance link is workspace-scoped and can only connect artifacts that
  belong to the same workspace. Links are append-only and deterministic to
  traverse.
- Duplicate content is deduplicated per workspace. A duplicate idempotency
  key must have the same input identity or fails with a conflict; a duplicate
  content hash returns the existing artifact and does not overwrite chunks or
  the manifest.
- Disclosure always returns the canonical content hash, integrity result, and
  source/provenance references. It is bounded by requested and server maximum
  byte, token, and item budgets; partial raw content is never returned.
- Deletion is a tombstone transition, not an identity reuse. It is allowed
  only after the artifact is archived, its retention deadline has passed, and
  no authoritative reference remains. The hash, manifest, reference history,
  and deletion audit state remain queryable; raw chunks are then removed.
- Integrity verification fails closed on a missing chunk, unexpected chunk
  index, size mismatch, chunk digest mismatch, or reconstructed content-hash
  mismatch. A deleted artifact is reported as deleted rather than silently
  treated as valid raw content.

## Schema and transaction boundaries

Migration `020_artifact_storage.sql` adds:

- `fornix.artifacts`: workspace identity, content hash, media type/kind,
  immutable size/chunk metadata, manifest/disclosure metadata, lifecycle,
  integrity, retention, and timestamps;
- `fornix.artifact_chunks`: workspace/artifact-scoped ordered byte chunks with
  immutable chunk hashes;
- `fornix.artifact_refs`: append-only links from model/tool/evidence/agent
  sources to an artifact, including idempotency and actor metadata;
- `fornix.artifact_provenance`: typed artifact-to-artifact edges with bounded
  metadata; and
- indexes/constraints/triggers for workspace isolation, hash shape, size
  bounds, append-only raw bytes, and deterministic lookup.

Artifact creation and its first source reference run in one transaction. The
transaction inserts-or-locks the canonical hash row, verifies an existing
duplicate, inserts chunks only for a new row, inserts the manifest, and then
inserts the reference. Any failure rolls back all authoritative rows. Model
call completion uses the same transaction for its response artifact link and
terminal ledger update.

## Disclosure and retention

`gist` and `detail` are bounded derived strings in the manifest. `raw` reads
ordered chunks only after the requested byte/token budget can accommodate the
complete artifact; otherwise it returns identity and `truncated=true` without
partial raw bytes. Provenance traversal is breadth-first with explicit depth
and node limits and stable relation/source ordering.

Archive is an explicit, idempotent state transition that preserves raw bytes.
Deletion requires no authoritative references and leaves a tombstone. The
initial implementation does not move bytes to a second tier: Postgres TOAST
plus deterministic chunking is the only storage path. A future cold tier must
preserve the same hash and transactionally maintain the tombstone/reference
contract.

## Reuse and licensing

The design reuses Fornix's existing workspace-scoped evidence, provenance,
idempotency, actor, disclosure, and migration patterns. FornixDB contributes
the conceptual gist/detail/raw and retention model; agentmemory contributes
immutable origin/supersession, bounded traversal, and explicit retention
transitions; Orloj contributes transactional ownership/cleanup discipline.
No source code is copied from the reference repositories. Fornix remains MIT
licensed. Kronaxis-fabric is not used as a code source because its BSL 1.1
license is incompatible with the project's intended MIT distribution.

## Cost and efficiency budget

- Default artifact chunk size is 256 KiB; maximum artifact size is 64 MiB.
- Each new artifact performs one hash computation in memory, one canonical
  insert/lock, one manifest insert, and one chunk insert per chunk. Duplicate
  content performs a bounded hash lookup and verification without rewriting
  raw bytes.
- Raw storage is approximately the byte payload plus Postgres row/index/TOAST
  overhead. Chunking adds one row and digest per 256 KiB; it avoids one
  unbounded row and makes integrity checks/retries bounded.
- Disclosure performs one artifact read plus one ordered chunk read for raw;
  gist/detail avoids loading raw bytes. Model completion adds one artifact
  reference transaction and, for a new response body, the chunk writes.
- The implementation reports operation latency, SQL query counts where the
  store can measure them, relation bytes, deduplication, and disclosure
  truncation. It does not claim object-store-scale economics; Postgres-only
  raw retention remains the main cost limitation.

## Acceptance tests

- Fresh and existing databases apply migration 020 cleanly and preserve all
  previous migration checksums.
- Concurrent identical uploads create one canonical artifact per workspace;
  duplicate idempotency requests are deterministic and conflicting reuse is
  rejected.
- Cross-workspace reads, references, provenance links, and model artifact
  links fail closed.
- Raw bytes are unchanged by duplicate writes; direct chunk mutation is
  rejected; integrity verification detects deliberate corruption or missing
  chunks.
- Gist/detail/raw disclosure preserves the hash and provenance, never exceeds
  byte/token/item budgets, and never returns partial raw content.
- Supersession/provenance references remain auditable; authoritative refs block
  deletion; archive/deletion transitions are idempotent and crash-safe.
- A transaction failure leaves no orphan artifact, chunk, or authoritative
  reference.
- Model-call response completion creates one durable artifact link, is safe to
  replay, and preserves redaction/no-credential guarantees.
- Existing unit, race, integration, HTTP smoke, and qualification checks stay
  green.

## Remaining limitations after this slice

There is no external object store, resumable upload protocol, background
garbage collector, physical partitioning, encryption-at-rest policy distinct
from the database, or transparent migration of every historical inline
payload. Those are deliberate follow-up items; the authority and identity
contract are established first.
