# Loop 13 completion — durable artifact storage

Status: complete for the smallest production-quality Postgres vertical slice.

## Delivered

- Added typed artifact contracts for immutable content, ordered chunks,
  manifests, references, provenance links, disclosure, and retention.
- Added migration `020_artifact_storage.sql`. It creates workspace-scoped
  artifact/chunk/reference/provenance tables, composite workspace foreign
  keys, hash/size constraints, immutable raw/identity triggers, append-only
  history triggers, and the model-call response artifact link.
- Added `ArtifactStore` with transactional put/deduplicate/reference,
  workspace-scoped lookup, bounded gist/detail/raw disclosure, deterministic
  provenance traversal, integrity verification, archive, tombstone deletion,
  and failure-injection rollback coverage.
- Integrated successful model-call response evidence into the same transaction
  as terminal model-call completion. The existing redacted inline ledger field
  remains for compatibility; `response_artifact` is the immutable content
  pointer and contains no credentials.
- Added authenticated HTTP endpoints:
  `POST /v1/artifacts`, `POST /v1/artifacts/disclose`, and
  `POST /v1/artifacts/provenance`. They inherit workspace RBAC and propagate
  the authenticated actor into references.
- Added migration-aware Go tests, concurrency/deduplication tests, disclosure
  budget tests, provenance/workspace tests, raw immutability and corruption
  tests, model transaction crash tests, retention/deletion safety tests,
  latency/storage measurements, CI wiring, Makefile support, and v0.22 smoke
  coverage.

## Measured local results

The Postgres integration measurement used 20 new artifacts with approximately
32 KiB each, one 256 KiB chunk per artifact, and one source reference each:

- create plus reference: p50 `2.02 ms`, p95/max `4.93 ms`;
- relation size for artifacts, chunks, refs, and provenance: `811,008` bytes;
- rows: 20 artifacts, 20 chunks, 20 references;
- concurrent identical uploads: 12 writers, one canonical artifact and one
  reference effect;
- disclosure verifies ordered chunks and the SHA-256 content hash before
  returning data, so gist/detail/raw reads trade extra database work for
  deterministic corruption detection;
- full local smoke chain remained green through v0.22, including the existing
  projection measurement (p50 `8.52 ms`, replay `1,331 events/s`) and the new
  artifact HTTP hash/isolation check.

The SQL shape for a new artifact is one canonical insert, one bounded integrity
update, one insert per chunk, one reference insert, and one final identity
read. Duplicate content uses the workspace/hash unique index, locks and
verifies the canonical row, then performs only reference work. Raw disclosure
adds one ordered chunk scan; gist/detail can be derived without returning raw
bytes, but the current implementation verifies chunks for every disclosure.

## Correctness and failure semantics

Content identity is `sha256(exact raw bytes)` within a workspace. Chunks and
manifests cannot be overwritten; references and provenance cannot be edited or
deleted. A transaction failure rolls back artifact, chunks, and references
together. A duplicate model completion is a no-op after the first terminal
ledger update. A model artifact failure leaves the model call running and no
artifact reference, allowing deterministic retry. An authoritative reference
blocks deletion; non-authoritative artifacts can be archived and tombstoned
after their explicit retention deadline while preserving identity/history.

## Reuse and license

The implementation reuses Fornix's existing migration, workspace, actor,
idempotency, evidence-disclosure, and model-ledger patterns. FornixDB's
gist/detail/raw and retention concepts, agentmemory's immutable provenance and
bounded traversal, and Orloj's transactional cleanup discipline informed the
design; no reference source code was copied. The repository remains MIT
licensed. Kronaxis-fabric code was not copied because its BSL 1.1 license is
not compatible with the intended MIT distribution.

## Remaining limitations

- Historical evidence, tool, and agent payloads remain inline until their
  producers are migrated one at a time; model response evidence is the first
  integrated producer.
- Postgres is intentionally the only storage authority. There is no object
  store, resumable upload protocol, background garbage collector, cold tier,
  partitioning, encryption policy beyond database controls, or backup/restore
  drill.
- Raw verification currently reconstructs the complete artifact in bounded
  process memory. The 64 MiB cap keeps this predictable, but a future streaming
  verifier should be benchmarked before raising it.
- Retention is explicit store API state transition; scheduled retention
  sweeping and operational artifact metrics remain follow-up work.
