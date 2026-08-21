# Durable repository ingestion and indexing foundation

Status: active implementation note for Task 19.

## Objective

Task 19 replaces the Task 18 fixture-only indexing path with a bounded,
workspace-scoped ingestion job. The job discovers an explicitly mounted local
repository, records an immutable manifest snapshot, indexes deterministic text
chunks and optional derived symbols in transactional batches, and resumes from
Postgres after interruption. Existing `fornix.chunks` and `fornix.symbols`
remain the retrieval indexes; this feature does not create a second chunk
database or make embeddings authoritative.

The path is:

```text
authenticated source admission → canonical root validation → deterministic
discovery/manifest → append-only file snapshot → bounded checkpoint →
transactional chunk/symbol index batch → supersession/stale projection →
report/evidence references → retrieval
```

## Research and reuse decisions

- Orloj contributes explicit bounded chunk windows, resource/watch lifecycle,
  Postgres-backed persistence, and controller-style failure boundaries. Its
  watcher stream is not copied because Fornix's source of truth is a durable
  job/checkpoint rather than an in-memory event stream.
- DeepSeek Harness contributes root canonicalization, plain-argv filesystem
  boundaries, output limits, stable path handling, and crash-tested checkpoint
  discipline. Fornix independently implements the local discovery contract in
  Go and keeps Postgres as the authority.
- agentmemory contributes resumable checkpoint/action semantics, idempotent
  deduplication, diagnostics, and bounded maintenance patterns. Its mutable KV
  state is replaced with transactional Postgres rows and append-only source
  snapshots.
- ClawMem contributes deterministic source/chunk normalization, watcher
  change detection, source/graph lineage, and replay-oriented history. LLM
  extraction and graph mutation are intentionally outside this slice.
- FornixDB contributes immutable raw/detail/gist separation, retention-aware
  source identity, and storage-cost measurement. Ingestion stores raw source
  bytes only in the existing chunk plane and keeps reports bounded.
- No reference source is copied. DeepSeek Harness and ClawMem/FornixDB are MIT;
  Orloj and agentmemory are Apache-2.0. Kronaxis Fabric is BSL 1.1 and is not
  reused. Fornix remains MIT.

## Invariants

1. A job belongs to exactly one authenticated workspace, repository identity,
   source root, actor, request, and idempotency key.
2. A source root must be absolute, exist, resolve to a directory, and be within
   the workspace's explicitly configured `tool_root`. Relative roots, path
   traversal, and symlink entries are rejected or skipped conservatively;
   symlink escapes never enter the manifest.
3. Discovery is deterministic: normalized slash paths, stable ignore rules,
   regular-file checks, bounded size/type policy, sorted paths, raw-byte SHA-256
   hashes, and mode bits produce one stable manifest hash.
4. The manifest and file snapshot are append-only job history. A changed path
   creates a new source record with a supersession reference; a removed path
   creates an auditable removed record. Prior chunks, symbols, evidence, and
   hashes are never overwritten.
5. A checkpoint advances only in the same Postgres transaction as its complete
   file batch, derived index updates, lineage rows, and batch event. A crash
   before commit changes none of them; a crash after commit is safe to replay.
6. Replaying a committed batch is idempotent by workspace/content hash and job
   file identity. Checkpoints never move backwards.
7. Task-bound jobs require the current task owner and fence while each batch
   transaction holds the task lease row. Stale workers fail closed.
8. Existing chunks are the retrieval index and are reused through a typed
   service boundary. Symbols are rebuildable derived indexes; source/history
   records carry the durable audit trail.
9. Chunk ranges, symbol identities, report hashes, and ordering are stable for
   identical source bytes and configuration. No model call is needed for the
   default path.
10. Embeddings are disabled by default. They run only when explicitly enabled,
    an embedding provider is available, the chunk and byte budgets allow them,
    and the job has measurable reason to enrich the index. Embedding failure
    never prevents the deterministic text index from committing.
11. Reports contain hashes, counts, bounded errors, timings, and storage/cost
    measurements, not raw prompts, credentials, or unbounded repository text.
12. All status, resume, cancel, report, and disclosure operations enforce
    authenticated actor/workspace authorization and bounded pagination.

## Schema changes

Migration `026_repository_ingestion.sql` adds:

- `ingest_jobs`: workspace-scoped immutable source identity plus mutable
  lifecycle counters and task-fence admission metadata;
- `ingest_checkpoints`: one monotonic ordinal/counter/hash cursor per job;
- `ingest_files`: append-only manifest snapshots with present/removed state,
  content/mode hashes, supersession links, and derived index counters;
- `ingest_symbols`: append-only symbol snapshots for audit and deterministic
  rebuild comparison;
- `ingest_lineage`: source-to-chunk/symbol/artifact/evidence references;
- indexes and checks enforcing workspace, status, hash, counter, and active-job
  invariants.

The migration preserves `repository_ingests`, `fornix.chunks`, and
`fornix.symbols`. Existing Task 18 records remain readable; new durable jobs
may mirror a bounded compatibility row in `repository_ingests` only after
their authoritative job transaction commits.

## Checkpoint and crash semantics

Submission discovers and inserts the immutable manifest snapshot in one
transaction. Processing selects the next ordinal range from the checkpoint,
locks the job/checkpoint, validates the task fence when present, and writes:

- chunk rows through the shared chunk upsert boundary;
- symbol rows plus append-only symbol snapshots;
- source lineage rows;
- supersession/stale derived-index updates;
- the next checkpoint and counters;
- a typed batch event and evidence.

All writes commit together. A pre-commit failure rolls back the batch. A
post-commit retry sees the advanced checkpoint and does not repeat work. A
worker may be replaced after expiry; the new worker uses the same job fence
requirements and cannot use the old task fence.

## API/CLI/MCP contract

- `POST /v1/operator/ingest/jobs`: discover and submit a bounded job;
- `GET /v1/operator/ingest/jobs/{id}`: read job, checkpoint, and report
  metadata;
- `POST /v1/operator/ingest/jobs/{id}/resume`: process a bounded batch;
- `POST /v1/operator/ingest/jobs/{id}/cancel`: durably cancel future batches;
- `POST /v1/operator/ingest/dry-run`: discover and report without mutation;
- `GET /v1/operator/ingest/jobs`: bounded deterministic pagination;
- CLI `fornix ingest submit|status|resume|cancel|dry-run` and equivalent MCP
  calls.

The existing `/v1/chunks` endpoint remains compatible. The reference workflow
uses the durable job path and consumes its report rather than posting fixture
chunks directly.

## Cost and storage budget

Default limits are deliberately conservative: 10,000 files per job, 64 MiB
per file, 512 MiB total source bytes, 100 files per transaction, 4 KiB chunk
overlap, and no embeddings. Report and error payloads are bounded. Each batch
uses one transaction plus bounded queries; embedding calls are zero by default
and at most one per indexed chunk when explicitly enabled within the job
budget. Metrics report discovery time, batch latency, SQL statement count,
source bytes, indexed bytes, deduplication, symbol count, and resume rate.

## Acceptance tests

- fresh and existing databases apply migration 026 cleanly;
- identical manifests deduplicate by workspace/repository/idempotency and
  conflicting identities fail closed;
- paths, `..` traversal, absolute escapes, symlink directories, and symlink
  files fail closed or are conservatively omitted;
- ignore rules, mode bits, normalized paths, chunk ranges, symbols, manifest,
  and report hashes are stable;
- a crash before batch commit leaves checkpoint and indexes unchanged;
- a crash after commit resumes without duplicate chunks, symbols, lineage, or
  events;
- concurrent submissions/workers preserve one active job and one checkpoint;
- stale task fences cannot process or finalize a job;
- changed and removed files remain auditable and old hashes remain readable;
- embeddings are skipped when disabled, over budget, or unavailable;
- cross-workspace status/disclosure/resume/cancel operations fail closed;
- the reference workflow uses a durable ingest job and remains deterministic;
- tests, race checks, builds, CI, all prior smokes, and replay remain green.
