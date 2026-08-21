# Loop 19 completion — durable resumable repository ingestion

Status: implemented and locally qualified.

## Delivered

- Added typed repository ingestion contracts in
  `internal/contracts/ingest.go`: source policy, immutable file manifest,
  chunks, symbols, checkpoints, job lifecycle, batch requests, and bounded
  reports.
- Added `026_repository_ingestion.sql` plus the compatibility-only migration
  `027_ingestion_skipped_files.sql`. They preserve migration 025 and the
  existing `fornix.chunks`/`fornix.symbols` compatibility read models while
  adding append-only job/file/symbol/lineage history and a durable checkpoint.
- Added deterministic discovery in `internal/ingest`: canonical configured
  mount checks, normalized slash paths, stable ordering, default/custom ignore
  rules, regular-file and UTF-8 bounds, content hashes, mode/size manifest
  inputs, traversal protection, and fail-closed symlink handling.
- Added deterministic rune-window chunking and bounded language-aware symbol
  extraction for Go, Python, JavaScript, TypeScript, and Rust. Existing source
  rows are not overwritten as authoritative history; current chunks/symbols
  are compatibility indexes and ingest lineage retains the source relation.
- Added `IngestStore` with idempotent submission, manifest identity, previous
  snapshot supersession/removal records, bounded transactional batch writes,
  task-fence validation, source re-verification, chunk/symbol deduplication,
  append-only lifecycle events, checkpoint advancement, cancellation, and
  crash hooks for rollback tests.
- Embedding is opt-in. The offline/default path performs zero embedding work;
  enabled requests call the configured embedder only for bounded 768-dimension
  vectors and skip provider failures rather than making indexing depend on a
  model service.
- Added authenticated HTTP routes for dry-run, submit, list, status, resume,
  cancel, and bounded workspace-scoped job access. The source mount is loaded
  from the durable workspace record, not accepted from the caller as authority.
- Added matching deterministic CLI commands and MCP tools. The reference
  workflow now submits and resumes a durable ingest job before task claim,
  retrieval, agent execution, artifact/evidence production, and replay.
- Added unit and Postgres integration tests for deterministic discovery,
  symlink/traversal rejection, source mutation, duplicate submission,
  concurrent advancement, crash rollback, resume, cross-workspace reads, and
  chunk/symbol indexing. Added v0.29 smoke and CI coverage.

## Qualification evidence

- Existing database upgraded cleanly through migration 026 while the service
  remained ready.
- `go test ./...`, the focused ingestion Postgres tests, `make build`, and
  Python/MCP syntax checks passed in the pinned Docker Go toolchain.
- The durable ingestion smoke passed dry-run, submit, bounded resume, and
  duplicate submission checks against the running Docker service.
- The v0.27 reference workflow still passed after changing it from manual
  `/v1/chunks` writes to the durable ingest job path.
- In the local Postgres/Docker run, the two-file reference source produced one
  manifest identity, two indexed chunks, one symbol, and two committed
  batches when using batch size one. Repeating the job reused the identity and
  produced no additional authoritative ingest rows.
- A warm local HTTP dry-run over the two-file mounted fixture measured about
  1.0 ms p50 and 3.3 ms p95 across ten requests. This includes deterministic
  discovery and report compilation, not Go toolchain startup. The v0.29
  compiled-binary smoke completed in under three seconds including Docker
  process startup.
- After the local qualification runs, the shared development database held 41
  ingest jobs, 82 file snapshots, 24 symbol snapshots, and 72 lineage rows;
  relation sizes were approximately 216 KiB, 144 KiB, 64 KiB, and 128 KiB
  respectively. The existing content-addressed chunks relation held 71 rows
  in approximately 1.4 MiB. These are shared-dev measurements, not capacity
  projections for a large repository.
- Storage is proportional to the existing content-addressed chunks plus
  ingest metadata and lineage. Identical chunk content is reused within a
  workspace; prior snapshots remain auditable. No broker, object store,
  Redis, NATS, or model service is required for the offline path.

## Remaining limitations

- The batch API is operator-driven; a background ingest scheduler and automatic
  watcher-to-job handoff are not included yet. Callers must resume bounded
  batches explicitly.
- Source bytes are re-read from the configured mount rather than copied into
  Postgres. This bounds database storage, but the mount must remain available
  and unchanged until the job completes; a mutation fails closed and leaves
  prior committed batches intact.
- Symbol extraction is deliberately conservative regex-based extraction, not
  a full parser/tree-sitter index. It is deterministic and safe, but language
  coverage and symbol graph quality are limited.
- Embedding counters/provider health are intentionally conservative. Ollama or
  another embedder is not required; richer provider availability probing and
  cumulative token/cost accounting belong in the next retrieval/indexing
  quality step.
- The current derived `fornix.symbols` compatibility index soft-deletes the
  latest path view while `ingest_files`, `ingest_symbols`, and lineage preserve
  prior snapshots. A full historical symbol projection/rebuild API is still
  future work.
- Report artifacts are supported for oversized reports, but normal small
  reports remain inline. Query-count and p95 telemetry require the existing
  observability instrumentation to be expanded around ingestion transactions.

## Next task prompt

### Task 20 — Build Fornix’s deterministic ingestion scheduler and index quality gate

Before coding:

1. Read the chats directory end to end, `AGENTS.md`, docs 00, 14, 48, 49,
   50, and this completion note.
2. Study DeepSeek Harness context/corpus scheduling and cost boundaries;
   Orloj watcher/controller/retry/checkpoint lifecycle; agentmemory leases,
   cleanup, diagnostics, and resumable actions; ClawMem replay/source graph
   handling; and FornixDB retention, tiering, and disk-cost tests. Do not copy
   Kronaxis BSL-licensed source.
3. Compare the current ingest store, agent-run scheduler, task fences,
   retrieval planner, symbol/chunk compatibility indexes, observability
   ledger, and operator CLI.
4. Write a feature note covering scheduler ownership, fairness, retries,
   crash recovery, source mutation, embedding budgets, quality thresholds,
   workspace isolation, retention, licensing, SQL/storage/cost budgets, and
   acceptance tests.

Implement the smallest production-quality vertical slice:

- Add a Postgres-backed ingest queue with workspace-scoped worker leases and
  monotonic fencing, deterministic FIFO/fair selection, heartbeat, expiry,
  takeover, release, cancellation, and bounded retry/dead-letter behavior.
- Require the ingest worker fence for every batch/checkpoint mutation and make
  stale workers fail closed.
- Add resumable scheduled processing for queued/running jobs without a broker;
  preserve the current explicit CLI/API resume path.
- Add cumulative embedding token/byte/cost gates and provider-availability
  classification. Offline indexing must remain model-free.
- Add deterministic source-quality reports for skipped/binary/changed files,
  chunk deduplication, symbol coverage, stale-file counts, batch latency, SQL
  work, storage growth, and resume throughput.
- Add replay/rebuild checks proving incremental and clean indexing produce the
  same manifest, chunk, symbol, context, and report hashes.
- Add CI, Docker/smoke coverage, operator commands, architecture docs, and
  measured limitations. Keep Postgres as the only authority and introduce no
  broker, Redis, NATS, object store, LLM framework, or new infrastructure.

Acceptance criteria:

- One active ingest worker owner exists per workspace/job.
- Stale fences cannot advance checkpoints or derived indexes.
- Expired jobs are reclaimed deterministically and bounded retries dead-letter
  without losing authoritative history.
- Duplicate delivery and crash recovery create no duplicate chunks, symbols,
  artifacts, events, or evidence.
- Incremental and rebuild runs have identical stable hashes.
- Embedding work never exceeds cumulative configured budgets and is skipped
  when providers are unavailable or early deterministic stages satisfy need.
- Changed and removed files remain auditable and workspace/RBAC boundaries fail
  closed.
- Existing tests, race checks, builds, CI, smokes, and the full reference
  workflow remain green.
