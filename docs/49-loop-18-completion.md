# Loop 18 completion — operator control and reference workflow

Status: implemented and qualified.

This historical Loop 18 note is superseded for repository ingestion by
`docs/50-repository-ingestion-foundation.md` and
`docs/51-loop-19-completion.md`. The reference workflow portion remains the
operator/API/CLI qualification record; its fixture indexing path now delegates
to the durable ingest job described in Loop 19.

## Delivered

- Migration `025_operator_reference_workflow.sql` adds workspace metadata,
  bounded repository-ingest records, and append-only operator workflow audit.
- Typed workspace/bootstrap/ingest/audit contracts and a transactional
  `OperatorStore` were added under `internal/contracts` and `internal/store`.
- Workspace mode now has an explicit `FORNIX_BOOTSTRAP_KEY` route. API keys
  remain hashed at rest; generated tokens are returned only by explicit
  create/rotate/bootstrap responses and are never part of events or reports.
- The authenticated operator API covers workspace bootstrap/inspection,
  identity disable/list/create, role bind/list/unbind, API-key
  list/create/rotate/revoke, and bounded ingest registration/status.
- `cmd/fornix` retains server mode and adds deterministic JSON operator commands
  for health, workspace, identity, role, API-key, ingest, task, run, retrieval,
  evaluation, metrics, artifact, evidence, and reference-workflow operations.
- The MCP shim adds workspace propagation and bounded task, retrieval, run,
  replay, artifact-disclosure, and evidence-disclosure tools. It remains an
  HTTP compatibility client rather than a second business-logic path.
- The fake provider has one deterministic reference-workflow tool turn and
  stops requesting tools after a tool result. The registered repository tool
  is `/bin/cat`, structured-argv only, no shell, no network, bounded output,
  and workspace-root constrained.
- Docker mounts the source at `/workspace`, includes a small fixture repository,
  and documents `make smoke-reference-workflow`. The optional OpenAI smoke is
  skipped without `FORNIX_OPENAI_API_KEY` and never prints the key.
- Read-only retrieval snapshot release was corrected to rollback rather than
  commit after optional-stage failures, preserving deterministic degraded
  traces instead of returning `ErrTxCommitRollback`.

## Qualification evidence

- `make test`: passed with Postgres integration tests when the Docker app worker
  was disabled to prevent it claiming test workspaces.
- A fresh isolated PostgreSQL database applied migration `025` and reached
  readiness successfully; the temporary database was then dropped.
- `make build`: passed for `fornix`, watcher, and offline evaluator.
- `make vet`: passed.
- CI-equivalent `go test -race ./...`: passed.
- `make python-check`: passed.
- Broad `make smoke`: v0.10 through v0.26 passed; v0.27 passed after the
  development/workspace-key distinction was fixed. The v0.28 OpenAI smoke
  skipped cleanly because no key was present.
- Workspace-mode reference workflow passed end to end: bootstrap, fixture
  chunk ingestion, task creation/claim, retrieval, fake model, fenced read-only
  tool, task completion, report artifact, linked evidence, and replay.
- The tightened v0.27 smoke requires `state == succeeded` and passed under
  workspace-scoped authentication with the configured bootstrap key.
- Cross-workspace operator path returned HTTP 403.
- Compiled reference workflow wall time was approximately 0.29 seconds on the
  local Docker/Postgres setup after images were warm. The first `go run` smoke
  includes toolchain download and is not a service latency measurement.
- Existing measured baselines from the qualification smokes: projection p50
  about 10.9 ms / replay throughput about 1,341 events/s; task claim+complete
  p50 about 5.5 ms; retrieval p50 about 1.77 ms with 3 SQL queries; consumer
  lease p50 about 0.86 ms; artifact create+reference p50 about 2.11 ms.
- Current local relation sizes include roughly 33 MB control events, 9.4 MB
  artifact chunks, 25.2 MB evidence, 1.5 MB artifacts, and 0.64 MB retrieval
  surfaces. These are shared development-database sizes, not per-workflow
  projections.

## Remaining limitations

- Repository ingestion is a bounded CLI fixture/index path, not yet a durable
  resumable repository-ingest job with complete tree-sitter symbol extraction,
  deletion detection, checkpoints, and large-repository backpressure.
- The first workflow creates a fresh task per invocation; task creation does not
  yet have a typed idempotency key, so operators should use a stable external
  workflow identity until that boundary is added.
- The operator CLI is intentionally JSON-first and does not yet provide a TUI,
  interactive approval prompt, shell completion, or a general config file.
- The native MCP server remains a Python stdio shim backed by HTTP. It has
  workspace propagation but does not yet expose every administrative lifecycle
  operation.
- Local process execution is bounded and shell-free but is not a kernel-level
  sandbox. Remote model execution remains at-least-once at the provider boundary.
- Bootstrap is a single configured secret, not OAuth/SSO or KMS-backed secret
  management. Postgres remains the only authority and a single-node deployment.
- The Docker reference path assumes the application can see the repository at
  `/workspace`; arbitrary host paths require an explicit volume and workdir
  mapping.

## Next task prompt

### Task 19 — Build Fornix’s durable resumable repository ingestion and indexing substrate

Before coding:

1. Read the chats directory end to end.
2. Read `AGENTS.md`, `docs/00-fornix-foundation.md`,
   `docs/14-production-readiness-qualification.md`,
   `docs/48-operator-reference-workflow-foundation.md`, and this completion
   note.
3. Study the local reference repositories before implementation:
   - DeepSeek Harness repository/context ingestion, file discovery, ignore
     rules, context assembly, and cost-boundary patterns.
   - Orloj file watchers, repository/resource indexing, task/controller
     lifecycle, artifact/evidence capture, and failure recovery.
   - agentmemory loaders, cleanup/checkpoint patterns, diagnostics, and
     resumable action lifecycle.
   - ClawMem ingestion, chunk normalization, graph/source linking, and replay.
   - FornixDB ingestion, gist/detail/raw records, deduplication, retention, and
     disk-cost measurement.
4. Compare the current `repository_ingests`, `/v1/chunks`, symbol endpoints,
   retrieval planner, artifact/evidence stores, task scheduler, operator CLI,
   and watcher. Do not copy BSL-licensed Kronaxis code.
5. Write a feature note before implementation covering source identity,
   manifest semantics, path/ignore safety, chunk/symbol determinism,
   checkpoint/restart semantics, deletion/supersession, workspace isolation,
   task fencing, idempotency, embedding cost gates, storage budget, licensing,
   and acceptance tests.

Implement the smallest production-quality vertical slice:

- Add typed `RepositorySource`, `IngestJob`, `IngestCheckpoint`,
  `IngestFile`, `IngestChunk`, `IngestSymbol`, and `IngestReport` contracts.
- Add a migration for append-only ingest jobs, file manifests, bounded
  checkpoints, and source-to-artifact/evidence lineage. Preserve migration 025
  compatibility and existing indexed rows.
- Add a durable workspace-scoped ingest job API and CLI command that accepts a
  local repository root only through an explicit configured mount/reference.
- Discover files deterministically with normalized slash paths, size/type
  bounds, explicit ignore rules, symlink escape rejection, and stable ordering.
- Compute a manifest hash from path, size, mode, and content hash; make
  duplicate submissions idempotent and conflicting identities fail closed.
- Process files in bounded batches with a durable checkpoint. A crash before a
  batch commit leaves no partial authoritative batch; a crash after commit is
  resumable without duplicate chunks, symbols, artifacts, or evidence.
- Reuse the existing `/v1/chunks` storage semantics through a typed service
  layer; do not create a second chunk database. Add deterministic chunk ranges,
  content hashes, source references, and optional symbol extraction.
- Make embeddings explicitly gated by provider availability, budget, and
  measured need. The default fake/offline path must not require Ollama or an
  LLM.
- Detect removed/changed files without overwriting history. Mark stale source
  records/symbols or append supersession metadata while preserving prior hashes
  and provenance.
- Require authenticated workspace authorization and task fencing for task-bound
  ingest runs. Propagate actor, request, idempotency, causation, and correlation
  identities.
- Integrate ingest into the reference workflow so the workflow consumes a
  durable ingest job/report rather than manually posting fixture chunks.
- Add bounded status, dry-run, resume, cancel, and report disclosure through
  HTTP, CLI, and MCP with deterministic cursors and no raw prompt/credential
  leakage.
- Add concurrency, duplicate submission, path traversal, symlink escape,
  ignore-rule, workspace-isolation, crash-before/after-checkpoint, resume,
  deletion/supersession, embedding-gate, storage-budget, and replay tests.
- Add CI, Make commands, Docker/smoke coverage, architecture documentation,
  measured discovery/chunk/index latency, SQL work, bytes per source file,
  deduplication ratio, replay/resume throughput, and remaining limitations.

Acceptance criteria:

- Fresh and existing databases migrate cleanly.
- Identical source manifests produce one durable ingest identity per workspace.
- Path and symlink escapes fail closed.
- Repeated or resumed jobs produce identical manifest, chunk, symbol, context,
  and report hashes.
- A crash before checkpoint commit leaves the prior checkpoint and indexed
  state unchanged; a crash after commit resumes safely.
- Removed or changed files remain auditable and do not overwrite authoritative
  history.
- Workspace and RBAC boundaries fail closed.
- Embedding/expensive work is skipped when the deterministic budget is already
  satisfied or the provider is unavailable.
- Existing tests, race checks, builds, CI, all prior smokes, and the complete
  reference workflow remain green.
