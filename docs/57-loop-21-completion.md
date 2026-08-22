# Loop 21 completion — approval-gated repository change artifacts

Status: implemented on `feat/repository-change-artifacts`; ready for CI and
maintainer review before merge into protected `main`.

## Delivered

- Added typed repository-change contracts in
  `internal/contracts/change.go`: source snapshots, structured operations,
  budgets, proposals, approvals, applications, conflicts, recovery failures,
  and bounded disclosure results. Stable request and packet hashes exclude
  delivery IDs, timestamps, raw prompts, and credentials while retaining
  source, operation, policy, and expected-tree identities.
- Added migration `029_repository_changes.sql` with workspace-scoped proposal,
  operation, approval, application, transition, and artifact-link history.
  Operations, approvals, transitions, and artifact links are append-only;
  current status is an operational projection over the preserved history.
- Added `RepositoryChangeStore` with transactional idempotent proposal,
  approval, application admission, terminal verification, task-fence checks,
  typed event append, content/diff artifact linking, and bounded disclosure.
  Proposal content artifacts and authoritative proposal rows commit together.
- Added deterministic planning and structured filesystem execution in
  `internal/change`. Supported operations are create, replace, delete, rename,
  and chmod. The executor accepts no shell string, does not invoke a shell,
  rejects traversal and symlink escapes, rechecks source preconditions, uses
  bounded artifact reads, and verifies the resulting tree hash.
- Added explicit external-effect semantics. A filesystem operation is
  external to Postgres and therefore at-least-once: a partial effect becomes
  `recovery_required`, a source mismatch becomes `conflicted`, and no code path
  claims exactly-once mutation. Dry runs perform no durable mutation and no
  filesystem write.
- Integrated task ownership/fencing at proposal and application boundaries.
  Stale task workers fail closed before admission or finalization. Successful
  applications create the existing verified Work Receipt with source,
  operation-artifact, proposal, application, result-tree, actor, replay, and
  external-boundary references.
- Added authenticated HTTP routes, deterministic CLI commands, and MCP tools
  for dry-run, propose, get, approve, apply, and bounded disclosure. All paths
  reuse the same service/store authority and workspace/RBAC checks.
- Added unit, Postgres integration, concurrency, duplicate-delivery,
  artifact-link, stale-fence, dry-run, crash-rollback, disclosure, and
  end-to-end Docker smoke coverage. The new v0.31 smoke verifies workspace
  bootstrap, proposal idempotency, exact approval, application, Work Receipt
  verification, disclosure, and duplicate application replay.
- Repaired an existing model-call concurrency defect found by the full-suite
  run. `ModelCallStore.Start` now resolves conflicts from either of its
  workspace-scoped unique identities—request ID or idempotency key—without
  leaking a raw unique-constraint error.

## Qualification evidence

The following checks passed in the pinned Docker Go toolchain against the local
Postgres 17 development database:

- `go test ./... -count=1` passed across all packages and all database-backed
  tests.
- `go test -race ./... -count=1`, `go vet ./...`, and `go build ./...` passed.
- The model-call concurrent deduplication regression ran 20 times, and the
  request-ID conflict/replay test ran 10 times without a failure.
- The repository-change integration tests ran five times, covering concurrent
  proposal idempotency, artifact-backed content, approval packet mismatch,
  dry-run non-mutation, application replay, crash rollback, and stale task
  fencing.
- The live Docker v0.31 change smoke passed against the current source tree:
  proposal idempotency, approval, structured application, verified Work
  Receipt, disclosure, and duplicate application all returned the same
  durable identities. The first smoke attempt also verified that a post-write
  source conflict is rejected; the scenario was then corrected to test
  proposal idempotency before the create operation is applied.
- Python/MCP syntax and shell syntax checks passed for the changed scripts.

## Local cost and storage observations

This is a local qualification measurement, not a production capacity claim.
One single-file create proposal produced one proposal row, one operation row,
one content artifact reference, one bounded diff artifact reference, one
approval, one application, typed lifecycle events, and one Work Receipt. The
new change tables store hashes, bounded metadata, and references; raw file
bytes are stored once in the existing content-addressed artifact authority.
The proposal transaction performs bounded artifact deduplication, operation
and link inserts, transition/event writes, and one materialized read before
commit. Application performs one admission transaction and one finalization
transaction around the unavoidable external filesystem boundary; it performs
no model, embedding, broker, or shell work.

The focused Postgres change suite completed in roughly 1.3 seconds for five
repetitions on the warm local Docker/Postgres setup. The v0.31 compiled-binary
smoke completed successfully after Docker build/startup and exercised the
full HTTP/CLI path. These timings are useful regression signals, not latency
SLOs. A production benchmark still needs repository-size distributions,
concurrent workspace load, filesystem latency, WAL growth, and long-running
recovery measurements.

## Remaining limitations

- This slice writes only to an explicitly configured local filesystem mount.
  It does not create git commits, branches, pushes, pull requests, deploys, or
  remote changes, and it does not automatically re-index the resulting tree.
- Postgres cannot atomically commit with a local filesystem rename. The
  `applying` and `recovery_required` states are the deliberate reconciliation
  boundary; operators must inspect ambiguous partial effects before retrying.
- The executor is a structured file-operation boundary, not a kernel sandbox.
  Mount configuration, operating-system permissions, and host filesystem
  behavior remain deployment responsibilities.
- Approval is durable and exact-packet-bound, but there is no reviewer UI,
  cryptographic signature/notarization, or external policy engine yet.
- Change payloads are bounded by operation/file/total budgets. Creating nested
  directories implicitly is rejected; callers must prepare the configured
  mount or introduce an explicit, separately reviewed directory operation.
- The source snapshot and result tree are hashes and metadata; the repository
  itself remains the authoritative external content. Historical proposal and
  artifact references are preserved, but automatic post-application ingestion
  is deferred.
- The repository still needs a production deployment guide, HA/backup
  qualification, external secret-manager integration, RLS hardening, and
  capacity testing before it should be used for sensitive large-scale writes.

## Review gate

Push this branch as a draft PR. CI and an additional maintainer review are
required before merging to protected `main`. Review should specifically check
migration compatibility, workspace and task-fence fail-closed behavior,
approval packet binding, raw-byte redaction, crash/recovery classification,
and the external filesystem boundary.

## Next task prompt

### Task 22 — Build Fornix’s deterministic post-change validation and re-index handoff

Before coding, read the chats directory, `AGENTS.md`, docs 00, 14, 48, 49,
50, 54, 56, this completion note, and the current change, Work Receipt, ingest,
artifact, task, scheduler, and observability implementations. Study the
reference repositories’ validation, cleanup, checkpoint, repository-analysis,
and evidence patterns without copying Kronaxis BSL-licensed source.

Write a feature note covering validator admission, task fencing, validation
budgets, changed-tree identity, crash recovery, approval/result provenance,
re-index handoff, stale-source handling, workspace isolation, authorization,
storage/cost impact, and acceptance tests.

Implement the smallest production-quality vertical slice:

- Add typed validation requests, validation plans, check results, validation
  reports, and re-index handoff contracts.
- Allow only registered, structured, bounded read-only repository validators;
  never invoke an implicit shell or mutate the repository.
- Require the applied change’s packet hash, result tree hash, workspace, actor,
  and task fence to match the validation request.
- Persist validator runs, bounded stdout/stderr artifacts, result hashes,
  status transitions, and Work Receipt references transactionally.
- Make duplicate validation requests idempotent and stale workers fail closed.
- Add deterministic changed-tree ingestion handoff that creates a new
  authoritative ingest identity without overwriting the previous source.
- Support dry-run, bounded batches, cancellation, crash recovery, replay, and
  explicit abstention when a validator exceeds its budget or cannot establish
  a trustworthy result.
- Add CLI, HTTP, and MCP inspection paths, CI/smokes, race/crash/concurrency
  tests, architecture documentation, and measured latency, SQL, storage,
  validation throughput, and remaining limitations.

Keep Postgres as the only authority and introduce no broker, Redis, NATS,
object store, LLM framework, or new infrastructure.
