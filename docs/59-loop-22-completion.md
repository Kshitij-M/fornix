# Loop 22 completion — post-change validation and re-index handoff

Status: implemented on `feat/post-change-validation`; ready for CI and
maintainer review before merge into protected `main`.

## Outcome

Fornix now has a deterministic proof boundary after an approved repository
change. The path is:

```text
applied change
  → authenticated validation request
  → registered read-only validators
  → immutable result/evidence history
  → atomic report, handoff, event, receipt, and accounting commit
  → new ingestion identity
```

The implementation keeps Postgres as the control-plane authority. The local
repository is observed but never mutated by validation. Ingestion remains the
authority for indexing and receives a new idempotent source identity rather
than an in-place update.

## Delivered

- Added versioned contracts in `internal/contracts/validation.go` for
  requests, plans, budgets, validator references, results, failures, reports,
  durable runs, replay, disclosure, and re-index handoffs. Hashes exclude
  delivery IDs and volatile database timestamps while retaining workspace,
  change, source, validator, and budget identity.
- Added migration `030_post_change_validation.sql` with workspace-scoped
  validation runs, immutable result/attempt rows, append-only transitions,
  validation artifact links, and durable re-index handoffs. Existing schema
  migration checksums remain additive and compatible with fresh and already
  migrated databases.
- Added the explicit validator registry in `internal/validation`. The built-in
  set checks source/change authority, affected-file and byte budgets, path and
  symlink safety, observed result-tree equality, and bounded re-index discovery.
  Validators cannot invoke a shell, model, tool, broker, or external service.
- Added `ObserveAppliedPacketState` to the change planner so validation checks
  the post-application state without incorrectly re-running pre-application
  replace/delete preconditions against the new content.
- Added `ValidationStore` transactional APIs for admission, terminal commit,
  cancellation, result pagination, replay, disclosure, handoff lookup,
  handoff submission, and handoff failure. Authority checks resolve the
  applied change and proposal from Postgres; caller-supplied packet content is
  not trusted.
- Final validation commit stages results, canonical evidence, oversized report
  artifacts, validation artifact links, handoff state, transition history,
  typed events, observability/cost records, and a verified validation Work
  Receipt in one Postgres transaction. A new `WorkReceiptStore.FinalizeTx`
  boundary allows this composition without a nested commit.
- Added deterministic replay and hash-preserving disclosure. Replay reads
  stored result/event history only and never reads the live repository,
  creates artifacts, submits ingestion, or performs another external effect.
- Added authenticated HTTP routes for create, status, results, replay,
  resume, cancel, disclosure, and handoff inspection/submission. Added CLI
  commands under `fornix validation` and matching MCP tools, including resume.
  All routes reuse workspace RBAC and authenticated actor propagation.
- Added v0.32 smoke coverage for workspace bootstrap, approval-gated change
  application, validation idempotency, result ordering, replay, bounded
  disclosure, and submitted ingestion handoff.
- Added contract, validator, Postgres integration, concurrency, crash,
  dry-run, report-artifact, stale-fence, receipt, workspace-isolation, HTTP
  authorization, shell syntax, Python syntax, documentation, CI, and Make
  coverage.

## Correctness guarantees

### Authority and identity

The validation request must point to one applied change application and exact
proposal, packet, and expected-tree hashes. Postgres verifies that relationship
inside the admission transaction. The plan stores the sorted validator IDs and
versions, and result commits reject any result whose ordinal or validator
identity differs from that durable plan.

### Idempotency and concurrency

Validation identity is unique by `(workspace_id, idempotency_key)`. Concurrent
submissions resolve to one run. Result identity is deterministic by run and
ordinal; terminal duplicate delivery returns the committed run and never
creates another result, evidence record, handoff, observation, cost entry, or
receipt. Different payloads using an existing identity fail closed.

### Fencing and crash recovery

Task-bound validation carries the task owner and monotonic fence. Admission and
terminal commit both require the current live lease. A stale owner cannot
write results, evidence, artifacts, handoffs, events, accounting, or receipts.

The authoritative transaction is:

```text
lock and verify run/change/task authority
  → persist result and evidence rows
  → persist report/artifact/link and handoff
  → update run and append transition/event/accounting/receipt
  → commit
```

A forced failure before commit leaves the run pending with no partial result,
evidence, artifact reference, handoff, event, accounting, or receipt. A crash
after commit is safe to retry and replay because every durable identity is
deterministic and terminal state is immutable.

### Re-index handoff

A passed validation creates `reindex-<validation-run-id>` with the observed
tree hash and a newly discovered manifest identity. The server submits that
handoff to the existing ingestion authority using the handoff idempotency key.
Validation and ingestion are separate boundaries: if submission is unavailable
after the validation commit, the handoff is marked failed and can be submitted
again without re-validating or overwriting prior source history.

## Operator surface

With a running service and an authorized workspace key:

```sh
bin/fornix --workspace reference-local validation run \
  --repository reference-repo \
  --source-root /workspace/fixtures/reference-repo \
  --application-id <application-id> --proposal-id <proposal-id> \
  --packet-hash <packet-hash> --expected-tree-hash <expected-tree-hash>
bin/fornix --workspace reference-local validation status --id <validation-run-id>
bin/fornix --workspace reference-local validation results --id <validation-run-id>
bin/fornix --workspace reference-local validation replay --id <validation-run-id>
bin/fornix --workspace reference-local validation disclose --id <validation-run-id> --level detail
make smoke-validation
```

The full route and permission table is in
[`53-http-api-reference.md`](53-http-api-reference.md). The design invariants
are in [`58-validation-foundation.md`](58-validation-foundation.md).

## Qualification evidence

The focused local Postgres suite passed against the development database:

```sh
FORNIX_TEST_PG_DSN='<local-postgres-dsn>' go test ./internal/store -run 'TestValidation|TestWorkReceipt' -count=1
go test ./internal/contracts ./internal/validation ./internal/server -count=1
python3 -m py_compile scripts/fornix-mcp.py
sh -n scripts/test/v0.32-validation-smokes.sh
```

The v0.32 authenticated HTTP smoke passed with the compiled CLI and service.
It verified that an automatic approved change can be applied, validated,
replayed, disclosed within 32 KiB, and handed to ingestion with a durable job
identity. The same smoke was run twice while developing the route and handoff
path; the final run completed all assertions.

The complete local smoke matrix also passed from the baseline through v0.32:
events, projections, consumer leases, tasks, retrieval, provenance, model,
tools, agent runs, scheduling, identity, artifacts, artifact-backed outputs,
observability, retrieval quality, retrieval evaluation, the reference workflow,
resumable ingestion, work receipts, repository changes, and post-change
validation. The opt-in OpenAI smoke remains skipped when
`FORNIX_OPENAI_API_KEY` is absent. Docker-backed smoke fallbacks now translate
host loopback URLs and checkout paths into container-safe values, so the same
Make target is portable across Docker Desktop and Linux CI.

The Postgres integration cases cover:

- one durable run/result/evidence/handoff/receipt effect under duplicate and
  concurrent delivery;
- replay returning the same result hash, replay hash, and two validation
  lifecycle events with a filterable run identity;
- crash-before-commit rollback followed by resume with one result set;
- dry-run producing no validation rows;
- oversized reports using one content-addressed artifact and one validation
  artifact link;
- stale task-fence rejection before any terminal write; and
- cross-workspace reads failing closed.

## Measurements and cost boundary

These are local regression measurements, not production SLOs. On the warm
development Postgres 17 container, the focused validation/receipt integration
cases completed in about one second for the four primary scenarios. The
compiled v0.32 HTTP smoke completed in under one second after the service was
already ready. Rebuild and container startup are excluded from both figures.

The default five-validator run reads only the bounded affected tree and writes
five result/evidence pairs, one run, one transition pair, one handoff, one
completion event, one observation, one cost entry, and one validation receipt.
The normal report remains inline. When the report exceeds its configured byte
budget, the raw JSON is stored once in ArtifactStore and only the hash/reference
is retained inline; the artifact-link and receipt reference add no second copy
of the bytes.

Validator `sql_queries` is zero because the built-in validators operate on the
already bounded filesystem observation. The validation store performs a bounded
set of indexed Postgres reads/writes for authority, results, evidence,
handoff, receipt, event, and accounting. Per-statement SQL telemetry is not yet
exposed as a metric; this is an explicit measurement limitation rather than a
claim of zero database work.

Replay is proportional to the recorded result/event page and has no filesystem,
model, tool, or ingestion cost. The default path performs no embedding or LLM
work. Re-index submission creates a new ingest identity and therefore incurs
the existing ingestion cost only when the operator/server submits the handoff.

## Reuse and licensing

The implementation reuses Fornix’s change authority, task fences, EventStore,
EvidenceStore, ArtifactStore, WorkReceiptStore, IngestStore, observability
ledger, identity middleware, and CLI/HTTP/MCP conventions. Orloj, DeepSeek
Harness, ClawMem, agentmemory, and FornixDB informed independent design
choices around bounded checks, lifecycle, evidence, and replay. No reference
source was copied. Kronaxis Fabric source remains excluded because its
repository is BSL 1.1. Fornix remains MIT-licensed.

## Remaining limitations

- The initial validators are structural and deterministic; they do not run
  arbitrary test commands, compilers, linters, remote CI, or a sandbox.
- The live repository observation and Postgres commit cannot be one physical
  transaction. A repository may change immediately after observation; the
  recorded result describes the observed boundary and the handoff remains
  explicit.
- Handoff submission is at-least-once across the service-to-ingestion call.
  IngestStore idempotency prevents duplicate durable jobs, but the call itself
  is not a distributed transaction.
- The validation API currently exposes bounded lookup by run/handoff identity;
  a general cursor-based validation-run list and background validation worker
  are deferred.
- Per-statement database-work telemetry, large-repository capacity curves,
  backup/restore drills, PostgreSQL RLS hardening, HA qualification, and
  external secret-manager operations remain production-readiness work.

## Review gate

Before merge, CI and an additional maintainer review should verify migration
compatibility, receipt transaction composition, replay read-only behavior,
workspace/RBAC fail-closed behavior, task fencing, symlink/path safety, report
artifact links, the portable Docker smoke fallback, and the at-least-once
ingestion boundary. Final local qualification passed `make fmt-check`, full
Postgres-backed `make test`, `make test-race`, `make vet`, `make build`,
`make python-check`, `make docs-check`, `make hooks-check`, and
`git diff --check`. The five validation integration scenarios were also run
against a newly created database, confirming that the complete migration set
and migration 030 apply cleanly from an empty catalog; the temporary database
was removed after the check.

## Next task prompt

### Task 23 — Build Fornix’s operator-facing verified change workflow and repository validation policy packs

Before coding, read the chats directory, `AGENTS.md`, the latest architecture
and completion notes through this document, and study the change, validation,
ingestion, receipt, RBAC, artifact, observability, CLI, HTTP, and MCP paths.
Use the reference reuse matrix to study policy-pack and operator-workflow
patterns without copying Kronaxis BSL-licensed source.

Write a feature note covering policy-pack identity, validator admission,
approval scope, task fencing, repository trust boundaries, deterministic
configuration, rollout/rollback, auditability, cost budgets, and acceptance
tests. Then add the smallest production-quality policy-pack surface that lets
an authorized workspace select an explicit named validation policy, records
the policy hash in the change/validation/receipt provenance chain, and keeps
the default structural policy unchanged. Preserve all current replay,
idempotency, workspace, artifact, and crash guarantees. Add tests, CLI/HTTP/MCP
coverage, CI/smoke commands, public documentation, measurements, and explicit
limitations. Do not add a broker, new database, LLM framework, or external
service.
