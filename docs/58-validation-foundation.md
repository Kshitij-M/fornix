# Deterministic post-change validation and re-index handoff foundation

Status: design and implementation note for Task 22.

Audience: Fornix contributors, operators, reviewers, and adopters.

## Product problem

Fornix can now ingest a repository, construct bounded context, execute a
controlled model/tool run, create a Work Receipt, and apply an approved
structured change. Applying a change is not proof that the repository is
correct. A trustworthy repository operation also needs a deterministic answer
to four questions:

1. Did the applied filesystem state match the approved packet?
2. Did the resulting repository remain valid under the configured checks?
3. What evidence supports that conclusion?
4. How does the resulting tree become the next authoritative indexed source?

This slice establishes that proof boundary without introducing a broker,
second database, or model dependency:

```text
approved change
      → fenced validation run
      → immutable check results and evidence
      → deterministic validation decision
      → durable re-index handoff
      → Work Receipt update
```

Validation is deliberately read-only. Repository mutation remains owned by the
existing change service, and ingestion remains the authority for creating the
next indexed source snapshot.

## Scope and non-goals

This slice provides registered deterministic validators for change
preconditions, changed-file integrity, path/symlink safety, resulting tree
consistency, and re-index readiness. It persists validation history,
artifact/evidence links, state transitions, and a durable handoff into the
existing ingestion path. It adds authenticated HTTP, CLI, and MCP inspection
and control paths.

It does not provide arbitrary shell validation, a general code-execution
sandbox, remote CI, git commits or pull requests, automatic rollback, a
background ingest scheduler, or exactly-once filesystem semantics. Command
validators, if added later, remain a separate explicitly registered and
qualified capability.

## Authority boundary

Postgres is authoritative for validation request identity, validator plan,
run state, attempts, results, evidence/artifact links, state transitions,
re-index handoffs, events, checkpoints, audit actors, and replay inputs.

Validator output, reports, projections, and indexes are derived from the
recorded authority. They must never overwrite the applied change packet, source
snapshot, prior ingest, raw evidence, or Work Receipt history.

The local repository is a read-only observation boundary for live validation.
Postgres and a local filesystem cannot share one physical transaction. A
successful validation can therefore commit a handoff request while the actual
re-index remains a separate bounded, resumable operation. Ambiguity is
represented explicitly rather than hidden behind a success flag.

## Contracts and identity invariants

Validation requests, plans, runs, results, reports, evidence, and handoffs are
typed, versioned, normalized, and workspace-scoped. They preserve actor,
task, session, agent-run, change, repository-source, request, idempotency,
causation, and correlation references where applicable. Task-bound operations
also carry an owner and monotonic fence.

The stable validation identity includes the workspace, applied change ID,
packet hash, expected result tree hash, repository source identity, validator
plan/version, and canonical budget. Delivery IDs, timestamps, credentials,
raw prompts, and database-generated IDs are excluded from content hashes.

The stable handoff identity includes the workspace, source identity, change ID,
validation run ID, observed post-change tree hash, and the canonical handoff
schema/version. Equivalent requests return the original durable identity;
reusing an idempotency key for different canonical input fails closed.

## Validator admission

Only explicitly registered validators may run. Registration provides a stable
validator ID, schema version, deterministic ordering key, capability metadata,
and hard limits. The registry rejects duplicate IDs, invalid schemas, and
validators that cannot declare bounded behavior.

The initial validator set is intentionally small:

- **Change preconditions:** affected source paths still match the approved
  pre-state and source identity.
- **Changed-file integrity:** creates, replacements, deletes, renames, and
  mode changes match the packet’s expected post-state.
- **Path and symlink safety:** resulting paths remain within the configured
  source mount and do not introduce traversal or symlink escapes.
- **Tree consistency:** normalized file metadata and content produce the
  expected deterministic result tree hash.
- **Re-index readiness:** the changed tree can produce a new source/manifest
  identity and a bounded ingestion handoff without replacing the previous
  source.

Validators are read-only and deterministic for the same scoped source state
and recorded inputs. They do not call models, brokers, remote services, or
arbitrary commands. Missing capability, unavailable source, contradictory
evidence, or exceeded budget produces an explicit failure or abstention.

## Lifecycle and state invariants

The durable lifecycle is:

```text
pending → running → passed
                 → failed
                 → abstained
                 → cancelled
                 → recovery_required
```

Transitions are append-only and monotonic. Terminal runs cannot be mutated by
ordinary retry or duplicate delivery. Each transition records the expected
prior state, resulting state, actor, identity metadata, reason, and relevant
hashes. A retry creates an attempt/result record; it does not erase a previous
failure.

The final outcome is deterministic:

- `passed` means every required validator completed successfully and the
  resulting evidence is valid.
- `failed` means a required check established a negative result.
- `abstained` means the system could not establish a trustworthy answer within
  its declared capability or budget.
- `cancelled` means durable cancellation prevented completion.
- `recovery_required` means the external observation or durable transition is
  ambiguous and requires explicit reconciliation.

No state is considered verified merely because a worker reached a local return
statement.

## Fencing, concurrency, and crash behavior

Task-bound validation validates the current task owner and fence before work
and again in the transaction that records each authoritative result,
completion, or handoff. A stale worker fails closed. Checkpoints and lease
updates cannot move backwards.

The transaction boundary is:

```text
load and validate authority
  → observe bounded source state
  → persist result/evidence/artifact/state/handoff
  → append event and observation
  → commit
```

If a crash occurs before commit, no authoritative validation mutation exists.
If it occurs after a result commit, resume and replay return the same result.
If it occurs after handoff commit, no second handoff is created. A crash while
reading the source is an incomplete attempt, not a successful validation.

Replay consumes recorded validator definitions, inputs, outputs, evidence,
transitions, and handoff decisions. Replay never reads a live repository,
executes a process, sends network traffic, creates an artifact or ingest job,
mutates Postgres, or advances a checkpoint. A live re-validation, if exposed,
must be named and audited separately from replay.

## Workspace and authorization boundaries

Every validation and handoff query includes the authenticated workspace
predicate. Change, source, task, artifact, evidence, receipt, and ingest
references are resolved within the same workspace. A body, query, or path
workspace value cannot override the principal’s workspace.

Validation creation, execution, inspection, disclosure, replay, cancellation,
resume, and handoff operations use explicit RBAC permissions. Actor identity is
propagated to events, transitions, observations, evidence, artifacts, and
receipts. Credentials, raw prompts, and arbitrary user text are never stored
in validation records, reports, metrics, or errors.

## Evidence, provenance, and artifacts

Every result has an evidence status and at least one authoritative source or
change reference. Evidence includes stable content hashes, source identity,
workspace, validator identity, and the observation boundary. Missing, stale,
contradictory, or cross-workspace references fail closed.

Reports are bounded. Oversized output is stored once in the existing
content-addressed ArtifactStore and linked transactionally to the validation
run. Raw bytes remain immutable; inline fields contain only bounded summaries
and hashes. Work Receipts link the change, validation run, result tree hash,
evidence, report artifact, re-index handoff, and replay identity without
creating a second authority.

## Re-index handoff semantics

A successful validation creates one durable handoff that references the
repository source, applied change, validation run, observed result tree hash,
and expected new manifest identity. The handoff is idempotent and resumable.

The handoff requests a new ingestion identity. It does not update the old
source in place. Changed files produce new chunk/symbol lineage and prior
records remain auditable. Removed files are represented as stale or removed
source records according to the existing ingestion semantics.

The handoff may be created in the same Postgres transaction as the final
validation result. The filesystem and indexing work remain separate external
boundaries. If indexing later fails, that failure is visible and retryable; it
does not invalidate or erase the prior validated history.

## Database and transaction design

The next migration after the current sequence adds workspace-scoped validation
runs, result/attempt history, transitions, evidence/artifact links, and
re-index handoffs. The migration is additive, checksum-validated, and
compatible with fresh and already-migrated databases.

The store API owns idempotency, workspace checks, state transitions, fence
validation, result persistence, evidence/artifact linking, handoff creation,
bounded reads, and replay inputs. Unique identities and append-only protections
prevent duplicate effects and historical mutation.

The normal validation path performs bounded filesystem reads, deterministic
hashing, bounded SQL writes, and artifact deduplication only when needed. It
does not use models, embeddings, brokers, or caches. All report and result
reads are paginated and budgeted.

## Cost and storage budget

The feature must enforce hard limits for validator count, files inspected,
bytes read, report bytes, wall-clock time, database work, retry attempts, and
re-index batch size. Defaults must be conservative and configurable through
existing bounded configuration patterns.

The qualification record must distinguish measured versus estimated values
and report validation latency, source-read throughput, SQL work, WAL/storage
growth, artifact bytes, deduplication, handoff creation latency, replay
throughput, and recovery latency. These are local regression measurements and
not production SLO claims.

## Reuse and licensing

This slice reuses Fornix’s event store, task leases, repository change service,
Work Receipt authority, ingestion service, artifact/evidence/provenance stores,
identity/RBAC middleware, observability ledger, and CLI/HTTP/MCP conventions.

The design is informed by Orloj and agentmemory for durable lifecycle,
checkpoints, leases, and recovery; ClawMem for bounded evidence and replay
gates; FornixDB for immutable disclosure, lineage, and cost accounting; and
DeepSeek Harness for explicit repository boundaries. These patterns are
independently reimplemented. Kronaxis Fabric source is not copied because of
its BSL 1.1 license. Fornix remains MIT licensed.

## Acceptance test plan

The implementation must include contract, registry, validator, store,
concurrency, crash, replay, re-index, API, CLI, MCP, redaction, and smoke
tests. The tests must prove:

- identical inputs produce identical plans, results, reports, and hashes;
- duplicate requests and deliveries produce one durable effect;
- stale fences cannot write or finalize results;
- invalid transitions and terminal mutations fail closed;
- fresh/existing migrations work;
- cross-workspace references are rejected;
- precondition, content, path, symlink, tree, and source/index checks behave
  deterministically;
- limits are never exceeded;
- crash-before-commit leaves authority unchanged;
- crash-after-commit is safely resumable and replayable;
- replay has no external effects;
- successful validation creates one handoff;
- changed/removed files remain auditable;
- reports and artifacts preserve hashes and provenance;
- authorization, actor propagation, pagination, dry-run, and redaction hold;
- existing tests, race checks, builds, CI, and smokes remain green.
