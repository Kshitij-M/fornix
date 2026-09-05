# Task 22 — Deterministic post-change validation and re-index handoff

Status: implementation specification and tracking record.

## Objective

Build Fornix’s deterministic, evidence-backed post-change validation and
re-index handoff layer. Every applied repository change must be able to be
validated, associated with authoritative evidence, resumed after a crash,
replayed without external effects, and handed back to the existing ingestion
and indexing authority without overwriting source history.

The target product path is:

```text
ingest → retrieve → propose → approve → apply → validate → re-index → prove
```

Postgres remains the only authority for control state, events, checkpoints,
validation history, evidence, artifacts, and handoff decisions.

## Required preparation

Before implementation, inspect the current Git state, fetch the protected
`origin/main`, and create a feature branch from it. Do not modify protected
`main` directly. Preserve unrelated working-tree changes and do not use
destructive Git commands without explicit approval.

Read the complete project research in `chats/` and read:

```text
AGENTS.md
docs/00-fornix-foundation.md
docs/01-product-vision.md
docs/13-reference-reuse-matrix.md
docs/14-production-readiness-qualification.md
docs/16-event-state-delta-foundation.md
docs/18-projection-subscription-foundation.md
docs/22-task-execution-foundation.md
docs/24-retrieval-context-foundation.md
docs/28-model-gateway-foundation.md
docs/30-tool-runtime-foundation.md
docs/32-agent-loop-foundation.md
docs/36-identity-rbac-credential-foundation.md
docs/38-artifact-storage-foundation.md
docs/40-artifact-output-integration-foundation.md
docs/42-observability-evaluation-foundation.md
docs/44-retrieval-evaluation-quality-foundation.md
docs/48-operator-reference-workflow-foundation.md
docs/49-loop-18-completion.md
docs/50-repository-ingestion-foundation.md
docs/51-loop-19-completion.md
docs/54-work-receipt-foundation.md
docs/55-loop-20-completion.md
docs/56-repository-change-foundation.md
docs/57-loop-21-completion.md
```

Study these current implementation areas:

```text
internal/contracts/
internal/change/
internal/ingest/
internal/retrieval/
internal/agentloop/
internal/scheduler/
internal/store/events.go
internal/store/tasks.go
internal/store/agent_runs.go
internal/store/ingestion.go
internal/store/repository_changes.go
internal/store/work_receipts.go
internal/store/evidence.go
internal/store/artifacts.go
internal/server/
cmd/fornix/
cmd/fornix-eval/
```

Study the reference repositories in `reference_repos/`, prioritizing:

- Orloj task claims, controllers, execution engine, checkpoints, retry,
  evidence, artifacts, and failure handling.
- agentmemory action lifecycle, dependencies, leases, checkpoints, cleanup,
  diagnostics, and replay.
- ClawMem gold evidence, retrieval/source validation, replay, abstention, and
  bounded acceptance gates.
- FornixDB immutable raw/detail/gist disclosure, lineage, retention,
  evaluation, and disk-cost measurement.
- DeepSeek Harness repository context, filesystem boundaries, validation, and
  cost controls.

Do not copy Kronaxis Fabric source. Its BSL 1.1 license is incompatible with
copying into this MIT-licensed repository. Reimplement applicable patterns
independently and preserve required third-party notices.

## Required feature note

Before production implementation, write and review:

```text
docs/58-validation-foundation.md
```

The note must cover validator admission, validation state transitions, task
fencing, changed-tree identity, budgets, crash/recovery semantics, evidence
and provenance, re-index handoff, stale-source handling, workspace isolation,
authorization, idempotency, storage/cost impact, reuse decisions, licensing,
and acceptance tests.

## Scope

Implement the smallest production-quality vertical slice:

- typed validation requests, plans, definitions, check results, reports,
  failures, budgets, replay requests, and re-index handoffs;
- only explicitly registered, structured, bounded, read-only validators;
- applied-change packet, source, workspace, actor, task, session, run, and
  fencing identity checks;
- append-only validation results and state-transition history;
- idempotent requests and duplicate-result delivery;
- transactional evidence and artifact links;
- deterministic changed-tree re-index handoff into the existing ingestion
  authority;
- explicit failure, cancellation, retry, abstention, and recovery states;
- replay from sequence zero and from a checkpoint with no external effects;
- authenticated HTTP, CLI, and MCP inspection and control surfaces;
- unit, integration, concurrency, crash, replay, redaction, and smoke tests;
- CI, Make commands, architecture documentation, and measured qualification.

Do not add a broker, cache, second database, object store, LLM dependency,
unrestricted shell execution, or a new service.

## Contract requirements

Add typed contracts under `internal/contracts/`. Use existing Fornix naming,
normalization, hash, redaction, workspace, and identity conventions.

The contracts should include, or provide equivalent typed forms for:

- `ValidatorRef`
- `ValidationDefinition`
- `ValidationBudget`
- `ValidationRequest`
- `ValidationPlan`
- `ValidationRun`
- `ValidationResult`
- `ValidationEvidence`
- `ValidationReport`
- `ValidationFailure`
- `ValidationReplayRequest`
- `ReindexHandoff`
- `ReindexHandoffStatus`

Every durable request and result must preserve schema version, workspace,
actor, task/session/agent-run references when present, change reference,
repository source reference, request identity, idempotency key, causation ID,
correlation ID, timestamps, and fencing token when task-bound.

Use typed enums for lifecycle states, outcomes, failure classes, and handoff
states. Document units, ownership, mutability, authority, derivation,
estimation, and redaction semantics in exported Go comments.

## Migration requirements

Add the next forward-only migration after the current repository migration
sequence. Verify the exact number first; the expected migration is likely
`internal/store/migrations/030_*.sql`.

The migration must support fresh and existing databases and preserve all
previous migration checksums and records. Add workspace-scoped authority for:

- validation runs;
- validator definitions or registration metadata if persistence is needed;
- validator attempts/results;
- validation state transitions;
- validation evidence and artifact links;
- re-index handoffs;
- idempotency and uniqueness identities;
- bounded polling, inspection, and replay indexes.

Use composite workspace references and indexes wherever possible. Add
append-only protections for historical rows, explicit state/outcome fields,
fencing checks, and constraints that prevent ambiguous cross-workspace links.

## Validator registry and execution

Create an appropriate `internal/validation/` package with an explicit
deterministic validator registry.

The registry must:

- require explicit registration;
- reject duplicate IDs and incompatible schemas;
- resolve validators in stable order;
- expose bounded capability inspection;
- reject unregistered or over-budget validators;
- enforce workspace and authorization context;
- never dynamically execute arbitrary code.

Implement deterministic validators for the smallest useful vertical slice,
including:

1. Change precondition verification.
2. Changed-file content and hash verification.
3. Path and symlink safety verification.
4. Resulting repository tree consistency.
5. Changed-source/index handoff readiness.

If command-based validators are added, they must be registered, read-only,
structured-argv only, bounded by time/bytes/environment/workdir limits, and
executed through the existing policy boundary. Never invoke an implicit shell.

The validation runner must:

1. Load the authoritative applied change artifact.
2. Verify workspace, source, actor, task, and fence identities.
3. Verify source preconditions.
4. Resolve the validator plan deterministically.
5. Execute validators in stable order.
6. Enforce item, byte, time, SQL, retry, and report budgets.
7. Persist immutable results and evidence.
8. Compute stable result and report hashes.
9. Create a re-index handoff only after successful validation.
10. Append typed lifecycle events and observations.
11. Preserve all authoritative history.

Unknown, missing, stale, contradictory, or over-budget evidence must produce
an explicit failure or abstention. It must never silently pass.

## State and recovery semantics

Use an explicit monotonic state machine, for example:

```text
pending → running → passed
                 → failed
                 → abstained
                 → cancelled
                 → recovery_required
```

Define legal transitions, invalid transitions, terminal-state behavior,
retry classification, cancellation, and duplicate delivery.

Required crash semantics:

- before validation commit: no authoritative validation mutation;
- after validator output but before commit: result can be recomputed or
  discarded without duplicate authority;
- after result commit but before completion: resume is idempotent;
- before handoff creation: no handoff exists;
- after handoff creation: replay/resume returns the same handoff;
- during lease renewal or checkpoint advancement: stale workers fail closed;
- after final commit: replay is deterministic and side-effect free.

Filesystem inspection is a read-only external boundary. Replay must consume
recorded validator inputs/results and must not read a live repository, execute
commands, create artifacts, create ingest jobs, send network traffic, mutate
Postgres, or advance checkpoints.

If live re-validation is exposed, give it a separate operation name and do
not call it replay.

## Re-index handoff

Create a durable, idempotent handoff to the existing ingestion/indexing
authority. It must include:

- workspace and repository source identity;
- applied change ID and validation run ID;
- expected and observed post-change tree hashes;
- source/manifest identity where available;
- actor, task/session/run, request, idempotency, causation, and correlation
  references;
- state, retry, failure, creation, and completion metadata.

The handoff must preserve the prior indexed source, create a new source
identity for the changed tree, support changed/removed-file supersession, and
never overwrite authoritative history. It must be safe when a matching ingest
job already exists.

Explicitly document that local filesystem writes and Postgres commits cannot
be one physical transaction. A successful validation may durably request
re-indexing while the actual ingestion remains resumable and independently
observable.

## Change, receipt, evidence, and authorization integration

Link validation and handoff records into the existing repository change and
Work Receipt authorities without creating a parallel receipt system.

Receipts must distinguish applied, validated, re-indexed, verified, failed,
abstained, and recovery-required outcomes. A change is not fully verified just
because application succeeded.

Reuse existing:

- change artifact and result tree hashes;
- task leases and fencing;
- events and projections;
- artifact storage and disclosure budgets;
- evidence and provenance;
- identity/RBAC authorization;
- observability and cost ledger;
- ingestion/source/chunk/symbol services;
- CLI, HTTP, MCP, migration, and smoke conventions.

Require separate permissions for creating, running, reading, disclosing,
resuming, cancelling, and replaying validation. Propagate the authenticated
actor everywhere. Credentials and raw prompts must never appear in logs,
events, metrics, evidence, artifacts, reports, or errors.

## API, CLI, and MCP requirements

Add bounded, authenticated, workspace-scoped surfaces for:

- create validation;
- dry-run validation;
- inspect validation status/results;
- disclose bounded reports;
- replay validation;
- resume or cancel validation;
- inspect and resume re-index handoffs;
- inspect validation metrics.

Use existing route and command conventions. Suggested HTTP resources are:

```text
POST /v1/validations
GET  /v1/validations/{id}
GET  /v1/validations/{id}/results
GET  /v1/validations/{id}/report
POST /v1/validations/{id}/replay
POST /v1/validations/{id}/resume
POST /v1/validations/{id}/cancel
GET  /v1/reindex-handoffs/{id}
POST /v1/reindex-handoffs/{id}/resume
```

Exact names may follow existing conventions. Dry-run must be side-effect
free. Pagination, batch limits, and disclosure budgets are mandatory.

## Test requirements

Add well-structured unit, Postgres integration, race, concurrency, crash,
replay, API, CLI, MCP, and smoke tests.

Cover:

- contract normalization, hashes, redaction, limits, and invalid states;
- deterministic registration and validator ordering;
- precondition, content/hash, path/symlink, tree, and handoff validators;
- missing, stale, contradictory, and cross-workspace evidence;
- duplicate request and duplicate-result delivery;
- stale fencing token and checkpoint rejection;
- terminal-state and illegal-transition rejection;
- transaction rollback and orphan prevention;
- crashes at every validation, result, handoff, lease, and checkpoint
  boundary;
- replay from zero and from checkpoint without external effects;
- changed and removed file supersession;
- re-index handoff deduplication and resumability;
- authorization, actor propagation, pagination, disclosure, and redaction;
- HTTP, CLI, and MCP semantic equivalence;
- dry-run non-mutation;
- cost, storage, latency, SQL-work, and replay measurements.

## Smoke, CI, and documentation requirements

Add a versioned smoke such as:

```text
scripts/test/v0.32-validation-smokes.sh
```

The smoke must bootstrap a workspace, ingest a fixture, create/approve/apply
a structured change, validate it, create a handoff, inspect the receipt,
replay validation, and verify stable hashes. Use the fake provider and no
network or optional infrastructure.

Add appropriate Make targets, CI coverage, and development documentation.
Create the implementation completion note:

```text
docs/59-loop-22-completion.md
```

Update the documentation map, README/API references, and qualification notes.

## Acceptance criteria

- Fresh and existing databases migrate cleanly.
- Validation is deterministic, replayable, and workspace-scoped.
- Unauthorized and cross-workspace operations fail closed.
- Duplicate requests and duplicate deliveries produce one durable effect.
- Stale workers cannot write results, advance checkpoints, or create handoffs.
- Validator order, results, hashes, reports, and decisions are stable.
- Budgets are never exceeded.
- Missing or contradictory evidence fails closed or abstains explicitly.
- Successful validation creates exactly one re-index handoff.
- Handoffs are resumable, bounded, and idempotent.
- Changed and removed files remain auditable.
- Crashes before commit leave authority unchanged.
- Crashes after commit are safely replayable.
- Replay performs no external effects and does not mutate Postgres.
- Work Receipts link validation and handoff evidence correctly.
- Credentials, prompts, and arbitrary raw user text are absent from reports.
- HTTP, CLI, and MCP operations have equivalent semantics.
- All existing tests, race checks, builds, CI, and smokes remain green.
- New validation smokes pass.
- Latency, SQL work, storage growth, throughput, and limitations are reported.

## Delivery requirements

Before opening a PR:

1. Run formatting, unit tests, integration tests, race tests, vet, builds,
   migration checks, documentation checks, and all smokes available in the
   development environment.
2. Inspect generated reports, artifacts, logs, and errors for secret leakage.
3. Review `git diff` and `git status` for unrelated changes.
4. Commit the feature branch and push it.
5. Open a PR against protected `main`.
6. Require CI and the repository’s additional maintainer review.

At the end of the implementation loop, report changed files, migration
details, tests, smoke results, measurements, security review, limitations,
and the recommended prompt for the next task.
