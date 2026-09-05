# Loop 23 completion — validation policy packs and verified change admission

Status: implemented and locally qualified on 2026-09-05; ready for CI and
maintainer review before merge into protected `main`.

## Executive result

Fornix now has a durable, workspace-scoped policy authority for post-change
verification. Operators can define an immutable declarative policy version,
activate it, bind it as the workspace default, compare it with another exact
version, retire it, and inspect its audit history. Change proposals and
validation runs can resolve that policy once and carry its exact hash through
the verified-change proof chain.

The implementation keeps the important product boundary intact:

```text
authenticated workspace actor
  → resolve an immutable validation policy
  → admit a bounded change/validation operation
  → pin policy ID, version, and hash
  → validate deterministically and preserve evidence
  → emit auditable change, handoff, receipt, event, and cost history
```

Policy resolution is deterministic and Postgres-backed. The resolver is pure;
the selected policy is durably pinned and audited by lifecycle and admitting
mutations rather than by a side effect in the resolver itself. It does not add
an LLM, policy interpreter, broker, cache, Redis, NATS, external policy
service, or new infrastructure.

## Why this slice exists

Before Loop 23, Fornix had deterministic structural validators but callers had
to repeat validator lists, budgets, approval settings, and re-index choices.
That made it difficult to standardize repository operations across workspaces,
review the admission decision, or reproduce why a historical change was
accepted.

Policy packs make the admission decision named, reviewable, content-addressed,
and durable. They are intentionally a bounded control-plane contract rather
than an executable policy language. The product still uses models for
interpretation where appropriate, but the safety boundary remains exact state,
registered validators, hard budgets, and auditable decisions.

## Delivered implementation

### Typed contracts and pure resolution

- Added `internal/contracts/policy.go` with policy references, declarative
  packs, immutable versions, rules, budgets, approval configuration, safety
  floors, lifecycle requests, audit records, comparisons, and resolution
  results.
- Policy bodies normalize identity, validator order, approval modes, and
  bounded budgets before hashing.
- Canonical SHA-256 hashes include workspace and policy identity, rules,
  budgets, approval, re-index behavior, task-fence requirements, and safety
  floors. Delivery IDs, timestamps, credentials, and arbitrary audit prose are
  excluded.
- Added `internal/policy/resolver.go`. It resolves only exact registered
  validator references, injects the mandatory preconditions/files/safety/tree
  validators, and rejects unknown, duplicate, cross-workspace, hash-mismatched,
  or weakening requests.
- Policy budgets and approval are monotonic: callers may tighten them but may
  not widen limits or bypass required approval. Workspace isolation, actor
  propagation, task fencing, evidence integrity, append-only history, and
  replay safety are non-disableable floors.
- Operation-specific approval scopes are evaluated against the admitting
  operation and operation types. If a policy has `require_for` scopes but the
  caller supplies no operation context, resolution requires approval
  conservatively.

### Postgres authority and migration

- Added migration `031_validation_policy_packs.sql` with workspace-scoped:
  policy identities, immutable versions, validator rules, default bindings,
  lifecycle transitions, audit records, and idempotency records.
- Added nullable policy identity columns and composite workspace foreign keys
  to changes, approvals, applications, validation runs, re-index handoffs,
  Work Receipts, events, observations, and cost entries.
- Added bounds, all-or-none identity checks, one-active-version enforcement,
  append-only triggers, and a database trigger that rejects mutation of an
  immutable policy body or identity.
- Existing rows remain valid as compatibility records with no policy pin.
  Fresh databases and already-migrated databases converge through the existing
  embedded, checksum-validated migration runner.
- Added forward-only migration `032_validation_policy_fk_repair.sql` to repair
  composite policy foreign keys independently when an existing database has
  partially applied the additive constraints. Migration 031 remains immutable
  under checksum validation.

### Transactional lifecycle store

`internal/store/policies.go` now provides:

- idempotent create with immutable-body conflict detection;
- exact get and bounded list pagination;
- activate, default-bind, and retire transitions;
- default and exact policy resolution;
- side-effect-free dry-run resolution;
- bounded audit pagination; and
- deterministic version comparison.

Lifecycle operations lock the policy identity before changing active state, so
concurrent activation cannot leave two active versions. Policy creation,
transition history, audit, and typed policy events commit together. A failed
transaction leaves no policy body, lifecycle row, audit record, event, or
evidence mirror.

Retirement prevents new admission but does not rewrite old proposals, runs,
receipts, or events. Rollback means activating an earlier immutable version.
The default affects future admission only; historical work keeps its pinned
policy snapshot and hash.

### Admission integration

Policy selection is integrated into the existing change and validation
authorities:

- change proposals resolve and pin the policy before persistence;
- policy budgets and approval requirements are applied to the proposal;
- caller-supplied tighter budgets are preserved, and the store rechecks the
  effective change budget before persistence;
- approvals and applications reject a conflicting policy reference;
- validation plans persist the exact policy and effective validator set;
- policy-controlled validation records whether a re-index handoff is required;
- result replay rehydrates the run-level policy pin before hashing;
- handoffs, receipts, events, observations, and cost entries carry policy
  identity; and
- existing unselected requests retain the historical compatibility behavior.

The policy reference never replaces task ownership. Task-bound operations still
require the current workspace task fence, and stale workers fail closed.

### Authenticated operator surfaces

Added workspace-authorized HTTP routes:

```text
GET/POST /v1/policies
GET      /v1/policies/{policy_id}/{version}
POST     /v1/policies/{policy_id}/{version}/activate
POST     /v1/policies/{policy_id}/{version}/default
POST     /v1/policies/{policy_id}/{version}/retire
POST     /v1/policies/resolve
POST     /v1/policies/dry-run-resolve
POST     /v1/policies/compare
GET      /v1/policies/audit
```

Added policy capabilities: `policy:read`, `policy:create`,
`policy:activate`, `policy:retire`, `policy:resolve`, and `policy:compare`.
The authenticated actor and workspace override spoofable body fields. Request
bodies, pagination, idempotency, errors, and audit reason sizes are bounded;
credentials and arbitrary raw text are not copied into events or telemetry.

Added equivalent `fornix policy` CLI commands and `fornix__policy_*` MCP tools
for list, create, get, activate, default, retire, resolve, dry-run resolve,
compare, and audit. These surfaces call the same HTTP authority rather than
creating a second policy implementation.

## Research and reuse decisions

The following reference implementations were studied before coding:

- Orloj policy enforcement, capability admission, lifecycle controller, and
  fence-aware authorization;
- DeepSeek Harness permission presets, approval seams, and explicit
  capability boundaries;
- ClawMem retrieval/evidence gates, abstention, stale-edge handling, and
  replay discipline;
- agentmemory scoped lifecycle, audit, dry-run, cleanup, and diagnostics; and
- FornixDB bounded budgets, immutable detail/raw thinking, and storage-cost
  measurement.

The behavior was independently reimplemented in Fornix contracts and stores.
No reference source was copied. Kronaxis Fabric was not copied because its
BSL 1.1 license is incompatible with Fornix's MIT distribution. The existing
MIT license remains authoritative; future source reuse must undergo the same
license, notice, and attribution review.

## Acceptance and qualification matrix

| Acceptance area | Result | Evidence |
| --- | --- | --- |
| Contract normalization and stable hashing | Pass | `internal/contracts/policy_test.go`, `internal/policy/resolver_test.go` |
| Mandatory validator injection and registry lookup | Pass | Pure resolver tests |
| Budget tightening and approval monotonicity | Pass | Pure resolver tests |
| Caller tightening and persisted packet budget enforcement | Pass | Change integration suite |
| Foreign workspace and supplied-hash rejection | Pass | Contract and Postgres policy tests |
| Fresh migration chain including 031 and forward repair 032 | Pass | Temporary fresh Postgres database |
| Existing database migration/checksum path | Pass | Local development Postgres database |
| Idempotent create and conflicting reuse | Pass | `TestPolicyLifecycleIsIdempotentAuditableAndWorkspaceScoped` |
| Concurrent create serialization | Pass | `TestPolicyConcurrentCreateAndTransitionHaveOneDurableEffect` |
| Concurrent activation one-active invariant | Pass | `TestPolicyConcurrentActivationLeavesOneActiveVersion` |
| Active-policy transaction lock during admission | Pass | Change and validation integration suites |
| Immutable policy body at database boundary | Pass | `TestPolicyVersionBodyIsImmutableAtDatabaseBoundary` |
| Crash-before-commit rollback | Pass | `TestPolicyCreateCrashRollsBackBodyHistoryAndEvent` |
| Dry-run side-effect freedom | Pass | Lifecycle test compares audit/event counts before and after |
| Default/exact resolution and retirement behavior | Pass | Policy store lifecycle and resolver tests |
| Change proposal policy pin and approval propagation | Pass | Change integration suite |
| Validation policy pin and replay hash stability | Pass | `TestValidationPinsWorkspaceDefaultPolicyAndReplaysItsReference` |
| HTTP permission routing and workspace authorization | Pass | Server authorization tests and policy smoke |
| CLI and MCP policy surface | Pass | `v0.33-policy-smokes.sh` |
| Full Go tests with Postgres | Pass | `FORNIX_TEST_PG_DSN=... go test ./... -count=1` |
| Race tests with Postgres | Pass | `go test -race ./... -count=1` |
| Formatting, vet, Python, shell, docs, and diff checks | Pass | Make/static checks |

## Measured local impact

These are warm local development measurements, not production SLOs.

- The focused six-test policy Postgres package completed in about **0.80 s**
  of Go package test time, or **2.19 s** wall time including the local Go
  process/toolchain overhead.
- The full Postgres-backed Go suite passed after the policy and replay
  integration changes. Package-level timings were dominated by the existing
  change, scheduler, projection, server, and migration suites rather than pure
  policy hashing.
- Policy resolution is in-memory after the authoritative snapshot is loaded;
  the resolver performs no SQL and no external work. Lifecycle operations use
  indexed workspace/policy lookups and one transaction for body, rules,
  transition, audit, idempotency, and event writes.
- A normal policy-bearing proposal or validation adds one exact policy lookup
  and three 64-byte identity fields to each participating row. It does not
  duplicate raw prompts, model output, source files, or tool output.
- The current development database reported approximately **32 KiB** for the
  policy identity table, **248 KiB** for policy versions, **48 KiB** for rules,
  **32 KiB** for defaults, **136 KiB** for transitions, **144 KiB** for audit,
  and **136 KiB** for idempotency. These values include local qualification
  rows and PostgreSQL relation-page overhead; they are not a capacity model.
- Policy JSON is bounded to 64 KiB, policies to 64 rules, and list/audit pages
  to 100 records. Storage growth is therefore driven by intentionally retained
  policy versions and audit history, not unbounded caller text.
- Replay reads the recorded policy snapshot and result/event pages. It does
  not read the live filesystem, call a model or tool, submit ingestion, or
  create a new policy. Replay cost is proportional to the bounded history
  being disclosed.
- The implementation does not yet expose uniform per-statement SQL timing for
  every store operation. This remains an explicit observability limitation;
  the policy layer does not claim zero database work.

## Remaining limitations

- Policies are declarative and limited to the built-in validator registry. They
  are not a general policy programming language and cannot express arbitrary
  shell commands, test runners, custom executable validators, or remote policy
  plugins.
- Policy administration is workspace-scoped but does not yet provide
  organization-wide policy distribution, delegated governance workflows,
  policy-as-code review, or SSO/OAuth integration.
- Policy metadata is propagated across the verified change path and relevant
  accounting records; some producer-specific model/tool rows retain their
  existing schema and rely on their parent change/run/receipt reference.
- The default compatibility policy remains implicit for legacy callers. Teams
  that need an explicit compliance decision must select and activate a policy
  version before admission.
- Postgres remains the only authority. Backup/restore drills, high-availability
  deployment, policy-history compaction, external object storage, and a
  production capacity benchmark remain outside this alpha qualification.

## Files and operations

The primary Task 23 files are:

- `internal/contracts/policy.go` and its unit tests;
- `internal/policy/resolver.go` and its unit tests;
- `internal/store/migrations/031_validation_policy_packs.sql`;
- `internal/store/policies.go` and its Postgres integration tests;
- policy propagation in change, validation, receipts, events, and
  observability stores;
- `internal/server/policies.go`, server routing/auth, CLI, and MCP surfaces;
- `scripts/test/v0.33-policy-smokes.sh`, Make, and CI updates;
- `docs/60-validation-policy-packs-foundation.md`; and
- this completion record.

No commit or push was performed in this task because the current request did
not authorize repository publication. The working tree is ready for review,
branch commit, and the repository's protected-main PR process.

## Recommended next task prompt

### Task 24 — Build Fornix’s deterministic repository verification profiles and policy-aware validation execution

Before coding:

1. Read the chats directory end to end.
2. Read `AGENTS.md`, `docs/00-fornix-foundation.md`,
   `docs/13-reference-reuse-matrix.md`,
   `docs/14-production-readiness-qualification.md`,
   `docs/50-repository-ingestion-foundation.md`,
   `docs/58-validation-foundation.md`,
   `docs/60-validation-policy-packs-foundation.md`, and
   `docs/61-loop-23-completion.md`.
3. Study DeepSeek Harness bounded execution and context verification,
   Orloj command/tool admission, timeout and failure handling, ClawMem
   evidence/abstention gates, agentmemory diagnostics and evaluation, and
   FornixDB budget and provenance patterns. Compare them with Fornix's current
   validator registry, structured-argv tool runtime, task fencing, artifact
   store, evidence store, policy resolver, and Work Receipt path. Do not copy
   Kronaxis Fabric source because it is BSL 1.1.
4. Write a feature note before implementation covering validator profile
   identity, command admission, read-only versus mutating checks, sandbox and
   mount boundaries, timeout/output/resource budgets, task fencing, crash and
   retry semantics, evidence/artifact capture, policy compatibility, replay
   safety, workspace isolation, licensing, cost impact, and acceptance tests.

Implement the smallest production-quality vertical slice:

- Add typed VerificationProfile, VerificationCheck, CheckCommand,
  VerificationRun, VerificationResult, and VerificationEvidence contracts.
- Add immutable workspace-scoped verification profiles and exact content hashes
  behind the existing policy registry; do not create an executable policy
  language.
- Allow only explicitly registered deterministic read-only checks and
  structured argv commands. Deny shell interpolation, undeclared environment,
  path escapes, network access, unbounded output, and writes by default.
- Resolve profile and policy versions before validation admission and pin their
  exact hashes in validation runs, results, events, evidence, artifacts,
  receipts, and cost records.
- Execute checks with bounded wall time, output bytes, file reads, process
  count, and total cost. Require the current task fence for task-bound runs.
- Persist check attempts transactionally with validation state, preserve raw
  output as redacted/content-addressed artifacts, and never overwrite prior
  evidence or verification history.
- Add deterministic retry classification, cancellation, crash recovery,
  duplicate delivery, stale-worker rejection, and explicit abstention when a
  required check cannot be safely executed.
- Ensure replay uses recorded check outputs only and never starts a process,
  reads the live repository, invokes a provider, or performs an external
  effect.
- Integrate one profile into the reference workflow and the verified change
  packet path. Keep the offline fake-provider path as the default.
- Add authenticated HTTP, CLI, and MCP inspection/run/disclosure surfaces with
  bounded pagination, dry-run behavior, RBAC, redaction, and workspace
  isolation.
- Add migration, unit/integration/concurrency/crash/replay/workspace tests,
  CI, Make commands, Docker smokes, public architecture documentation, and
  measured latency, SQL work, artifact/storage growth, check throughput, and
  remaining limitations.

Acceptance criteria:

- Identical recorded inputs produce identical profile, check, result, evidence,
  artifact, receipt, and replay hashes.
- Unauthorized or undeclared checks fail closed.
- A check cannot escape its configured workspace mount or execute an implicit
  shell.
- Hard time, output, file, process, and cost budgets are never exceeded.
- Duplicate submissions create one durable check effect.
- Stale task workers cannot start or finalize task-bound checks.
- Crashes before commit leave no partial authoritative result; crashes after
  commit are safely replayable.
- Failed or unavailable required checks produce deterministic abstention rather
  than an unverified success.
- Replay performs no process, model, tool, network, or filesystem effect.
- CLI, HTTP, and MCP semantics agree; cross-workspace reads/writes fail closed.
- Existing tests, races, builds, CI, smokes, and Task 23 policy behavior remain
  green.
