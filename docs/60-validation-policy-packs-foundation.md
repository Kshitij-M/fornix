# Validation policy packs foundation

Status: Task 23 design and implementation note

## Problem and product boundary

Fornix currently has a safe deterministic change and validation path, but every
caller must repeat the same validator list, budget, approval mode, and
re-index choice. That makes safe repository work difficult to standardize,
review, reproduce, and operate across workspaces. Policy packs make those
decisions durable and named without turning policy into an executable plugin
system.

A policy pack is a versioned declaration of admission rules. It selects
registered validators, sets hard limits, requires an approval mode, controls
whether a successful validation must hand off to ingestion, and records the
safety floor that can never be disabled. It is part of the authoritative
provenance chain, not a mutable configuration lookup performed during replay.

The default policy is an implicit compatibility policy. When no policy is
selected, Fornix retains the existing structural validator set, budgets,
approval behavior, task-fence checks, evidence checks, and handoff behavior.
Selecting a policy makes its identity explicit and pins the exact policy
version for the lifetime of the change and validation run.

## Authority and identity

Postgres owns policy identity, immutable versions, active/default bindings,
lifecycle transitions, and audit history. A policy document supplied over HTTP,
CLI, or MCP is untrusted input. The policy service normalizes it, resolves
validator references against the in-process registry, computes a canonical
SHA-256 hash, and persists the normalized snapshot before it can admit work.

The canonical policy hash includes workspace, policy identity, version,
validator IDs and versions, budgets, approval configuration, re-index rules,
task-fence requirements, and safety floors. It excludes database IDs,
timestamps, credentials, delivery IDs, and arbitrary caller text. Policy IDs,
versions, validator references, and list sizes are bounded and normalized.

Every policy-bearing record carries the workspace, policy ID, version, and
canonical policy hash. Existing change proposals, applications, validation
runs, handoffs, receipts, lifecycle events, observations, cost entries, and
audit records retain the historical hash they used. No policy update rewrites
old records. Policy resolution itself is deliberately read-only; lifecycle
operations and the enclosing admitted mutation persist the selected policy
reference and audit identity in their own transaction.

## Policy invariants

- A policy version is immutable after creation; lifecycle state is recorded in
  append-only transitions rather than by editing the policy body.
- A policy is resolved by exact `(workspace_id, policy_id, version)` or by the
  authenticated workspace default. A missing, malformed, conflicting, retired,
  or cross-workspace reference fails closed.
- Every referenced validator must be registered at the requested exact version.
  Duplicate validators are rejected and the persisted order is canonical.
- The mandatory safety validators are always present:
  `change.preconditions`, `change.files`, `change.safety`, and `change.tree`.
  Policy authors may tighten the global limits but cannot remove safety,
  workspace isolation, actor propagation, task fencing, evidence integrity,
  append-only history, or replay safety.
- Policy budgets may be lower than the global validation envelope, never higher.
  A request may further tighten a policy budget but cannot widen it.
- Approval requirements are monotonic. A policy may require approval when the
  caller asks for automatic approval, but no policy can bypass a required
  approval or turn a denied operation into an automatic approval.
- A `require_for` scope is matched against the admitting operation and
  operation types. An automatic policy with scopes but no operation context
  fails safe by requiring approval.
- Retirement prevents new change proposals and validation admissions. Existing
  proposals and runs retain their pinned snapshot and remain auditable; resume
  rejects a retired or changed policy when the run requires a new admission.
- Rollback is activation of an earlier immutable version, never mutation or
  deletion of the newer version.

## Resolution and execution semantics

Policy resolution is a pure operation over normalized request data, the
registered validator catalog, and the durable policy snapshot. It returns a
`PolicyResolution` containing the exact policy reference, hash, effective
validators, tightened budgets, approval decision, handoff requirement, and
audit identity. It never evaluates shell commands, SQL, prompts, credentials,
network requests, or callbacks contained in a policy; those fields are not
accepted by the contract. A pure resolution does not independently write an
audit or event; the admitting transaction records the selected reference and
decision alongside its authoritative effect.

The change proposal request hash includes the selected policy reference and
hash. The validation request and durable plan include the same snapshot. The
validation store checks that result ordinals and validator versions match the
policy-resolved plan. Replay uses the recorded policy snapshot/hash and never
resolves the current workspace default or executes live policy logic.

Policy metadata is included in the change/validation/handoff receipt and
event payloads, and in bounded observability/cost dimensions. Raw policy JSON,
prompts, credentials, and arbitrary user text are not copied into metrics,
errors, or public reports.

Task-bound changes and validations retain the existing monotonic task fence.
Policy resolution and terminal writes check the current workspace task lease;
stale workers fail closed before creating effects. Policy identity does not
replace task ownership.

## Lifecycle, authorization, and audit

The lifecycle is:

```text
created → active → retired
             ↘ active (rollback by activating another immutable version)
```

Creation, activation, default binding, retirement, comparison, and resolution
are authenticated workspace operations with explicit RBAC permissions.
Lifecycle decisions and admitted policy-bearing mutations record the actor,
request/idempotency identity, policy reference, old/new status or binding,
result, and canonical policy hash. Pure resolution remains read-only and
returns a deterministic audit identity for the enclosing mutation to store.
Repeating the same idempotent operation returns the authoritative result with
`deduped=true`. Reusing an identity with different normalized content fails
closed.

The workspace default is one binding to an active exact version. Concurrent
activation/default updates use a transaction and deterministic locking. A
default change affects only future admissions; it cannot alter existing
proposal or run hashes. A dry-run resolution reads policy and registry state
but writes no policy, binding, audit, event, receipt, or validation record.

## Migration strategy

Migration `031_validation_policy_packs.sql` is additive, forward-only, embedded,
and protected by the existing migration checksum runner. It adds workspace-
scoped policy identities, immutable policy versions, validator references and
rules, default bindings, lifecycle transitions, audit records, and policy
idempotency identities. It also adds nullable policy metadata to change
proposals/applications, validation runs, re-index handoffs, Work Receipts, and
the relevant event/accounting records.

Composite workspace foreign keys and uniqueness constraints prevent a policy
or binding from crossing workspaces. JSON documents are bounded by database
checks. Policy body/history tables are protected by append-only triggers;
status and default bindings are changed only through the policy store, which
also appends an audit transition in the same transaction. Existing rows remain
valid with empty policy metadata, representing the compatibility policy.

## Failure, duplicate, crash, and replay behavior

Admission locks the relevant policy version and authoritative change row. A
crash before commit rolls back the policy identity, audit record, event,
binding, or propagated metadata being written. The active policy is rechecked
under the same transaction before a policy-bearing change or validation is
committed, so retirement cannot race admission. A crash after commit is safe
to retry because policy identity, request hash, and idempotency key are unique.

A duplicate proposal or validation request with the same normalized policy
reference returns the original durable object. A duplicate request with a
different policy reference, version, hash, budget, or approval mode returns an
idempotency conflict. A stale task worker cannot create or finalize a
policy-bearing change, validation, handoff, receipt, event, or accounting
effect.

Replaying a run reads the pinned policy snapshot and immutable result/event
history. It does not call the registry, read the live filesystem, invoke a
model/tool, submit ingestion, or mutate policy state. The resulting hashes are
stable under duplicate delivery and restart.

## Storage, SQL, and cost budget

Policy bodies and validator lists are small bounded JSON documents. Creation
and lifecycle operations use one transaction with indexed workspace/policy
lookups, one immutable version row, and append-only audit/transition rows.
Normal change/validation overhead is limited to reading the pinned policy and
carrying its 64-byte hash; no new external service or cache is introduced.
The policy package performs no SQL. Per-statement SQL timing remains a
measurement limitation until the existing observability layer exposes query
counts uniformly.

Qualification measures policy resolution, create, activation, default binding,
comparison, replay, and concurrent activation latency; rows touched and
relation size; hash computation duration; propagated change/validation
overhead; and audit growth. Measurements distinguish local observations from
production SLOs and estimates. No embedding, LLM, broker, Redis, NATS, cache,
second database, or policy service is introduced.

## Reference reuse and licensing

Orloj informed explicit policy admission, capability checks, lifecycle
controllers, and fence-aware execution. DeepSeek Harness informed declarative
permission/approval seams and explicit capability registration. ClawMem
informed fail-closed evidence gates, abstention, and replay-safe decisions.
agentmemory informed scoped lifecycle/audit, dry-run, cleanup, and diagnostic
discipline. FornixDB informed immutable policy metadata, hard budgets,
tier-aware storage thinking, and cost measurement.

These are independent design inputs; no reference source is copied. Kronaxis
Fabric remains excluded because its BSL 1.1 license is incompatible with
Fornix's MIT license. Any future third-party reuse must be reviewed separately
for license, notice, and attribution obligations.

## Acceptance tests

The implementation must prove:

- fresh and existing databases apply migration 031 cleanly;
- normalization and canonical policy hashes are stable;
- default policy behavior preserves existing proposal/validation hashes and
  results;
- unknown, duplicate, unregistered, retired, cross-workspace, and weakening
  policy references fail closed;
- budgets never exceed global limits and request overrides can only tighten;
- policy versions are immutable, activation/default/retirement/rollback are
  transactional, idempotent, concurrent-safe, and audited;
- explicit policy identity propagates through changes, approvals,
  applications, validations, handoffs, receipts, events, observations, and
  costs;
- duplicate delivery creates one effect and conflicting idempotency fails;
- stale task fences cannot mutate policy-bearing work;
- crashes before commit leave no partial policy/history/effect and crashes
  after commit replay safely;
- replay uses the recorded snapshot and performs no external effects;
- HTTP, CLI, and MCP resolution return equivalent hashes;
- unauthorized and cross-workspace operations fail closed;
- dry-run resolution is side-effect free and bounded;
- previous tests, races, builds, CI, and all smokes remain green.

Measured qualification and remaining limitations are recorded in
`docs/61-loop-23-completion.md`.
