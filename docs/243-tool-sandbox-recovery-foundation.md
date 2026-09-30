# Tool sandbox recovery foundation

Status: implementation added in the current worktree; Postgres integration and a real non-local sandbox provider remain unqualified.

## Problem

Fornix persists a tool run before dispatch and already marks uncertain external
effects as `recovery_required`. The sandbox adapter now has a stable,
hash-bound runtime identity and a reconciliation contract. The remaining gap is
that generic effect reconciliation does not finalize the associated
`tool_runs` result, artifact links, and lifecycle event. Treating those as
separate commits can leave the control plane disagreeing with the runtime.

## Scope and non-goals

This slice adds the recovery contract for an already-reserved, non-local
sandbox attempt, its fenced Postgres finalizer, and an authenticated API. It
does not implement a container, OCI, gVisor, or microVM runtime; the default
installation still registers only the local process backend. The path cannot
be exercised against a production runtime until an attempt-aware provider is
implemented and qualified. It does not retry or re-execute an uncertain
attempt, add a new service, or promise exactly-once external execution.

## Invariants

1. Recovery targets one immutable `ToolRun`, domain-effect link, effect, and
   sandbox attempt. Workspace, tool request, parent operation, reservation,
   task/agent ownership, tool definition, and sandbox profile identities must
   agree with their durable records.
2. Only a provider observation in `completed` state with the exact attempt
   identity and a matching bounded result hash may produce a terminal tool
   result. `absent`, `running`, `stopped`, and `unknown` remain
   `recovery_required`; none authorizes a retry.
3. The sandbox provider is selected by the recorded backend. Recovery never
   falls back to another backend and never invokes `RunAttempt`.
4. A fresh, workspace-scoped Postgres effect lease and monotonically
   increasing fence authorize reconciliation. A stale recovery owner cannot
   mutate any durable result, link, event, artifact, or effect state.
5. The authoritative result, artifacts, tool lifecycle event, effect
   transition, and domain-link transition commit atomically. A rollback leaves
   all of them unchanged; a repeated request returns the already-committed
   result only when identity and hashes match.
6. Recovery preserves the original tool-run/task/agent identity and records the
   authenticated recovery actor separately in the appended history. It does
   not impersonate the expired execution worker. Workspace authorization is
   checked before provider lookup; cross-workspace requests fail closed.
7. Provider observations and persisted evidence contain bounded output and
   redacted metadata only. Credentials, raw environment values, and arbitrary
   provider diagnostics are not returned or logged.

## Persistence and schema

Reuse the existing `tool_runs`, `operation_effects` and effect-transition,
`domain_effect_links` and link-transition tables, event store, artifact store,
authorization audit, and effect lease tables. The identity bridge in
`internal/contracts/sandbox_execution.go` is the canonical runtime identity.
No migration is planned: the runtime attempt is derived from existing
immutable link/run data, and final results use existing columns. Add a schema
change only if implementation proves a required fact cannot be reconstructed
or durably represented without weakening identity checks.

## Authorization, ownership, and idempotency

The recovery API is an operator action, not a worker continuation. Require the
workspace-scoped operation-execution permission, authenticated actor
propagation, and a fresh effect recovery lease. The lease owner is unique per
HTTP delivery; user identity is not reused as a worker identity. Capture
expected effect/link versions and a bounded idempotency key. Repeated requests
are safe only after checking the completed result, effect, and link against the
same attempt and result hashes.

An expired task or agent worker must never resume execution through recovery.
Recovery may append the observed outcome only under the current effect fence;
the stored task and agent fences remain immutable evidence of who reserved the
attempt. Any product rule requiring a *current* task owner to accept the
result is outside this slice and must be explicit rather than inferred.

## Reuse and licensing

Reuse Fornix's sandbox identity/observation contracts, `SandboxRegistry`,
Postgres effect lease and transition stores, domain-effect links, redaction,
tool artifactization, event append, and embedding-recovery transaction shape.
No reference-repository source is copied in this slice. Existing project
licensing remains unchanged; this work adds no third-party dependency.

## Cost and operational budget

Recovery is one bounded provider lookup plus a bounded number of local indexed
reads/writes, all scoped to one workspace and effect. It performs no model
call, prompt processing, command execution, vector search, or unbounded scan.
Large output follows the existing tool artifact threshold and content-addressed
deduplication policy. Expected additional storage is one recovery transition,
one tool terminal event/observation, and any output bytes not already stored.

## Acceptance tests

- Exact completed observation atomically finalizes tool run, effect, link,
  artifacts, event, and observations.
- Repeated identical reconciliation has one durable effect; mismatched
  identity/hash conflicts.
- Stale/expired effect fences and stale expected versions fail closed.
- Absent, running, stopped, unknown, malformed, and oversized observations do
  not finalize or re-execute the attempt.
- Provider lookup is exact-backend only; no fallback or `RunAttempt` call.
- Workspace and authorization boundaries reject cross-workspace or
  underprivileged recovery before provider access.
- Injected transaction failures leave every authority unchanged and allow a
  later safe retry.
- Output remains bounded and redacted; credentials do not appear in API
  responses, events, errors, or evidence.
- Existing focused tests, race tests, all-package compile/vet, docs checks, and
  applicable smoke tests pass. Full Postgres integration acceptance requires a
  configured test database with required extensions.

## Known boundary after this slice

Until an attempt-aware non-local sandbox provider is installed and qualified,
the shipped default cannot demonstrate process recovery across a host or
container crash. The deterministic contract/store/API path can be tested with a
fake provider, but that is not evidence of a production sandbox runtime.
