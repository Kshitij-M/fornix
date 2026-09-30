# Agent tool resume and path-containment hardening

Status: implementation plan for the next Issue #40 production-hardening slice.

## Problem

The current tool path has three related boundary gaps. A read-only path
argument whose final component does not exist can bypass symlink resolution
failure handling; a resumed agent run can disclose a persisted tool schema to
the model before rechecking current registration and policy; and duplicate
OpenAI tool-call IDs can collide with the agent loop's durable idempotency
identity.

These gaps do not invalidate existing registered-tool checks, executor
budgets, task/agent-run fencing, or durable run history. They do mean that the
path-containment and “revalidate before egress” claims need stronger fail-closed
behavior.

## Invariants

- A path argument advertised under `ReadOnlyWorkdir` must resolve to an
  existing path inside the canonical authorized root. If resolution is
  ambiguous or fails, the process is not invoked.
- Lexical containment is necessary but not sufficient; symlink resolution
  must not escape the authorized root, including when a requested leaf is
  missing.
- Before every model dispatch with tools, the current registry definition
  must reproduce the persisted model-visible name, description, schema, and
  definition hash, and current workspace/actor/task/session policy admission
  must allow the capability. Failure stops before provider egress.
- Tool catalog revalidation is an admission check, not a replacement for
  execution-time argument policy, approval, sandbox budget, or task/agent-run
  fencing.
- OpenAI tool-call IDs are unique within one response. A duplicate ID rejects
  the response before the agent loop can reserve colliding tool-run identities.
- All new failure paths are deterministic, bounded, redacted, and preserve
  append-only run history. No provider retry is introduced.

## Implementation scope

- Harden `internal/tool/executor.go` path handling and add a regression test
  using a symlinked directory plus a missing output leaf. Assert that the
  outside target is not created.
- Add a bounded agent-loop preflight immediately before `ModelGateway.Complete`
  that resolves each persisted tool by its stored name, recomputes the
  registered catalog entry, and runs current `ToolCatalogAuthorizer` admission
  using the run's complete scope.
- Reject duplicate normalized tool-call IDs in the OpenAI response parser and
  cover repeated IDs with different tools/arguments.
- Add focused tests for policy revocation on resume, registration drift on
  resume, and no provider/process call after rejection.

## Schema, reuse, license, and cost

- No migration, public contract change, dependency, or infrastructure is
  required. Existing run catalog hashes and provider response parsing remain
  the durable and adapter boundaries.
- Reuse the existing `ToolDefinition` normalization, `DefinitionHash`,
  `ToolCatalogAuthorizer`, and executor path checks; do not create a second
  policy authority.
- This is original Fornix code informed by the existing internal architecture.
  No third-party source is copied, so no new license obligation is introduced.
- Work is bounded by the existing maximum registered-tool catalog. Each model
  dispatch adds one registry lookup and authorization check per declared tool;
  there is no SQL query or persisted-byte increase. Duplicate-ID detection is
  linear in the provider's already bounded response call count.
- Missing path arguments may now be rejected before process start, even when
  their nearest lexical parent is inside the root. This is an intentional
  conservative rule for the path-restricted read-only contract.

## Acceptance and verification

- The symlink-plus-missing-leaf regression is rejected and creates no file
  outside the authorized root.
- Existing valid in-root path behavior and non-path tool behavior remain
  unchanged.
- A revoked capability or changed definition on a resumed run fails before
  model invocation; no model request is recorded.
- An unchanged and still-authorized catalog remains usable across model turns.
- Duplicate OpenAI tool-call IDs fail provider-response normalization; unique
  calls still parse and map to registered tool names.
- Focused agent-loop, tool, model, race, vet, formatting, docs, and existing
  offline qualification checks pass. PostgreSQL and full provider HTTP suites
  remain subject to the documented local environment prerequisites.
