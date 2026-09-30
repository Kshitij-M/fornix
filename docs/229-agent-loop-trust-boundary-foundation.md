# Agent-loop trust-boundary foundation

Status: implemented locally; Task 102 completion is in
[`230-loop-102-completion.md`](230-loop-102-completion.md), with trusted tool
schema binding added in [`231-agent-tool-schema-authority-foundation.md`](231-agent-tool-schema-authority-foundation.md).

## Problem

The agent loop currently writes retrieved context into model history as a
`system` message. Evidence and retrieved records may contain user-controlled or
otherwise adversarial instructions; they must never inherit the authority of
trusted control instructions. Separately, a model can request a globally
registered tool even when that tool was not included in the run's persisted
tool catalog. The catalog is sent to the model but is not currently an
execution-time allowlist.

## Invariants

- Retrieved context is data, not policy. It is appended as a non-privileged
  `user` message with its deterministic context hash, source references, and
  evidence hashes intact. Neither delimiters nor this message role are claimed
  to eliminate prompt injection; authorization and effect controls remain
  independent.
- Every model-returned tool function name must exactly match a normalized name
  in that run's persisted `AgentRun.Tools` catalog before the global registry
  is consulted or `ToolInvoker.Execute` can be reached.
- At run creation, every declared function name must resolve to a registered
  tool alias. This preflight prevents paying for a model call that could only
  terminate with a tool-registration failure.
- Declaring a tool is not authorization. A declaration only narrows the
  maximum capability set. Registration, workspace/actor policy, capability
  admission, approval, task/agent-run fencing, budgets, and durable effect
  reservation remain required.
- Tool catalog identity is immutable for the lifetime of a durable run. The
  Postgres commit boundary compares the proposed catalog to the locked stored
  catalog and rejects attempts to replace it during a transition.
- Approval, retry, and recovery resume through the same tool-dispatch guard;
  none is a bypass path.
- Rejections fail closed before tool execution. Replays remain deterministic,
  workspace-scoped, and append-only.

## Schema, API, and compatibility

No migration or new public field is required. `AgentRunRequest.Tools` already
defines the provider-visible function catalog and is persisted as part of the
run request identity. Normalize and reject duplicate case-insensitive function
names so the model/tool mapping is unambiguous. The commit store verifies that
the immutable catalog and request hash match the locked row. Existing runs
retain their stored catalog and are subject to the same exact-name check; an
empty catalog therefore authorizes no tool call.

The provider-neutral model contract permits only the existing standard message
roles. Keep the context message as `user` rather than inventing a role that
OpenAI-compatible providers cannot serialize. Use stable, explicit framing in
the message body to identify the following material as untrusted evidence.

## Recovery, authorization, and failure behavior

An undeclared call on an existing or recovered run becomes a deterministic
tool-phase agent failure with no tool execution or external effect. A newly
declared but unregistered function is rejected before durable run reservation
and any model call. A declared and registered call continues through the
existing policy/approval/tool-run and fence pipeline. Existing at-least-once
semantics for already-reserved external effects are unchanged.

## Research and reuse decisions

DeepSeek Harness keeps assembled system instructions separate from a
user-role runtime-context message and preserves ordered tool-call history.
Orloj builds a governed runtime from the agent's configured tool set and
rejects unknown tools in strict mode before invoking the adapter. Reuse these
boundary patterns, not source code. DeepSeek Harness is MIT and Orloj is
Apache-2.0; no third-party code is copied and no license notice or dependency
change is required. Kronaxis BSL source remains excluded.

## Cost and storage impact

The read-only catalog membership check is bounded by the existing maximum of
64 declared tools and adds no SQL query or infrastructure. The immutable
catalog check reuses the already-locked agent-run row; no migration or new
table is needed. The safe context framing adds a small fixed number of bytes to
the existing bounded history message. Denied tool calls may append the normal
failure event/checkpoint, but perform zero tool/model egress.

## Acceptance tests

- Adversarial retrieved text stays in a user-role evidence message; system
  messages are not synthesized from retrieval, and source/evidence identities
  remain present. Identical packs retain deterministic context/history hashes.
- A globally registered but undeclared tool fails before `Execute` and records
  a deterministic failure.
- An unknown model-returned tool fails closed before registry execution.
- An unknown undeclared tool fails closed; a declared but unregistered tool is
  rejected before run reservation or model invocation.
- A declared and registered tool still traverses the existing policy and
  approval checks and executes at most once per durable idempotency identity.
- Approval/retry/recovery resume rechecks the persisted declaration.
- Agent-run reservation reloads the same catalog, and the Postgres commit
  boundary rejects catalog mutation; stale state versions remain rejected.
- Duplicate catalog names are invalid, workspace isolation remains intact,
  and no retrieved prompt or credential enters the failure event.
- Run focused fake-provider tests, race tests, vet, docs checks, and the
  existing smokes. PostgreSQL persistence/reload cases remain DSN-gated and
  must not be described as qualified until run against a disposable database.
