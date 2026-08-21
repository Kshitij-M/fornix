# Deterministic bounded agent-loop foundation

Status: implemented vertical slice
Date: 2026-08-19

## Purpose

This slice adds the smallest durable orchestration boundary that can connect
Fornix's existing context compiler, model gateway, task fencing, and tool
runtime. It is a bounded ReAct-style state machine. It is not an autonomous
swarm, a workflow engine, or a claim of exactly-once remote execution.

Postgres remains the only authority. The append-only control-event stream is
the audit and replay input; the `agent_runs` row is a versioned checkpoint
projection containing the minimum state needed to resume without repeating a
committed model or tool effect.

## State-machine invariants

1. Every run is scoped by a non-empty workspace and has one immutable request
   identity. `(workspace_id, idempotency_key)` creates at most one run.
2. A run transition uses an optimistic `state_version` compare-and-swap. A
   stale duplicate worker cannot commit a second transition.
3. Every authoritative transition appends one typed event and updates the run
   checkpoint in the same Postgres transaction. Event history is never
   overwritten; the checkpoint is rebuildable from that history plus the
   durable model/tool ledgers.
4. A task-bound run must carry the exact current task owner and fencing token.
   The store checks workspace, owner, fence, unreleased state, and expiry
   before model admission, before tool admission, and before checkpoint
   commit. A fence expiry during an external call can still consume that
   external call; its result cannot become an authoritative run transition.
5. Model output is appended to history before tool execution. Tool calls are
   persisted as pending work before execution. Tool results are committed in
   model order, one bounded transition at a time.
6. A durable model-call or tool-run idempotency key is deterministic from run,
   turn, step, call ID, and attempt. A crash after the external effect but
   before the run checkpoint replays the durable ledger result and does not
   intentionally issue a second effect.
7. Once model content has been emitted, the loop does not fallback or retry
   that model step. Provider-level retry/fallback remains governed by the
   existing model gateway before content emission.
8. Hard limits cover turns, model steps, tool calls, context bytes, output
   tokens, wall-clock time, and cumulative cost. The loop abstains with a
   stable termination reason when a limit or required evidence is unavailable.
9. Approval, retry, cancellation, and external-wait states are explicit and
   durable. A pending approval or retry never silently runs work while a
   caller is away.
10. Replay is a pure fold over the ordered run events and canonical checkpoint
    payloads. Timing, database IDs, and latency metadata do not affect the
    replay hash.

## Loop and checkpoint semantics

The initial vertical slice uses `Advance` as one durable phase boundary:

```text
pending/running → model admission → model checkpoint
model checkpoint → pending tool admission → tool checkpoint
all tool results → next model phase
model final answer → succeeded
approval request → awaiting_approval
retryable tool/model failure → awaiting_retry
external integration → awaiting_external → external completion
explicit cancellation → cancelled
```

`Run` may call bounded `Advance` phases until the run reaches a terminal or
waiting state. A process crash before a checkpoint leaves the previous state
unchanged. A crash after a checkpoint is safe to resume: model and tool
identities point to their durable ledgers, and a duplicate transition loses
the state-version race or reuses the existing terminal record.

## Model/tool ordering

The model receives the normalized conversation history and a deterministic
catalog of registered tool capabilities. A response may contain text or
structured tool calls. Calls are executed sequentially in response order in
this first slice; this keeps database work and replay ordering simple. Tool
arguments use a strict JSON envelope containing `argv`, optional `env`, and
optional `workdir`. The executable is registered by Fornix and is never
selected through an implicit shell.

The loop records the assistant tool-call message before executing tools, then
records each model-facing tool result after the durable tool ledger settles.
Approval is owned by the existing tool approval table and is resumed by the
same deterministic tool-call identity.

## Budgets and cost policy

Defaults are deliberately conservative: 8 turns, 32 model steps, 64 tool
calls, 4 MiB context, 32,768 output tokens, 30 minutes wall-clock, and USD
10.00 cumulative model cost. Callers can lower limits but cannot exceed the
contract maximums. The loop uses provider usage when available and bounded
deterministic estimates only when a provider omits usage. Database work is
one short transaction per checkpoint plus the existing model/tool ledger
transaction; external model/tool latency is reported separately.

## Schema changes

Migration 015 adds workspace-scoped `fornix.agent_runs` with immutable request
identity, versioned state, normalized budget, bounded history/pending-tool
checkpoint JSON, task fence references, cumulative usage/cost, failure,
termination, retry, and timing metadata. The unique workspace/idempotency
constraint makes creation idempotent. Run lifecycle events are appended to
the existing control event table and mirrored evidence table.

## Reuse, licensing, and cost decisions

The design reuses architectural patterns from DeepSeek Harness (turn/step
boundaries, model-ordered tool results, cancellation and durable session
history), Orloj (bounded ReAct execution, checkpoint recovery, typed failure
classification, and tool-call authorization), and agentmemory (action
checkpointing, replay, and diagnostic lifecycle). It is an independent Go
implementation; no reference source is copied. Kronaxis BSL 1.1 code is not
used. Fornix remains under its existing MIT license.

No broker, Redis, NATS, LLM framework, or second database is introduced. The
main cost controls are deterministic context compilation before the model,
hard cumulative budgets, durable call deduplication, and no model call when a
checkpoint or tool ledger already contains the answer.

## Acceptance tests

- Fresh and existing databases apply migrations 015 and 016 cleanly without
  changing prior migration checksums.
- Identical fake-provider input produces the same terminal state and replay
  hash.
- Duplicate run creation and duplicate worker delivery produce one run,
  transition, model effect, and tool effect.
- A crash before a checkpoint leaves the previous run state unchanged; a
  crash after it is replayable and does not repeat a committed ledger effect.
- Tool calls execute in model order and a model response with no tool call
  terminates deterministically.
- Interactive approval pauses and resumes without duplicate tool execution.
- Retryable failures enter `awaiting_retry`, honor the bounded retry budget,
  and terminate deterministically when exhausted.
- Cancellation is durable and prevents subsequent model/tool work.
- External wait and completion are durable, bounded, and duplicate-safe.
- Context, token, turn, tool-call, time, and cost budgets never exceed their
  configured maxima.
- Stale task workers cannot admit or commit run work; workspace A cannot read
  or mutate workspace B.
- Replay from zero and replay from the latest checkpoint produce the same
  canonical state hash.
- Existing unit, integration, race, build, vet, CI, and smoke checks remain
  green. Measurements report transition latency, SQL work, token/cost usage,
  storage growth, replay throughput, and remaining limitations.

## Known intentional limitations

Remote model calls and local tool processes are at-least-once at their
external boundaries. The local tool backend still does not provide complete
kernel-level filesystem/network isolation. Durable policy administration,
large artifact storage, multi-agent sub-run orchestration, and a distributed
supervisor/HA worker topology remain later slices. The Loop 11 worker is a
Postgres-backed single-node pull/reconcile primitive.
