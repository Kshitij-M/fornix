# Durable multi-step workflow runtime foundation

Status: implemented alpha foundation for Issue [#44](https://github.com/Kshitij-M/fornix/issues/44) on `feat/issue-44-workflow-runtime`; qualification and the multi-domain reference workflow remain follow-on work.

## Product purpose

Fornix must govern a production operation that contains more than one model
turn or repository action. A workflow may read an API, inspect a database,
ask for approval, wait for a human or callback, validate a result, and then
perform an explicitly admitted effect. The workflow runtime is the durable
coordination layer for those steps; it is not a replacement for the model
gateway, tool runtime, connector registry, task authority, or external system.

The runtime preserves the universal product promise:

> Delegate serious work to AI without losing the ability to bound,
> understand, recover, verify, and replay it.

Model reasoning is optional. A workflow plan can be fully deterministic and
can contain model, tool, connector, approval, human-input, callback,
validation, wait, and compensation steps without requiring an LLM.

## Smallest production-quality vertical slice

This loop will deliver:

- typed workflow run, step state, checkpoint, transition, wait, budget, and
  failure contracts;
- a Postgres-backed workflow state projection linked to the existing generic
  operation authority;
- deterministic dependency-aware scheduling with bounded read-only parallel
  fan-out and serialized effectful steps;
- step-level idempotency and append-only transition history;
- crash-safe checkpoint advancement in the same transaction as step state;
- explicit approval, human-input, callback, retry, cancellation, and
  recovery-required waits;
- injected step executors with a deterministic fake/replay executor for tests;
- replay from sequence zero or a checkpoint without invoking external systems;
- stable terminal and Work Receipt references without replacing authoritative
  operation, model, tool, connector, evidence, or artifact records.

The existing single-run `internal/agentloop` path remains supported. This
slice gives it a generic runtime seam; it does not remove or silently rewrite
the repository reference workflow.

## Invariants

1. **One authority.** Postgres owns operation identity, workflow current state,
   step state, checkpoints, transitions, idempotency, and lease/fence checks.
   In-memory scheduler state is only a bounded optimization.
2. **Plan identity is immutable.** A normalized DAG and policy/effect snapshot
   produce one plan hash. A new plan is a new operation; an existing run is
   never reinterpreted under a changed plan.
3. **Dependencies are explicit.** A step is runnable only when every declared
   dependency succeeded. Read-only siblings may be selected in stable ordinal/
   ID order up to a bounded fan-out. Effectful siblings are serialized by
   resource/effect key.
4. **Step commits are atomic.** A successful step result, step transition,
   workflow checkpoint, operation transition, event, and idempotency record
   commit together. A crash before commit leaves the prior checkpoint and
   step state unchanged; a crash after commit is a safe duplicate replay.
5. **Fences fail closed.** A worker must hold the current operation lease and,
   when task-bound, the current task fence. Stale workers cannot apply a step,
   checkpoint a workflow, complete a wait, or record an external effect.
6. **External effects are not exactly once.** The runtime records an explicit
   at-least-once boundary and recovery-required state. It may retry only before
   content/effect dispatch under the step policy. Compensation is an explicit
   step, never an inferred rollback guarantee.
7. **Waits are durable.** Approval, human input, timer, callback, external
   completion, retry, and recovery-required states prevent subsequent work
   until the exact bound request is satisfied.
8. **Cancellation is monotonic.** A durable cancellation prevents future
   steps. In-flight external work may remain uncertain and is surfaced as
   recovery-required rather than hidden.
9. **Budgets are hard.** Step count, parallel fan-out, retries, output bytes,
   tokens, wall time, database work, and cost are checked before admission and
   after recorded results. No step can make a budget negative or bypass the
   operation/admission boundary.
10. **Replay is inert.** Replay consumes recorded step responses, transitions,
    evidence, and artifacts. It never invokes a model, tool, connector,
    callback, network, or compensation handler.
11. **Workspace isolation is structural.** Workflow, operation, actor, task,
    session, step, evidence, artifact, capability, and wait references must
    share one workspace. Cross-workspace data fails closed.
12. **Receipts are additive.** A workflow receipt links to operation, step,
    evidence, artifact, policy, and replay hashes; it does not replace their
    authoritative histories.

## State model

```text
created → running → succeeded
                 ├→ awaiting_approval → running
                 ├→ awaiting_human    → running
                 ├→ awaiting_callback → running
                 ├→ awaiting_retry   → running
                 ├→ recovery_required → running | failed
                 ├→ cancelled
                 ├→ failed
                 └→ dead_letter
```

Step states are independently monotonic (`planned`, `ready`, `running`,
`awaiting_*`, `succeeded`, `failed`, `cancelled`, `recovery_required`). A
workflow cannot become terminal while a required step is unresolved. The
runtime derives runnable work from committed state and canonical plan order,
so a worker restart does not depend on an in-memory queue.

## Transaction and schema plan

Migration `038_workflow_runtime.sql` will add bounded workspace-scoped tables:

- `workflow_runs`: current workflow projection, immutable operation/plan hashes,
  budgets, state version/hash, wait and terminal metadata. Ownership and
  fencing are delegated to the linked generic operation lease, so there is one
  authority rather than two competing worker leases;
- `workflow_step_states`: one current row per plan step with attempt, status,
  input/output/evidence/artifact hashes, effect classification, and retry time;
- `workflow_transitions`: append-only run/step checkpoint history with prior
  state hash, next state hash, worker fence, event sequence, and command hash;
- `workflow_idempotency`: duplicate command identity bound to run/step,
  command hash, transition version, and outcome hash.

The existing `OperationStore.CreateTx` seam makes operation identity and the
initial workflow projection one transaction. Runtime step commits use the
same Postgres transaction and append an event through `EventStore.AppendTx`.
History tables are append-only. Current projections are bounded lookup
surfaces and never become the only replay input.

## Reuse and licensing

- Reuse Fornix `OperationRequest`, `OperationPlan`, `OperationStep`, operation
  leases/fences, admission/effect stores, event store, Work Receipt links,
  actor/workspace contracts, and existing model/tool/retrieval boundaries.
- Reuse the architectural ideas of Orloj execution engines/checkpoints,
  DeepSeek Harness turn/context/tool seams, agentmemory action leases and
  replay diagnostics, ClawMem recorded retrieval/action flow, and Temporal's
  durable workflow concepts. Implementations remain independent; no reference
  source is copied.
- Do not copy Kronaxis-fabric source because its BSL 1.1 license is not
  compatible with Fornix's MIT distribution. No new runtime dependency or
  infrastructure is planned.

## Cost and stability budget

- Maximum steps remain bounded by `contracts.MaxOperationSteps` (64) and the
  workflow fan-out/metadata budgets are lower than that default unless a
  caller explicitly tightens them.
- One successful step performs bounded Postgres work: lock current run,
  validate lease/fence and dependencies, reserve idempotency, append one
  transition/event, update one run row and one step row, and commit.
- Read-only parallel scheduling is bounded by a caller-supplied fan-out and
  returns steps in canonical plan order. Writes are serialized by effect and
  resource identity.
- Replay reads bounded transition history and recomputes hashes only. It does
  not incur model, connector, tool, network, or callback cost.
- Measurements will report focused latency, SQL statement/row work, history
  storage, replay throughput, and duplicate/crash behavior. They are local
  observations, not production capacity claims.

## Acceptance tests

- linear, branch, join, retry, and compensation plans normalize and hash
  deterministically;
- duplicate run/step commands return one durable outcome and one effect;
- dependency scheduling is stable; bounded read-only siblings can run in
  parallel, while effectful/resource-conflicting steps serialize;
- approval, human-input, timer, callback, and recovery waits pause and resume
  only through exact workspace/run/step/idempotency bindings;
- stale operation/task fences fail before step execution and cannot move a
  checkpoint forward;
- crashes before commit leave run/step/checkpoint/history unchanged;
- crashes after commit replay safely without repeating the executor;
- cancellation prevents subsequent steps and preserves uncertain effects;
- step, retry, output, token, byte, wall-time, SQL, and cost budgets never
  exceed their configured ceilings;
- replay from zero and from a checkpoint produce the same state/receipt hash;
- replay invokes no model, tool, connector, callback, network, or compensation
  function;
- cross-workspace plans, inputs, evidence, artifacts, approvals, task fences,
  and actors fail closed;
- existing agent loop, repository workflow, all tests, race checks, CI,
  smokes, migration checks, and documentation remain green.

## Deliberate limitations

This slice will not provide a distributed workflow scheduler, a multi-agent
swarm, implicit rollback of external systems, arbitrary user code, a broker,
Redis/NATS, Temporal, or a second database. It will not claim that a generic
workflow plan makes every domain adapter production-safe; connector-specific
authorization, verification, egress, and compensation remain adapter and
Issue #40 qualification responsibilities.
