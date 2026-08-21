# Tool registry, policy, approval, and execution foundation

Status: implementation note for Loop 9.

## Scope and invariants

This slice adds a bounded local-process tool boundary. PostgreSQL remains the
authority for tool-run identity, approval state, idempotency, lifecycle
events, and task-fence checks. A tool is executable only when it is explicitly
registered and an ordered policy rule matches its workspace, actor, task,
session, capability, and arguments. No matching rule means deny.

The execution request carries an immutable request hash, workspace scope,
actor/task/session references, causation/correlation IDs, and an idempotency
key. Reusing an idempotency key with a different request fails closed. A
terminal duplicate returns the durable result without starting another
process; an in-flight duplicate returns a conflict and does not guess whether
an external process ran.

The process boundary accepts a structured `argv` array only. The registered
definition fixes the executable and optional argument prefix; the request may
only supply arguments within that definition's limits. No implicit shell is
created, shell interpreters are rejected as registered executables, and the
child receives no ambient environment unless explicitly allow-listed. Tool
output, request evidence, and failure details are bounded and redacted before
durable persistence.

Task-bound calls validate the current task execution owner and monotonically
increasing fence before process start and again in the same transaction as
terminal run/event persistence. If a lease expires while an external process
is running, the process may have executed once, but the stale worker cannot
commit an authoritative result. This is intentionally at-least-once at the
external process boundary, not exactly-once execution.

## Policy and approval semantics

Policy modes are closed and deterministic:

- `automatic`: an explicit matching rule allows execution immediately;
- `pre_approved`: a durable approved grant for this exact request is required;
- `interactive`: a durable pending approval is created and execution stops;
- `denied`: the run is recorded as denied and no process starts.

Approval is one-shot and request-bound. The approval record stores the request
hash, scope, requester, decision-maker, expiry, and decision reason. Missing,
expired, mismatched, or denied approvals fail closed. Approval decisions are
auditable lifecycle events and cannot expand a tool's registered executable or
budget.

Policy rules are ordered by explicit priority and specificity. An explicit
deny outranks an allow at the same match; ties resolve deterministically by
rule ID. Rules can constrain workspace, actor, task, session, tool ID,
capability, argument prefix, environment keys, and working-directory root.
The in-process policy registry is deliberately small for this slice; durable
policy administration and tenant RBAC remain a later identity boundary.

## Migration and durable records

Migration `014_tool_execution.sql` adds workspace-scoped:

- `tool_runs`: one durable idempotent run ledger with request hash, redacted
  request/result evidence, timing, output, failure, approval, task fence, and
  terminal status;
- `tool_approvals`: one-shot pending/approved/denied/expired decisions tied to
  an exact request hash;
- supporting indexes and constraints for workspace isolation, positive
  budgets, and terminal metadata.

Tool lifecycle events use the existing append-only event store and evidence
store. Run rows are execution metadata and never replace authoritative event
history or raw source records. The event and run transition are written in
one transaction where the mutation is authoritative.

## Reference reuse and licensing

The local scan covered DeepSeek Harness tool execution, sandbox, approval,
permission, credential, and scheduler seams; Orloj tool contracts, policy
authorizer, approval grant, stable errors, CLI runtime, governed runtime, and
approval migration; agentmemory actions, leases, checkpoints, replay, cleanup,
and diagnostics.

The implementation reuses the following behaviors independently:

- DeepSeek's pre/execute/post separation, immutable final outcomes,
  fail-closed sandbox provider seam, one-shot approval vocabulary, structured
  argv, and cancellation-aware bounded execution;
- Orloj's explicit tool/capability authorization, stable failure codes,
  retryable classification, approval grant binding, and policy precedence;
- agentmemory's keyed lifecycle ownership, dependency-aware action state,
  replay/audit discipline, and fail-closed cleanup behavior.

No reference source is copied. DeepSeek Harness, ClawMem, and FornixDB are
MIT; Orloj and agentmemory are Apache-2.0; Kronaxis Fabric is BSL-1.1 and is
not a source for this slice. Fornix remains MIT. Future copied code must keep
its original notice and license.

## Cost, safety, and operational budget

Defaults are conservative: 30-second wall time, 256 KiB stdout, 256 KiB
stderr, 64 arguments, 16 KiB per argument, 64 environment entries, 32 KiB
environment bytes, and 64 KiB bounded JSON evidence. Callers may lower these
budgets but cannot exceed the registered/global caps. A successful local run
performs one reservation transaction, one process execution, and one terminal
transaction; a duplicate performs one ledger lookup and no process execution.
The expected database cost is one bounded JSONB ledger row plus three lifecycle
events per accepted run (`requested`, `started`, and terminal) and their
bounded evidence rows. There is no model call, broker, or new service in the
tool hot path.

The direct local executor can bound time, argv, environment, working
directory, and output, but cannot provide kernel-level network isolation or a
complete filesystem sandbox on every host. Definitions requiring those
guarantees are rejected in this slice. A future sandbox provider may replace
the local executor behind the same contract after measured need and a host
enforcement qualification.

## Acceptance tests

- contract normalization, request hashing, stable errors, redacted evidence;
- registry registration, alias collision, deterministic capability lookup;
- deny-by-default and workspace/actor/task/session/capability policy matching;
- automatic, pre-approved, interactive, denied, expired, and mismatched
  approvals;
- structured argv execution without shell expansion;
- timeout, output, argv, environment, and workdir budgets;
- durable duplicate reservation and terminal replay with one process effect;
- stale task fence rejection before start and before finalization;
- concurrent writers, workspace isolation, crash/in-flight recovery, and
  append-only lifecycle event ordering;
- fresh/existing migrations, unit/integration tests, smoke, CI, and measured
  latency/database/storage impact.
