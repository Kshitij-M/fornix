# Loop 9 completion — tool registry, policy, approval, and execution

Status: complete for the bounded local-process vertical slice.

## Delivered

- Typed `ToolDefinition`, `ToolRequest`, `ToolResult`, `ToolFailure`,
  `ApprovalRequest`, `ApprovalDecision`, `SandboxProfile`, `ToolPolicyRule`,
  and `ToolRun` contracts with stable hashes and redacted evidence.
- Explicit case-normalized registry with collision-safe aliases and stable
  listing.
- Deny-by-default policy matching workspace, actor, task, session, tool, and
  capability, with deterministic priority/specificity ordering and bounded
  policy-level sandbox limits.
- Automatic, pre-approved, interactive, and denied modes. Interactive grants
  are durable, one-shot, request-hash-bound, expiry-aware, and auditable.
- Migration `014_tool_execution.sql` with workspace-scoped run and approval
  ledgers, idempotency uniqueness, lifecycle indexes, and status constraints.
- Structured argv local execution with no implicit shell, no inherited
  environment, allow-listed environment keys, bounded timeout, arguments,
  working directory, output, and evidence. Known shell executables are
  rejected from registration.
- Atomic Postgres lifecycle transitions and typed append-only events for
  request, approval request/decision, start, success, failure, and denial.
- Task-bound execution validates the current task lease/fence before start and
  again while finalizing. Stale workers fail closed; an external process can
  still be at-least-once if the worker dies after spawn.
- HTTP execution and approval decision routes, v0.18 smoke coverage, CI
  integration coverage, development commands, and architecture qualification.

## Verification

Executed successfully on the Docker-backed development stack:

```text
make fmt
make test
FORNIX_TEST_PG_DSN=... make test
make check
FORNIX_URL=... FORNIX_KEY=... FORNIX_TOOL_PG_DSN=... scripts/test/v0.18-tool-smokes.sh
```

The unit suite covers registry collisions, default deny, workspace/capability
scope, literal argv handling, timeout/output limits, request redaction,
duplicate replay, interactive approval continuation, stale-fence rejection,
and in-flight duplicate fail-closed behavior. The Postgres suite covers
concurrent reservation, terminal replay, one-shot approval auditing, stale
task-fence rejection, workspace-scoped cleanup, and lifecycle event count.

## Measured cost and performance

Measurements were taken on 19 August 2026 against the local Docker Compose
service and Postgres instance, so they are development-stack observations, not
capacity guarantees:

- Fifty sequential authenticated `fornix.echo` HTTP calls, each with a new
  durable idempotency key, measured p50 `2.793 ms`, p95 `4.290 ms`, and max
  `16.586 ms`, including three Postgres lifecycle transactions and local
  process spawn. The result excludes network distance and real tool runtime.
- A successful accepted run appends three immutable control events and their
  evidence records, plus one bounded mutable `tool_runs` row. A terminal
  duplicate performs a reservation/read transaction and does not spawn a
  process or append another lifecycle event.
- The local database relation sizes after the smoke/performance sample were
  `tool_runs=80 kB`, `tool_approvals=64 kB`, and 17 tool lifecycle events in
  the shared development database. These include relation/index page minimums
  and are not per-row capacity estimates.
- Default caps are 30 seconds, 256 KiB stdout, 256 KiB stderr, 64 argv
  entries, 16 KiB per argument, 64 environment entries, 32 KiB environment
  bytes, and 64 KiB evidence. Callers can tighten but not raise them.

## Remaining limitations

- Policy administration is in-process; tenant-aware identity, roles, scoped
  credentials, and durable policy management remain future work.
- The local process backend bounds the described inputs and outputs but cannot
  provide complete kernel-level filesystem or network isolation on every host.
  A sandbox provider seam and host enforcement qualification are still needed
  before untrusted tools can be enabled.
- Large tool outputs still require the future content-addressed artifact
  plane; this slice retains only bounded Postgres evidence.
- A process crash after spawn and before terminal persistence can cause
  upstream duplicate work. No exactly-once external process guarantee is
  claimed.
- There is still no model-driven agent loop, tool-call planner, background
  worker, cancellation API, or general model-run orchestration surface.

