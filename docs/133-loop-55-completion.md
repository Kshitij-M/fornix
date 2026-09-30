# Loop 55 completion — durable effect dispatch boundary

Status: implemented as a qualified vertical slice on the current feature
branch. This is not a declaration that every provider or domain adapter is
production-ready.

## What changed

- Added `internal/effectdispatch`, a provider- and domain-neutral dispatcher
  that requires a normalized operation, authoritative plan step, durable
  admission, operation attempt, effect reservation, dispatching transition,
  live authority validation, adapter invocation, and fenced reconciliation.
- Added exact attempt/effect/request/reservation identity fields to
  `contracts.EffectAuthority`.
- Added workspace-scoped provider idempotency uniqueness in migration 053.
- Added dispatchable-admission validation. A denied or unresolved approval
  cannot cross the adapter boundary. The incident workflow mirrors its
  already-durable workflow approval into a generic approval record, and the
  dispatcher verifies that Postgres-owned approval state before dispatch; no
  caller-controlled approval boolean is trusted.
- Allowed generic admission to bind to a capability represented by an
  authoritative multi-step operation plan, while retaining operation, target,
  workspace, and actor checks.
- Migrated the generic HTTP effect path and the fake incident remediation
  workflow to the dispatcher. The workflow passes its existing operation lease
  and task fence into the executor; it does not create a second lease system.
- Added reserved-state recovery. A duplicate delivery may resume only from
  `reserved`, where no dispatch intent was committed. A `dispatching`,
  `dispatched`, acknowledged, terminal, or recovery-required effect is
  replayed or reconciled without a blind second provider call.
- Added final live-authority validation after provider return and before
  durable effect/result finalization.
- Added safe uncertain-outcome classification that preserves a machine-readable
  cause without exposing provider payloads or credentials in the returned
  error text.

## Qualification evidence

Against the disposable Postgres instance configured through
`FORNIX_TEST_PG_DSN`:

| Check | Result |
| --- | --- |
| Fresh migration application through migration 053 | Passed |
| Dispatcher reservation-before-invocation | Passed |
| Duplicate delivery | Passed; invoker called once |
| Concurrent duplicate delivery | Passed; invoker called once |
| Approval-pending dispatch | Passed; caller cannot spoof approval and invoker is not reached |
| Stale operation fence | Passed; invoker not reached |
| Crash before dispatch-intent commit | Passed; reserved effect resumed safely |
| Possible provider dispatch / unknown outcome | Passed; second delivery did not invoke again |
| Incident approval, workflow completion, receipt, and replay | Passed |
| Focused Postgres packages (`effectdispatch`, `server`, `store`, `incident`) | Passed |
| Offline contract, policy, workflow, and dispatcher tests | Passed |

The dispatcher adds multiple bounded Postgres commits around one external
effect. It intentionally does not hold a database transaction open while a
provider runs. Exact latency and row-growth measurements still need a repeatable
capacity run on CI hardware; the current tests qualify correctness, not a
production SLO.

## Explicit limitations

1. Admission, attempt reservation, effect reservation, dispatch intent, and
   final operation-result recording are separate Postgres transactions. They
   are individually idempotent and fenced, but not yet one cross-store
   transaction. The external call can never be made atomic with Postgres.
2. Repository-change application, tool execution, and model calls retain their
   specialized durable ledgers. They reserve before their own external
   boundary and preserve at-least-once semantics, but they are not yet linked
   to a generic `operation_effects` row.
3. A process crash after a provider accepts an effect and before Fornix learns
   the result still requires provider-specific reconciliation. Fornix does not
   claim exactly-once remote execution.
4. The local tool executor is not a kernel sandbox, and the fake incident
   connector does not contact a real monitoring system. Production adapters
   still need live provider conformance, verification, compensation, and
   credential-rotation qualification.
5. The public adapter SDK still exposes lower-level execution interfaces. The
   production server registry rejects unsafe effectful composition, but an
   eventual external SDK must make the shared dispatcher the only supported
   effectful entry point.
6. A long-lived database created from an earlier, incompatible migration
   history fails closed on checksum mismatch. The migration runner does not
   rewrite applied history; operators must use the supported upgrade/reseed
   procedure before running the dispatcher smoke against that database.

## Next task prompt

```text
Task 56 — Unify domain ledgers with the durable effect dispatcher.

Read the chats directory, AGENTS.md, docs/00-fornix-foundation.md,
docs/14-production-readiness-qualification.md,
docs/128-effectful-adapter-authority-foundation.md,
docs/132-durable-effect-dispatcher-foundation.md, and
docs/133-loop-55-completion.md. Study the current RepositoryChangeService,
Tool Executor/ToolRunStore, Model Gateway/ModelCallStore, AgentLoop, MCP
bridge, and effect dispatcher. Re-read the relevant Orloj execution engine,
DeepSeek Harness prepared-call and no-retry-after-content patterns,
agentmemory checkpoint/recovery patterns, and FornixDB immutable artifact and
cost-accounting patterns without copying incompatible source.

Write a feature note before coding. Add the smallest safe cross-reference
contract or adapter around each specialized ledger so repository changes,
tool runs, model calls, agent-run steps, and MCP mutations carry a generic
operation/effect identity, provider idempotency metadata, actor/workspace/
causation/correlation references, and exact operation/task/credential fences.
Do not duplicate a second effect ledger. Preserve each domain record as the
authoritative detail while making the dispatcher reservation the common
pre-dispatch gate. Add transactional link creation, bounded reconciliation,
crash/restart tests, concurrent duplicate tests, stale-fence tests, replay
tests, and workspace-isolation tests. Require provider-specific reconciliation
after possible remote acceptance, never claim exactly-once external execution,
and keep Postgres as the only authority. Update HTTP, CLI, MCP, runbooks,
smokes, CI, measurements, and limitations. Do not add a broker, Redis, NATS,
LLM framework, or new infrastructure.

As a compatibility precondition, qualify the upgrade path for databases that
were created before the current operation migration history. Never rewrite a
recorded checksum or silently accept schema drift; use a forward-only repair
migration or an explicit reseed procedure with backup and operator evidence.
```
