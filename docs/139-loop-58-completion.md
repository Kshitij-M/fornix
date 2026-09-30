# Loop 58 completion — agent-run ownership and recovery authority

Status: implemented and qualified on `feat/issue-40-production-qualification`.
This is a bounded universal-control-plane slice, not a production-readiness
declaration.

## Outcome

Fornix now uses one ownership rule for both background scheduling and direct
agent-run API mutations:

```text
workspace + run
  → Postgres lease
  → monotonic fence
  → heartbeat
  → fenced agent checkpoint
  → release or expiry takeover
```

Direct HTTP create, advance, cancel, wait, and external-completion paths now
acquire a durable `AgentRunLease`. A competing owner fails closed, an expired
owner can be taken over with a higher fence, and a heartbeat failure cancels
the active callback rather than allowing a false success response.

The scheduler no longer attempts an empty workspace claim. The server supplies
a bounded, deterministic active-workspace inventory and performs one explicit
workspace-scoped claim transaction per workspace. Newly bootstrapped active
workspaces are discovered on the next polling cycle.

The production agent loop also refuses to mutate an owned Postgres run without
an explicit lease in context. This closes the pre-check gap where a budget
failure could otherwise mutate state before the normal checkpoint validation.

## Recovery seam

Migration 054 already provided an append-only domain-link transition table, so
Task 58 did not add another migration. The new typed
`DomainEffectLinkTransitionRequest` and store methods support only this
fail-closed lifecycle:

```text
linked → recovery_required → reconciled
```

Transitions require the exact observed version, workspace-scoped actor,
idempotency key, and hash-only proof or failure code. Duplicate commands replay
the existing transition. A stale expected version, conflicting identity, or
attempt to reopen a reconciled link is rejected. The immutable link row and
original effect history are never overwritten; reads project the latest
append-only transition status.

This is a reconciliation seam, not a claim that an unknown provider or process
outcome has been verified. Provider-specific model/tool reconciliation and
atomic specialized-ledger/domain-link composition remain explicitly open.

## Tests and qualification

- Offline focused suites passed for contracts, store, server, scheduler, and
  agent loop.
- A fresh disposable `pgvector/pgvector:pg16` database migrated cleanly and
  the Postgres-backed store, scheduler, and server suites passed.
- Direct lease coverage includes same-owner reuse, competing-owner rejection,
  expiry takeover, monotonic fencing, stale validation, and cleanup.
- Domain-link coverage includes recovery and reconciliation transitions,
  duplicate replay, stale-version rejection, projected current status, and
  workspace isolation.
- No persistent development database was touched. Temporary containers,
  images, Go caches, and Docker build cache were removed after qualification.

## Cost and storage impact

The direct API path adds one short lease transaction, bounded heartbeat writes
while work is active, and one release update. Scheduler enumeration adds one
bounded workspace-list query per poll and retains one Postgres transaction per
workspace claim; it does not create a cross-workspace RLS transaction. Link
reconciliation adds one append-only row per transition and no payload storage.
Exact p50/p95/p99 latency, WAL, pool saturation, and long-term storage growth
remain deployment measurements rather than local SLO claims.

## Remaining limitations

1. Embedding calls still bypass the generic effect authority and need a scoped,
   durable embedding-call ledger before vector generation can claim the same
   recovery guarantees.
2. Model, tool, and filesystem-specific reconciliation does not yet verify
   provider/process state or atomically update every specialized ledger with
   its generic link.
3. Cancellation contends safely with an active worker but does not yet create a
   separate durable cancellation intent for a worker currently holding the
   lease.
4. Process restart, live-provider idempotency, provider billing, HA Postgres,
   and high-concurrency production load remain unqualified.

## Next task prompt

```text
Task 59 — Build Fornix’s scoped embedding-call authority and provider boundary.

Read AGENTS.md, docs/00-fornix-foundation.md,
docs/13-reference-reuse-matrix.md, docs/14-production-readiness-qualification.md,
docs/24-retrieval-context-foundation.md, docs/28-model-gateway-foundation.md,
docs/136-universal-domain-effect-dispatch-foundation.md,
docs/138-agent-run-recovery-authority-foundation.md, and
docs/139-loop-58-completion.md. Study every current embedding caller,
internal/model/provider.go, the Ollama provider, model-call storage,
effectdispatch.Runtime, AdmissionStore effect leases, ingestion checkpoints,
retrieval budgets, and workspace/RBAC boundaries.

Write a feature note before coding covering request identity, workspace/actor/
task/session scope, provider idempotency, model/vector dimensions, retry and
uncertain-outcome semantics, embedding cost gates, ingestion crash recovery,
backfill isolation, schema compatibility, licensing, SQL/storage cost, and
acceptance tests.

Implement the smallest production-quality slice:

- Add typed EmbeddingEndpoint, EmbeddingRequest, EmbeddingResponse,
  EmbeddingUsage, EmbeddingFailure, and EmbeddingCall contracts.
- Add a workspace-scoped durable embedding-call ledger with request hash,
  idempotency, provider request identity, measured/estimated usage, vector
  hash, result status, and explicit recovery_required state.
- Put Ollama embedding behind a deterministic registry/gateway and preserve
  the existing memo/chunk/vector dimensions.
- Route embedding calls through the same child-operation dispatcher and typed
  domain-effect link before any provider callback.
- Require actor, workspace, causation/correlation, task owner/fence, and run
  lease scope where applicable; reject unscoped direct embedding calls.
- Make duplicate requests replay one durable call record, retry only explicit
  transient failures, and never blindly repeat an uncertain external call.
- Propagate typed scope through ingestion, retrieval, memo writes, and bounded
  backfills; fix any cross-workspace backfill path.
- Gate embedding work by provider availability, measured need, and explicit
  byte/token/cost budgets. Offline ingestion must still work without Ollama.
- Add fresh/upgrade migrations, crash/concurrency/stale-fence/duplicate/
  workspace-isolation/replay tests, CI, Make/smoke coverage, and measured
  latency/SQL/storage/recovery-backlog reports.

Keep Postgres as the only authority. Do not add a broker, Redis, NATS, object
store, LLM framework, or exactly-once external-execution claim.
```
