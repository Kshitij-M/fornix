# Task 57 — universal domain-effect dispatch foundation

Status: implemented and qualified as a bounded universal production-
qualification slice; see [`137-loop-57-completion.md`](137-loop-57-completion.md)
for evidence and remaining limitations.

Fornix already has a durable operation authority, fenced leases, external-effect
reservations, and typed links to specialized ledgers. The remaining production
risk is composition: model calls, structured tools, repository-change
filesystem application, and agent-owned steps still enter their specialized
ledgers through different paths. A caller could therefore obtain a correct
domain record without proving that the common operation/effect boundary was
committed before the external boundary.

This slice makes the shared dispatch seam mandatory for those paths while
keeping each domain ledger authoritative for its own facts. It is an additive
composition change; it does not replace model usage, tool output, filesystem
verification, agent checkpoints, or the existing HTTP/incident paths.

## Scope and non-goals

The implementation covers one deterministic dispatch adapter for each of the
model, tool, repository-change, and agent-step boundaries. The adapter creates
or reuses a workspace-scoped child operation, attaches a one-step plan,
acquires a monotonic operation lease, and invokes `effectdispatch.Dispatch`.
The domain callback performs the provider, process, or filesystem action only
after the dispatcher has recorded admission, attempt, effect reservation, and
dispatch intent.

The slice does not claim a kernel sandbox, atomic filesystem transactions,
exactly-once remote model/tool delivery, or exactly-once crash recovery. A
provider/process/filesystem call that may have crossed its boundary remains
at-least-once and enters an explicit recovery state when its outcome is
uncertain.

## Invariants

1. Every effectful model, tool, repository-change, and agent-owned step has a
   stable child operation identity derived from workspace, parent operation or
   run identity, domain kind, and domain idempotency key.
2. The child operation, one-step plan, lease, admission decision, external
   effect reservation, and domain-effect link are workspace-scoped and
   idempotent. Repeating a command cannot create a second local effect.
3. The operation/effect reservation and domain-ledger reservation are both
   visible before the provider, process, or filesystem callback runs. Where a
   specialized store exposes a transaction seam, the link is committed with
   the domain reservation; otherwise the dispatcher link is the mandatory
   pre-dispatch reference and reconciliation remains explicit.
4. The live operation lease, task fence, agent-run ownership, credential
   lease/source facts, signed schema catalog, and controlled-egress facts are
   checked immediately before dispatch and before terminal result publication.
   A stale owner fails closed.
5. A model retry is allowed only before content is emitted and only when the
   typed failure is explicitly retryable. A tool or filesystem failure that
   may have crossed its boundary is recovery-required, never a blind retry.
6. Domain ledgers remain authoritative: `model_calls` owns usage and provider
   facts, `tool_runs` owns bounded output and approval, change applications own
   packet and tree verification, and agent runs own checkpoints and state
   transitions. Generic links contain hashes/references, never raw payloads.
7. Duplicate delivery returns the existing child operation/effect/domain
   identity and replays its durable result. It never calls a provider or
   process a second time when the local effect is already dispatching or
   terminal.
8. Unknown external outcomes are represented as `recovery_required` with
   provider/process/filesystem identity and response/result hashes when known.
   Reconciliation is bounded and provider/domain-specific; it must not invent
   success.
9. Raw prompts, credentials, argv, environment values, file bytes, and
   provider/process payloads never enter operation contracts, domain links,
   events, logs, metrics, or errors. They remain behind the existing redacted
   evidence/artifact boundaries.
10. Replay consumes generic operation/effect/link and specialized ledger
    history. Replay never resolves credentials or invokes a provider,
    executable, or filesystem mutation.

## Composition

```text
domain request
  -> stable child operation + one-step plan
  -> fenced operation lease
  -> admission + generic effect reservation
  -> domain-effect link
  -> live authority validation
  -> provider/process/filesystem callback
  -> domain result/evidence/artifact
  -> generic effect reconciliation + replayable link
```

Agent loops use the same seam for each dynamic model/tool step. The agent-run
checkpoint remains the orchestration boundary; a child operation is a bounded
effect identity, not a replacement for the run state machine.

## Schema and migration strategy

No new effect ledger is introduced. Migration 054 remains the cross-reference
authority. If implementation requires a child-operation parent reference or a
domain-specific recovery marker, add a forward-only additive migration with
nullable fields and bounded checks; never rewrite existing migration
checksums or manufacture missing historical links. Existing specialized rows
remain readable and are not backfilled by guessing external identities.

## Reuse and licensing

The slice reuses `OperationStore`, `AdmissionStore`, `effectdispatch.Dispatch`,
`DomainEffectLinkStore`, existing specialized stores, and the existing model,
tool, change, and agent interfaces. Its execution ordering is informed by
Orloj's fenced execution, DeepSeek Harness's prepared-call/no-retry-after-
content rule, agentmemory's lease/checkpoint recovery, and FornixDB's
immutable evidence model. These are architectural references only. No
Kronaxis-fabric source is copied because it is BSL 1.1. Fornix remains MIT
licensed and no broker, Redis, NATS, object store, or LLM framework is added.

## Cost and operational budget

Each effectful domain step adds a bounded operation/attempt/effect/link write
and indexed reads for live authority. It adds no network hop or background
service. Qualification must measure p50/p95 pre-dispatch and finalization
latency, SQL statements and pool wait, rows/bytes per child effect, duplicate
hit rate, and recovery backlog. The target is bounded database work that is
small compared with the external provider/process/filesystem action; these
are measurements, not production SLOs.

## Acceptance tests

- fresh and upgrade migrations preserve existing history;
- model, tool, repository-change, and agent-step callbacks cannot run before
  generic effect reservation and link binding;
- duplicate delivery creates one child operation/effect/domain link and one
  external invocation;
- concurrent delivery preserves one local effect and stable hashes;
- stale operation, task, agent-run, credential, schema, and workspace facts
  fail closed before the callback;
- crashes before dispatch leave a recoverable reservation with no external call;
- crashes after possible external dispatch do not blind-retry and are visible
  to bounded reconciliation;
- model retries stop after content emission and obey the durable budget;
- domain ledgers remain transactionally consistent with artifact/evidence
  references;
- replay from zero and from a checkpoint is deterministic and side-effect
  free;
- HTTP, CLI, agent, and MCP mutation paths use the same composition layer;
- redaction, workspace isolation, unit, integration, race, smoke, CI, and
  documentation checks remain green.

## Known limits after this slice

Filesystem effects can still be partially applied and require verification;
remote providers can still accept a request before the local process learns
the outcome; local-process execution is not a kernel sandbox; and a complete
bounded reconciler for every provider/domain remains a later qualification
task. Fornix must continue to disclose these limits in its public API and
operator documentation.
