# Embedding Call Authority Foundation

Status: implemented foundation; hardening in progress

Scope: Task 59

Decision: embedding generation is a scoped, durable external effect rather than a direct provider shortcut.

## Why this boundary exists

Fornix already treats model calls, tool runs, agent steps, and other external work as bounded, attributable effects. Embedding generation is currently the exception: several ingestion, memo, symbol, and retrieval paths call the Ollama provider directly. That makes provider work difficult to deduplicate, audit, recover after an uncertain transport failure, attribute to a workspace and actor, and replay without accidentally invoking a provider again.

This slice introduces one provider-neutral embedding-call authority. It does not change the authoritative chunk, memo, symbol, or retrieval records. Those records continue to own their domain state; the embedding ledger records the external attempt and its resulting vector identity.

## Invariants

1. Every embedding request is scoped to exactly one workspace and authenticated actor.
2. A task-bound request must carry the task owner and fencing token. Stale task workers fail closed before provider execution.
3. The request identity is deterministic from the source content hash, provider/model, scope, and explicit request metadata. Raw text is never persisted in the ledger, event payload, evidence, logs, or errors.
4. A workspace and idempotency key can create only one durable embedding-call record. A conflicting request hash is rejected.
5. Provider output is accepted only when it satisfies the configured dimension and vector-size budget. The stored vector hash is computed from canonical float32 bytes.
6. Authoritative source rows are never overwritten by a retry or replay. Existing vector projections may be updated only through their existing domain transaction after the embedding call is durably reconciled.
7. A provider failure that may have been accepted remotely is recorded as `recovery_required`; the runtime never blindly retries an uncertain external call.
8. Replaying a successful call reads its durable vector and does not call Ollama, OpenAI, or another remote provider. The replayed vector hash must match the persisted canonical hash.
9. Provider absence or a provider without the explicit `EmbeddingProvider`
   capability is a valid deterministic outcome for offline operation. Retrieval
   and ingestion must retain their lexical/structured fallback behavior.
10. All reads, writes, recovery transitions, and disclosure paths enforce workspace isolation and actor/task scope.

## Planned schema

Migration 056 adds `fornix.embedding_calls`, an append/update ledger with:

- workspace, request, idempotency, causation, and correlation identities;
- source kind, source ID, and source content hash;
- provider/model reference and redacted request evidence;
- actor, task, session, task owner, and fencing metadata;
- running, succeeded, failed, and recovery-required lifecycle state;
- attempt count, timing, provider request ID, measured/estimated usage, and bounded failure data;
- vector data, canonical vector hash, and dimension for deterministic local replay.

The vector remains in Postgres because Postgres is Fornix's sole authority in this stage. The ledger is workspace-scoped and protected by row-level security. No raw input text is stored. Existing domain vector columns and inline compatibility fields remain unchanged until their callers are migrated through the typed gateway.

## Provider and retry semantics

The gateway resolves providers through the existing deterministic registry. The current Ollama implementation remains the default embedding provider when explicitly configured. The fake provider remains available for offline tests. A provider call is at-least-once at the remote boundary: a transport failure can mean that the remote service accepted the request. For that reason, only failures known to be local validation or pre-dispatch budget failures are safe to retry automatically. Unknown outcomes require reconciliation or a new, explicit idempotency decision.

Embedding calls do not inherit chat-completion fallback semantics. Fallback is allowed only before provider dispatch and only when the caller explicitly supplies an alternate provider. Once a provider may have received the request, the call is terminally `recovery_required` until reconciled.

## Scope propagation

The typed request carries workspace, actor, task/session references, task owner/fence, causation, correlation, provider/model, source identity, and bounded budgets. Ingestion jobs use their durable actor and task scope. Interactive memo, symbol, and retrieval queries use the authenticated request actor and workspace. Backfill queries must include workspace predicates; a global unscoped backfill is not acceptable.

## Cost and storage budget

The default request is bounded to 2,000 input bytes, a 768-dimensional vector, 30 seconds, and no implicit provider cost. The ledger adds one small durable row and one vector per unique workspace/content/provider/model identity. Duplicate submissions do not add vectors. Hash-only evidence is used for request disclosure. Large operational reports remain subject to the existing artifact thresholds; this slice does not introduce an object store or a second database.

Expected work per first call is one insert, one attempt update, one provider request, and one terminal update. Replay is one indexed ledger read and no provider call. The implementation will report these costs with focused tests and the existing SQL/latency qualification commands.

## Reuse and licensing

The implementation reuses Fornix's provider registry, typed scope contracts, effect-dispatch runtime, task fencing, Postgres migration runner, and existing Ollama/fake provider adapters. It does not copy source from Kronaxis Fabric, whose BSL 1.1 license is incompatible with Fornix's MIT distribution. The design follows the same general safety ideas already present in Orloj, DeepSeek Harness, agentmemory, ClawMem, and FornixDB without copying their implementation code.

## Acceptance tests

- Fresh and upgraded databases apply migration 056 cleanly.
- Fake-provider calls are deterministic, idempotent, and replayable from Postgres.
- Duplicate concurrent submissions produce one ledger record and one provider invocation.
- Conflicting idempotency keys fail closed.
- Workspace, actor, task, session, and stale-fence checks reject cross-scope calls.
- Input, timeout, dimension, vector, cost, and output budgets are enforced before persistence or provider dispatch.
- Authentication, quota, invalid-request, timeout, transport, and provider failures are classified without secret leakage.
- Uncertain provider failures enter recovery-required and are not blindly retried.
- Existing successful calls can be replayed without Ollama, OpenAI, or another external call.
- Ingestion, memo, symbol, RAG, and backfill paths use the scoped gateway; backfill cannot read or mutate another workspace.
- A crash before ledger commit leaves no authoritative embedding call; a crash after commit is safely replayable.
- Existing unit, race, full Postgres, package smoke, build, and documentation checks remain green.

## Deliberate remaining limitations

This vertical slice does not claim exactly-once provider execution. It does not introduce a new vector index, object store, broker, or remote reconciliation service. Stale pending/running calls now move only through explicit `RecoverStale` handling and are not retried automatically. Provider-specific idempotency support, batch embedding optimization, atomic composition of every domain vector projection with its embedding ledger transition, and retrieval-stage cost gating for legacy semantic endpoints remain follow-up work; the current qualification is therefore a ledger/gateway qualification, not a production-readiness declaration for every embedding consumer.
