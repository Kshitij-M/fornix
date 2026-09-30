# Loop 59 completion — scoped embedding-call authority

Status: implemented foundation; qualification limited to the embedding authority

Scope: universal transformation / provider-effect authority

Migrations: 056, 057, 058

## Outcome

Fornix embeddings now have a typed, workspace-scoped execution boundary. The
server, ingestion callback, memo paths, symbol paths, RAG paths, and bounded
memo backfill no longer call a provider through an unscoped `embed(text)`
shortcut. They construct a request with workspace, actor, source identity,
provider/model, content hash, and deterministic idempotency identity. The
embedding gateway reserves a durable call, routes the external call through
the shared child-operation/effect dispatcher, validates the vector, and stores
the result for replay.

## Delivered

- Added [embedding contracts](../internal/contracts/embedding.go) for scoped
  requests, bounded budgets, usage, failures, terminal states, replayable
  vectors, and canonical float32 vector hashes.
- Added migration 056 with `fornix.embedding_calls`, workspace RLS, request and
  idempotency uniqueness, bounded evidence/metadata, fixed 768-dimensional
  vector integrity, task-fence pairing, lifecycle constraints, and recovery
  indexes.
- Added [EmbeddingCallStore](../internal/store/embedding_calls.go) with
  transactional `Start`, `Attempt`, `Finish`, and `Get` operations. Duplicate
  calls resolve to one record; conflicts fail closed; successful vectors can
  be replayed without a provider call.
- Added [EmbeddingGateway](../internal/model/embedding_gateway.go) with
  deterministic provider lookup through the explicit `EmbeddingProvider`
  capability, input/time/dimension budgets, redacted evidence, provider
  failure classification, and explicit `recovery_required` handling for
  uncertain outcomes. Chat-only providers cannot be selected accidentally.
- Removed the direct-provider fail-open path: production embedding calls now
  require both the durable call recorder and the fenced external-effect
  runner. Successful replay verifies the stored canonical vector hash.
- Added explicit stale-call recovery. Pending/running calls are never retried
  by timeout or duplicate delivery; a bounded operator/reconciler transition
  moves them to `recovery_required`.
- Added current task-lease validation to embedding attempt and terminal paths
  for task-bound requests, including owner, monotonic fence, workspace, release,
  and expiry checks.
- Added `embedding_call` to the domain-effect vocabulary and routed the
  provider callback through the existing fenced child-operation dispatcher.
  Generic operation/effect rows contain hashes and references; the specialized
  embedding ledger remains the vector detail authority.
- Migrated ingestion to pass typed chunk identities, job actor/task/session
  scope, task fences, causation/correlation, and bounded embedding budgets.
- Migrated memo, RAG, symbol, query, and memo-backfill paths to the typed
  gateway. Backfill is now workspace-scoped and bounded to 128 records by
  default (with a hard 512-record request ceiling).
- Added contract, gateway, durable-store, duplicate, replay, recovery,
  concurrent-start, vector-integrity, cross-workspace, and budget tests.
- Added typed, hash-only provider reconciliation contracts. A provider must
  implement the explicit `EmbeddingReconciler` capability and return the
  original provider request identity, source hash, and complete vector. The
  production Ollama provider intentionally does not implement this capability.
- Added migration 058 with reconciliation-attempt audit rows, explicit query
  retention metadata, an `expired` tombstone state, and bounded unreferenced
  query-vector sweeps. Query cache reservations are serialized by a bounded
  PostgreSQL advisory key and reuse one successful workspace-scoped vector.

## Qualification evidence

The following checks passed on the feature worktree:

- `go test ./... -count=1` (offline).
- Disposable `pgvector/pgvector:pg17` database with migrations through 056:
  `TestEmbeddingCallStoreLifecycleAndReplay` and
  `TestEmbeddingCallStoreConcurrentStartDeduplicates` passed.
- Disposable database `go test ./... -count=1` passed, including existing
  Postgres integration tests and the new migration/store tests.
- `git diff --check` passed.

The focused gateway/store tests also cover the required durable effect seam,
canonical replay hash validation, and stale in-flight recovery. The new
Postgres stale-recovery case is included in the disposable qualification
suite when `FORNIX_TEST_PG_DSN` is supplied.

The persistent development database was not used or modified. The temporary
Postgres container and downloaded qualification image were removed after the
run. Go build and test caches were cleaned before qualification commands.

## Cost, latency, and storage impact

The new first-call path performs one scoped ledger insert, one attempt update,
one generic child-effect/typed-link sequence, one provider request, and one
terminal ledger update. A successful replay is one indexed Postgres read and
zero provider calls. Local disposable lifecycle/store tests completed in
approximately 0.31 seconds for the two new cases; this is a correctness
qualification measurement, not a production throughput benchmark.

A 768-dimensional vector contains 3,072 bytes of float32 payload plus the
pgvector header and row/index/WAL overhead. The ledger intentionally stores a
replayable copy while existing memo/chunk/symbol projections retain their
derived vectors, so storage is temporarily duplicated. No ANN index is added
to the ledger. High-volume query embeddings will require later retention or
content-addressed result compaction; that work is intentionally deferred.

## Explicit semantics and limitations

- External embedding execution remains at-least-once. Ollama does not provide
  a provider idempotency guarantee through this adapter, so a transport
  ambiguity becomes `recovery_required`; it is never blindly retried.
- Generic effect rows and the specialized embedding row are sequenced through
  the existing dispatcher but are not one shared SQL transaction. A crash
  between those authorities is recoverable through the existing link/recovery
  state, not claimed to be impossible.
- The retrieval store remains model-free. Callers must obtain a query vector
  through the gateway only when their deterministic retrieval gate justifies
  the cost; this loop does not make vector retrieval unconditional.
- The OpenAI adapter remains chat-only for embeddings. Ollama and the fake
  provider remain the supported embedding implementations in this slice.
- Provider reconciliation is fail-closed unless the effect runner also exposes
  the distinct reconciliation boundary. A future recovery coordinator must
  atomically finalize the generic operation effect, domain-effect link, and
  specialized embedding ledger under a recovery lease; this slice does not
  claim those authorities are already one SQL transaction.
- Legacy memo, symbol, and RAG semantic routes still perform their existing
  provider call before lexical preflight. The unified retrieval planner's
  gate remains correct, but the endpoint adapters need a separate adaptive
  rollout and regression qualification.
- Existing legacy direct SQL target writes remain subject to their prior
  authorization/RLS qualification boundaries; this loop fixes the provider
  call identity and the highest-risk unscoped backfill, not every legacy SQL
  handler’s transaction composition.

## Next task prompt

```text
Task 60 remainder — Complete provider-specific embedding reconciliation and
adaptive query-embedding lifecycle.

Read the chats directory, AGENTS.md, docs/00-fornix-foundation.md,
docs/14-production-readiness-qualification.md,
docs/136-universal-domain-effect-dispatch-foundation.md,
docs/138-agent-run-recovery-authority-foundation.md,
docs/140-embedding-call-authority-foundation.md, and this completion note.

Implement an authenticated recovery coordinator that uses the existing effect
recovery lease and one Postgres transaction to finalize the generic effect,
domain-effect link, embedding ledger, and reconciliation audit. Keep the
provider-specific proof requirement and never blind-retry Ollama. Migration
058 now provides bounded query-vector retention and source/model deduplication
at the ledger layer; add explicit actor/cost usage attribution, adaptive
preflight for legacy semantic endpoints, and regression tests without adding
a broker, Redis, NATS, object store, or new infrastructure. Preserve replay,
fencing, RLS, actor scope, redaction, and at-least-once external semantics.
```
