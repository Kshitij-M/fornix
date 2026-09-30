# Loop 61 completion — atomic embedding recovery and adaptive retrieval

Status: foundation implemented; qualification and provider rollout remain
explicitly open

Task 61 closes the highest-risk gap left by the embedding lifecycle work: an
ambiguous provider outcome can now be resolved through the existing effect
recovery authority without allowing the specialized embedding ledger, generic
effect, domain link, audit, and event history to diverge. It also brings the
legacy memo, symbol, and RAG search routes behind a deterministic, versioned
embedding gate.

## Delivered

- Added transaction-local `AdmissionStore.UpdateEffectTx` and
  `EmbeddingCallStore.ResolveRecoveryTx` seams. Neither helper commits; the
  coordinator owns the transaction boundary.
- Added `EmbeddingRecoveryCoordinator.Finalize`, which validates provider
  proof identity, expected versions, workspace scope, and result hashes before
  composing effect, link, ledger, audit, and event writes.
- Added migration 059 for bounded recovery operation/effect/link references,
  recovery fence facts, expected versions, and causation/correlation fields.
  Constraint installation is retry-safe.
- Added migration 060 for append-only, workspace-scoped query-use attribution.
  It records cache hits, duplicate work, gate reasons, actor, usage
  classification, and cost flags without persisting raw query text or vectors.
- Added the authenticated `POST /v1/embedding-calls/reconcile` route. It is
  hash-and-version-only, uses RBAC/workspace authentication, obtains provider
  proof from the configured reconciler, and exposes only redacted hashes and
  statuses.
- Added adaptive/legacy/required/disabled embedding modes to memo, symbol, and
  RAG retrieval. Adaptive mode runs deterministic lexical/name preflight first;
  explicit semantic mode remains provider-required; disabled semantic requests
  fail closed rather than constructing an empty vector query.
- Added stale-fence, crash rollback, duplicate, concurrent replay, query-use
  conflict, redaction, and deterministic gate tests.

## Qualification evidence so far

- Offline focused packages pass: `internal/contracts`, `internal/model`,
  `internal/server`, and `internal/store`.
- A fresh tmpfs-backed `pgvector/pgvector:pg17` database passed the focused
  embedding-call, recovery-coordinator, and domain-link integration tests.
- The recovery integration test exercises migration application, rollback at
  the effect transition boundary, lease release/takeover, stale-fence
  rejection, successful finalization, duplicate replay, concurrent replay,
  and conflicting query-use attribution.

Full repository tests, race checks, `make check`, build, documentation checks,
package smoke tests, and the clean-room database matrix are still required
before this loop is release-qualified. The temporary database uses no
persistent Docker volume; the development database is not part of the test.

## Measured impact and storage

The recovery path adds one transaction-local row lock per authority, one small
append-only audit row, one typed event, and no duplicate vector payload. Query
preflight is bounded by `top_k`; a cache hit adds an indexed canonical lookup,
one idempotent query-use row, and bounded observation/cost rows. A provider
call is skipped when deterministic evidence satisfies the gate. Vector storage
continues to be governed by the existing source/query retention classes.

The focused database package completed in under one second on the local
disposable instance. This is a correctness signal, not a throughput SLO. A
768-dimensional float32 vector is 3,072 payload bytes before PostgreSQL row,
WAL, and index overhead.

## Remaining limitations

1. Ollama and OpenAI do not implement embedding outcome reconciliation in this
   repository. An uncertain call remains `recovery_required`; no exactly-once
   remote execution is claimed.
2. The compatibility `EmbeddingGateway.Reconcile` path still exists for
   legacy in-process callers. It is not the production server path and should
   be removed or made explicitly test-only in a future compatibility cycle.
3. Legacy ranking SQL still uses the database clock for recency weighting in
   memo, symbol, and RAG result scoring. The unified retrieval planner remains
   deterministic; full request-reference-time propagation is the next retrieval
   consistency task.
4. Provider pricing and measured usage are only as accurate as the provider
   response. Missing provider usage is recorded as estimated, never exact.
5. Process restart, live-provider reconciliation, high-concurrency load/soak,
   backup/restore, signer rotation, and production catalog distribution remain
   part of Issue #40 qualification.

## Next task prompt

```text
Task 62 — Complete deterministic retrieval consistency and embedding cost
qualification.

Read the chats directory, AGENTS.md, docs/00-fornix-foundation.md,
docs/14-production-readiness-qualification.md,
docs/111-universal-production-roadmap-status.md,
docs/143-embedding-reconciliation-and-query-lifecycle-foundation.md,
docs/145-embedding-recovery-adaptive-retrieval-foundation.md, and
docs/146-loop-61-completion.md.

First inspect every legacy memo, symbol, and RAG query path and the unified
retrieval planner. Replace implicit database-clock ranking with an explicit
bounded request reference time, preserve stable tie-breaking, and apply all
filters to deterministic preflight before an expensive stage can be skipped.
Define the cost/usage reconciliation rules for measured, estimated, cached,
duplicate, and failed embedding work. Add provider pricing configuration only
through existing bounded policy/configuration seams; never persist prompts,
credentials, or raw vectors in usage records.

Make the production recovery route and provider gateway fully qualified for
duplicate, expired-lease takeover, stale-worker, cross-workspace, crash, and
redaction behavior. Decide whether the compatibility Reconcile method should
be removed, deprecated, or made test-only, and document the decision.

Add deterministic API/retrieval integration tests proving identical requests
produce identical ordering, gate reasons, usage classification, cost ledger
entries, and context hashes. Add fresh/existing Postgres migration tests,
race tests, bounded load measurements, smoke coverage, CI/Make targets, and
update the universal roadmap with measured limitations. Keep Postgres as the
only authority and do not add a broker, Redis, NATS, object store, or LLM
orchestration framework.
```
