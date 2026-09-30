# Loop 60 completion — embedding recovery and query-vector lifecycle foundation

Status: partial completion; fail-closed provider proof and bounded query
lifecycle are implemented, while cross-authority recovery finalization and
adaptive legacy retrieval gating remain open
Scope: universal transformation / provider-effect authority
Migrations: 058

## Outcome

Fornix now has a typed recovery boundary for ambiguous embedding calls and a
bounded lifecycle for derived query vectors. A provider can resolve an
ambiguous call only through an explicit `EmbeddingReconciler` capability using
hash-only identity and a provider request identity. The gateway never accepts
an operator-supplied vector and never retries generation implicitly. Ollama
does not implement the capability, so an uncertain Ollama call remains
`recovery_required`.

Query embeddings are classified separately from authoritative source
embeddings. Concurrent workspace-local cache misses serialize on a bounded
PostgreSQL advisory key. Successful query vectors can be reused across the
memo, symbol, and RAG query source kinds when provider/model/source identity
matches. Failed or expired calls do not poison future explicit request
identities. A bounded sweep expires only unreferenced query vectors, retaining
source/vector hashes, provenance, and a tombstone.

## Delivered

- Added `EmbeddingReconciliationRequest` and `EmbeddingReconciliationResult`
  contracts with workspace, request, provider, model, source-hash, actor,
  session, task-fence, and provider-request identity.
- Added the `EmbeddingReconciler` provider capability and a separate
  `EmbeddingReconciliationEffectRunner` seam. Normal embedding generation and
  outcome reconciliation cannot share an effect identity accidentally.
- Preserved provider request IDs from typed failures and validated provider,
  model, request, source, vector, and canonical vector hashes before recovery.
- Added migration 058 with query retention metadata, explicit `expired`
  tombstones, append-only reconciliation audit rows, workspace RLS, integrity
  constraints, and an append-only mutation guard.
- Added `ResolveRecovery` and bounded `SweepExpiredQueryVectors` store APIs.
  Recovery and audit commit together in the specialized ledger transaction;
  expiration is dry-run capable, bounded, idempotent, and attachment-safe.
- Added cross-route query-vector reuse with PostgreSQL advisory-key
  serialization. Raw query text remains in memory and is absent from durable
  evidence and hashes except through its content identity.
- Added unit, gateway, Postgres lifecycle, reconciliation, deduplication,
  retention, tombstone, duplicate, and attachment tests.
- Updated public architecture, roadmap, and limitation documentation.

## Qualification evidence

The feature worktree passed:

- `go test ./... -count=1` offline.
- `go test ./... -count=1` against a fresh tmpfs-backed
  `pgvector/pgvector:pg17` database with migrations through 058.
- `go test -race ./... -count=1`.
- `make check` including tests, vet, Python compilation, Markdown checks, and
  shell syntax checks.
- `make build`.
- `scripts/test/v0.36-package-smokes.sh`.
- `git diff --check`.

The qualification database used no persistent Docker volume and was removed
after the run. The persistent development database was not used or modified.
Go caches and Docker build cache were cleaned before qualification commands.

## Measured impact

The query reuse path performs one transaction-local advisory lock, one indexed
source/provider/model lookup, and either a durable insert or a replay read.
Recovery locks one embedding row, appends one small audit row, and updates the
specialized ledger. Expiration uses a bounded ordered `FOR UPDATE SKIP LOCKED`
selection followed by one update per candidate; it does not scan or index
vectors for ANN retrieval.

The focused Postgres embedding store tests completed in approximately 0.89s on
the local disposable database; the full Postgres package completed in
approximately 11.64s. These are correctness measurements, not throughput
claims. A 768-dimensional float32 vector is 3,072 payload bytes before
PostgreSQL, WAL, and row overhead. Expiration releases that vector payload but
retains a small hash/tombstone audit record.

## Remaining limitations

- The production recovery coordinator does not yet atomically finalize the
  generic operation effect, domain-effect link, specialized embedding ledger,
  and recovery audit in one SQL transaction under the existing effect-recovery
  lease. The production effect runner therefore fails closed unless its
  reconciliation boundary is explicitly configured.
- The reconciliation store transition is not an authenticated HTTP/operator
  API yet. No raw vector or source text endpoint should be added; the future
  API must use the existing workspace RBAC and effect-recovery lease.
- Query cache usage attribution, cache-hit metrics, provider pricing, and
  measured-versus-estimated embedding cost are not complete.
- Legacy memo, symbol, and RAG semantic routes still invoke embedding before
  deterministic lexical/name preflight. The unified retrieval planner remains
  model-free and gated; endpoint-level adaptive rollout is still required.
- Ollama and OpenAI do not provide an embedding reconciliation implementation
  in this repository. No exactly-once remote embedding execution is claimed.

## Next task prompt

```text
Task 61 — Build the atomic embedding recovery coordinator and adaptive query
retrieval gate.

Read the chats directory, AGENTS.md, docs/00-fornix-foundation.md,
docs/14-production-readiness-qualification.md,
docs/136-universal-domain-effect-dispatch-foundation.md,
docs/138-agent-run-recovery-authority-foundation.md,
docs/140-embedding-call-authority-foundation.md,
docs/142-embedding-target-attachment-foundation.md,
docs/143-embedding-reconciliation-and-query-lifecycle-foundation.md, and this
completion note.

Add transaction-local admission/effect and domain-link recovery APIs. Use the
existing effect recovery lease and monotonically fenced ownership; never add a
second embedding lease. Finalize the generic effect, domain-effect link,
embedding ledger, reconciliation audit, and typed event atomically. Add an
authenticated workspace/RBAC operator surface that accepts only hashes,
provider proof identity, expected versions, and bounded idempotency metadata.
Never accept raw text, credentials, or an arbitrary vector. Keep Ollama
fail-closed.

Then add an adaptive retrieval orchestrator for memo, symbol, and RAG routes:
run deterministic structured/lexical/name stages first, invoke the embedding
gateway only when a measurable confidence/budget gate requires it, reuse the
workspace query cache safely, attribute cache hits and provider cost per
actor, and preserve legacy explicit semantic-mode behavior behind a versioned
flag. Add concurrency, stale recovery-fence, crash, cross-workspace, redaction,
cost, cache-hit, quality-regression, and replay tests. Keep Postgres as the
only authority and do not add brokers, Redis, NATS, object storage, or an LLM
orchestration framework.
```
