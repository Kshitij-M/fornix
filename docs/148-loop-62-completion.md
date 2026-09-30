# Loop 62 completion — deterministic retrieval consistency and cost qualification

Status: implemented and locally qualified; production deployment qualification
remains open

Task 62 closes the consistency gap between Fornix's typed retrieval planner
and its compatibility memo, symbol, and RAG routes. It also records the cost
boundary for embedding reuse without inventing provider billing data.

## Delivered

- Added bounded `reference_time` handling to memo and RAG requests. Ranking
  uses the explicit UTC value and returns it for replay/evaluation capture.
- Removed legacy database-clock recency expressions from memo and RAG ranking.
  Future-dated records receive zero age instead of gaining an unbounded
  recency advantage.
- Made semantic memo ordering use the full composite score with stable `id`
  tie-breaking.
- Made RAG ranking stable by score and chunk ID.
- Corrected RAG filter composition so type and source-path filters are
  mandatory constraints, while embedding/lexical signals remain OR-combined
  candidate conditions.
- Applied type and source-path filters to deterministic RAG preflight before an
  adaptive embedding skip can be decided.
- Kept `min_score` conservative: adaptive preflight does not skip the provider
  when the threshold requires the full composite score.
- Added a database-backed HTTP integration test covering explicit time,
  replay-stable memo output, workspace-scoped chunk filtering, and deterministic
  RAG response metadata.
- Hardened one short lease test to use a CI-safe 250ms TTL while retaining its
  expiry/takeover assertion; the previous 30ms value could expire during a
  cold Postgres round trip before the code under test ran.

## Qualification evidence

The following completed successfully on the feature branch:

- `go test ./internal/server -count=1`
- focused Postgres retrieval integration against a disposable
  `pgvector/pgvector:pg17` container on tmpfs
- `go test ./internal/server -count=1` with the disposable database
- `go test ./... -count=1` with a fresh disposable Postgres/pgvector database

The temporary database had no persistent volume. Existing user volumes were
not modified. The Task 62 integration test caught and fixed a real predicate
composition bug before the full qualification run passed.

## Measured impact

- Schema/storage impact: no migration and no new durable table.
- Retrieval impact: one bounded deterministic preflight query only for
  adaptive RAG; explicit legacy/required modes retain their existing provider
  behavior.
- Cache impact: existing canonical embedding lookup plus hash-only query-use
  attribution; no duplicate vector payload is introduced.
- Cost impact: provider calls are skipped only when the bounded gate is
  satisfied. Missing provider billing remains unknown/estimated and is never
  presented as measured cost.
- The local full database suite completed successfully; these timings are
  qualification evidence, not production SLOs. Production p95/p99, WAL,
  lock-wait, pool-saturation, and provider billing accuracy still require the
  deployment load matrix.

## Remaining limitations

1. Omitted reference time is intentionally live-time behavior. Callers must
   persist and replay the returned value for strict reproducibility.
2. Ollama and the current OpenAI path do not provide embedding outcome
   reconciliation; an ambiguous remote embedding remains recovery-required.
3. Provider-specific embedding pricing is not configured in this slice. Cost
   reports correctly distinguish known, estimated, and unknown values but do
   not claim billing precision without provider data.
4. Legacy route responses are deterministic after their inputs are fixed, but
   the unified retrieval planner remains the preferred surface for richer
   context budgets, provenance, and context-pack hashes.
5. Full production qualification remains open for hosted role-separated RLS,
   managed secret-manager integration, live connector conformance, backup/
   restore, HA, retention, load/soak, security abuse suites, and signed
   release/catalog operations. This completion note does not close Issue #38.

## Next task prompt

```text
Task 63 — Qualify the universal authority envelope across live effectful
adapters and deployment boundaries.

Read the chats directory, AGENTS.md, docs/00-fornix-foundation.md,
docs/14-production-readiness-qualification.md,
docs/111-universal-production-roadmap-status.md,
docs/145-embedding-recovery-adaptive-retrieval-foundation.md,
docs/147-deterministic-retrieval-consistency-foundation.md, and the current
production qualification runbook.

Use a disposable role-separated Postgres environment to qualify unset-context
RLS, migration rollout, connection-pool reset, workspace isolation, and
fail-closed behavior. Add live adapter conformance for HTTP/API, SQL-readonly,
repository, and the first effectful domain adapter. Verify trust/schema
catalog distribution, managed credential source-version binding, controlled
egress, exact operation/effect/credential fences, duplicate delivery, stale
worker rejection, uncertain external outcomes, and recovery replay.

Add bounded load/soak measurements and runbooks for backup/restore, retention,
provider outage, key rotation, and incident response. Do not add a broker,
Redis, NATS, object store, or LLM orchestration framework. Keep Postgres as
the authority, preserve append-only history, and report every deployment
limitation rather than calling the universal harness production-ready before
the evidence exists.
```
