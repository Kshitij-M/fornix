# Task 62 — Deterministic retrieval consistency and embedding cost qualification

Status: implemented and locally qualified
Scope: universal transformation / deterministic retrieval and embedding cost
control
Migrations: none

## Why this task exists

Fornix has two retrieval surfaces: the newer typed retrieval planner and the
older memo, symbol, and RAG HTTP routes. The planner already records bounded
plans, traces, evidence references, and context hashes. The legacy routes still
serve important compatibility traffic, so they must obey the same determinism
and cost rules. Before this task, some legacy ranking expressions used the
database clock, RAG filters could influence ranking candidates incorrectly,
and semantic ordering did not always use the full composite score.

This task aligns those routes with the universal control-plane contract. It
does not make repository work the product boundary: memos, symbols, chunks,
and embeddings are domain adapters over the same workspace-scoped request,
budget, provenance, and replay rules that other production-system adapters
use.

## Invariants

1. Ranking must be a function of the authenticated workspace, normalized
   request, authoritative records, and an explicit reference time. Ranking SQL
   must not call `now()` or another process/database clock.
2. A missing reference time is captured once at the request boundary and
   returned to the caller. A replay or evaluation must persist and resend that
   value; omitted reference time is intentionally “current time,” not an
   assertion that two independent live requests are identical.
3. Reference time is UTC, microsecond-truncated, non-zero, not before the Unix
   epoch, and not more than five minutes in the future. This bounds clock
   abuse while allowing small client clock skew.
4. Ranking ties are resolved by an immutable, stable identifier after the
   composite score. Semantic search orders by the same composite score used in
   the response, not by an unrelated distance-only expression.
5. Ranking signals are candidate conditions and are OR-combined. User filters
   such as workspace, type, and source path are constraints and are
   AND-combined. A filter can never be satisfied accidentally by a different
   ranking signal.
6. Adaptive preflight uses the same workspace, lexical query, type, and path
   filters as the bounded RAG candidate query. A minimum composite score is
   evaluated only after full ranking, so adaptive mode keeps the expensive
   stage enabled when lexical preflight cannot prove the threshold.
7. Query embeddings are workspace-scoped, request-idempotent, and attributed
   to a route and gate reason without storing raw query text, credentials, or
   vectors in the query-use ledger.
8. Provider usage is classified as measured or estimated. Missing provider
   usage is never silently called exact. Provider cost is known only when the
   provider or an explicit configured pricing policy supplies it; a zero or
   absent cost is not fabricated into a dollar estimate.
9. A cache hit creates at most one idempotent query-use attribution for its
   workspace, actor, route, and source hash. It does not create a second
   authoritative embedding call or duplicate provider cost.
10. All legacy route responses preserve the workspace, embedding mode, gate
    reason, and reference-time facts needed to reproduce a bounded decision.
11. Existing embedding recovery, fencing, redaction, and append-only history
    remain authoritative. This task does not weaken provider at-least-once
    semantics or claim exactly-once remote execution.

## API and compatibility decisions

The memo search and RAG request contracts accept `reference_time`. The symbol
route does not use recency and therefore does not need a time input. Existing
clients that omit the field continue to work; the server captures and returns
the canonical value whenever recency is part of the route's ranking.

RAG `filters.type` applies to the chunk metadata type and `source_paths` uses
the existing bounded shell-glob-to-SQL-LIKE translation. Multiple source path
patterns remain an OR within the source-path filter, while the complete path
filter remains mandatory. `min_score` remains a post-ranking threshold and is
bounded to the interval [0, 1].

The compatibility `EmbeddingGateway.Reconcile` method remains available for
small in-process recorders and tests. The production server uses the leased,
transactional recovery coordinator. Removing the compatibility method in this
task would create an unnecessary API break; its non-production exactly-once
boundary is documented rather than hidden.

## Schema and migration decision

No migration is required. The change uses existing columns and ledgers:

- request reference time is a bounded HTTP contract value;
- ranking and filter behavior are SQL semantics over existing memo/chunk
  records;
- embedding usage and cost remain in migrations 056, 058, 059, and 060;
- replay and query-use attribution continue to use existing hashes and
  idempotency keys.

Not changing an already-applied migration is deliberate. Any future need for
durable reference-time storage belongs in a new append-only retrieval-surface
or request-history migration, not in a checksum-changing edit to old files.

## Crash, duplicate, and workspace semantics

The retrieval request itself is read-only. A crash during a provider-backed
query can leave the embedding call in its existing pending, failed, or
recovery-required state; the provider recovery authority decides whether it
may be reconciled. A crash after query-use capture is replayable because the
use row and observation/cost keys are idempotent. A duplicate request reuses
the canonical embedding call and cannot create a second query-use effect.

Every SQL lookup includes workspace scope. The query-use ledger, embedding
call ledger, observability rows, and recovery authority use the same scope and
actor metadata. Cross-workspace source hashes or request IDs are not cache
keys that can escape the workspace boundary.

## Cost and storage budget

Deterministic preflight adds only a bounded indexed lexical query before an
embedding provider call. If it satisfies the top-k confidence rule, provider
work and the associated vector bytes are avoided. Cache reuse adds a bounded
canonical lookup and one small hash-only attribution row. A miss adds the
existing bounded provider call, vector storage only when a domain target is
attached, and the existing append-only usage/observation rows.

Measured input bytes and provider-reported usage are retained when available.
Fake and provider paths that do not report a billing amount remain explicitly
unknown or estimated according to their usage source. Fornix does not invent
pricing for Ollama or a future embedding provider. This keeps cost budgets
conservative and prevents a false precision claim from becoming an admission
decision.

## Reuse and licensing

- Reuse the typed embedding gateway, query-use ledger, retrieval-surface
  recorder, workspace transaction helpers, and existing route compatibility
  seams.
- Reuse deterministic ordering and bounded disclosure patterns from Fornix's
  retrieval planner instead of adding a second ranking framework.
- Study of Orloj, ClawMem, agentmemory, and FornixDB informed the gates,
  replay, provenance, and cost distinctions; no source was copied.
- No Kronaxis Fabric source is copied because its BSL 1.1 license is
  incompatible with Fornix's MIT distribution.

## Acceptance tests

- Explicit reference times produce identical memo and RAG ordering and are
  returned in the response.
- Zero, pre-epoch, and excessively future reference times fail closed.
- Memo and RAG ranking are stable under equal scores through identifier
  tie-breaking.
- RAG type and source-path filters exclude records that match only a ranking
  signal; workspace isolation remains enforced.
- RAG minimum-score requests do not incorrectly skip the expensive stage.
- Adaptive lexical/name preflight skips embeddings only when its own bounded
  filters and confidence rule are satisfied.
- Duplicate and cross-route query embeddings create one canonical call and
  one idempotent query-use attribution per scoped identity.
- Measured, estimated, cached, duplicate, failed, and unknown usage remain
  distinguishable in durable observations and cost records.
- No prompt text, credentials, or raw vectors appear in query-use records,
  observations, errors, or evidence.
- Existing unit tests, race tests, fresh/existing Postgres tests, builds,
  documentation checks, and package smokes remain green.
