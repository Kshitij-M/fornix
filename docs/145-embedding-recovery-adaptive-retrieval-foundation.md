# Task 61 — Atomic embedding recovery and adaptive retrieval foundation

Status: implemented foundation; production qualification remains in progress
Scope: universal transformation / provider-effect authority and retrieval cost control
Migrations: 059–060

## Why this task exists

Fornix already records embedding calls, reserves a generic external effect, and
keeps a specialized embedding ledger. That separation is useful only when the
two authorities cannot disagree after a recovery decision. The current
reconciliation path updates the specialized ledger and its audit history, but
does not yet commit the generic effect, the domain-effect link, and the typed
recovery event in the same transaction. A crash in that interval can leave an
embedding that is locally resolved but globally still uncertain.

The current semantic search routes also invoke a provider before deterministic
structured, lexical, or symbol-name evidence has had a chance to satisfy the
request. This makes the expensive path the default for some legacy routes and
makes query cost harder to explain. Task 61 closes both gaps while keeping the
existing Postgres authority and public compatibility boundaries.

## Invariants

### Recovery authority

1. The existing generic effect-recovery lease is the only recovery ownership
   authority. This task must not introduce a second embedding-specific lease.
2. Every recovery finalization is fenced by workspace, effect, owner, and
   monotonically increasing effect fence. A stale owner fails closed before any
   authoritative mutation.
3. The generic effect transition, domain-effect-link transition, embedding
   ledger transition, reconciliation audit row, and typed event either all
   commit or none commit.
4. Recovery accepts provider proof identity, source hash, model/provider
   identity, expected versions, bounded metadata, and a verified vector only
   through a trusted provider adapter. It never accepts raw text, credentials,
   or an arbitrary operator-supplied vector.
5. Duplicate recovery with the same request and proof is idempotent. A
   different proof, provider request identity, expected version, or command
   hash fails with a conflict and cannot overwrite history.
6. A provider that cannot reconcile an uncertain request remains
   `recovery_required`. In particular, Ollama must fail closed; no implicit
   second remote call is claimed to be exactly once.
7. Raw input remains absent from durable recovery records, events, errors, and
   evidence. Source and response hashes are retained as bounded evidence.

### Adaptive retrieval

1. Deterministic structured, lexical, and symbol/name stages run first.
2. The embedding gateway is called only when a measurable gate says that the
   configured quality or result budget is not satisfied, or when the caller
   explicitly selects the versioned semantic mode.
3. The existing legacy semantic behavior remains available behind an explicit,
   versioned mode so compatibility is not silently changed.
4. Query-vector reuse is workspace-scoped and keyed by normalized source
   identity, provider, model, embedding schema/version, and policy. A cache hit
   must not be attributed as fresh provider work.
5. Cache hits, measured provider usage, estimated usage, database work, and
   duplicate work are attributable to the authenticated actor without
   recording prompts or unbounded labels.
6. All result ordering, thresholds, abstentions, truncation, cache keys, and
   context hashes are deterministic. Time-dependent scoring must use an
   explicit request/reference time rather than an implicit database clock.
7. Workspace isolation is enforced at every query, cache, ledger, and
   observation boundary.

## Crash and concurrency semantics

Recovery ownership is acquired before contacting a provider and is finalized
under the same effect fence in one SQL transaction. A crash before that commit
leaves the recovery lease and the prior state visible; a later valid owner may
take over after expiry. A crash after commit leaves one terminal effect, one
link transition, one ledger resolution, one audit record, and one event. A
retry of the same proof is a no-op; a retry with a different proof is rejected.

Concurrent recovery workers serialize on the locked effect/link/ledger rows.
The expected effect and link versions are checked again inside the transaction
so a stale worker cannot advance a checkpoint-like version or publish a second
outcome. Cross-workspace identifiers are rejected before row locks are used.

Adaptive retrieval may race only at the read/cache boundary. Query-vector
creation is serialized by the existing bounded PostgreSQL advisory key and is
idempotent. An embedding provider failure never destroys deterministic results;
the request returns the best bounded deterministic result or a typed abstention
according to the caller’s explicit mode and budget.

## Schema plan

Migration 059 adds only the bounded authority references and version facts
needed to join embedding reconciliation with the generic effect and domain
link without replacing either history. Migration 060 adds a hash-only,
workspace-scoped query-use ledger with actor, provider, route, gate, cache,
usage, and cost classification. It does not store raw query text, credentials,
vectors, or arbitrary labels. Existing migration 058 retention and tombstone
semantics remain compatible. As with every numbered Fornix migration, 059 is
immutable after application; a failed migration transaction can be retried
with the exact same file, while later hardening must use a new migration.

## Reuse and licensing decisions

- Reuse Fornix’s existing `AdmissionStore` effect leases and transaction-local
  helpers rather than inventing an embedding lease.
- Reuse `DomainEffectLinkStore.TransitionTx`, the append-only event store,
  `EmbeddingCallStore`, and the existing workspace transaction context.
- Reuse the typed retrieval planner’s deterministic gates and bounded context
  contracts; legacy routes will call a shared adaptive seam rather than copy
  ranking logic.
- Use design patterns from Orloj, ClawMem, agentmemory, and FornixDB only as
  architectural references. No source is copied from Kronaxis Fabric because
  its BSL 1.1 license is incompatible with Fornix’s MIT distribution.
- New code remains under the repository’s MIT license and must not persist
  provider credentials or raw prompts.

## Cost and storage budget

The deterministic preflight must not perform provider work. A cache hit adds a
bounded indexed lookup and a small usage record. A cache miss adds one bounded
embedding call subject to the request’s input, timeout, token-equivalent,
provider, and dollar budget. Recovery adds one small append-only audit row and
does not duplicate vector payloads. Query vectors remain expirable and are
tombstoned rather than silently deleted; authoritative vectors remain
retained while referenced.

The implementation must expose measured versus estimated usage, provider cost,
database work, cache-hit ratio, and replay throughput. It must not claim that
remote execution is exactly once or that missing provider usage is measured.

## Acceptance tests

- A fresh and an existing database migrate through the new migration without
  changing earlier authoritative rows.
- Recovery finalization commits generic effect, domain link, embedding ledger,
  audit, and event atomically.
- A crash before commit leaves all five authorities unchanged.
- A crash after commit is safely replayable and does not create a second
  transition or event.
- Stale effect-recovery fences fail closed; expiry permits one fenced takeover.
- Duplicate proof is idempotent; conflicting proof is rejected.
- Cross-workspace recovery, links, query cache, and observations fail closed.
- No raw text, credentials, or arbitrary vectors appear in API input, logs,
  events, evidence, or reports.
- Deterministic preflight skips embeddings when its confidence/result budget is
  satisfied.
- Explicit semantic mode still invokes the gateway within its budgets.
- Cache hits are deterministic, actor-attributed, and cheaper than misses.
- Provider failure yields deterministic fallback or abstention without losing
  lexical results.
- Identical requests produce identical plan, ordering, usage classification,
  context hash, and replay result.
- Existing unit, race, Postgres, package-smoke, build, documentation, and CI
  checks remain green.

## Delivered implementation boundary

- `EmbeddingRecoveryCoordinator` composes the generic effect transition,
  domain-effect-link transitions, embedding ledger resolution, reconciliation
  audit, and typed event in one caller-owned transaction. A failure hook is
  available for deterministic rollback qualification.
- The production HTTP recovery route accepts workspace/request identity,
  expected versions, bounded idempotency, and a lease TTL only. Provider proof
  is obtained through the registered reconciler; raw vectors, source text,
  credentials, and arbitrary evidence are not accepted at the API boundary.
- Recovery ownership remains the existing effect-recovery lease. API delivery
  owners are unique per request, while the authenticated principal remains the
  audit actor. This prevents same-principal concurrent deliveries from sharing
  a fence.
- Memo, symbol, and RAG legacy routes now support versioned adaptive, legacy,
  required, and disabled embedding modes. Adaptive mode runs deterministic
  lexical/name preflight first; filtered RAG requests fail open to the
  expensive stage rather than using an unfiltered gate. Query-use records and
  observability/cost attribution are hash-only and idempotent.
- The focused Postgres test covers migration, transaction rollback, successful
  finalization, stale takeover fencing, duplicate and concurrent replay, and
  conflicting query-use attribution.

The compatibility method `EmbeddingGateway.Reconcile` remains available for
small in-process recorders and tests. Production server recovery uses
`ReconcileWithLease` and the coordinator; the compatibility method must not be
treated as an exactly-once external execution guarantee.
