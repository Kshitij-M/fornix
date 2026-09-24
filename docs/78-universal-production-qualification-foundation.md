# Universal production qualification: authority and recovery gates

Status: implementation note for Issue [#40](https://github.com/Kshitij-M/fornix/issues/40).

Audience: maintainers and connector authors extending Fornix from its first
repository adapter to production-system operations.

## Why this slice exists

The universal work-control-plane promise is only as strong as its shared
authority boundary. A connector can be domain-neutral and still be unsafe if a
worker can use a stale task fence, if a duplicate command is rejected after a
lease expires, or if driver text is persisted as an unredacted failure. This
slice qualifies the generic operation authority before adding more domains.

It does not claim that Fornix is already a production-grade multi-tenant
control plane. Network egress, external secret managers, connector trust,
backup/restore, load/soak, and deployment HA remain explicit Issue #40 gates.

## Invariants

### Authority and workspace scope

1. Postgres is the authority for operation projections, leases, transitions,
   attempts, external-effect reservations, callbacks, and event history.
2. Every read and write includes the workspace key. A caller cannot use an
   operation, task, attempt, effect, callback, event, or artifact from another
   workspace.
3. A task-bound operation stores a task-fence snapshot for audit, but the
   snapshot is not authority. Each mutating command re-reads and locks the
   current task lease and assignment in the same transaction.
4. The operation lease and task lease are both required for task-bound work;
   takeover or expiry of either fence fails closed.
5. Fences are positive, monotonic, and bounded to the database signed integer
   range. Overflow is a hard error rather than a wraparound.

### Idempotency and crash semantics

1. Idempotency is checked after the authoritative operation row is locked but
   before lease validation. A committed duplicate remains readable even after
   its worker lease expires; a new mutation still requires a current fence.
2. A command key is bound to its complete canonical command hash, including a
   plan hash when a plan is supplied. Reusing a key for different work fails
   closed.
3. Create, transition, attempt reservation, effect reservation, callback
   recording, event append, and projection updates commit in one transaction
   where they describe one authority decision.
4. A crash before commit leaves no visible partial effect. A crash after commit
   is safe to replay and cannot invoke a connector, model, tool, or callback.
5. Replay validates the initial anchor, contiguous versions, from/to status,
   previous-state hash, state hash, legal transition, event existence, and the
   current projection hash.

### Data minimisation and external effects

1. Durable operation failures contain stable codes, retryability, bounded
   attempt metadata, effect status, and optional detail hashes only. Raw driver
   errors, prompts, credentials, response bodies, and arbitrary user text do
   not enter operation rows, events, metrics, or receipts.
2. External effects are explicitly at-least-once unless a provider idempotency
   contract is recorded. `recovery_required` is an honest terminal boundary
   for uncertain delivery; replay never retries the effect.
3. Links are references and hashes, not authority replacement. Source
   authorities remain responsible for validating their own identity and
   workspace.

## Scope and schema strategy

This slice prefers forward-compatible code and tests over a new table. Existing
operation tables already contain the required typed state, hash chain, fences,
and append-only triggers. A migration is added only when a database invariant
cannot be enforced safely by the current schema. Existing migrations remain
forward-only and are never rewritten.

The next schema qualification gate will add typed link authorities and
effect-state constraints only after measuring the migration against existing
databases. No destructive backfill is part of this slice.

## Research and reuse decisions

- Orloj's fenced session checkpoint writes are reused as the model for locking
  the authoritative row and committing history plus projection together.
- agentmemory's lease lifecycle and checkpoint behavior inform expiry,
  takeover, and crash tests, but its in-memory keyed locks are not authority
  for Fornix; PostgreSQL row locks remain authoritative.
- ClawMem's replay harness principle is adopted: replay reads the real durable
  history and does not mirror or invoke external handlers.
- DeepSeek Harness's credential/invariant separation informs the rule that
  credentials and provider diagnostics never enter durable operation contracts.
- No source is copied. The references are permissively licensed or used only
  for architectural comparison; Kronaxis Fabric remains excluded because its
  BSL 1.1 license is incompatible with Fornix's MIT distribution.

## Cost and performance budget

The target overhead for a task-bound generic mutation is one additional locked
task-lease read and one operation-lease read inside the existing transaction;
there is no network call and no new infrastructure. Qualification records must
report p50/p95 latency, SQL statement count, rows locked, WAL/storage delta,
duplicate-work rate, and replay throughput. The target is bounded work per
command and no unbounded JSON fields.

## Acceptance tests

- a task takeover fences a stale operation worker even when its operation lease
  has not expired;
- stale operation and task fences fail closed for transitions, attempt/effect
  reservations, callbacks, and lease acquisition;
- duplicate committed commands succeed deterministically after lease expiry;
- reusing an idempotency key with a changed request or plan fails closed;
- replay rejects a missing event, broken version chain, altered previous hash,
  altered state, illegal status transition, or projection mismatch;
- operation IDs containing `/` or `~` produce valid, unambiguous state-delta
  paths;
- raw credentials, prompts, provider errors, and arbitrary failure text do not
  appear in durable operation state or events;
- concurrent writers preserve one effect and one transition per key;
- crashes before commit leave no partial state, and committed state replays
  without external effects;
- existing unit, race, Postgres integration, smoke, and documentation checks
  remain green.

## Remaining qualification gates

This note is deliberately narrower than Issue #40. The following are still
separate implementation and evidence gates: defense-in-depth tenant
enforcement, credential lease adapters, outbound destination policy, connector
and schema trust, quotas/backpressure, worker fairness, event/artifact
retention, backup/restore, HA/failover, migration compatibility drills,
prompt-injection and confused-deputy tests, load/soak/failure injection, and
operator support bundles.
