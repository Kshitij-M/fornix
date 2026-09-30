# Embedding reconciliation and query lifecycle foundation

Status: implemented as a fail-closed foundation; authenticated cross-authority
finalization and adaptive legacy retrieval remain open
Scope: universal transformation / provider-effect authority

## Decision

An embedding request can cross a remote boundary without leaving a durable
result. A timeout therefore does not mean that the provider did nothing. Fornix
must never turn that uncertainty into a blind retry. Recovery is allowed only
when the selected provider exposes a reconciliation capability that can prove
the result for the original request identity, or can return an explicit
provider failure for that identity. Ollama does not currently expose such a
capability and will remain `recovery_required` until an operator chooses a new
request identity.

Reconciliation is itself an external read/effect boundary. It receives only
hashes, bounded metadata, the original provider identity, and the provider's
request identity; raw source text is never reconstructed or sent by the
recovery path. Its durable effect identity is distinct from the original
generation identity, so a reconciliation attempt cannot accidentally be
treated as a second generation request.

Query embeddings are derived retrieval work, not authoritative source data.
They are recorded for idempotency and cost accounting, but successful query
vectors must have an explicit retention class and deadline. Expiration removes
only the vector payload, preserves source/request/vector hashes and an audit
tombstone, and makes the original request non-replayable rather than silently
reissuing a remote call.

## Invariants

1. Recovery is workspace-scoped and requires `recovery_required` status.
2. A reconciler must be selected through the typed provider registry; arbitrary
   caller-supplied vectors are rejected.
3. The provider, model, source hash, workspace, and original provider request
   identity must match the durable call. A mismatched response fails closed.
4. A reconciliation result is committed only after live task-fence validation
   when the original call was task-bound.
5. Reconciliation attempts are idempotent and append-only in their audit trail;
   conflicting results never overwrite an earlier result.
6. Recovery never calls Ollama, retries generation, or uses raw source text.
7. Query-vector retention is bounded by class and deadline. Expiration keeps
   hashes and provenance, never deletes an authoritative target attachment,
   and is deterministic under duplicate sweep delivery.
8. Cross-workspace reads, writes, provider responses, and retention sweeps
   fail closed through transaction-local workspace scope and RLS.

## Schema and database work

Migration 058 adds an explicit embedding-call retention class/deadline and
terminal `expired` state, plus an append-only reconciliation-attempt audit
table. The existing vector integrity constraints are widened only to permit a
hash-preserving expired tombstone. Successful non-query calls and attached
vectors remain replayable. Query expiration is permitted only when no target
attachment exists.

The current implementation exposes this policy through the typed gateway and
`EmbeddingCallStore`. Production reconciliation remains disabled unless the
configured effect runner implements the distinct reconciliation boundary. This
is intentional: enabling a provider proof without atomically updating the
generic operation effect and domain-effect link would create split authority.

The ledger-level query cache key is workspace, source hash, provider, and
model. A bounded PostgreSQL advisory transaction key serializes concurrent
misses; an existing query call is reused without contacting the provider.
Retention dry-runs are read-only, and expiration preserves vector/source
hashes plus a tombstone while removing only the unreferenced vector payload.

## Reuse and licensing

The slice reuses the existing provider registry, embedding-call ledger,
generic fenced effect dispatcher, task-fence validation, RLS transaction helper,
and canonical vector hashing. It applies the recovery and retention patterns
studied in Orloj, agentmemory, ClawMem, and FornixDB without copying source.
Kronaxis Fabric remains excluded because its BSL 1.1 license is incompatible
with direct source reuse in this MIT repository.

## Cost and storage budget

Reconciliation adds one bounded provider query and one small audit row per
attempt; it must not repeat embedding generation. A successful replay remains
provider-free. Query expiration removes approximately 3,072 vector payload
bytes per 768-dimensional row before PostgreSQL/WAL/index overhead while
retaining a small tombstone. The sweep uses bounded batches and no full-table
vector scan in the normal path.

## Acceptance tests

- A recovery-required Ollama call fails closed because no reconciler exists.
- A provider-specific reconciler can resolve a matching result exactly once.
- Provider, request identity, source hash, vector hash, and workspace mismatches
  fail closed without changing the call.
- Reconciliation honors task expiry, stale fences, duplicate delivery, and
  concurrent recovery.
- Recovery uses a separate durable effect identity and never re-runs generation.
- Query calls receive deterministic retention metadata.
- Dry-run and real retention sweeps are bounded, idempotent, and workspace
  isolated.
- Expired query vectors cannot be replayed or attached, while hashes and audit
  history remain available.
- Attached or authoritative vectors cannot be expired.
- Offline, disposable-Postgres, race, build, CI, and smoke suites remain green.
