# Effect result atomicity and credential lease authority

Status: implementation note for Loop 105. This is a bounded correctness fix,
not a production-readiness declaration.

## Problem and decision

Two authority seams were weaker than their documented guarantees. First, the
dispatcher committed the terminal generic effect and domain-link transition,
then wrote the operation result in a second transaction. A crash between those
commits could leave a successful-looking effect with no durable operation
result, and duplicate dispatch returned early without surfacing the gap.
Second, model, HTTP, and federation egress accepted a credential lease even
when its fence/revocation fields were zero or no durable lease validator was
available.

This slice will use existing Postgres authority and typed interfaces; it adds
no migration or service. Terminal effect, domain-link, and operation-result
writes will commit in one transaction. Credential resolver implementations
must validate their lease against current authority before egress, and lease
envelopes must carry positive fencing and revocation epochs. If the provider
has already replied but local finalization aborts, a duplicate must not invoke
it again: it reports a recovery-required condition unless a durable result is
already present.

## Invariants

1. An operation result requested by a dispatcher is committed in the same
   workspace transaction as the final effect-state and domain-link changes.
2. Failure at any point in that transaction leaves all three authorities at
   their prior committed state; no partial terminal success is observable.
3. Duplicate delivery of a committed terminal dispatch reads and returns the
   original immutable operation result. It never invokes the adapter again.
4. An old terminal effect without its requested result is not success. It
   fails closed with an explicit result-recovery error and does not re-dispatch.
5. An in-flight/uncertain effect remains nonterminal and is not re-dispatched
   merely to regenerate a lost result. Provider/domain reconciliation owns
   recovery after the external boundary may have been crossed.
6. A usable credential lease has a nonzero monotonically issued fence and
   revocation epoch, exact workspace/reference/purpose scope, nonempty secret,
   source version, and valid expiry.
7. Every lease-backed outbound request invokes the resolver's authoritative
   `ValidateLease` before credential bytes are attached to a request. Model,
   HTTP, and federation egress fail closed when validation is unavailable or
   rejects the lease.
8. Lease secret bytes remain excluded from serialization, logs, durable state,
   and errors. Validation compares non-secret lease identity only.

## Transaction and recovery semantics

```text
provider reply held in process
  -> one Postgres transaction:
       final effect transition
       domain-link transition
       operation result + operation transition + authority link
  -> commit
```

If the transaction aborts, durable state remains at the previous
acknowledged/linked point. The provider may already have accepted the call,
which is still an at-least-once external boundary. A later duplicate reads the
effect state. It may replay a committed operation result; if no result exists,
it returns a typed recovery-required error and never calls the provider to
reconstruct data that is no longer available. A provider/domain-specific
reconciler or operator must resolve this state; the generic dispatcher does
not invent success or synthesize a result from hashes.

## API/schema and reuse

- Add a transaction-scoped `OperationStore.RecordResultTx` method and reuse the
  existing result insertion, operation transition, event, task-fence, and
  authority-link logic.
- Extend the credential lease resolver contract to require current-authority
  validation. Keep the durable `CredentialLeaseStore` as the production
  implementation; update explicit in-memory test resolvers to provide a
  deterministic validation function.
- Require positive `Fence` and `RevocationEpoch` in `Lease.Normalize`, matching
  the constraints already enforced by migration 047 and the durable store.
- No table, migration, raw-payload field, or new dependency is needed.
- Reuse the existing dispatcher, transaction composition callback, immutable
  operation result, domain-effect link, credential lease store, and failure
  hooks. This is independently implemented from established fencing and
  transaction patterns; no reference-repository source is copied. Fornix
  remains MIT-licensed; no Kronaxis BSL 1.1 source is used.

## Cost and risk budget

The successful effect-finalization path uses the same bounded SQL statements
inside one transaction rather than opening a second result transaction. It
reduces the crash window and transaction round trips but holds the finalization
transaction slightly longer. Credential validation adds one bounded authority
check at each provider boundary; the durable Postgres implementation performs
indexed lease/reference reads. It introduces no background work or storage
growth beyond the operation result that was already required.

The main risk is lock-order interaction between effect and operation rows.
Tests must exercise concurrent duplicate delivery, and PostgreSQL qualification
must remain a release gate. No latency or throughput SLO is asserted without
running the disposable supported database topology.

## Acceptance tests

- Successful dispatch commits effect, domain-link, and result together.
- A failure injected after result writes but before commit rolls back all
  finalization authorities; no result/effect/link partial commit is visible.
- Retrying a committed duplicate returns the exact original result and invokes
  the adapter once total.
- Retrying a legacy terminal effect with a missing requested result fails
  closed and does not invoke the adapter.
- A possibly-dispatched nonterminal effect is never re-invoked to reconstruct
  lost response data.
- Concurrent duplicates preserve one result, one terminal transition, and one
  adapter invocation.
- Zero fence, zero revocation epoch, missing validator, revoked lease, expiry,
  scope mismatch, and source-version mismatch all fail before outbound I/O.
- Model, HTTP connector, and federation tests prove their local authority
  validator is called and secret bytes do not escape.
- Existing store, model, HTTP, federation, race, smoke, and documentation
  checks remain green; database-backed cases are reported as unverified when a
  supported disposable PostgreSQL/pgvector instance is unavailable.
