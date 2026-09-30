# Embedding target attachment foundation

Status: implemented for transactional domain-target attachment; recovery and
query-vector lifecycle have a bounded store/provider foundation, while the
authenticated cross-authority recovery coordinator remains open
Scope: universal transformation / Task 60 attachment slice

## Decision

An embedding call is an external-effect record, while a chunk, memo, or symbol
is a domain projection. The two authorities must not be confused. A successful
embedding result is therefore linked to a projection through an immutable,
workspace-scoped attachment record containing the source and vector hashes.

For repository ingestion, memo/symbol HTTP writes, RAG chunk upserts, and
bounded memo backfill, the target projection and embedding attachment are
committed by the same Postgres transaction. Ingestion additionally includes
the lineage row, checkpoint, job counters, and lifecycle events. A crash rolls
back the target transaction. A previously committed embedding result is never
reissued merely because its projection attachment was not committed; the
attachment can be resumed from the durable ledger and vector hash.

## Invariants

1. An attachment belongs to exactly one workspace, embedding request, target
   kind, and target ID.
2. The embedding call must be `succeeded`; pending, running, failed, and
   `recovery_required` calls cannot be attached.
3. The attachment source and vector hashes must equal the successful ledger
   hashes. A mismatch fails closed and never overwrites an existing link.
4. Repeating the same attachment is idempotent. Repeating the target identity
   with different hashes is a conflict.
5. Attachment rows are append-only. Corrections create a new embedding call
   and a new lineage record; history is not overwritten.
6. RLS and transaction-local workspace context protect both the ledger and
   attachment table.
7. The ingestion task fence is checked before the target transaction begins;
   the attachment is inside that same transaction, so a stale ingestion worker
   cannot commit a chunk/checkpoint/link batch.

## Schema and database work

Migration 057 adds `fornix.embedding_target_attachments` with a composite
foreign key to `embedding_calls`, immutable identity fields, source/vector
hash checks, target/request indexes, and workspace RLS. It intentionally does
not add foreign keys to every domain table because Fornix supports multiple
domain adapters; each adapter remains responsible for validating its target
inside its own transaction.

The integrated adapters are repository ingestion, memo create/update, symbol
upsert, RAG chunk upsert, and bounded memo backfill. Each records the embedding
request identity and inserts the attachment after the target ID is known.
Ingestion also composes its checkpoint, lineage, and lifecycle event in the
same transaction.

## Reuse and licensing

This reuses Fornix's `beginWorkspaceTx`, embedding ledger, ingestion checkpoint
transaction, RLS conventions, and existing content/vector hash contracts. It
does not copy Kronaxis Fabric source; Kronaxis is BSL 1.1 while Fornix is MIT.
The design follows the same general immutable-lineage and checkpoint ideas
already studied in Orloj, ClawMem, agentmemory, and FornixDB.

## Cost and storage

Each attached target adds one small indexed row containing hashes and IDs; it
does not duplicate raw text or vector bytes. The ingestion batch adds one
ledger verification query and one idempotent insert per embedded chunk. The
replay path remains provider-free. The attachment table is intentionally not
vector-indexed.

## Acceptance tests

- Migration 057 applies to a fresh and an existing database.
- A successful embedding can be attached once and replayed idempotently.
- Hash-conflicting attachments fail closed without modifying the prior link.
- Non-successful embedding calls cannot be attached.
- Ingestion commits chunk, lineage, attachment, checkpoint, and events
  together; an injected pre-commit crash leaves all unchanged.
- Repeated ingestion does not issue a duplicate provider call or attachment.
- Workspace-crossing attachment requests are rejected by validation/RLS.
- Existing offline, disposable-Postgres, race, build, CI, and smoke suites
  remain green.

## Deliberate limitations

Ollama has no provider idempotency API in this adapter, so it remains
`recovery_required`; Fornix never retries it blindly. Migration 058 and the
typed gateway/store seams now support provider-specific, hash-only proof and
bounded unreferenced query-vector expiration. The generic operation/link
states are not yet finalized in the same transaction as the specialized
embedding ledger, and the legacy semantic endpoints still need adaptive
preflight before this can be called a complete retrieval-cost qualification.
