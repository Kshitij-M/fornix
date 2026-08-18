# Task 7 — Provenance graph and selective disclosure foundation

Status: implementation note (Task 7)

## Goal

Fornix needs a durable evidence boundary between authoritative control-plane
history and the context that a caller is allowed to see. This slice adds an
append-only, workspace-scoped evidence table and typed provenance edges. It
does not replace events, memos, chunks, symbols, or task rows; those remain
authoritative in their existing tables. Evidence records preserve the exact
raw bytes plus a SHA-256 content hash and expose deterministic gist → detail →
raw disclosure.

## Invariants

1. Every evidence row has a non-empty workspace, source reference, raw payload,
   and hash computed by Fornix from the exact stored bytes. A caller-supplied
   hash is never trusted.
2. Evidence and provenance rows are append-only. Updates and deletes fail at
   the database trigger boundary; supersession and contradiction are new
   metadata/edge records, never edits to the old fact.
3. A source reference plus deduplication key is idempotent inside a workspace.
   An exact repeat returns the existing row without a second effect. Reuse of
   the same key with different bytes or derived text fails closed.
4. Every edge and every disclosure query carries an explicit workspace
   predicate. Composite foreign keys prevent cross-workspace edges even when
   numeric IDs happen to match.
5. `supersedes` points from the newer record to the older record. Its explicit
   column supports cheap lineage checks; the corresponding typed edge makes
   history traversable. Contradictions are symmetric for traversal but remain
   explicitly typed and auditable.
6. Traversal has caller-provided depth/node bounds, cycle protection, and a
   stable order (`depth`, relation, endpoint IDs, edge ID). Disclosure has hard
   byte and token budgets and never returns a partial raw payload as if it were
   complete.
7. Gist and detail are derived disclosures of the same immutable evidence
   hash. A disclosed raw payload is integrity-checked before it leaves the
   store. A stable result hash excludes timing and query-count diagnostics.
8. Control events are integrated in the same transaction: a committed event
   gets one attributable evidence row, while duplicate idempotency delivery
   returns before creating another row. The event store remains the authority
   for event order and replay.

## Schema and transaction design

Migration `010_provenance_disclosure.sql` adds:

- `fornix.evidence_records`: workspace, source reference, dedupe key, kind,
  media type, gist, detail, immutable `BYTEA` raw payload, raw size, SHA-256,
  optional `supersedes_id`, and timestamps;
- `fornix.provenance_edges`: workspace-scoped typed endpoints, relation,
  JSONB metadata, and creation time;
- composite foreign keys and indexes for workspace-safe lookup, hash lookup,
  supersession, and deterministic edge expansion;
- append-only triggers and size/shape checks.

Evidence creation and edge creation use one transaction. A supersession input
validates the predecessor in the same workspace, inserts the successor and
typed edge, then rejects a supersession cycle before commit. Duplicate edges
use `ON CONFLICT DO NOTHING`; they are not silently rewritten.

The current event append path writes the event and its evidence row under the
same caller-owned transaction. A failed evidence insert rolls the event back.
Distinct event IDs remain distinct attributable observations even if their raw
payload bytes are equal; duplicate delivery of one event ID is handled by the
existing idempotency record.

## Disclosure semantics

- `gist`: gist only, plus hash/source/lineage metadata;
- `detail`: gist followed by bounded detail;
- `raw`: gist/detail followed by raw only when the complete raw payload fits
  the remaining budget. Otherwise the result is explicitly truncated and
  retains the evidence hash and raw size, so callers cannot mistake a prefix
  for authoritative raw evidence.

Contradiction and supersession metadata are visible at every level, subject to
the same item/depth/node budgets. Provenance expansion is optional, bounded,
and deterministic. A content hash covers the request’s effective level,
visible content, source hash, lineage metadata, and ordered edge metadata.

## Reuse and licensing

The design reuses architecture, not source code:

- Orloj: transaction-owned durable boundaries and explicit evidence references;
- ClawMem: eligibility/cycle guards and bounded deterministic graph expansion;
- agentmemory: typed relations, citations, integrity verification, and
  supersession/retention discipline;
- FornixDB: gist-first disclosure, provenance-first output, and immutable
  supersede-with-history.

The referenced Orloj and agentmemory implementations are Apache-2.0 and
ClawMem/FornixDB are MIT, but no reference code is copied into this change.
Fornix remains MIT-licensed. Kronaxis-Fabric’s BSL-1.1 code is not reused.

## Cost, storage, and operational budget

This is intentionally Postgres-only. Each integrated event adds one bounded
evidence row and three indexed text/identity values, with raw bytes retained
once per attributable event. A normal disclosure performs one source read,
one supersession read, one contradiction read, and one bounded graph read when
provenance is requested; the response reports that query count. The default
raw payload cap is 4 MiB, traversal defaults are depth 2 and 64 nodes, and
disclosures have explicit byte/token caps. No embedding, LLM call, broker,
Redis, NATS, object store, or background compaction is introduced.

## Acceptance tests

- fresh and existing databases apply migration 010 and retain migration
  checksums;
- exact duplicate evidence is idempotent; conflicting reuse is rejected;
- raw mutation/deletion is rejected and read-time hash verification fails
  closed;
- gist/detail/raw disclose the same evidence hash and obey hard budgets;
- superseded records remain readable, auditable, and replayable;
- contradiction and supersession edges are deterministic and workspace-local;
- traversal is cycle-safe, bounded, and stable across repeated requests;
- cross-workspace disclosure and edge creation fail closed;
- event append and evidence append commit or roll back together;
- repeated disclosure has the same content hash; concurrent writers preserve
  uniqueness and integrity;
- existing unit, integration, migration, smoke, and CI checks remain green;
- tests report representative disclosure latency, query count, and storage
  impact, with remaining limits called out in the completion note.

## Known limits of this vertical slice

Raw payloads are bounded Postgres bytes because object storage is explicitly
out of scope. Evidence aliases for different origins are represented by
distinct attributable records rather than a mutable alias list. The graph is
typed and bounded but has no learned confidence or temporal decay; those are
future policy layers over this authority boundary.
