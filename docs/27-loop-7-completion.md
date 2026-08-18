# Loop 7 completion: durable provenance and selective disclosure

Status: complete
Date: 2026-08-18

## Delivered

- Added typed source-record, provenance-edge, disclosure-request, and
  disclosure-result contracts. Requests carry explicit workspace, level,
  hard byte/token budgets, graph depth/node bounds, and an optional provenance
  gate.
- Added migration `010_provenance_disclosure.sql` with immutable
  `fornix.evidence_records` and `fornix.provenance_edges`, composite
  workspace-safe foreign keys, deterministic lookup indexes, size checks, and
  append-only database triggers.
- Added `internal/store/evidence.go`. The store computes SHA-256 over exact raw
  bytes, deduplicates an exact source/deduplication identity, rejects
  conflicting reuse, validates integrity on reads, records supersession and
  contradiction edges transactionally, and rejects supersession cycles.
- Added bounded incoming/outgoing provenance traversal. Expansion uses one
  indexed Postgres query per hop, cycle protection, deterministic relation and
  endpoint ordering, and explicit truncation when depth/node limits are met.
- Added deterministic gist → detail → raw disclosure. Gist/detail text is
  UTF-8-safe and budgeted; raw is returned only in full when it fits. Every
  result retains the authoritative evidence hash/raw size and emits a stable
  content hash independent of query timing.
- Integrated event append with evidence creation in the same transaction. A
  duplicate idempotency delivery returns before creating another evidence
  effect; the event store remains the source of sequence/order truth.
- Added authenticated HTTP endpoints for evidence creation, edge creation,
  disclosure, and traversal; v0.16 smoke coverage; Makefile commands; CI
  integration; and focused concurrency, replay, duplicate, failure,
  workspace-isolation, supersession, cycle, append-only, truncation, and
  latency/storage tests.

## Qualification

- Existing development Postgres applied migration 010 through the checksum
  guarded migration runner; repeated application remained clean.
- Evidence tests passed for exact concurrent duplicate writes (12 writers,
  one created row), stable disclosure hashes, event/evidence duplicate
  behavior, workspace isolation, supersession/contradiction traversal,
  append-only mutation rejection, cycle rejection, and hard budgets.
- The representative local disclosure benchmark runs 20 gist disclosures
  with provenance disabled and reports p50/p95/max latency and the combined
  evidence/edge relation size in the test log. The qualification run measured
  p50 `1.304 ms`, p95/max `2.836 ms`, and combined relation size `933,888 B`
  (the relation size includes prior test workspaces and index/page overhead).

## Database work, storage, and cost

An event append adds one bounded evidence row in the same transaction as the
event. A disclosure with provenance disabled performs three bounded reads
(source, supersession metadata, contradiction metadata); provenance adds at
most one indexed read per requested hop. The raw payload cap is 4 MiB, gist is
16 KiB, detail is 4 MiB, default traversal is depth 2/64 nodes, and no model,
embedding, broker, Redis, NATS, object store, or background worker is added.

Raw bytes are retained once per attributable source reference. Distinct event
IDs remain distinct records even if their raw payloads match, preserving
causal attribution; exact retries use the existing event idempotency key.
Evidence aliases, retention tiering, temporal decay, large artifact storage,
and learned confidence remain future layers.

## Reference and license decision

The implementation reuses architecture from Orloj, ClawMem, agentmemory, and
FornixDB but copies no source. Their respective MIT/Apache-2.0 licenses do not
alter Fornix’s MIT license. Kronaxis-Fabric’s BSL-1.1 code is not used.

## Remaining limitations

- Raw evidence is bounded Postgres `BYTEA`; large external artifact storage is
  intentionally not present yet.
- The graph has typed edges and bounded deterministic traversal but no temporal
  decay, confidence learning, stale-edge lifecycle, or graph projection cache.
- The event integration creates attributable records for control events; memo,
  chunk, symbol, task-result, prompt, and tool-output ingestion still need
  explicit evidence adapters.
- Identity/role authorization is still the existing shared bearer key; it is
  not a tenant-grade authorization system.
