# Task 53 — effectful adapter authority binding

Status: feature note for the next universal production-qualification slice.

Fornix now has durable operation admission, append-only authority links,
managed credential leases, signed capability-schema catalogs, and a generic
external-effect reservation. The remaining gap is that these facts are not
yet carried as one verifiable envelope through an effectful adapter boundary.
Without that binding, an adapter could be admitted against one schema
snapshot or credential source and later report a result under another.

This slice makes the boundary explicit and hash-addressable. It does not make
external calls exactly-once, does not store secret bytes, and does not turn
the process-local registry into an authority. Postgres remains the authority
for admission, leases, effect reservations, operation results, and their
append-only links.

## Invariants

1. Every effectful operation records the exact schema-catalog hash and
   revision used for admission. A missing, expired, revoked, downgraded, or
   mismatched catalog fails closed before the adapter boundary.
2. Every managed credential claim records the lease ID, monotonically fenced
   lease fence, revocation epoch, opaque source version, and source expiry.
   Secret bytes never enter a contract hash, event, error, log, artifact, or
   authority-link row.
3. Source version and source expiry are checked against the live credential
   lease in the same transaction that appends an authority link or reserves an
   external effect. A lease renewal or source rotation cannot silently reuse a
   stale authority claim.
4. Schema and credential facts are workspace-scoped. A cross-workspace
   reference, task fence, operation fence, lease fence, or catalog revision is
   rejected before durable mutation.
5. Authority links are append-only and idempotent by workspace, operation,
   stage, and delivery identity. New optional fields are omitted when empty so
   historical link hashes remain stable after migration.
6. Reserving an external effect is a durable before-call boundary. Provider
   execution remains at-least-once unless the provider independently supports
   and honors the recorded idempotency key. Fornix never claims exactly-once
   remote execution.
7. Replay consumes recorded schema, credential-source, effect, result, and
   receipt facts. Replay never resolves a secret or re-executes an adapter.

## Authority envelope

```text
signed schema catalog + registered capability
             │ exact hash/revision
             ▼
workspace admission + policy decision
             │ actor/request/idempotency + task/operation fence
             ▼
managed credential lease
             │ lease fence/epoch + source version/expiry
             ▼
durable external-effect reservation
             │ provider idempotency and delivery semantics
             ▼
adapter call → result/evidence/receipt authority link
```

The process registry is a verified cache. The credential resolver is the
secret boundary. The operation store is the lifecycle authority. The adapter
must receive only the typed, bounded authority context needed for its call;
raw credentials are cleared at the boundary and are never serialised.

## Schema and migration strategy

Migration `052_effect_authority_facts.sql` adds nullable/empty-default
schema-catalog and credential-source metadata to operation authority links and
external-effect reservations. Existing rows remain readable, satisfy their
original constraints, and retain their original canonical hashes. New
effectful rows require the complete fact set when a catalog or managed lease
is claimed. The migration adds bounded checks and workspace-local lookup
indexes, but does not rewrite authoritative history.

The contract schema version remains independently versioned from operation,
credential, and catalog records. A future link schema can require fields that
are currently optional; this slice keeps compatibility explicit rather than
pretending old rows have facts they never recorded.

## Reuse and licensing

The design reuses Fornix's existing `AdmissionInput`, signed schema catalog,
credential lease store, external-effect reservation, operation result store,
and append-only authority-link helpers. It follows the lease/fence and
provider-idempotency patterns studied in Orloj, DeepSeek Harness,
agentmemory, and OpenBao/Vault-style secret boundaries, but is independently
implemented under Fornix's MIT license. No Kronaxis Fabric BSL 1.1 source is
copied, and no broker, object store, secret service, or LLM framework is
introduced.

## Cost and operational budget

- Admission adds bounded in-memory catalog verification and no network hop.
- Effect reservation adds one bounded row write and indexed metadata; it never
  stores secret bytes or provider payloads.
- Authority-link validation adds indexed reads for the live lease/catalog
  facts in the transaction already required for the operation mutation.
- The expected overhead is small relative to an external call, but production
  qualification must measure p50/p95 admission and reservation latency,
  transaction time, index growth, link storage, and contention under duplicate
  delivery.
- Retention, signer rotation, credential-source rotation, provider
  idempotency, and external reconciliation remain explicit operational duties.

## Acceptance tests

- Fresh and existing databases apply migration 052 cleanly.
- Admission and result links preserve schema catalog hash/revision and
  managed credential source version/expiry/fence/epoch.
- Missing, stale, revoked, expired, cross-workspace, or mismatched facts fail
  closed without a partial operation/effect/link mutation.
- Duplicate admission, effect reservation, and result delivery produce one
  durable effect and one canonical link per delivery identity.
- Concurrent effect reservations preserve one idempotent reservation and do
  not permit stale operation/task/credential fences.
- Historical links keep their pre-052 stable hashes.
- Provider idempotency and at-least-once semantics are explicit in the stored
  effect record; no exactly-once claim is made.
- Crash/rollback before commit leaves no orphan effect or authority link;
  replay after commit is deterministic.
- Workspace isolation, RLS, authorization, redaction, race checks, full
  tests, documentation checks, and universal smoke checks remain green.
