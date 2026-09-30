# Task 83 — Deployment-admission consumption for generic effects

Status: implemented foundation; external deployment truth remains
deployment-owned qualification.

## Why this exists

Task 82 made release qualification a durable, hash-only admission decision.
That decision is useful only if a generic connector operation must consume it
before crossing an external boundary. Task 83 closes that seam without making
the operation store a second release authority and without narrowing Fornix to
one domain such as repository work.

The resulting boundary is:

```text
operation request
  └─ hash-only deployment admission reference
       └─ revalidated in the same Postgres transaction as effect reservation
            └─ external adapter may be dispatched only after durable reservation
```

The reference is an assertion about a qualified deployment artifact. It is not
a bearer token, deployment command, signature, manifest, credential, or proof
that an external system actually executed an effect.

## Invariants

1. A reference is workspace-scoped and contains only bounded identifiers and
   cryptographic hashes: release, artifact, gate, trust snapshot, and decision.
2. An operation's canonical identity includes its admission reference. A
   retry cannot replace the authority facts while retaining the same logical
   operation hash.
3. A newly reserved generic external effect re-evaluates the reference from
   the deployment-evidence tables in the same transaction that inserts the
   durable effect reservation.
4. The reference must match the current release hash, artifact hash, gate hash,
   trust-snapshot revision/hash, and complete decision hash. Missing, blocked,
   expired, revoked, or stale facts fail closed.
5. Duplicate reservation delivery returns the already durable effect identity;
   it does not create a second effect or silently re-admit a changed request.
6. Existing deployments may keep the feature disabled during migration. When
   `FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS=true` (and automatically in
   production), new effect reservations without a reference are rejected.
7. Read-only and observation capabilities do not cross the external-effect
   reservation boundary and therefore do not require a deployment reference.
8. Fornix still makes no exactly-once claim about the external provider. The
   existing effect lease, provider idempotency, reconciliation, and recovery
   model remains authoritative for at-least-once external work.

## Transaction and crash semantics

The operation store validates the reference after locking the operation and
before inserting `operation_effects`. The deployment-evidence evaluator reads
the release, current trust snapshot, evidence gate, and verification inside
that caller-owned transaction. A rollback leaves both the validation attempt
and the effect reservation absent. A commit makes the reservation and all
operation authority facts visible together.

The read-only admission API continues to provide an operator-facing decision.
It does not mutate expired rows, and its decision hash is the exact value that
must be copied into an operation reference. A release or verification change
causes a later reservation to fail rather than rewriting historical operation
or evidence records.

## Schema and migration decision

No migration is required. `OperationRequest` is already persisted as bounded
JSONB and its canonical request hash is the durable operation identity. The
new `deployment_admission` field is additive, normalized, and omitted when not
used, preserving old operation rows and replay behavior. Deployment release,
verification, gate, and trust-snapshot tables remain the sole authority for
their facts.

## Reuse, licensing, and security

This slice reuses Fornix's existing deployment-evidence gate, release
verification, generic operation store, effect reservation, workspace
transaction, and idempotency seams. No source is copied from Kronaxis Fabric;
its BSL 1.1 license remains out of scope. The implementation remains covered
by Fornix's MIT license.

Raw attestations, manifests, image layers, provider responses, credentials,
and arbitrary operator text never enter the reference, operation hash, event,
or error. The only new configuration value is a boolean policy switch; it
does not accept secret material.

## Cost and performance budget

The required path adds bounded indexed reads for one release, one gate, and
one verification, plus the existing operation transaction. There is no new
service, queue, cache, payload copy, or background job. Expected overhead is
one small read-only authority evaluation inside the effect reservation
transaction; storage growth is limited to the reference JSON already stored
with the operation request and its resulting hash.

Operators should measure reservation latency, SQL statement count, and blocked
admission rate separately from adapter/provider latency. The database remains
the authority; a local cache would make revocation and snapshot changes unsafe.

## Acceptance tests

- a normalized reference is deterministic, bounded, hash-sensitive, and free
  of raw/secret fields;
- cross-workspace references fail during operation normalization;
- operation hashes change when any authority hash changes;
- a current reference validates inside the caller's transaction;
- a stale decision hash fails closed without mutation;
- release registration and verification use the same default gate kinds as
  read-only admission evaluation;
- duplicate effect reservations preserve one durable effect;
- strict mode rejects new effect reservations without references;
- revocation, expiry, trust-snapshot drift, gate drift, and artifact mismatch
  reject later reservations;
- transaction rollback leaves no effect reservation or authority link;
- replay exposes the original reference and hash without re-execution;
- all existing unit, race, vet, migration, smoke, and documentation checks
  remain green.

## Deliberate limitation

This task does not execute deployments, verify an artifact against a registry,
or guarantee provider-side exactly-once behavior. It establishes the missing
control-plane admission contract so domain adapters can consume qualified
release facts consistently. External deployment adapters, stronger evidence
producers, and operator workflows remain separate future slices.
