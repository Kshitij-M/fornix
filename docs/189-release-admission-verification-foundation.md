# Task 82 — Release verification and deployment admission foundation

Status: feature note before implementation.

## Problem

Task 81 can answer whether a durable release has the required qualification
evidence, but it does not yet bind that answer to the artifact or image that a
deployment claims to run. A production control plane must be able to say:

```text
this workspace/deployment
  → this immutable release identity
  → this exact trust snapshot
  → this exact qualification gate hash
  → this exact verified release/image artifact hash
  → this bounded startup/admission decision
```

Without that binding, a healthy qualification gate can be detached from the
binary, container image, or release manifest actually being admitted. The
smallest useful next slice is a durable verification reference and a read-only
admission decision. Fornix must not become a deployment orchestrator in this
slice.

## Scope and non-goals

In scope:

- typed, hash-only release verification and admission contracts;
- migration `069` with workspace-scoped verification rows and append-only
  lifecycle events;
- transactional verification registration bound to an existing release, the
  current Task 81 gate hash, and the exact current trust snapshot;
- deterministic admission evaluation with explicit blocking reasons;
- optional production/readiness enforcement through explicit configuration;
- authenticated HTTP/CLI inspection and verification operations;
- replay, RLS, redaction, duplicate, stale, expiry, and crash-boundary tests.

Out of scope:

- downloading, signing, verifying, or executing a container/image/binary;
- HSM/KMS, Sigstore, Rekor, cosign, registry, cloud, or package-manager
  integration;
- deployment rollout, rollback, failover, backup/restore, or provider calls;
- treating an artifact hash or operator assertion as proof that the external
  artifact actually ran;
- replacing Task 79/80 trust or Task 81 evidence history.

## Invariants

1. Verification is workspace/deployment/release scoped and references only
   bounded hashes, identity, source metadata, and outcome facts. Raw manifests,
   signatures, tokens, credentials, image layers, and prompts are not stored.
2. A verification row must reference an existing immutable release and must
   match its release hash, target hash, trust-snapshot revision/hash, and the
   current deterministic Task 81 gate hash in the same transaction.
3. The artifact kind is a closed vocabulary (`release`, `image`, `binary`, or
   `manifest`), and the artifact identity is a canonical SHA-256 hash.
4. Duplicate verification requests return the original row only when all
   identity fields match. A conflicting idempotency key or release/kind pair
   fails closed.
5. A verification can expire or be revoked without deleting history. Historical
   decisions remain replayable and are never rewritten.
6. Admission is ready only when the release exists, its trust snapshot is
   current, its required evidence gate is ready, the gate hash equals the
   verification binding, the verification is verified and unexpired, and the
   requested artifact hash matches the verified identity.
7. Missing, stale, revoked, expired, failed, unknown, or mismatched facts
   produce deterministic blocking reasons. There is no fail-open fallback.
8. Readiness enforcement is opt-in in development and automatic in production
   only when an explicit release identity is configured. A production process
   cannot silently invent a release ID.
9. Evaluation is read-only after verification registration. No admission path
   executes an external operation or claims exactly-once deployment.
10. All reads and writes use workspace-scoped transactions and RLS. Actor,
    request, idempotency, causation, and correlation metadata remain auditable
    without secret material.

## Schema and storage

Migration `069_release_admission_verification.sql` adds:

- `qualification_deployment_release_verifications`, one immutable current
  verification per workspace/deployment/release/artifact kind;
- `qualification_deployment_release_verification_events`, append-only
  registration/revocation history;
- workspace RLS, bounded JSON actor metadata, hash checks, closed status/kind
  constraints, idempotency uniqueness, and lookup indexes.

The table stores references and hashes only. It does not duplicate the raw
qualification import or artifact bytes. Expected storage is one bounded row
and one append-only event per verification, plus normal index/WAL overhead.

## Reuse and licensing

The implementation reuses the existing Task 79 signer/import authority, Task
80 trust snapshot loader, Task 81 release/evidence store and gate computation,
workspace transaction helpers, RBAC, and CLI request client. No reference
repository source is copied. The implementation remains original Fornix code
under the repository MIT license. External deployment/signing systems remain
deployment-owned and are represented only through hash references.

## Cost and failure budget

- Registration: one workspace transaction, one release lock, one bounded gate
  read, one verification insert, and one append-only event.
- Admission: one read-only workspace transaction with bounded release,
  verification, trust-snapshot, and evidence rows; no network calls.
- HTTP/CLI payloads remain hash-only and bounded. No raw artifact bytes are
  accepted or persisted through this surface.
- Startup checks are bounded by the existing workspace inventory and do not
  retry indefinitely. One failed workspace keeps readiness false and exposes a
  redacted reason.

## Acceptance tests

- exact release/gate/snapshot/artifact binding is required;
- duplicate verification is idempotent and conflicting identity fails closed;
- stale gate hash, stale snapshot, wrong target, unknown kind, failed evidence,
  expired verification, and revoked verification block admission;
- verified admission produces a stable decision hash and replay-equivalent
  output;
- cross-workspace reads/writes fail closed under RLS;
- actor, credentials, raw manifests, and arbitrary text never appear in rows,
  errors, logs, decisions, or CLI output;
- crash before verification commit leaves no verification/event row;
- crash after commit is safely replayable and does not duplicate the row;
- production configuration requires an explicit release ID and readiness is
  false until the release admission decision is ready;
- development remains backward-compatible when release admission is not
  explicitly required;
- existing unit, race, CI, smoke, migration, and documentation checks remain
  green.

## Measured limitations to report

The implementation must report local unit latency and bounded SQL shape, but
must not call that a production SLO. PostgreSQL integration, RLS behavior,
concurrent verification races, startup against a real release catalog, and
deployment artifact truth require a disposable/hosted qualification
environment. The system proves binding and admission logic, not the truth of
external release signatures or the availability of the deployment target.
