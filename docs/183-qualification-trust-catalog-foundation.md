# Task 79 — Qualification trust catalog and authorized evidence import

Status: feature note; implementation follows.

## Problem and scope

Task 78 added deterministic Ed25519 signatures for bounded qualification
bundles, but its embedded public key is only an integrity hint. A production
operator must be able to answer a stronger question:

> Was this evidence signed by a currently trusted key for this deployment and
> workspace, at the time it was imported?

This slice adds a qualification-specific trust catalog and a durable import
ledger. It deliberately does not reuse the connector/capability trust policy
as if the two evidence domains had the same lifecycle. Connector admission
trust and deployment qualification attestation remain separate authorities.

The catalog stores public verification material and lifecycle metadata only.
Private keys, secret-manager values, raw deployment credentials, and provider
payloads remain outside Fornix.

## Invariants

1. A trusted signer is scoped to exactly one workspace and deployment identity.
   Its key ID and public key are immutable once registered. Rotation registers
   a new key and marks the superseded key unusable for new imports while
   preserving its row and audit history.
2. A signer is import-eligible only when its algorithm, public key, key ID,
   deployment scope, status, and validity window all match the request. The
   embedded key in a signed bundle never authorizes itself.
3. Import scope is explicit and fail-closed: workspace, deployment, target
   hash, signer key ID, signed subject hash, and observation hash must agree.
   A target-hash mismatch is a stale-bundle rejection.
4. The signed bundle is verified before any durable import row is created.
   The original bounded signed bytes are retained together with their SHA-256
   source hash, signed subject hash, observation hash, provenance reference,
   actor, and import identity. Existing evidence is never overwritten.
5. `(workspace, deployment, signed_hash)` is the durable evidence identity.
   Repeating the same bytes is idempotent. Reusing an identity with different
   bytes or a different idempotency request is a conflict, not an overwrite.
6. Dry-run validation performs the complete trust, signature, scope, size, and
   conflict checks but commits no signer, import, or audit mutation.
7. Signer lifecycle and import audit rows are append-only. Mutable current
   status is a lookup optimization; the event history remains authoritative
   for rotation, revocation, supersession, and import provenance.
8. All reads and writes use transaction-local workspace context plus explicit
   workspace predicates. Cross-workspace and cross-deployment lookups fail
   closed even when a caller supplies a valid signed bundle.
9. Disclosure is bounded and redacted. Public keys, hashes, statuses, actor
   references, and source references may be disclosed; private keys, raw
   credentials, DSNs, prompts, and unrestricted payloads are not representable.

## Contract and schema changes

- Add `QualificationTrustedSigner`, signer status constants, import request,
  import record, and bounded signer/import page contracts.
- Add migration 066 with:
  - `qualification_trusted_signers` for workspace/deployment/key lifecycle;
  - append-only `qualification_trusted_signer_events`;
  - `qualification_imports` for the immutable signed bytes and import facts;
  - append-only `qualification_import_events`;
  - unique identity and idempotency indexes, bounded JSON/byte checks, and
    workspace RLS policies.
- Add a Postgres `QualificationTrustStore` with transactional register,
  rotate, revoke, list, authorized import, dry-run, bounded disclosure, and
  deterministic duplicate/conflict behavior.
- Add authenticated HTTP routes and operator CLI commands for signer
  registration/rotation/revocation/listing and authorized import/disclosure.

The import path accepts a bounded `json.RawMessage` for the signed envelope so
the exact submitted bytes can be retained. The parsed contract is separately
normalized and verified; the stored source hash is over those submitted bytes.

## Rotation, revocation, and validity semantics

- Registration of an existing key ID is idempotent only when every immutable
  field matches. A different public key under the same identity fails closed.
- Rotation requires an active predecessor in the same workspace/deployment,
  creates a new key ID, and atomically marks the predecessor superseded. The
  predecessor's history remains readable but cannot authorize a new import.
- Revocation is immediate and append-only. It cannot be silently undone by
  re-registering the same key ID.
- `valid_from` and `valid_until` are checked against the caller's explicit
  reference time, which makes tests and replay deterministic. The normal
  import path uses UTC now; a future or expired signer fails closed.

## Authorization and API behavior

Signer lifecycle requires `qualification:admin`; import requires
`qualification:import`; disclosure requires `qualification:read`. The
authenticated actor is stored with every successful lifecycle/import event.
Development mode may use the existing explicit wildcard compatibility path,
but production authorization remains workspace-scoped and fail-closed.

The durable `import` route returns `created`, `deduplicated`, or a stable
conflict/not-trusted error. It does not execute a model, tool, connector,
broker, or deployment operation. `dry_run=true` returns the same validation
facts without inserting a row. Pagination uses bounded limits and opaque
cursor values derived from durable ordering; disclosure returns hashes and
metadata plus the bounded signed envelope only when explicitly requested.

## Reuse and licensing

The implementation reuses Task 78's normalized signature subject and
verification, `AuditActor`, existing `beginWorkspaceTx`/RLS boundaries,
Postgres migration checksums, operator HTTP authorization, strict bounded JSON
readers, and existing redacted error mapping. It independently reimplements
the qualification-specific lifecycle rather than conflating it with the
connector trust catalog. No Kronaxis source is copied; Kronaxis remains BSL
1.1 and Fornix remains MIT-licensed.

## Cost and storage budget

The trust decision is a bounded indexed lookup plus one transaction. Signer
rows contain a fixed-size Ed25519 public key and bounded metadata. Import rows
are capped at the existing qualification evidence limit (128 KiB) and are
deduplicated by workspace/deployment/signed subject. Dry-runs perform no
durable writes. No model token, provider call, broker, Redis, object store, or
new service is introduced.

## Acceptance tests

- Fresh and existing databases apply migration 066 cleanly.
- Registering the same signer is idempotent; conflicting key material fails.
- Rotation atomically authorizes the new signer, rejects the superseded signer,
  and preserves the old signer and lifecycle events.
- Revoked, expired, not-yet-valid, unknown, malformed, and cross-deployment
  signers fail closed.
- A valid signed bundle imports once; identical replay returns the same
  durable record without overwriting bytes or adding a second import effect.
- Same signed identity with different bytes or idempotency identity conflicts.
- Cross-workspace, cross-deployment, and stale target bundles fail closed.
- Dry-run performs no signer/import/audit mutation.
- Bounded pagination is deterministic and workspace isolated.
- Disclosure preserves source bytes, signed/observation hashes, and provenance
  while excluding secret-shaped fields and unbounded content.
- Concurrent register/rotate/revoke/import attempts preserve one authority
  outcome; a crash before commit leaves no import row, and replay after commit
  is idempotent.
- Existing tests, race checks, builds, CI, smokes, and offline qualification
  remain green.

## Explicit limitations

The catalog is a Fornix authority for *accepting evidence*, not a deployment
identity provider. Key generation, private-key custody, HSM/KMS ceremonies,
signer distribution, and operator approval remain deployment-owned. A trusted
signature still does not prove that the underlying deployment claim is true;
it proves only that an authorized catalog key signed the bounded evidence.
