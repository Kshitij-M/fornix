# Loop 78 completion — signed deployment-owned qualification evidence

Status: implemented repository-owned qualification slice; not a
production-readiness declaration.

## Delivered

- Added bounded `QualificationSignature` and `SignedQualificationBundle`
  contracts using Go standard-library Ed25519.
- Bound signatures to the normalized workspace, target, report hash, manifest
  hash, runner version, commit hash, environment-name set, schema version, and
  hash-only redacted observation.
- Kept private keys outside Fornix. The signed contract contains only a public
  key, key reference, signature, signed subject hash, and bounded evidence.
- Added offline cryptographic verification and optional deployment-selected
  key ID/public-key binding.
- Added deterministic signed-bundle merge over the existing qualification
  merge path. Exact duplicate signed input is idempotent; conflicting case or
  check evidence fails closed; an aggregate requires an explicit new signer.
- Added strict bounded CLI commands:
  - `fornix qualification sign`
  - `fornix qualification validate-signed`
  - `fornix qualification import`
  - `fornix qualification merge-signed`
  - `fornix qualification hash-signed`
- Added raw/hex key parsing, restrictive atomic output permissions, unknown
  field rejection, scope checks, redaction tests, Make coverage, and CI
  coverage.
- Updated the qualification runbook and universal roadmap status. The
  repository explicitly does not treat a local signature as hosted production
  proof.

## Verification

The focused signed qualification tests cover deterministic signatures,
tampering, trust-key mismatch, malformed algorithms, duplicate merge,
conflicting evidence, workspace/target scope, bounded raw/hex key handling,
strict unknown-field rejection, restrictive file permissions, and private-key
redaction. `make qualification-signed` is the offline CI entry point.

The signed path performs no database, provider, model, tool, broker, network,
or key-service operation. The optional PostgreSQL effect-authority probe from
Task 77 remains separately gated by an explicit disposable DSN and is not
invoked by this task.

## Cost, storage, and limitations

Ed25519 signing and verification use fixed-size key/signature material and
bounded local CPU. Signed envelopes remain within the existing 128 KiB
qualification limit. No migration, service, artifact table, or persistent
qualification row was added. Atomic files are mode `0600`; operators are
responsible for retaining or disposing of them.

The embedded public key is sufficient for offline integrity verification but
is not a trust catalog. Key rotation, revocation, signer authorization,
deployment evidence retention, and hosted import persistence remain
deployment-owned. A valid signature proves key possession and subject
integrity; it does not prove that the deployment check was truthful or that
HA/PITR, failover, credential rotation, provider idempotency, sandbox, or
load/soak behavior occurred.

## Next task prompt

Task 79 — build the deployment trust catalog and revocation/rotation-aware
qualification import boundary.

Before coding, read the chats directory, `AGENTS.md`, docs 14, 90, 111, 159,
161, 163, 165, 175, 177, 179, 181, and this completion note. Study the
existing identity/RBAC, credential trust catalog, certificate, authority-link,
qualification signature, and artifact retention paths. Add a feature note
covering trust-key lifecycle, key rotation, revocation, validity windows,
workspace/target binding, import idempotency, audit history, storage limits,
operator authorization, and acceptance tests.

Implement the smallest production-quality vertical slice:

- Add workspace- and deployment-scoped trusted qualification signer records
  with key IDs, public-key hashes, validity windows, revocation, supersession,
  and append-only audit history.
- Resolve signed qualification imports only through the trusted catalog;
  embedded public keys must never authorize themselves.
- Reject revoked, expired, not-yet-valid, superseded, cross-workspace,
  cross-target, stale, malformed, duplicate-conflict, and over-budget bundles.
- Make import identity and duplicate replay deterministic and idempotent.
- Preserve the original signed bytes/hash and source/provenance references;
  do not overwrite prior evidence when a signer rotates.
- Keep private keys outside Fornix and never log, store, or expose secret
  material.
- Add a dry-run import path, bounded pagination, audit disclosure, and
  deterministic operator CLI/API behavior.
- Add migration, concurrency, rotation, revocation, expiry, workspace
  isolation, replay, redaction, crash, and authorization tests.
- Keep PostgreSQL as the only authority; do not add a broker, key service,
  object store, or external trust infrastructure.

Acceptance: only catalog-authorized signers import; rotation preserves old
history; revocation and validity windows fail closed; duplicate imports have
one durable effect; conflicting evidence remains auditable; dry-run is
side-effect free; workspace and target boundaries fail closed; existing tests,
race checks, builds, CI, smokes, and offline qualification remain green.
