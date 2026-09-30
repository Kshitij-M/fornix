# Loop 79 completion — deployment-owned qualification trust and authorized import

Status: implemented repository-owned trust/import slice; not a production
deployment certification.

## Delivered

- Added workspace- and deployment-scoped `QualificationTrustedSigner` and
  bounded import contracts. Only public Ed25519 verification material is
  represented; private keys are not accepted by the API or persisted.
- Added migration 066 with current signer state, append-only signer lifecycle
  events, immutable accepted imports, append-only import events, unique
  workspace/deployment identities, signed-byte limits, and transaction-local
  workspace RLS.
- Added `QualificationTrustStore` with transactional registration, atomic
  rotation, immediate revocation, validity-window checks, authorized import,
  duplicate replay, conflict detection, dry-run validation, bounded listing,
  and explicit signed-envelope disclosure.
- Added strict signed JSON decoding. The exact submitted signed bytes and
  source hash are retained, while ordinary list responses disclose only
  bounded metadata and hashes.
- Added separate authorization capabilities:
  `qualification:read`, `qualification:import`, and `qualification:admin`.
  Authenticated actor and workspace scope are propagated to successful signer
  and import records.
- Added authenticated HTTP surfaces for signer registration/rotation,
  revocation, listing, authorized import, import listing, and explicit
  disclosure.
- Added operator CLI commands for signer lifecycle, authorized import, dry
  run, bounded list, and disclosure. Existing offline `qualification import`
  remains offline validation; `import-authorized` is the durable route.

## Verification

Focused tests cover public-key-only normalization, scope validation, route
permission mapping, concurrent registration, rotation, supersession,
revocation, dry-run side-effect behavior, duplicate import identity,
conflicting idempotency, exact-byte disclosure, and workspace-bounded store
queries. `make qualification-trust` runs the focused contract/server/CLI
checks and the PostgreSQL store tests when `FORNIX_TEST_PG_DSN` is explicitly
provided.

The local environment in which this loop was implemented did not provide a
PostgreSQL DSN, so the live migration and store integration cases were
compiled and skipped locally. CI runs them against its disposable pgvector
PostgreSQL service. No Docker image, container, database, cache, or build
artifact was created by this loop.

## Cost, storage, and limitations

Trust decisions are one bounded indexed lookup plus a transaction protected by
one workspace/deployment advisory lock. Signer rows contain a fixed 32-byte
public key and bounded metadata. Import rows retain at most 128 KiB of signed
JSON per unique workspace/deployment/signed hash, plus append-only audit facts.
No model, provider, broker, Redis, object store, or key service is introduced.

This slice proves Fornix can authorize evidence acceptance against a durable
catalog. It does not perform key generation, HSM/KMS custody, signer
distribution, mTLS/workload identity, deployment truth verification, backup or
restore certification, or provider/live-system qualification. The catalog is
an acceptance authority, not a claim that the signed deployment evidence is
truthful. Production still needs a deployment-owned rotation ceremony and
distribution/lag policy.

## Next task prompt

Task 80 — build deployment-wide qualification trust distribution and startup
conformance.

Before coding, read the chats directory, `AGENTS.md`, docs 14, 90, 111, 159,
161, 163, 165, 175, 177, 179, 181, 183, and this completion note. Study the
existing trust catalog, signed schema catalog, connector admission, credential
lease, controlled egress, authority-link, readiness, and deployment
qualification paths. Write a feature note covering catalog publication and
distribution, signer rotation ceremony, revocation propagation, revision
monotonicity, startup readiness, workspace/deployment isolation, offline
behavior, rollback, audit, storage, cost, and acceptance tests.

Implement the smallest production-quality vertical slice:

- Add a durable deployment qualification trust snapshot with monotonic
  revision, source hash, signer identity, validity window, and revocation
  state; never replace history.
- Add an authorized publish/load path that verifies a catalog snapshot using
  the existing trusted signer authority and rejects unsigned, downgraded,
  expired, revoked, cross-deployment, or conflicting snapshots.
- Bind startup/readiness and authorized import to the loaded snapshot revision.
- Add bounded refresh, stale-snapshot fail-closed behavior, rotation and
  revocation propagation tests, and deterministic replay/hash checks.
- Keep private keys and deployment secrets outside Fornix; use no broker,
  external key service, object store, or new infrastructure.
- Add CLI/API inspection, migration, CI, smoke, documentation, and measured
  database/storage/latency evidence.

Acceptance: only the authorized current snapshot admits imports; revisions
never move backward; revocation and expiry propagate deterministically;
startup fails closed when required trust is absent or stale; duplicate
publication is idempotent; old snapshots remain auditable; workspace and
deployment boundaries fail closed; existing tests, race checks, builds, CI,
smokes, and offline qualification remain green.
