# Loop 51 completion: managed credential resolution and source-version authority

Status: implemented and locally qualified on the universal production-
qualification branch; not yet committed or merged.

## Outcome

Fornix now has a workspace-scoped managed credential resolver boundary. A
deployment can inject a Vault/OpenBao/cloud/KMS-backed manager without placing
vendor SDKs or secret bytes in the control plane. The durable lease records the
opaque manager source version and source expiry alongside the existing Fornix
fence and revocation epoch.

## Delivered

- Made `credentials.SecretResolver` workspace-aware.
- Added `SecretManager`, `ResolveRequest`, `ResolvedSecret`, and
  `ManagedSecretResolver` contracts with fail-closed scope, version, expiry,
  and redaction validation.
- Added a bounded JSON-over-HTTPS manager adapter with metadata-only requests,
  private token injection, redirect rejection, controlled egress construction,
  response limits, and no response-body error leakage.
- Added migration `050_credential_source_versions.sql` with bounded
  `source_version` and optional `source_expires_at` lease metadata.
- Bound acquisition and renewal to exact source version and source expiry.
- Kept source bytes in memory only; lease/event/database records contain no
  secret value.
- Preserved the explicit owner-only local profile using the compatibility
  source version `local-unversioned`.
- Added unit and PostgreSQL integration tests for workspace scope, manager
  response validation, redirect/size failures, redaction, source-version
  binding, expiry bounding, renewal, fencing, and existing lease behavior.

## Qualification evidence

The focused credentials tests, focused credential-lease tests, fresh-Postgres
credential tests, and the complete repository test suite passed locally. A
fresh disposable database was used for migration and lease tests; it was not
the persistent development database.

The controlled HTTP adapter uses the same egress boundary as other Fornix
HTTP paths. Its generic protocol is intentionally not a claim that every
secret-manager vendor is automatically supported; each deployment adapter must
map its vendor response into the typed contract and repeat the conformance
tests.

## Cost and storage observation

The migration adds two bounded scalar fields and one lookup index to the lease
authority. Managed acquisition performs one bounded secret-manager request and
one existing lease transaction; the database lock is acquired only after the
network resolution completes. No external service is added to the local
development profile.

In a disposable PostgreSQL 17/pgvector database after one credential-lease
qualification, the measured total relation sizes were 80 KiB for
`credential_leases` and 48 KiB for `credential_lease_events`. The relation
sizes include PostgreSQL page and index overhead and are local observations,
not a production capacity estimate.

Production deployment evidence remains necessary for manager p50/p95 latency,
source-rotation propagation, resolver failure rate, token zeroization,
workload-identity/mTLS authentication, and cache policy. Fornix does not cache
secret bytes in this slice.

## Remaining limitations

- No vendor-specific Vault, OpenBao, AWS, GCP, or Azure adapter is shipped in
  the core repository. The HTTP protocol is a bounded deployment seam.
- Existing callers that construct legacy leases without a source version are
  normalized to `local-unversioned`; production managed callers must use the
  versioned resolver.
- Generic operation adapters still need to be wired so every effectful
  capability supplies verified trust-policy and credential-lease facts before
  admission. Task 50 records those facts when supplied but does not invent
  them.
- Signer-rotation ceremony, signed image/catalog distribution, deployment
  role separation, backup/restore, HA, load/soak, sandbox, live connector,
  adversarial security, and release qualification remain open Issue #40 gates.
