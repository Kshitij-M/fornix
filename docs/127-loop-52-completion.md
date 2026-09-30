# Task 52 completion — signed capability-schema admission and catalog distribution

Status: implemented and locally qualified on the current feature branch.

Task 52 closes the next universal control-plane gap after managed credential
resolution: a deployment can now publish, verify, persist, load, and install a
workspace-scoped signed catalog that binds a registered connector capability
to its exact input and output schema fingerprints. The catalog narrows
admission; it is not a dynamic schema executor and it does not replace typed
adapter validation.

## Delivered

- Added `internal/connector.SchemaEntry` and `SchemaCatalog` contracts with
  bounded connector/capability hashes, input/output schema versions and hashes,
  deterministic ordering, stable hashes, Ed25519 signatures, validity windows,
  signer identity, and workspace scope.
- Added explicit process-registry installation paths for unsigned development
  snapshots and verified monotonic signed catalogs. Signed mode rejects missing,
  unsigned, expired, downgraded, tampered, or schema-mismatched catalogs.
- Added migration `051_signed_schema_catalog.sql` for append-only,
  workspace-scoped catalog history with RLS, revision/hash uniqueness, bounded
  JSON, signature windows, and an append-only mutation trigger.
- Added `TrustCatalogStore.PublishSignedSchemaCatalog`,
  `CurrentSignedSchemaCatalog`, and `InstallCurrentSchemaCatalog`. Publication
  verifies the active durable signer inside a workspace transaction, serializes
  revision decisions, and treats duplicate delivery as a no-op.
- Kept raw schema documents, executable validators, prompts, credentials, and
  provider payloads out of the catalog and its errors.
- Added connector and Postgres tests for deterministic ordering and hashes,
  signature verification and tampering, expiry, exact input/output admission,
  monotonic installation, unsigned replacement rejection, duplicate
  publication, concurrent duplicate delivery, signer revocation, and foreign
  workspace reads.
- Added `make smoke-universal-schema` and included it in the aggregate smoke
  target. Added the qualification runbook row and updated the public roadmap
  to make Task 53 the next handoff.

## Qualification evidence

The following checks passed against this branch:

| Check | Result |
| --- | --- |
| `go test ./... -count=1` | Passed without a database DSN; database-qualified tests skip as designed |
| `FORNIX_TEST_PG_DSN=... go test ./... -count=1` | Passed against disposable PostgreSQL database `fornix_task52_20260924`; migrations reached `051_signed_schema_catalog` |
| Schema catalog store tests | Passed, including duplicate and concurrent publication |
| Relation measurement | `fornix.trust_schema_catalogs` measured 131,072 bytes for 20 isolated append-only test catalog rows on the local PostgreSQL instance |
| Schema catalog store timing | Targeted Go test wall time 0.90 seconds including build/test startup; this is not a service SLO |
| Persistent database safety | The persistent development database was not dropped or modified by qualification cleanup; only the named disposable database is in scope |

The full race, vet, build, formatting, documentation, package, and schema
smoke qualification passed on this dirty branch. The aggregate Docker smoke
was not rerun because only the development Postgres container was running;
the existing HTTP/runtime smokes remain covered by the prior qualification
record and are unaffected by this opt-in catalog slice. A commit or pull
request still requires the repository's normal CI and maintainer review.

## Authority and replay behavior

The durable signer/catalog tables are the Postgres authority. The in-process
registry is only a verified execution cache and can be rebuilt from the current
catalog. Catalog publication is append-only; a same-workspace same-hash retry
has one durable row and a conflicting or lower revision fails closed. Admission
checks the installed catalog before connector execution. Replaying an
operation uses recorded capability/schema references and does not call a
schema service, model provider, connector, or external tool.

The current slice exposes the catalog hash and revision as bounded admission
facts. It deliberately does not rewrite every existing authority-link row to
retrofit schema fields, because doing so would create a migration-time hash
compatibility problem for historical links. Adapter-wide authority linkage is
the next task.

## Cost and storage impact

Catalog publication is one bounded Postgres transaction plus local Ed25519
verification. Catalog installation is one bounded read plus local signature
verification; normal admission is an in-memory exact lookup and does not make
a network call. The catalog stores hashes and versions, not raw schemas. The
local relation measurement above is an initial storage observation, not a
capacity guarantee; production sizing still needs entry-count, signer-rotation,
catalog-retention, replication, and multi-workspace load measurements.

## Remaining limitations

- No vendor-specific catalog distribution service, rollout/lag protocol, or
  signed image/release verification is included.
- Signer rotation is represented by durable registration/revocation and
  monotonic verification, but the deployment ceremony, key custody, workload
  identity, and emergency recovery process remain deployment-owned.
- Existing effectful adapters do not yet automatically supply catalog and
  managed-credential facts to every operation authority link. That is Task 53.
- The catalog contains schema fingerprints only. Typed adapters remain
  responsible for validation, result normalization, provider idempotency, and
  external-effect verification.
- Universal production readiness still requires sandbox qualification, backup
  and HA evidence, retention and load/soak limits, adversarial security tests,
  live connector conformance, and operational support runbooks.
