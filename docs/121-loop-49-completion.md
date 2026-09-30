# Loop 49 completion: managed credential and durable trust catalog

Status: implemented on the universal production-qualification branch.

## Delivered

- Added migration `048_trust_catalog.sql` for workspace-scoped public signer
  records, signer lifecycle events, and append-only signed policy snapshots.
- Added RLS, bounded JSON constraints, unique workspace/revision and policy
  hash identities, and append-only history triggers.
- Added `TrustCatalogStore` for idempotent signer registration, explicit
  revocation, detached Ed25519 policy publication, monotonic revision
  serialization, and verified current-policy loading.
- Kept private signing keys and provider credentials outside Postgres.
- Added integration tests for clean migration, verified load, duplicate
  publication, immediate revocation, and concurrent duplicate revision
  delivery.
- Documented the relationship between durable catalog authority, process-local
  registry cache, credential lease fences, external secret managers, cost, and
  failure semantics.

## Verification

The focused integration qualification passed against a fresh PostgreSQL 17 /
pgvector database:

```text
FORNIX_TEST_PG_DSN=... go test ./internal/store -run '^TestTrustCatalog' -count=1 -v
```

The normal unit suite, race suite, vet/build checks, documentation checks,
universal trust/egress smokes, local CLI smoke, and Docker local-runtime
workflow remain green on this branch.

## Measured local cost

The catalog adds three bounded Postgres tables and no external service. One
policy publication uses one short transaction plus a workspace advisory lock;
one current-policy read uses one indexed query and local Ed25519 verification.
No secret-manager or network request is made by the catalog. Production
latency, signer rotation lag, resolver cache hit rate, and catalog growth must
be measured in the target deployment.

## Remaining Issue #38 gates

The next work is complete transactional linkage of policy/catalog hashes,
lease fences, capability definitions, operation effects, Work Receipts, and
artifact/evidence references; then sandbox/backend qualification, backup/HA,
retention, load/soak, adversarial security, live connector conformance, and
release compatibility. Fornix remains a universal control-plane foundation,
not a blanket production-readiness claim for every system.
