# Task 52 — signed capability-schema admission and catalog distribution

Status: feature note for the next universal production-qualification slice.

Fornix already signs a trust allowlist containing connector and capability
definition hashes. Those definition hashes include input and output schema
hashes, but schema compatibility is only implicit. A deployment needs a
durable, independently inspectable schema catalog so startup and admission can
prove that the registered adapter, operation request, and result contract all
refer to the same schema snapshot.

This slice adds a signed schema catalog beside the existing signed capability
trust policy. It does not replace capability definition hashes or make JSON
schema execution dynamic. The catalog is a second fail-closed admission fact;
the connector remains responsible for typed validation and the operation
authority remains responsible for lifecycle and replay.

## Invariants

1. A schema entry is workspace-scoped and binds one exact connector hash,
   capability definition hash, input schema version/hash, and output schema
   version/hash.
2. The catalog is immutable, detached-signed, monotonic by decimal revision,
   bounded in entry count and encoded size, and verified against the active
   workspace signer before installation or durable publication.
3. A signed schema catalog cannot authorize an unregistered or differently
   hashed capability. It narrows admission; it never creates a capability.
4. When signed-schema mode is enabled, missing, expired, revoked, downgraded,
   cross-workspace, or mismatched catalogs fail closed before execution.
5. Operation input schema version/hash must match the registered definition
   and the installed catalog entry. Result validation continues to enforce the
   output schema at the connector boundary.
6. Catalog revisions and hashes are available as bounded admission facts for
   the durable operation layer. This slice does not retrofit every historical
   authority-link row; adapter-wide linkage is an explicit follow-up so that
   older links remain hash-stable.
7. Replay reads the recorded schema/catalog references and never fetches or
   executes an external schema service.

## Authority split

```text
signed catalog publisher
  → Postgres trust_schema_catalogs (public metadata + signature)
       ↓ verify active signer, window, revision, and hashes
process registry schema snapshot
       ↓ exact connector/capability/schema admission
typed connector validation and result schema checks
```

The catalog stores fingerprints and versions, not executable validators or
unbounded schema documents. Future schema documents may be content-addressed
artifacts, but that is not required for this slice.

## Schema change

Migration `051_signed_schema_catalog.sql` adds append-only, workspace-scoped
schema-catalog rows containing revision, catalog hash, bounded entry JSON,
signature metadata, validity window, and audit actor. It adds RLS,
workspace/revision/hash uniqueness, bounded JSON checks, and an append-only
mutation trigger. Existing trust policies remain readable and valid; signed
schema enforcement is opt-in until a deployment installs a catalog and
enables the registry requirement.

## Reuse and licensing

The implementation reuses `CapabilityDefinition` schema hashes, the existing
Ed25519 trust-signing helpers and signer catalog, `TrustCatalogStore`, the
process-local `connector.Registry`, and existing operation request/result
validation. No dynamic plugin loader, policy language, broker, or vendor SDK
is introduced. The code is independently implemented under Fornix's MIT
license; no Kronaxis Fabric BSL 1.1 source is copied.

## Cost and operational budget

- Publishing or loading a catalog is one bounded Postgres transaction/query
  plus local Ed25519 verification. Admission performs an in-memory exact
  lookup and does not make a network call.
- Entry count and encoded JSON are bounded so a malicious catalog cannot turn
  startup or admission into an unbounded memory operation.
- The catalog adds one append-only relation and one revision/hash index. The
  schema stores hashes and versions, not raw schema documents.
- Production qualification still needs signer rotation, catalog distribution
  lag, startup behavior when catalogs are stale, cache invalidation, and
  compatibility policy for rolling deployments.

## Acceptance tests

- Identical definitions produce identical catalog hashes and stable entry
  ordering.
- Tampered, unsigned, expired, revoked, cross-workspace, duplicate, and
  downgraded catalogs fail closed.
- Concurrent duplicate publication is idempotent; conflicting same-revision
  publication is rejected.
- A registered capability with a mismatched input/output schema is rejected in
  signed-schema admission mode.
- A signed catalog cannot authorize a different capability definition hash.
- Signer rotation accepts the new signer only after registration and rejects
  the revoked signer immediately.
- Workspace data cannot cross catalog reads or registry installation.
- Reinstalling the same verified catalog is deterministic and idempotent.
- Catalog metadata and errors contain no credentials, prompts, raw schema
  documents, or arbitrary manager text.
- Fresh/existing migration, crash/rollback, concurrency, race, full tests,
  and universal smokes remain green.
