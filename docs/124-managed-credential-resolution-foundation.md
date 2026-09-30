# Task 51 — managed credential resolution and authority distribution

Status: feature note for the next universal production-qualification slice.

This slice closes a specific authority gap in the universal execution plane:
Fornix already records workspace-scoped credential references and fenced leases,
but the resolver contract does not carry workspace identity and a lease does
not retain the version returned by an external secret authority. A deployment
could therefore configure a resolver that silently resolves the wrong
workspace or rotates a provider value without leaving a durable version fact.

The goal is not to embed a cloud-vendor SDK or store provider secrets in
Postgres. The goal is a narrow, provider-neutral boundary that a deployment can
back with Vault, OpenBao, a cloud secret manager, or a KMS-backed service while
Fornix remains the authority for workspace scope, lease fencing, revocation,
and audit history.

## Invariants

1. Every resolution request carries a non-empty workspace, validated logical
   reference, provider, and purpose. A workspace-less managed lookup is
   rejected.
2. A managed source returns secret bytes only in memory, an opaque source
   version, and an optional source expiry. The source version and expiry are
   never written to logs, evidence, prompts, artifacts, or model/tool output.
3. A credential lease is bound to the workspace, reference, purpose, Fornix
   revocation epoch, Fornix fencing token, and managed-source version. A stale
   fence, rotated reference, revoked reference, source-version mismatch, or
   expiry fails closed before an external request.
4. The secret-manager request contains only non-secret metadata. Authentication
   to that manager is injected through a private token source and is never
   serialized or included in error text.
5. The resolver and lease authority do not claim exactly-once provider use.
   Remote model and connector calls remain at-least-once; the lease only
   bounds and audibly identifies the credential authority used.
6. The existing owner-only local profile is development compatibility mode. It
   remains available only through explicit local configuration and is not
   presented as tenant-isolated managed secret storage.
7. Resolver errors are stable and redacted. Response bodies, authorization
   headers, secret values, and arbitrary manager text never cross the error or
   durable-state boundary.

## Authority split

```text
external secret manager/KMS
  └─ secret bytes + opaque source version
       ↓ (in-memory only)
workspace-scoped resolver
  └─ validates scope, provider, purpose, expiry, and budgets
       ↓
Postgres credential lease authority
  └─ Fornix fence, revocation epoch, source-version identity, audit events
       ↓
model/connector boundary
  └─ validate exact lease immediately before sending bytes
```

The external manager owns secret material and its own rotation policy. Fornix
owns which workspace and capability may use a logical reference, which lease
is current, whether that lease is revoked, and which non-secret source version
was admitted.

## Schema change

Migration `050_credential_source_versions.sql` adds a bounded, non-secret
`source_version` to `fornix.credential_leases`. Existing rows are backfilled
to the explicit compatibility value `local-unversioned`; this preserves
historical auditability without pretending that old local leases were resolved
through a managed authority. New managed leases must provide a non-empty
source version. Lease validation compares the supplied version exactly.

The lease-event metadata may contain the same opaque source version for
diagnostics. It must not contain a secret, source response, URL query value,
authorization value, or arbitrary manager payload.

## Managed-manager protocol

`internal/credentials/managed.go` defines the provider-neutral contracts. The
resolver accepts an injected `SecretManager`; Fornix does not depend on a
particular vendor SDK. `internal/credentials/http_manager.go` implements a
bounded JSON-over-HTTPS protocol for deployments that expose a compatible
broker. It requires a caller-supplied controlled `http.Client`, rejects
redirects and oversized bodies, sends a metadata-only request, and reads only
the bounded response fields `secret`, `version`, and `expires_at`. Production
deployment code must construct that client with Fornix's controlled egress
policy and an injected private token source.

The HTTP protocol is an adapter seam, not a universal secret-manager
interoperability claim. Vendor-specific adapters belong outside the authority
package and must pass the same conformance tests.

## Rotation and expiry

- Fornix credential-reference rotation revokes all active leases in the same
  transaction and advances the Fornix epoch.
- A managed source rotation changes its opaque source version. A new lease
  records the new version; the old lease remains invalid if its Fornix
  reference was rotated or its lease was taken over.
- A source expiry earlier than the requested lease TTL bounds the lease to the
  earlier time. An already expired source response is rejected.
- Renewals do not refresh secret bytes or silently adopt a new source version.
  A caller must acquire a new lease to observe rotation.

## Reuse and licensing

The implementation reuses the existing `credentials.Secret`, redaction
helpers, `CredentialLeaseStore`, controlled egress client, and model/HTTP
lease hooks. It introduces no broker, Redis, NATS, cloud SDK, or LLM
dependency. It is independently implemented under Fornix's MIT license. No
Kronaxis Fabric source is copied; that repository's BSL 1.1 license remains
outside Fornix's distributable boundary.

## Cost and operational budget

- A managed acquisition performs one bounded manager resolution plus one short
  Postgres lease transaction. The resolver must not hold a database lock while
  making a network request.
- The HTTP adapter caps request and response bodies, uses an explicit timeout,
  disallows redirects, and performs no retries by default. Secret acquisition
  retries must be owned by the manager or an explicitly configured caller so
  duplicate secret reads are visible and bounded.
- Source versions, lease IDs, fences, and hashes are small metadata. Secret
  bytes are held only for the immediate provider call and must be cleared when
  practical.
- Production qualification still needs manager latency, cache hit rate,
  rotation lag, lease-acquisition failure rate, and zeroization evidence in
  the target deployment.

## Acceptance tests

- Workspace-scoped requests cannot resolve a different workspace's reference.
- Missing workspace, provider, purpose, source version, or valid expiry fails
  closed.
- HTTP manager requests contain no secret and reject redirects, oversized
  bodies, malformed JSON, invalid expiry, and secret-bearing errors.
- Secret values never appear in logs, errors, JSON, events, artifacts, or
  lease metadata.
- A fresh managed acquisition records its source version and exact lease fence.
- Source-version, reference-version, revocation, expiry, and stale-fence
  mismatches are rejected before use.
- Renewal preserves source version and does not silently fetch a replacement.
- Duplicate and concurrent acquisitions produce monotonic fences and no
  ambiguous active authority.
- Rotation revokes old leases and permits the new source version.
- Existing local development resolver behavior remains green only through the
  explicit compatibility path.
- Workspace-isolation, crash/rollback, race, migration, and full-smoke checks
  remain green.
