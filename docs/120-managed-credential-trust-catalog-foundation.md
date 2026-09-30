# Managed credential and durable trust catalog foundation

Status: implemented as the next universal qualification slice.

Fornix is a control plane for work against many production systems. A
workspace must be able to refer to credentials and executable capabilities
without placing secret material in the control plane, while operators must be
able to prove which signed capability catalog was active when work was
admitted. This slice makes that authority durable and auditable.

## Problem and scope

The credential lease table already fenced short-lived use, but its resolver
was an injected seam and signed connector trust was held only in a process
registry. A restart or a second worker therefore needed an explicit way to
load the same trust decision. The new catalog stores public verification keys
and signed policy snapshots in Postgres while keeping private keys and
provider secret values outside Postgres.

The slice adds:

- append-only signed trust policy history per workspace and monotonic revision;
- workspace-scoped Ed25519 public signer records with explicit revocation and
  append-only signer lifecycle events;
- transactional, idempotent policy publication with workspace-level revision
  serialization;
- verified current-policy loading that rechecks expiry, signature, policy
  hash, signer status, workspace, and entry ordering on every read;
- actor metadata on signer registration and policy publication;
- migration and RLS coverage for all catalog tables;
- integration tests for publication, duplicate requests, revocation, and
  concurrent revision delivery.

Credential values continue to be resolved by `credentials.SecretResolver`.
`CredentialLeaseStore` resolves outside the SQL transaction, rechecks the
credential reference under lock, records a monotonic lease fence, and exposes
`LeaseValidator` for the final model/connector boundary. No private key or
provider secret is serialized by this slice.

## Invariants

1. Trust and credential records are workspace-scoped. Missing or unset
   workspace context is not an implicit global scope.
2. A trust policy is immutable after publication. Revisions are positive and
   strictly increasing per workspace; duplicate policy hashes are idempotent.
3. A policy can be published only by an active registered signer whose public
   key verifies the detached Ed25519 signature and whose policy window is
   valid. Unknown or revoked signers fail closed.
4. Signer IDs are immutable. Rotation uses a new signer ID; revocation is
   explicit and immediately prevents policy loading, even for an unexpired
   policy signed by that signer.
5. Current-policy reads select the highest non-expired policy with an active
   signer and verify it again before returning it. Catalog corruption, stale
   signatures, malformed entries, and cross-workspace records fail closed.
6. Policy entries contain only normalized connector and capability hashes.
   Actor metadata is bounded and contains identity references, never tokens or
   arbitrary request bodies.
7. A credential reference is an opaque logical pointer. Secret bytes are
   available only in the immediate resolver/adapter memory boundary and never
   enter SQL, events, artifacts, policy snapshots, errors, or metrics.
8. Lease acquisition and validation remain at-least-once around remote
   providers. A durable lease fence prevents stale use; it cannot claim
   exactly-once execution at an external provider.

## Authority and lifecycle

Postgres owns signer status, trust history, credential-reference lifecycle,
lease fences, and audit metadata. A process-local registry is an execution
cache: production composition must load and verify the durable snapshot before
admitting work. A deployment may cache the verified policy, but cache expiry,
signer revocation, or workspace mismatch must fail closed rather than silently
fall back to an unsigned built-in policy.

External secret managers own secret bytes and provider-specific rotation. The
lease store is the Fornix-side authority that binds a resolved secret to a
workspace, logical reference, purpose, expiry, revocation epoch, and fence.
The local owner-only resolver remains suitable for development only.

## Schema and API impact

Migration `048_trust_catalog.sql` adds `trust_signers`, append-only
`trust_signer_events`, and append-only `trust_policies`, each with workspace
RLS. `TrustCatalogStore` exposes registration, revocation, idempotent signed
publication, and verified current-policy loading. It deliberately does not
accept private signing keys or credential values.

The store should be composed with `connector.Registry` by registering the
verified public signer and installing the verified policy before production
operation admission. The existing unsigned `TrustWorkspace` path remains an
explicit local/development compatibility path until deployment configuration
provides a signed catalog.

## Reuse and licensing

The implementation reuses Fornix's existing `TrustPolicy`, `LeaseResolver`,
`LeaseValidator`, credential-reference schema, transaction-local workspace
context, RLS policy, and connector registry. Orloj's explicit provider/resource
admission, DeepSeek Harness credential references, and agentmemory lease and
recovery patterns informed the separation of logical references, short-lived
use, and durable authority. No reference source was copied. Kronaxis Fabric
remains excluded because its BSL 1.1 license is incompatible with Fornix's MIT
distribution.

## Cost and failure budget

Policy publication uses one bounded transaction and one workspace advisory
lock for revision allocation. Current-policy loading uses one bounded indexed
query plus signature verification over bounded metadata; it does not contact
an external service. Signer registration/revocation adds one lifecycle row
and one append-only event. Credential acquisition performs one resolver call
outside the database transaction and one short database transaction. Exact
provider latency and secret-manager cache performance remain deployment-
specific measurements.

## Acceptance tests

- fresh and existing databases apply migration 048 cleanly;
- policies with unknown, revoked, expired, tampered, malformed, or
  cross-workspace trust material fail closed;
- duplicate signer and policy publication is idempotent;
- concurrent publication creates one row for one policy hash and does not
  allocate duplicate revisions;
- revoking a signer prevents current-policy loading immediately;
- catalog rows and event history are workspace-isolated and append-only;
- private keys, credential values, and raw operation payloads are absent from
  catalog rows and error text;
- lease validation remains fenced through credential rotation/revocation;
- process restart can reconstruct the same verified trust policy from
  Postgres without network calls;
- existing unit, integration, race, smoke, build, and documentation checks
  remain green.

## Remaining qualification gates

This slice does not provide a hosted KMS/secret-manager implementation, signer
rotation ceremony, signed binary/image provenance, catalog distribution
service, or deployment-specific pool/backup/HA evidence. Those remain explicit
production qualification work. The durable catalog is a foundation, not a
claim that any external provider has been independently conformance-tested.
