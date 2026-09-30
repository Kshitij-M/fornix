# Loop 46 completion: durable credential lease authority and redacted model evidence

Status: implemented on the qualification branch; an external KMS/secret-manager
adapter and deployment-specific egress enforcement remain production gates.

## Why this loop matters

Fornix is a universal work control plane. Models, connectors, tools, and
workflow adapters all need the same answer to a sensitive question: may this
workspace use this logical credential for this provider and purpose right now?
That answer must remain durable and auditable without turning PostgreSQL into a
secret store.

## Delivered

- Migration `047_credential_leases.sql` adds workspace-scoped credential lease
  rows and append-only lease lifecycle events.
- Lease rows contain only the credential reference, provider/purpose binding,
  expiry, monotonic fence, revocation epoch, and lifecycle state. Secret bytes
  never enter PostgreSQL, events, artifacts, or JSON.
- Acquisition resolves a current reference through an injected
  `credentials.SecretResolver`, rechecks the reference under a row lock,
  expires previous holders, and commits a strictly increasing fence.
- Validation checks workspace, lease identity, fence, revocation epoch,
  reference status, and expiry immediately before the provider side effect.
- Rotation and revocation fence active leases in the same transaction as the
  credential-reference mutation. A stale provider therefore fails closed even
  when it still holds an in-memory secret copy.
- Renewal and release are exact-fence operations; stale holders cannot renew,
  release, or revive a replacement lease.
- Every lease resolver now includes an authoritative `ValidateLease` method;
  model, HTTP, and federation adapters call it at the outbound boundary. A
  missing validator or non-positive fence/revocation epoch is rejected before
  credential bytes are sent. The existing environment-backed path remains an
  explicit development-only compatibility path. See the later hardening in
  [`235-effect-result-and-credential-lease-atomicity-foundation.md`](235-effect-result-and-credential-lease-atomicity-foundation.md).
- Durable model request evidence is now structural: hashes, byte counts,
  roles, bounded counts, and metadata keys. Prompts, message bodies, tool
  schemas, and arbitrary metadata values are not persisted as request
  evidence.

## Invariants

1. A lease is valid only for one workspace, logical credential reference,
   provider purpose, fence, revocation epoch, and bounded expiry.
2. The database is the lease authority; the secret resolver is the secret
   authority. Neither replaces the other.
3. Rotation, revocation, and lease fencing commit atomically with their
   authoritative reference mutation.
4. Lease acquisition and external provider execution remain at-least-once.
   A lease bounds authorization and stale-worker behavior; it cannot make a
   remote API exactly once.
5. Replay, evaluation, and inspection never reacquire credentials or invoke a
   provider.
6. Request evidence is safe to persist but is not a substitute for the
   authoritative model-call response needed by deterministic duplicate
   handling. Response disclosure remains bounded and redacted at its existing
   artifact boundary.

## Failure and crash semantics

- A crash before lease commit leaves no durable lease or lifecycle event.
- A crash after lease commit leaves an expiring lease; takeover obtains a
  higher fence and the old holder is rejected by validation.
- A crash during rotation or revocation rolls back both the reference state and
  lease fencing.
- A crash after a remote provider accepts a request remains an at-least-once
  provider boundary and is reported as such in the model-call ledger.
- A missing or stale reference, resolver failure, expired lease, revoked
  reference, and fence mismatch fail closed without exposing secret material.

## Qualification and cost

The slice adds one context-setting statement and one bounded lease transaction
around acquisition/validation. Acquisition performs a current-reference read,
one secret-manager call, one locked recheck, one bounded update of prior
holders, a monotonic fence allocation, and one append-only event. Provider use
adds one validation transaction before the external call. Lease rows and
events are small metadata records; no secret or raw prompt storage is added.
Exact latency, pool, WAL, and storage impact must be measured on the target
deployment and are not inferred from local unit tests.

## Deliberate limits

- Fornix provides the provider-neutral lease contract and Postgres authority,
  not a KMS, HSM, cloud secret-manager, or encryption-at-rest policy.
- The local profile resolver is suitable for a single-user development
  runtime. Hosted deployments must inject a managed resolver and configure
  network/egress policy outside the process boundary.
- Lease validation does not prove the remote provider received the intended
  destination or request; connector-specific egress and verification remain
  separate gates.

## Acceptance coverage

- Fresh migration and role-separated RLS qualification include both lease
  tables.
- Unit tests prove structural request evidence excludes raw prompts and
  metadata values.
- Integration tests prove monotonic takeover fences, stale-token rejection,
  renewal, exact-fence release, revocation fencing, and workspace scope.
- Existing Go, race, migration, package, documentation, and smoke checks
  remain required before merge.

## Next boundary

Signed connector/capability trust admission is recorded in
[`117-signed-trust-admission-completion.md`](117-signed-trust-admission-completion.md).
The next universal production slice is actual deployment egress enforcement.
Those boundaries are intentionally separate from credential lease authority so
a valid secret cannot silently authorize an untrusted adapter or destination.
