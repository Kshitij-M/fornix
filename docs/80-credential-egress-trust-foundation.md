# Universal credential, egress, and capability-trust boundary

Status: core credential, managed-secret-manager adapter, and final egress
fencing paths are implemented across the registry/provider and Loop 106
composition slices. Deployment-specific secret-manager, trust distribution,
and egress qualification remain open production gates.

This slice narrows three production risks that become more serious as Fornix
goes beyond repository work: a connector version can be registered without an
explicit trust decision, a credential can be resolved without a bounded lease
contract, and adapters can silently invent different credential/egress
semantics. It adds reusable contracts, durable lease/revocation authority, and
opt-in signed trust checks without claiming that a local file store is an
external secret manager or that an in-memory registry is a complete signed
supply-chain distribution system.

## Invariants

1. Trust is workspace-scoped and exact. A capability is executable only when
   its connector identity and normalized definition hash are present in the
   active trust policy for that workspace.
2. Trust policy is immutable after construction and hashable. Signed policies
   are verified by a configured Ed25519 root, expire, and require a strictly
   increasing decimal revision. Replacing one is an explicit control-plane
   action; registering a new connector does not silently expand an existing
   policy.
3. Credential leases carry a logical reference, workspace, purpose, lease ID,
   positive fence and revocation epoch, source version, and expiry, but never
   serialize or log secret bytes. A malformed, missing, expired, revoked, or
   cross-workspace lease fails closed.
4. Every lease resolver must also validate the exact lease against its current
   authority. Model, HTTP, and federation paths revalidate after request
   construction and immediately before network I/O; validation is required by
   the resolver interface, not an optional type assertion. Credential-bearing
   request objects drop their authorization header after `Do` returns.
5. The lease interface is an adapter seam. Postgres remains the authority for
   operation admission and effects; an external secret manager owns secret
   material and lease revocation when an integration supplies one.
6. Destination policy remains adapter-enforced. The shared contract must be
   restrictive and hashable; HTTP bindings continue to enforce host, path,
   redirect, private-network, timeout, and byte limits at the request
   boundary.
7. Replay and conformance use recorded hashes and never reacquire credentials,
   contact a destination, or execute a connector.

## Scope and compatibility

- Credential migration `047_credential_leases.sql` is the durable authority
  for lease fences, expiry, revocation epochs, and lifecycle events. Trust
  snapshots remain process configuration in this slice, while the durable
  connector binding and operation/admission records remain the authorities
  for workspace state.
- Existing registries without an installed trust policy retain compatibility
  for tests and explicitly configured development adapters. The current server
  composition installs an unsigned built-in snapshot for the alpha path;
  hosted production composition must load a signed snapshot and enable
  `RequireSignedTrustPolicy(true)` before admitting work.
- Existing `CredentialResolver` functions remain source-compatible. The new
  lease resolver remains an explicit adapter seam, but any implementation
  must expose current-authority validation; implementations that cannot
  provide it fail closed before egress.
- No secret manager, broker, network proxy, or signing service is introduced.

## Research and reuse

- Orloj's explicit provider/resource registration and model gateway lookup
  inform exact identity matching.
- DeepSeek Harness credential references inform the separation between a
  logical reference and short-lived secret material.
- agentmemory lease/checkpoint patterns inform bounded expiry and fail-closed
  recovery; they are not authority for Fornix.
- The existing Fornix HTTP binding remains the egress implementation for its
  adapter; this slice does not weaken its SSRF, path, redirect, or private
  network checks.
- No reference source is copied. Kronaxis Fabric remains excluded because its
  BSL 1.1 license is incompatible with Fornix's MIT distribution.

## Cost and failure budget

Trust admission is an in-process map lookup and a bounded hash comparison; it
adds no SQL or network work. Lease validation is constant-time with respect to
secret size and has no database cost in the interface itself. External secret
manager implementations must report lookup latency, lease failures, cache
use, and rotation/revocation lag without placing request IDs or secret values
in metric labels.

## Acceptance tests

- an exact trusted connector/capability hash is admitted;
- an untrusted connector version, altered definition hash, or cross-workspace
  capability is rejected;
- trust snapshots produce the same hash independent of registration order;
- replacing a trust snapshot is explicit and does not mutate the old policy;
- a valid credential lease is accepted only for its workspace, reference,
  purpose, and unexpired time;
- expired, revoked, malformed, and cross-workspace leases fail closed;
- secret bytes cannot be marshaled, formatted, or included in a trust/lease
  diagnostic;
- replay/conformance paths do not invoke a lease resolver or external adapter;
- existing unit, Postgres integration, race, smoke, and documentation checks
  remain green.

## Remaining qualification gates

The built-in bounded HTTP secret-manager adapter is a protocol integration, not
a deployment qualification: this slice does not provision credentials or
prove the manager's identity, availability, rotation, or revocation behavior.
It also does not provide durable signed catalog distribution, a network egress
proxy, schema/release attestation, backup/restore drills, HA design, or load
qualification.
Those remain Issue #40 gates and must be implemented and measured before a
production-readiness claim.
