# Universal credential, egress, and capability-trust boundary

Status: implementation note for the next Issue [#40](https://github.com/Kshitij-M/fornix/issues/40) qualification slice.

This slice narrows three production risks that become more serious as Fornix
goes beyond repository work: a connector version can be registered without an
explicit trust decision, a credential can be resolved without a bounded lease
contract, and adapters can silently invent different credential/egress
semantics. It adds reusable contracts and fail-closed checks without claiming
that a local file store is an external secret manager or that a process-local
registry is a signed supply-chain authority.

## Invariants

1. Trust is workspace-scoped and exact. A capability is executable only when
   its connector identity and normalized definition hash are present in the
   active trust policy for that workspace.
2. Trust policy is immutable after construction and hashable. Replacing it is
   an explicit control-plane action; registering a new connector does not
   silently expand an existing policy.
3. Credential leases carry a logical reference, workspace, purpose, lease ID,
   and expiry, but never serialize or log secret bytes. A missing, expired, or
   cross-workspace lease fails closed.
4. The lease interface is an adapter seam. Postgres remains the authority for
   operation admission and effects; an external secret manager owns secret
   material and lease revocation when an integration supplies one.
5. Destination policy remains adapter-enforced. The shared contract must be
   restrictive and hashable; HTTP bindings continue to enforce host, path,
   redirect, private-network, timeout, and byte limits at the request
   boundary.
6. Replay and conformance use recorded hashes and never reacquire credentials,
   contact a destination, or execute a connector.

## Scope and compatibility

- No migration is required: the trust snapshot is process configuration in
  this slice, while the durable connector binding and operation/admission
  records remain the authorities for workspace state.
- Existing registries without an installed trust policy retain compatibility
  for tests and explicitly configured development adapters. Production server
  composition installs a snapshot after built-in registrations; an untrusted
  capability then fails at admission.
- Existing `CredentialResolver` functions remain source-compatible. The new
  lease resolver is optional and is preferred by adapters that can provide
  expiry/revocation semantics.
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

This slice is not an external secret-manager integration, signed connector
catalog, network egress proxy, tenant RLS implementation, backup/restore drill,
HA design, or load qualification. Those remain Issue #40 gates and must be
implemented and measured before a production-readiness claim.
