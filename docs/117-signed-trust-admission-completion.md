# Universal trust admission: signed connector capability snapshots

Status: implemented as an opt-in registry boundary; durable catalog storage,
key rotation operations, and deployment artifact signing remain release work.

## Problem

A connector name and capability version are not sufficient authority for a
production operation. Definitions can change schemas, effect classes,
credential requirements, or network behavior while retaining the same human
name. Fornix already pins exact connector and capability hashes; production
composition also needs to know who approved that snapshot and whether it has
expired or moved backwards.

## Delivered

- `connector.TrustPolicy` now supports a detached Ed25519 signature over the
  normalized workspace policy hash, workspace, decimal revision, signer ID,
  and validity window.
- `Registry.SetTrustSigner` installs a copied public verification key.
- `Registry.SetSignedTrustPolicy` verifies the signer, signature, hash, and
  validity window before installation, and rejects a non-monotonic signed
  revision for the workspace.
- `Registry.RequireSignedTrustPolicy(true)` makes unsigned compatibility
  snapshots fail closed at admission. The existing `RequireTrustPolicy` switch
  still controls whether an unconfigured workspace is rejected.
- Unsigned `TrustWorkspace` remains available for isolated development and
  tests; it cannot replace an installed signed policy once signed mode is
  required.
- Tests cover order-independent hashes, tampered entries, unknown signers,
  expired windows, signed admission, and downgrade rejection.

## Trust invariants

1. A signed policy authorizes only the exact connector and capability hashes
   included in its normalized entry set.
2. Signature bytes, private keys, and credential material never enter the
   request, operation, evidence, artifact, or event payload.
3. Signed revisions are decimal counters and must increase strictly for one
   workspace. A lower or equal revision cannot replace a current signed policy.
4. Validity windows are checked at installation; callers should refresh before
   expiry rather than silently extending a stale snapshot.
5. Trust admission does not replace actor authorization, policy admission,
   credential lease validation, destination/egress enforcement, or external
   effect reconciliation.
6. No model, tool, connector, broker, or network call occurs during signature
   verification or policy installation.

## Example composition

```go
registry := connector.NewRegistry()
registry.SetTrustSigner("release-key-1", trustedPublicKey)
registry.RequireTrustPolicy(true)
registry.RequireSignedTrustPolicy(true)

if err := registry.SetSignedTrustPolicy(policy, time.Now().UTC()); err != nil {
    return err // fail closed before a provider is reachable
}
```

The private signing key belongs to a release or policy-management system and
must not be embedded in Fornix or supplied through an operation request.

## Deliberate limits

The current slice verifies an in-memory signed policy. It does not yet provide
a Postgres catalog table, KMS-backed signer rotation, signed container/image
attestations, schema migration compatibility rules, or a deployment-level
root-of-trust distribution protocol. Those are required before claiming
hosted supply-chain qualification. The policy still has to be combined with
the durable credential lease, workspace RBAC, egress boundary, and operation
effect authority.

## Cost and failure behavior

Verification is bounded Ed25519 work over a fixed-size signature and a small
hash-based policy envelope; it performs no database or network work. A bad,
unknown, expired, tampered, or downgraded policy is rejected before admission.
An interrupted installation leaves the previous in-memory policy unchanged.
Because registry installation is process-local today, deployments must load
the same signed snapshot on every replica and must qualify restart/rotation
behavior before unattended use.
