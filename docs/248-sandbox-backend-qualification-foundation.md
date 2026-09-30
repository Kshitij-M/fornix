# Sandbox backend qualification foundation

Status: feature note for binding non-local sandbox registration and durable
attempt identity to signed, deployment-authorized runtime qualification.
Audience: Fornix security reviewers, runtime-provider authors, and operators.

## Problem and decision

Fornix already distinguishes read-only mounts, a read-only runtime root,
filesystem isolation, network isolation, process-tree termination, resource
limits, and recovery identity. A provider's `Available` flag and capability
list are still declarations, however. They do not establish that the exact
provider build, runtime configuration, image, and isolation test suite were
qualified by an authority the deployment trusts.

Before any non-local provider can be selected, require a signed, unexpired
qualification proof. Reuse Fornix's existing Ed25519-signed qualification
bundle and trust key policy. Do not add a second signature format, store
private keys in the control plane, copy sandbox implementation code, or treat
mock-provider tests as evidence of runtime isolation. The default
`local-process` profile remains explicitly limited and does not require or
receive a strong-isolation qualification.

## Invariants

1. A non-local provider is unavailable unless a trusted Ed25519 key verifies a
   qualification bundle for the configured deployment target.
2. The signed report must contain one passing sandbox case whose evidence hash
   matches the typed qualification evidence; the signed manifest must bind the
   same case and hash.
3. Evidence binds backend, provider build, runtime build, runtime configuration,
   effective capability report, immutable image digest, tested numeric budget
   envelope, conformance-suite hash, observation time, and expiry. Missing,
   stale, unknown, or mismatched facts fail closed.
4. Registration and every resolution compare the provider's current runtime
   identity and capability controls to the attested values. `Available` is a
   live readiness condition, not a substitute for qualification.
5. Read-only mount, read-only root, filesystem isolation, and network isolation
   remain independent controls. A signed test result may not imply a control
   that is absent from the report or the requested profile. A requested
   profile must also fit within the signed numeric envelope; generic CPU,
   memory, PID, scratch, timeout, output, argv, and environment capability
   names do not qualify arbitrary limits.
6. The qualification hash is recorded in the tool request evidence and bound
   into the sandbox execution identity. Recovery can inspect only the exact
   qualified backend/attempt; changed qualification cannot relaunch or
   reconcile an old attempt as if it were the same runtime. Runtime providers
   must revalidate the bound fingerprint atomically immediately before
   creating a runtime; registry preflight alone cannot close provider-internal
   check-to-use races.
7. No missing provider, invalid signature, expired qualification, runtime
   drift, or capability mismatch falls back to `local-process`.
8. Qualification proves only the bounded target/runtime/profile facts named
   by the attestation. It is not a general certification of a host, kernel,
   cloud account, operator process, or future runtime configuration.

## Contract and persistence impact

Add a hash-only `SandboxQualificationEvidence` and a proof wrapper containing
the existing `SignedQualificationBundle`. Add a `sandbox` qualification case
category. The trust input is a deployment-selected key ID, public key, and
target hash; the private signing key remains outside Fornix. Trust is
deployment-scoped by default; operators may pin a workspace in the trust
configuration when separate runtime evidence is required. This proof is not
an authorization grant and does not replace workspace RBAC.

Add `SandboxQualificationHash` to normalized tool request evidence and
`QualificationHash` to the versioned `SandboxExecutionIdentity`. These values
are non-secret hashes. Existing request JSON remains readable; a legacy
non-local recovery record without qualification identity fails closed rather
than inventing an attestation. No migration is planned: the tool request is
already stored as bounded JSON evidence, and the runtime identity is passed to
the provider. If a durable qualification history or revocation cursor is later
needed, it must use the existing deployment trust/qualification authority, not
a second sandbox ledger.

Legacy pending non-local attempts without the qualification hash are not
silently upgraded: they remain recovery-required and cannot be relaunched or
reconciled through the qualified path. Operators must inspect the exact old
backend attempt and resolve it using deployment-owned recovery procedures;
Fornix must not guess, substitute a backend, or mark an uncertain effect
complete.

## Reuse, licensing, and cost

Reuse the existing signed qualification bundle, trust-key selection,
capability-normalization, sandbox registry, and attempt-aware recovery
interfaces. Reference research supports keeping the controls orthogonal:
DeepSeek Harness reports backend-specific enforcement; Orloj separates
capability authorization from runtime selection and uses explicit container
controls; OpenSandbox separates root/workspace mounts from network policy.
These are architectural references only; no third-party source is copied.
The reviewed reference licenses are MIT for DeepSeek Harness and Apache-2.0
for Orloj and OpenSandbox. Any future code reuse must preserve the relevant
license headers/notices and undergo dependency review.

There is no new database, image pull, service, or dependency. Signature and
hash verification is bounded CPU work at registration/resolution; evidence
storage is a small provider configuration object. No benchmark claim is made
until a real runtime is available.

## Acceptance tests

- Local-process registration and default startup remain unchanged and continue
  to report the controls they do not enforce.
- A valid trusted proof admits only its exact backend and image; the returned
  qualification hash is deterministic.
- Missing proof/trust, unknown signer, wrong target, bad signature, tampered
  evidence hash, failed/skipped case, expired/future evidence, and a manifest
  mismatch all fail closed.
- Provider build/runtime/configuration/capability drift after registration
  makes resolution fail closed.
- Workspace-pinned qualification cannot resolve without the matching
  execution workspace; deployment-wide qualification remains an explicit
  trust configuration choice.
- Profiles exceeding the signed numeric budget envelope fail closed.
- Requested required capabilities must be present in both the current provider
  report and the signed evidence; isolated controls are not inferred from
  read-only mount or root claims.
- Duplicate tool requests bind the same qualification hash; changed
  qualification conflicts. Durable attempt identity and recovery bind that
  same hash and never fall back to another backend.
- Existing tests, formatting, static checks, CI, and smokes remain green.
- Real filesystem, network, resource-limit, process-tree, and crash-recovery
  claims remain unqualified until the signed evidence is produced by tests on
  the actual supported sandbox backend and deployment target.
