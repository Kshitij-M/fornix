# Task 86 feature note — deployment-owned boundary and provider evidence

Status: implementation design for the next universal production-qualification
slice. This note is a contract for evidence handling, not a claim that the
repository can prove a hosted network, secret manager, certificate authority,
or remote provider by itself.

## Problem and scope

Fornix already records the normalized `ExternalBoundaryAuthority` used by an
effect reservation: the egress-policy hash, destination-policy hash, explicit
network-boundary mode, and network-boundary hash. It also accepts signed,
deployment-owned qualification bundles and binds release admission to a
Postgres gate. The remaining gap is that a signed external-effect qualification
can describe *that* a deployment checked credential resolution, workload
identity/mTLS, DNS and proxy enforcement, provider idempotency, or recovery,
without carrying a typed, reviewable identity for those observations.

This slice adds that identity and threads it through the existing evidence and
release-admission paths. It does not add a secret manager, KMS, proxy,
firewall, DNS service, certificate authority, provider adapter, broker, or
deployment executor. Deployment code remains responsible for collecting and
signing observations from the systems it owns.

## Invariants

1. Boundary evidence is bounded, typed, hash-only, workspace/target-scoped
   through its enclosing qualification report, and contains no secret,
   credential, URL, prompt, certificate, token, header, or provider payload.
2. A passed evidence item must identify the exact external-boundary hash it
   observed and must carry a stable evidence hash. The boundary hash must be
   the normalized `ExternalBoundaryAuthority.StableHash()` for the effect it
   qualifies; Fornix never reconstructs it from deployment prose.
3. Evidence kinds are a closed vocabulary: credential resolution, workload
   identity, mTLS/certificate, DNS/rebinding, proxy/firewall, provider
   idempotency, and external recovery. Unknown kinds fail closed.
4. External-effect release evidence must contain exactly one passed boundary
   observation with a future expiry. This keeps a release-admission reference
   unambiguous while making deployment proof time-bounded; separate signed
   bundles can qualify different provider or connector boundaries.
5. The evidence link stores the derived boundary and evidence-set hashes. It
   never trusts caller-supplied copies and never overwrites an accepted import.
6. A release-admission reference may carry the exact boundary hash. In strict
   external-effect mode, the operation effect reservation must carry the same
   hash; missing or mismatched proof fails closed before dispatch.
7. Duplicate imports, evidence links, release verifications, and operation
   reservations remain deterministic and idempotent. A changed boundary is a
   conflict, not an update.
8. Replay and dry-run paths never contact a provider, secret authority,
   network, certificate authority, or external recovery system.

## Schema and API changes

- Add bounded `BoundaryQualificationEvidence` records to the existing signed
  `QualificationReport`. Report and observation hashes include the normalized
  records, so signatures cover them without a second evidence authority.
- Add `external_boundary_hash`, `boundary_evidence_hash`, and the
  `boundary_evidence_expires_at` validity boundary to deployment evidence
  links and release verifications. Migration `071` is additive and gives
  historical rows empty compatibility values; new strict external evidence
  must populate both hashes and a future expiry.
- Add the same hash-only fields to `DeploymentAdmissionReference` and
  `DeploymentAdmissionDecision`. Their stable identities therefore change when
  the deployment boundary proof changes.
- Add a small helper that validates an external-effect signed import contains
  one exact, passed boundary observation. The helper is used inside the same
  Postgres transaction that links evidence and evaluates release admission.

## Reuse and licensing

The implementation reuses the existing signed qualification envelope and
Ed25519 verification, trusted signer catalog, deployment evidence gate,
release-admission reference, `ExternalBoundaryAuthority`, operation effect
reservation, workspace transactions, RLS, and append-only event history. It
does not copy reference-repository source. Orloj, ClawMem, agentmemory, and
FornixDB informed the bounded evidence/replay model; Kronaxis remains excluded
because its repository is BSL 1.1. Fornix remains MIT licensed.

## Cost and storage budget

The added contract is a bounded list of short identifiers and SHA-256 hashes.
No raw deployment output is duplicated. Migration `071` adds two indexed hash
columns to two existing, low-cardinality authority surfaces. Evidence linking
adds one bounded read/decode of the already imported signed bundle inside the
existing transaction; operation reservation adds only a constant-time hash
comparison. The repository does not claim production latency or storage SLOs
without a disposable PostgreSQL qualification run.

## Acceptance tests

- Boundary evidence normalizes deterministically, rejects unknown kinds,
  partial hashes, secret-bearing fields, duplicate IDs, and oversized lists.
- Equivalent evidence order produces the same report, observation, and signed
  bundle hashes; tampering invalidates the signature.
- External-effect evidence linking derives one exact boundary hash and fails
  closed for missing, duplicate, failed, expired, or cross-target evidence.
- Release-admission references preserve the boundary hash and reject altered
  or absent boundary proof when strict external effects are enabled.
- Duplicate link, verification, and operation reservations have one durable
  effect; changed hashes are conflicts.
- Workspace isolation, RLS, dry-run, crash rollback, and stale release/fence
  behavior remain fail closed.
- Fake/read-only qualification remains offline; no test invokes a live model,
  connector, secret manager, proxy, DNS resolver, or external tool.
- Existing unit, race, vet, migration, smoke, documentation, and package
  checks remain green. PostgreSQL-backed migration and concurrency evidence is
  collected only when the configured disposable DSN is present.
