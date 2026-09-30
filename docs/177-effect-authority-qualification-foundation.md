# Task 76 — Authority-aware effect qualification

Status: implemented repository-owned qualification slice; not a production-
readiness declaration.

## Problem

Fornix's generic dispatcher and domain-effect link tests already exercise
reservation, fencing, duplicate suppression, uncertain recovery, and link
reconciliation against disposable PostgreSQL. The qualification bundle runner
does not yet have a typed way to capture those results. As a result, passing
tests are useful CI evidence but are not composable with the redacted
QualificationReport envelope.

## Scope

Add a provider-neutral, hash-only observation contract and probe adapter for
dispatcher-backed checks. The probe accepts an explicit authority-owned
callback or disposable PostgreSQL integration harness; it never discovers
credentials, endpoints, or databases implicitly. The default CLI remains
offline and read-only.

## Invariants

1. A successful effect qualification must prove reservation identity, one
   domain-link identity, reconciled result hash, receipt/link hash linkage,
   duplicate suppression, stale-fence rejection, workspace isolation, and
   replay stability.
2. An uncertain provider outcome must prove recovery-required state in both
   generic and domain authorities without claiming a result hash.
3. External invocation count is bounded and recorded as a measurement. The
   qualification layer never retries a provider call.
4. Evidence contains hashes, bounded state labels, and counts only. Raw
   requests, responses, SQL, DSNs, credentials, and provider errors are not
   representable.
5. A local fake or unit simulator can prove contract behavior but is labeled
   portable/local evidence; it cannot be reported as deployment qualification.
6. Cross-workspace, stale-fence, missing-authority, and incomplete observation
   inputs fail closed.

## Reuse and licensing

This slice reuses the existing effect dispatcher, domain-effect link
transitions, Work Receipt hash contracts, QualificationReport/Manifest, and
Postgres integration test fixtures. It follows Orloj's provider outcome and
fencing boundaries, ClawMem's replay/abstention evidence discipline, and
agentmemory's diagnostic lifecycle without copying source. Kronaxis BSL 1.1
source remains excluded; Fornix remains MIT-licensed.

## Cost and storage

The observation is an in-memory or bounded local report. No migration, service,
broker, provider call, or persistent row is added by the portable seam. The
disposable PostgreSQL probe is opt-in, bounded to one workspace and a small
number of effects, and must be cleaned by its existing integration harness.

## Acceptance tests

- Complete success observation produces a stable passing case.
- Missing hashes, incomplete flags, cross-workspace identity, and invalid
  invocation counts fail closed.
- Uncertain recovery cannot claim a result hash.
- Duplicate delivery records one invocation.
- Stale fences and foreign workspace access are recorded as required proof.
- Identical observations produce identical evidence hashes.
- The default offline CLI never invokes the probe.
- PostgreSQL-backed dispatcher tests can be adapted into the observation
  contract without printing secrets or raw provider data.

## Explicit limitation

This is qualification evidence composition, not production certification.
Live provider idempotency, deployment secret management, HA/PITR, sandbox
strength, and topology-specific performance still require deployment-owned
drills.
