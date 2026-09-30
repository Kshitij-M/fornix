# Live provider and recovery qualification foundation

Status: Task 71 feature note. This document defines the evidence contract and
safe local tooling for deployment qualification; it is not a production
readiness declaration.

## Problem and scope

Fornix now has separate qualification seams for connector conformance,
managed credential authority, certificate validation, PostgreSQL topology,
federation retention, and logical backup/restore. A deployment operator still
has to assemble those outputs manually, which makes comparisons unreliable and
encourages a dangerous shortcut: treating a healthy local test as proof of
availability, provider correctness, or recovery objectives.

Task 71 adds one typed, redacted qualification record that can carry evidence
from those existing seams. The record is deliberately process-local and
artifact-ready. It does not add a database, telemetry collector, provider
orchestrator, HA simulator, or secret manager.

## Qualification invariants

1. A qualification record contains only bounded identifiers, outcomes, public
   fingerprints, hashes, timings, and numeric measurements. It cannot contain
   DSNs, credentials, request payloads, prompts, response bodies, SQL text,
   certificate bytes, or arbitrary provider errors.
2. Every case is categorized and has a deterministic name, outcome, and
   bounded error code. Case and measurement ordering is normalized before the
   report hash is computed.
3. Report identity excludes wall-clock timestamps and observation duration
   unless a caller records them as an explicit bounded measurement. Replaying
   the same evidence therefore produces the same report hash.
4. Target identity is represented by a caller-provided hash, never by a raw
   endpoint, DSN, host, account, or secret reference. A deployment must retain
   the target mapping in its own protected evidence system.
5. A passed live-provider case proves only the exercised capability against the
   named deployment target. It does not prove another capability, provider
   idempotency, compensation, outage behavior, or exactly-once execution.
6. Recovery evidence is explicit. A backup fingerprint, PITR restore,
   failover, partition-maintenance, or identity-rotation drill is `passed`
   only when the deployment supplies the corresponding observation. An
   unexecuted drill is `skipped` or `blocked`, never inferred from health.
7. RPO/RTO values are measured facts with units. Missing measurements remain
   missing; zero is not used as a default success value.
8. Evidence is append-only from Fornix's perspective. The contract links
   hashes and fingerprints to external operator records but never overwrites
   authoritative operation, event, artifact, or provider history.
9. A workspace-scoped qualification preserves workspace identity and actor
   metadata where supplied. Deployment-wide topology evidence may use an
   explicit deployment target hash but must not be presented as workspace data.
10. All input limits are hard bounds: cases, measurements, recovery drills,
    identifier lengths, and report bytes. Oversized or malformed evidence
    fails closed.

## Contract and API decisions

The new `contracts.QualificationReport` is a typed envelope containing bounded
`QualificationCase`, `QualificationMeasurement`, and `RecoveryDrill` values.
`QualificationReport.Normalize` sorts and validates the values and computes a
stable hash. `QualificationBuilder` adapts existing connector conformance
reports without copying raw failures or provider payloads.

The existing `connector.RunConformanceReport` remains the execution source.
Its explicit `AllowExternalEffects` gate, authority admission, egress policy,
and at-least-once boundary are unchanged. The new adapter only converts its
redacted result into the common qualification shape.

The existing certificate and authority observations remain the source of
identity evidence. The existing backup/restore script remains the source of
logical restore fingerprints. The common envelope links those facts rather
than reimplementing them or making a shell script the system of record.

## Live provider boundary

The fake adapter remains the default for unit tests and replay. A live adapter
qualification must supply an explicit deployment binding, authenticated
credential authority, bounded timeout, destination policy, and a dedicated
workspace or external test account. Read-only checks are preferred. Effectful
checks require explicit approval, provider idempotency where supported, and a
verification or compensation procedure. Unknown outcomes remain
`recovery_required` and are never silently retried as if execution were
exactly once.

The qualification report records only the provider/capability identity,
outcome, bounded error class, latency, and evidence hash. It never stores the
live request or response.

## HA, PITR, partition, and certificate boundary

The local repository can validate prerequisites and report supplied evidence,
but only a deployment can execute and measure:

- primary/standby promotion and client reconnect;
- WAL archiving, encrypted backup, PITR restore, and restore ownership;
- measured RPO/RTO under the intended storage and network topology;
- partition creation, retention sweep, archive, and deletion safety;
- workload-identity/mTLS rotation, revocation, pin rollover, and expiry lag.

The common recovery record makes those gaps visible instead of manufacturing
success. A real drill must retain its target topology, commit/version,
operator, timestamps, source/restore fingerprints, replay hash, and bounded
failure output outside the public report.

## Reuse and licensing

The implementation reuses Fornix's connector registry and conformance suite,
effect authority, managed credential resolver, certificate policy, topology
probe, backup fingerprint, and federation retention ownership. The design
follows Orloj's backup/restore checklist and bounded telemetry dimensions,
DeepSeek Harness's versioned persistence validation and provider adapter
obligations, and agentmemory's diagnostic/checkpoint/lease discipline. No
reference source is copied. Kronaxis remains excluded because its repository
is BSL 1.1; Fornix remains MIT licensed.

## Cost and storage budget

The common report is in-memory and bounded. It adds no migration and no
automatic durable row. A serialized report is limited to the existing artifact
disclosure envelope if an operator chooses to retain it. Live tests are
bounded by explicit operation, timeout, response-byte, pool, and account
budgets. No provider call is made by offline unit tests or replay.

## Acceptance tests

- Identical redacted cases and recovery evidence produce identical report
  hashes regardless of map or input order.
- Raw DSNs, credentials, prompts, payloads, SQL, certificate bytes, and
  arbitrary errors cannot enter a normalized report.
- Duplicate case names, cross-workspace evidence, invalid fingerprints,
  negative measurements, unknown outcomes, and oversized reports fail closed.
- Connector conformance converts into the common report without losing its
  stable hash or effect opt-in boundary.
- Missing or unexecuted HA/PITR/partition/rotation drills remain explicitly
  skipped or blocked.
- RPO/RTO values preserve units and distinguish measured from absent.
- Existing connector, credential, certificate, topology, backup, migration,
  race, package, documentation, and smoke checks remain green.
