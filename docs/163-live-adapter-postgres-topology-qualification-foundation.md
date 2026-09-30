# Live adapter and PostgreSQL topology qualification foundation

Status: implementation feature note for Task 70. This note defines a
provider-neutral qualification boundary; it is not a claim that one local
database is highly available or that any vendor adapter has been certified.

## Problem and scope

Fornix now has durable operation authority, effect admission, provider-neutral
credential injection, certificate policy, and fenced retention ownership. The
remaining evidence gap is operational: teams need a safe way to qualify a
registered adapter against a controlled live binding and to measure the
PostgreSQL deployment assumptions that protect workspace isolation and
recovery.

This slice adds:

1. A bounded conformance report over the existing connector registry. It
   exposes capability identity, case status, redacted error codes, timings,
   and a stable report hash. Effectful execution requires an explicit opt-in
   authority envelope; the default qualification remains side-effect-free.
2. An opt-in PostgreSQL topology qualification. It verifies migration
   compatibility, transaction-local workspace context cleanup, concurrent
   pool behavior, connection acquisition latency, WAL/archive settings, and
   read-only/recovery facts. It does not simulate or claim an HA failover.

No broker, external collector, object store, cloud secret manager, or new
runtime service is introduced.

## Invariants

1. Conformance reports contain no request payloads, prompts, credentials,
   response bodies, SQL text, or arbitrary adapter errors. Only bounded case
   names, normalized error classes, capability identity, latency, and stable
   hashes are disclosed.
2. Report ordering is deterministic: capability identity and case order are
   normalized before hashing. Timing is an observation, not an input to the
   replay hash.
3. A read-only/observation capability may be qualified with a recorded or
   controlled response. A reversible, approval-required, irreversible, or
   external-communication capability cannot run through the qualification
   report without an explicit `AllowExternalEffects` flag and a complete
   `EffectAuthority`.
4. Stale definition hashes, cross-workspace targets, missing authority,
   missing credentials, and invalid evidence fail closed before dispatch.
5. Topology qualification requires an explicitly supplied disposable or
   deployment test DSN. It never chooses the development DSN implicitly.
6. Workspace context is transaction-local. Commit, rollback, cancellation,
   and pooled-connection reuse must leave no workspace context behind.
7. Topology facts are observations, not guarantees. Archive/WAL checks may be
   required by configuration, but a primary being healthy is never reported
   as proof of HA, PITR, failover, RPO, or RTO.
8. All measurements are bounded by a fixed operation count and pool limit.
   Qualification failures are redacted and do not become durable authority
   state unless an operator explicitly stores the resulting report as an
   artifact.

## Contract and API decisions

`ConformanceReport` and `ConformanceCase` are process-local contracts. A
report includes a schema version, workspace, capability identity, outcome,
bounded case observations, and a stable hash that excludes wall-clock timing.
The existing `RunConformanceSuite` remains the execution source; the report
layer is responsible for redaction and deterministic serialization.

`PostgresTopologyQualification` is an opt-in test harness controlled through
`FORNIX_TOPOLOGY_PG_DSN`, bounded operation/worker settings, and an optional
archive requirement. It reports server version, recovery/archive/WAL facts,
pool acquisition percentiles, waiting/canceled acquisition counters, and
workspace-context leakage results. It does not modify production data beyond
applying already-versioned migrations on the explicitly named target.

## HA, PITR, partition, and certificate boundary

The harness can verify prerequisites and preserve replay/fingerprint identity,
but it cannot prove failover without a deployment topology. Actual evidence
must be supplied by the deployment owner for:

- primary/standby promotion and client reconnect behavior;
- WAL archiving, backup encryption, PITR restore, and restore ownership;
- measured RPO/RTO under the intended storage class and pool limits;
- scheduled partition creation/retirement and retention-owner liveness;
- workload-identity/mTLS rotation, revocation, pin rollover, and expiry.

Those are explicit gates in the runbook rather than inferred from a healthy
single-node probe.

## Reuse and licensing

The implementation reuses Fornix's connector registry/executor, capability
definition hashes, effect authority envelope, workspace transaction helper,
RLS qualification scripts, backup fingerprints, and existing reference
adapters. The design follows Orloj's health/controller seams, DeepSeek
Harness's explicit capability boundaries, agentmemory's diagnostics and
bounded replay, ClawMem's recorded-result replay, and FornixDB's measurement
discipline. No reference source is copied. Kronaxis remains excluded because
its repository is BSL 1.1; Fornix remains MIT licensed.

## Cost and storage budget

Conformance reports are in-memory and bounded to one capability's cases. The
topology probe performs at most the configured acquisition operations and
bounded context transactions; it stores no payloads and emits no new durable
rows. A deployment may archive the redacted report as an existing artifact,
subject to its normal disclosure and retention policy.

## Acceptance tests

- Identical conformance inputs produce the same case order and report hash.
- Reports redact raw errors, payloads, SQL, credentials, and arbitrary text.
- Read-only fixture adapters pass conformance; stale definitions and
  cross-workspace targets fail closed.
- Effectful adapters require an explicit authority envelope and opt-in flag.
- Missing, stale, or incomplete effect authority is rejected before dispatch.
- Topology qualification is skipped without its explicit DSN and refuses an
  unbounded operation or pool setting.
- Commit, rollback, cancellation, and concurrent pooled transactions do not
  leak workspace context.
- WAL/archive/recovery facts are reported accurately; archive requirements
  fail deterministically when enabled and unavailable.
- Acquisition p50/p95/p99, pool counters, and storage-safe facts are bounded
  and reproducible enough for comparison.
- Existing tests, race checks, builds, docs, smokes, migrations, and backup
  fingerprints remain green.
