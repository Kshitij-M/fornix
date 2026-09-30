# Multi-domain reference workflows and qualification foundation

Status: implemented as a bounded fake-first qualification slice (Task 94).

## Purpose

Fornix is a domain-neutral work control plane. Its universal claim must be
exercised by more than a repository adapter or one incident example. This
slice adds deterministic reference scenarios for incident response, data
pipeline operations, customer support, and infrastructure maintenance.

The scenarios are qualification fixtures. They use the same generic
operation request, plan, effect classification, approval boundary, replay
hash, and Work Receipt expectations that a production adapter uses, but they
do not pretend to execute a provider, change a customer record, restart a
service, or mutate a data system. The fixture boundary is explicit so a
passing local test cannot be mistaken for live connector qualification.

## Invariants

- Every scenario is workspace-scoped and actor-scoped.
- Scenario identity, target identity, capability identity, and plan identity
  are hash-bound and deterministic.
- A scenario can contain read-only observation and validation steps without
  requiring an external authority.
- An effectful scenario step is always classified as an explicit effect. The
  fixture never silently upgrades observation into a write.
- Approval-required effects remain approval-required in the generated plan;
  the fixture cannot authorize them.
- The same normalized request produces the same step order, plan hash, trace
  hash, and replay hash across runs.
- Cross-workspace targets and actors fail closed.
- Raw prompts, credentials, arbitrary payloads, and provider responses are
  never placed in the scenario authority; only bounded hashes and references
  are retained.
- The fixture runner has no network, filesystem, model, broker, or database
  side effects. Postgres-backed operation, effect, and receipt stores remain
  the authorities for real work.

## Scenario coverage

| Scenario | Observation | Validation | Optional effect | Verification |
| --- | --- | --- | --- | --- |
| incident response | incident signal | diagnosis policy | remediation boundary | post-action health |
| data pipeline | source/window state | quality policy | replay/backfill boundary | output watermark |
| customer support | case state | response policy | ticket/update boundary | case status |
| infrastructure maintenance | service state | change policy | maintenance boundary | health/rollback |

The scenario names describe capability families, not built-in provider
integrations. A connector must register and qualify its own capability schema,
trust, credentials, egress, idempotency, verification, and recovery behavior.

## Replay and failure semantics

The fixture runner computes a deterministic trace from the normalized plan and
bounded synthetic step outcomes. Replaying it never calls an external system.
An effectful step is represented as an uncommitted boundary marker unless a
caller supplies an already-recorded effect result hash. Unknown external
outcomes therefore remain unknown; the fixture never claims exactly-once
execution.

The production workflow runtime continues to own leases, checkpoints,
recovery-required states, retries, and cancellation. This task adds scenario
coverage for those contracts, not a second scheduler or lease table.

## Reuse and licensing

The implementation reuses Fornix's existing domain-neutral contracts,
normalizers, plan hashing, effect classes, and replay conventions. It copies
no reference-repository source. Reference repositories remain architectural
inputs only; no BSL-licensed Kronaxis source is used. The added code remains
under the repository's MIT license.

## Cost and storage budget

The runner is offline and in-memory. It adds no database table, artifact, raw
payload, provider call, or container. Tests operate on four short plans and
bounded hashes. Production operation and receipt rows are unchanged; a real
deployment should measure those stores separately under its workload.

## Acceptance tests

- All four scenarios normalize and produce valid generic plans.
- Plan, trace, and replay hashes are stable across repeated runs.
- Reordering input metadata cannot change canonical hashes.
- Cross-workspace actor and target combinations fail closed.
- Read-only scenarios contain no effectful step.
- Effectful scenarios preserve the declared approval/effect classification.
- Replay performs no external calls and returns the same terminal hash.
- Unknown effect outcomes are represented as unresolved rather than success.
- The focused Make/CI qualification command and all existing tests remain green.

## Limitations

This slice proves generic contract portability and deterministic replay, not
live provider behavior. Before production use, each connector still needs a
separate qualification pack for authentication, rate limits, idempotency,
verification, outage recovery, network policy, sandboxing, and data retention.
The universal transformation remains incomplete until deployment-owned
database, credential, adapter, backup/restore, load, security, release, and
support gates have executable evidence.
