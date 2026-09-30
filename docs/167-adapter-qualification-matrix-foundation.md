# Adapter qualification matrix foundation

Status: Task 72 feature note. This is a deterministic fixture/replay
qualification surface; a passing matrix is not live provider certification.

## Problem and scope

Fornix has a shared connector conformance suite, but it is currently invoked
one capability at a time. That makes it easy for a new adapter to receive
unit coverage without appearing in a comparable qualification report. Task 72
adds a bounded matrix runner over explicitly supplied registries and typed
requests. The default path uses fixture or recorded adapters and cannot call
effectful capabilities without an explicit opt-in.

This slice covers the reusable runner and the built-in adapter matrix tests.
It does not invent live URLs, databases, provider keys, or external test
accounts. Deployment-owned live qualification remains an explicit operator
step.

## Invariants

1. Matrix entries are explicitly named and sorted before execution. Duplicate
   names, missing registries, invalid requests, cross-workspace requests, and
   unbounded entry counts fail closed.
2. Every entry uses the existing `RunConformanceReport` boundary. Raw adapter
   errors, request payloads, response bodies, credentials, SQL, and arbitrary
   provider text never enter the matrix report.
3. Read-only and observation entries may use fixtures or recorded responses.
   Reversible, approval-gated, irreversible, and external-communication
   entries are blocked unless the caller explicitly enables external effects
   and supplies the existing authority admission.
4. A matrix report preserves each connector's stable capability/report hash,
   workspace scope, bounded case outcomes, and deterministic ordering.
5. Matrix aggregation is monotonic: a failed case cannot be hidden by a later
   pass; a blocked case remains visible; skipped evidence is never promoted to
   passed.
6. The matrix has hard bounds on entries, context timeout, and report size
   inherited from the qualification contracts. No retry or fallback is added
   around an external call.

## Reuse and adapter coverage

The runner reuses the connector registry, request normalization, admission,
effect authority, conformance report, and common `QualificationReport`. Tests
exercise the fake incident adapter, HTTP API adapter with a local controlled
server, read-only SQL adapter with a fixture database, and repository adapter
with a deterministic inspector. The effectful fake remediation capability is
included only to prove fail-closed blocking.

This follows DeepSeek Harness's explicit adapter protocol and versioned
fixture/replay discipline, Orloj's model/tool/controller qualification seams,
agentmemory's bounded diagnostic/evaluation runs, and Fornix's own evidence
and authority rules. No reference source is copied. Kronaxis remains excluded
because its repository is BSL 1.1; Fornix remains MIT licensed.

## Cost and storage budget

The matrix is in-memory and bounded to 64 entries per run. Fixture tests make
no network or database calls. A live caller must provide its own timeout,
workspace, credential lease, egress policy, provider idempotency, and
verification authority; the runner does not claim exactly-once external
execution. No migration or durable row is added.

## Acceptance tests

- Identical matrix entries and fixture responses produce identical report and
  case hashes regardless of input order.
- HTTP, SQL, repository, and fake read-only capabilities pass shared
  conformance with no raw payload disclosure.
- Effectful remediation is blocked by default and cannot reach its handler.
- Duplicate entry names, cross-workspace entries, missing fixtures, malformed
  requests, and oversized matrices fail closed.
- A failed or blocked entry determines the aggregate outcome and cannot be
  masked by later passing entries.
- Workspace and target identities remain bound to every report case.
- Existing tests, race checks, builds, package checks, documentation checks,
  and smokes remain green.
