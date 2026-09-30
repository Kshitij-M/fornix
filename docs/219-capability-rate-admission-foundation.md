# Durable capability rate admission

Status: design note, written before implementation. This closes a concrete
production-qualification gap tracked by [Issue #40](https://github.com/Kshitij-M/fornix/issues/40).

## Problem and evidence

`CapabilityDefinition.RateLimitPerMinute` is normalized, bounded, signed into
the capability definition hash, and populated by connector adapters. A source
search shows that the value is not currently consumed by operation admission.
The generic admission path does enforce workspace/actor operation and cost
quotas, but two different actors can still admit the same workspace capability
at an unbounded aggregate rate.

The goal is to make the existing capability rate declaration an enforced,
durable admission limit—not to add a second scheduler or provider-side rate
limiter.

## Reference research and reuse decision

- Orloj's HTTP API uses an in-process `x/time/rate` token bucket for edge and
  authentication protection. It bounds stale per-IP entries and trusts
  forwarding headers only behind configured trusted proxies. This is a useful
  model for protecting one API process, but it cannot authoritatively enforce a
  workspace capability limit across replicas or restarts, so Fornix will not
  copy or depend on it for operation admission.
- DeepSeek Harness documents rate limiting as outside its LLM service; retry
  policy executes at durable agent-step boundaries. That separation supports
  keeping provider retry distinct from Fornix's admission decision.
- agentmemory has bounded `Retry-After` handling and provider circuit breakers.
  These improve provider resilience but do not reserve durable per-workspace
  capability capacity.
- Fornix's own `AdmissionStore` is the right authority: it already validates
  operation identity, serializes per-actor quota reservations, reads committed
  admission history, and appends a deterministic decision in one Postgres
  transaction.
- PostgreSQL documents that advisory-lock identifier spaces for one 64-bit
  key and two 32-bit keys do not overlap. Fornix can keep the existing actor
  lock and add a separate capability lock without cross-namespace collisions
  ([PostgreSQL advisory-lock functions](https://www.postgresql.org/docs/17/functions-admin.html)).

No reference source will be copied. The work adds no third-party dependency
and does not change Fornix's MIT licensing.

## Invariants and semantics

1. Every new non-duplicate admission is checked against the current capability
   limit inside the transaction that writes its durable decision.
2. The limit is a rolling 60-second window measured by the Postgres clock.
   The scope is `(workspace, connector name, capability name)`; connector and
   capability version changes do not reset the limit. This is deliberately
   conservative for a shared external account.
3. Existing `MaxOperationsPerWindow` and cost quotas remain independently
   enforced per workspace/actor. Both the actor and capability quota locks are
   acquired before reading counts; the actor lock keeps its existing bigint
   advisory-lock space, while the capability lock uses a dedicated two-int
   advisory-lock namespace. Every admission takes them in the same order.
4. An idempotent duplicate is resolved before quota reservation and consumes
   no second slot. Denied admissions do not consume capacity. An admitted
   operation awaiting approval does consume capacity, preventing unbounded
   approval-queue creation.
5. Caller-provided counts are never authority. The transaction-local count is
   derived from append-only admission decisions; it is excluded from the
   logical input hash and is not accepted from JSON requests.
6. A failed transaction commits neither the admission decision nor its quota
   usage. Postgres remains the only authority; no process-local counter,
   broker, Redis, or cache is introduced.
7. Rate limiting is workspace-scoped, not provider-account-global across
   workspaces. Deployments sharing one provider credential across multiple
   workspaces still need an upstream/provider-level account limit.

## Schema and implementation boundary

Migration 079 adds one partial expression index over workspace, connector
name, capability name, creation time, and decision ID for non-denied admission
rows. It adds no mutable counter table and stores no additional per-request
record. The admission input gains a store-derived, non-JSON count used only by
the pure policy evaluator, plus a stable `rate_limit_exceeded` denial reason.
`AdmissionStore.Admit` takes the capability lock, reads the actor operation/
cost counts and capability count with two indexed scans, evaluates the
immutable capability limit, and writes the normal append-only admission
decision. The generic workflow connector adapter uses this same admission
authority before executing read/observation steps; it fails closed if durable
admission is not configured. Effectful workflow steps continue through the
existing fenced effect dispatcher.

## Cost and operational budget

The expected cost is one additional transaction-level advisory lock command,
one additional indexed scan alongside the existing actor-quota scan, and one
B-tree index. The new lock call adds one database round trip on the admission
transaction. No durable usage row, model call, or new infrastructure is added.
The capability lock serializes admissions for one workspace/capability during
the short database transaction. Actual p50/p95/p99, lock-wait, WAL, and index
size measurements require the disposable Postgres qualification environment;
they will not be represented as measured until that run succeeds.

## Acceptance tests

- Pure policy permits counts below the limit and denies at the boundary with
  the stable rate-limit reason.
- The store ignores forged caller counts and derives usage from durable rows.
- Same-key retries return the original decision without consuming a slot.
- Concurrent different actors cannot exceed the shared capability limit;
  actor quotas continue to hold across different capabilities.
- Denied attempts do not consume capacity, while awaiting-approval decisions
  do.
- Different workspaces and different capability names do not share a bucket;
  a version change of the same connector/capability does.
- Crash/rollback before commit leaves neither a decision nor consumed quota.
- Generic workflow read/observation steps pass through durable admission, and
  a rate-denied step does not invoke its connector.
- A generic workflow executor without the durable admission store fails closed
  before a read-only connector call.
- Background read-only operation workers persist the same idempotent admission
  before connector execution.
- Incident workflow read/observation connector steps persist admissions before
  calling the adapter; missing effect authority never falls through to direct
  effectful execution.
- The migration applies to fresh and previously migrated databases, the full
  policy/store tests and race checks pass, and CI runs the Postgres scenario.
- Existing behavior outside admission, all smokes, documentation validation,
  and workspace isolation remain green.

## Remaining limitations

This enforces operation and generic-workflow connector admission. It does not
rate-limit unauthenticated HTTP traffic, provider requests outside those
admitted paths, or a provider account shared across workspaces. It also does
not establish a latency/SLO claim; qualification must measure the lock and
indexed scans under concurrent load.
