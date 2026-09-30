# Task 54 — Universal effect-authority conformance and startup qualification

Status: design note written before implementation. This document defines the
qualification boundary for effectful adapters; it is not a production-readiness
claim.

## Problem

Fornix already persists operation admission, leases, schema identity, managed
credential facts, effect reservations, results, and receipts. The remaining
risk is composition: an adapter can still be wired incorrectly, a process can
restart with stale in-memory trust state, or a worker can pass an authority
envelope whose operation lease expired immediately before dispatch. These are
universal control-plane risks. They apply equally to repository changes,
payments, ticketing, deployments, data jobs, communications, and future
connectors.

Task 54 makes the authority boundary an explicit adapter conformance rule and
qualifies its startup and refresh behavior. It does not attempt to make an
external system exactly-once.

## Invariants

1. Every effectful capability is either authority-aware or cannot be executed
   through the production executor.
2. An authority envelope is reference-only. It contains workspace, operation,
   task, schema, lease, and source-version facts; it never contains a secret,
   prompt, provider payload, or raw credential.
3. The durable operation admission and effect reservation are the Postgres
   authority. A process-local registry is only an execution cache.
4. The operation owner and fence, task owner and fence, schema catalog
   revision/hash, and credential lease/source facts must match the durable
   admission immediately before adapter dispatch.
5. A stale, expired, released, revoked, missing, downgraded, or mismatched
   authority fails closed. Historical links remain readable for audit.
6. A retry is allowed only before content or an external effect has been
   emitted and only when the capability's declared retry policy allows it.
7. Remote delivery remains at-least-once. Provider idempotency keys,
   verification, reconciliation, and compensation are adapter responsibilities.
   Fornix will not claim exactly-once remote execution.
8. Every lookup and mutation is workspace-scoped, and actor/causation/
   correlation/idempotency metadata follows the durable chain.

## Startup, refresh, and rollback semantics

At startup the server registers only explicit built-ins, then loads the
highest non-expired signed trust policy and schema catalog for each durable
workspace. Signatures are verified against active durable signers. A restart
must not silently recreate an unsigned process-local trust snapshot.

Development compatibility may allow a missing signed catalog when explicitly
configured. Invalid, expired, revoked, or conflicting catalog data is never
silently downgraded to that mode. Production requires both signed policy and
signed schema identity. The refresh path is bounded and paginated; it records
the loaded revision, hashes, signer, expiry, and load time so operators can
detect deployment lag. Newer revisions may replace an installed snapshot;
older revisions and signer downgrade attempts fail closed. Signer rotation is
represented by a new signer identity and remains auditable.

Before an operation is admitted, the active durable catalog is revalidated or
refreshed. This makes revocation and expiry effective without waiting for a
process restart. The additional Postgres reads are intentional: effect
authority is a safety boundary, not a best-effort cache. A future deployment
may use a bounded refresh lease only if it preserves the same fail-closed
expiry and revocation guarantees.

## Adapter conformance

Read-only and observation capabilities may use the ordinary capability seam.
Write, approval-required write, and other effectful capabilities must
implement the authority-aware seam and receive the exact admitted envelope.
The executor validates live operation/task ownership immediately before each
attempt, including retries. The adapter must use the supplied credential lease
and provider idempotency identity, perform destination-specific validation,
and return bounded redacted evidence. It must not acquire a replacement lease
or infer authority from mutable process state.

The conformance suite enumerates registered effectful capabilities and proves:

- authority-aware dispatch is implemented;
- workspace and operation identity are bound;
- stale operation/task fences are rejected before dispatch;
- an existing credential lease is reused rather than replaced;
- duplicate requests are idempotent;
- a crash before local commit is replayable;
- provider idempotency/verification/compensation metadata is explicit; and
- secrets are absent from errors, events, links, and evidence.

The offline fake incident adapter is a deterministic test adapter. It exercises
the same authority seam without pretending to provide external exactly-once
behavior.

## Schema and API impact

Task 54 adds no new authority table. It uses the Task 53 authority facts and
adds only the minimum process/runtime contract needed to validate the live
operation owner alongside its fence. Existing rows and historical hashes are
not rewritten. Startup status is an in-memory observation; durable policy,
catalog, signer, lease, operation, and effect records remain authoritative.

## Reuse and licensing

The design reuses Fornix's existing Postgres leases, signed trust catalogs,
credential lease resolver, connector registry, operation effect reservation,
and result/receipt authority links. Orloj's explicit provider registry,
execution-engine fences, and checkpoint discipline inform the seam. DeepSeek
Harness's explicit capability/plugin boundaries inform registration and
configuration. agentmemory's lease/checkpoint/replay patterns inform crash
tests. OpenBao's lease/revocation model informs source-version and expiry
checks. No reference implementation is copied. Kronaxis-fabric remains
excluded because its BSL 1.1 license is incompatible with Fornix's MIT
distribution.

## Cost and performance budget

The safety path should add bounded Postgres work: one catalog refresh/read per
workspace admission, one live operation lease validation immediately before
dispatch, and no unbounded payload reads. Catalog entries, authority facts,
and status labels remain size-bounded. The qualification reports admission and
dispatch latency, SQL statement count where measurable, connection pressure,
and relation growth. Remote provider latency and charges remain outside
Fornix's control-plane cost and are reported as at-least-once external work.

## Acceptance tests

- fresh and existing databases load and migrate cleanly;
- startup loads signed policy and schema catalogs, and restart preserves their
  revisions and hashes;
- catalog lag, expiry, signer revocation, downgrade, and invalid signatures
  fail closed;
- every effectful registered adapter passes the authority conformance check;
- stale operation and task fences fail before the adapter is called;
- lease expiry and takeover cannot be used by the old worker;
- exact credential lease/source version is reused without a new fence;
- duplicate delivery produces one durable reservation/result;
- crash before reservation commit and crash after external delivery are
  recoverable and replayable;
- workspace isolation, actor propagation, redaction, and bounded disclosure
  hold for every adapter;
- existing unit, race, integration, migration, CI, and smoke checks remain
  green; and
- qualification reports latency, SQL/storage impact, external at-least-once
  limitations, and any adapters still excluded from production mode.
