# Universal production roadmap status

Status: current public roadmap and qualification handoff; not a production
readiness declaration.

This document is the honest handoff for the transformation tracked by
[Issue #38](https://github.com/Kshitij-M/fornix) and qualified through
[Issue #40](https://github.com/Kshitij-M/fornix/issues/40). It distinguishes
what the control plane proves today from what a deployment must still prove
before Fornix can safely govern arbitrary production systems.

## What Fornix is now

Fornix is a domain-neutral, Postgres-authoritative work control plane with a
repository adapter and a fake-first incident workflow. The generic path now
has:

```text
typed intent
  → workspace/actor/capability admission
  → durable operation identity and idempotency
  → deterministic plan/queue claim and monotonic fence
  → adapter-owned heartbeat and bounded execution boundary
  → evidence/artifact/result/verification references
  → replayable state history and Work Receipt seams
```

The worker boundary added in Loop 43 is intentionally adapter-owned. It does
not silently choose a provider, resolve a secret, execute a remote effect, or
turn a process-local callback into a second authority.

## Verified on the current branch

- Generic operation identity, plans, lifecycle transitions, attempts, results,
  callbacks, effect reservations, recovery state, replay, and idempotency are
  durable in Postgres.
- Claims are bounded, deterministic, workspace-scoped, `SKIP LOCKED`, and
  protected by monotonic operation fencing. Active leases are excluded and
  expired leases are recoverable.
- The adapter-owned worker keeps a lease alive, cancels on lease loss, reports
  failures without hot-looping, releases successful claims, and preserves
  expiry-based takeover.
- Generic operation HTTP/CLI paths, CI integration, a real HTTP/CLI smoke, Go
  tests, race tests, migration checks, and documentation checks are green.
- The Postgres RLS foundation and capacity harness have qualification tests,
  but the development database role is still not a production tenant-isolated
  deployment role.

## Production gates still open

These are not documentation-only gaps. Each gate needs implementation,
deployment evidence, failure-injection coverage, and an operator runbook.

### P0 — authority and security

1. **Role-separated database enforcement.** Run the application with a
   non-owner, `NOBYPASSRLS` role; apply RLS or an equivalent boundary to every
   workspace-scoped historical surface, not only the generic operation tables;
   qualify migrations, grants, connection-pool reset behavior, and unset
   context fail-closed behavior.
2. **Credential lease integration.** Add a production secret-manager/KMS
   adapter contract with short-lived leases, audience/provider binding,
   rotation, revocation, audit, and redacted failure behavior. Provider values
   must never enter durable Fornix state.
3. **Signed trust and schema admission.** Sign and verify connector identity,
   capability definitions, input/output schemas, policy catalogs, and release
   artifacts. Reject unsigned, stale, downgraded, or tampered definitions at
   startup and operation admission.
4. **Central outbound policy enforcement.** Make destination, DNS, redirect,
   private-network, credential, and egress budgets enforceable at the actual
   network boundary. Add DNS-rebinding, redirect-chain, proxy-bypass, and
   confused-deputy tests; typed in-process policy alone is insufficient.

### P0 — execution correctness and isolation

5. **Worker composition and fairness.** Wire the adapter-owned worker into an
   explicit supervisor with per-workspace quotas, bounded backpressure,
   resource-level serialization, starvation-resistant ordering, connection
   pool budgets, and cancellation propagation. Keep external-effect
   reconciliation separate from ordinary queue claims.
6. **Complete mutation linkage.** Ensure every adapter operation links actor,
   policy revision, capability hash, operation fence, task/session identity,
   evidence, artifacts, cost, and Work Receipt references transactionally.
   Expose bounded list/status/inspection APIs without leaking raw payloads.
7. **Sandbox and boundary qualification.** Provide an explicit backend matrix
   for filesystem, process, network, SQL, HTTP, MCP, and container execution.
   State exactly what local process execution can and cannot isolate; qualify
   the stronger backend before unattended effectful work.

### P1 — durability and operations

8. **Backup, restore, and availability.** Add scheduled backups, WAL/PITR,
   restore-owner procedures, HA/failover design, migration forward/rollback
   rules, and deployment-specific RPO/RTO evidence. Recompute replay, artifact,
   provenance, and receipt hashes after restore.
9. **Retention and storage lifecycle.** Add event partitioning, artifact cold
   tiers or an explicitly bounded Postgres-only scale envelope, retention
   compaction, tombstones, reference safety, corruption alerts, and storage
   growth/restore benchmarks.
10. **Load and soak qualification.** Measure p50/p95/p99 latency, lock waits,
    WAL, pool saturation, duplicate work, workspace fairness, failure recovery,
    and replay throughput across single-node and intended production
    topologies. Local smoke numbers are not production SLOs.
11. **Security and support operations.** Add adversarial prompt-injection,
    retrieval contamination, SQL/path/shell injection, secret leakage,
    privilege-escalation, callback forgery, and API/MCP abuse suites. Add
    bounded support bundles, alerts, incident runbooks, and privacy-safe
    diagnostics.

### P1 — ecosystem and product qualification

12. **Live connector conformance.** Qualify HTTP/API, SQL, ticketing,
    monitoring, cloud, deployment, and other adapters independently. A fake
    connector proves control-plane semantics; it does not prove a provider's
    idempotency, verification, rate limits, or outage behavior.
13. **Workflow and multi-agent scale.** Extend the bounded workflow runtime
    with multi-step fairness, sub-run ownership, human approval, callback
    authenticity, compensation, and deterministic replay at the intended
    concurrency. Do not make “multi-agent” a bypass around the same authority.
14. **Release and compatibility policy.** Publish signed multi-platform
    binaries/images, SBOM and notices, API/contract compatibility rules,
    migration support windows, connector conformance requirements, and an
    upgrade/rollback guide.

## Dependency order

The next implementation slice should be worker composition and fairness
(item 5), but it must be designed alongside the role-separated database and
credential boundaries (items 1–2). The sequence is:

```text
database/credential authority
  → signed admission and outbound enforcement
  → worker supervisor, fairness, quotas, and resource serialization
  → complete evidence/artifact/receipt linkage
  → sandbox and live connector qualification
  → backup/HA/retention/load/security operations
  → ecosystem release and production support
```

Do not close Issue #38 or call Fornix production-ready until every applicable
gate has executable evidence, a measured operating envelope, and a recovery
runbook. “Universal” describes the contract and adapter model; it is not a
claim that every production system is already supported.

## Current recommended next task

**Task 44 — Build the universal operation supervisor, workspace fairness,
quota, backpressure, and resource-serialization substrate.** Preserve the
Loop 43 adapter-owned worker boundary, use Postgres as authority, and qualify
the supervisor with concurrent workers, stale fences, cancellation, fairness,
quota rejection, recovery, and replay tests before connecting additional live
providers.
